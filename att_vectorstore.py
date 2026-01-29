#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SYNC TOTVS RM CATALOG -> OPENAI VECTOR STORE (RAG OPTIMIZED)

OBJETIVO DA REFATORAÇÃO:
- Gerar múltiplos Markdowns por tabela (Chunking determinístico).
- Incluir âncoras de busca e palavras-chave por coluna.
- Classificar colunas (DATES, STATUS, VALUES, IDENTIFIERS, OTHER).
- Remover lógica de JOINs (agora tratados externamente).
- Validação de qualidade pós-geração.
"""

import os
import re
import json
import time
import argparse
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from openai import OpenAI

# ----------------------------
# CONFIG / CONSTANTS
# ----------------------------

DEFAULT_SYNONYM_MODEL = "gpt-4o-mini"  # Atualizado para modelo mais recente/rápido se disponível
DEFAULT_BATCH_SIZE = 20                # Lotes menores para garantir JSON válido
MD_SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._-]+")

# Limite para quebrar em arquivos categorizados
COLUMN_SPLIT_THRESHOLD = 120

@dataclass
class ColumnInfo:
    name: str
    description: str
    category: str = "OTHER"
    synonyms: List[str] = field(default_factory=list)
    business_terms: List[str] = field(default_factory=list)
    question_patterns: List[str] = field(default_factory=list)

@dataclass
class TableInfo:
    table_id: Any
    table_name: str
    table_desc: str
    columns: List[ColumnInfo] = field(default_factory=list)


def slugify_filename(name: str) -> str:
    name = name.strip().replace(" ", "_")
    name = MD_SAFE_FILENAME_RE.sub("_", name)
    name = re.sub(r"_+", "_", name)
    return name[:180]

def pg_connect() -> psycopg2.extensions.connection:
    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", "5432"))
    db = os.environ.get("PG_DATABASE", "postgres")
    user = os.environ.get("PG_USER", "postgres")
    pwd = os.environ.get("PG_PASSWORD", "")

    return psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=pwd, connect_timeout=10
    )


# ----------------------------
# CLASSIFICATION LOGIC
# ----------------------------

def classify_column(name: str, desc: str) -> str:
    """
    Classifica a coluna em: IDENTIFIERS, DATES, STATUS, VALUES, OTHER
    Baseado em heurísticas de nome e descrição.
    """
    n = name.upper()
    d = desc.upper()

    # 1. Identifiers (Alta prioridade)
    if any(x in n for x in ["COD", "ID", "CHAVE", "CHAPA", "MATRICULA", "IDENTIF"]) or \
       n.endswith("ID") or n.startswith("ID_"):
        return "IDENTIFIERS"

    # 2. Dates
    if any(x in n for x in ["DT", "DATA", "DATE"]) or \
       "DATA" in d or "DATE" in d:
        return "DATES"

    # 3. Status
    if any(x in n for x in ["SIT", "STATUS", "ST", "ATIVO", "INATIVO"]):
        return "STATUS"

    # 4. Values (Money, Quantities)
    if any(x in n for x in ["VLR", "VALOR", "SAL", "QTD", "TOTAL", "PRECO", "CUSTOM", "NET", "GROSS"]):
        return "VALUES"

    return "OTHER"


# ----------------------------
# DATA FETCHING
# ----------------------------

def fetch_catalog(conn) -> Dict[str, TableInfo]:
    """
    Busca tabelas e colunas.
    NÃO busca relacionamentos.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        print("  -> Buscando tabelas...")
        cur.execute("""
            SELECT id_tabela, tabela_codigo AS nome_tabela, COALESCE(descricao, titulo) AS descricao_tabela
            FROM tabela_catalogo
        """)
        tables = cur.fetchall()

        print("  -> Buscando colunas...")
        cur.execute("""
            SELECT id_tabela, coluna_nome AS nome_coluna, COALESCE(descricao, titulo) AS descricao_coluna
            FROM coluna_catalogo
            ORDER BY id_tabela, ordem
        """)
        cols_raw = cur.fetchall()

    print(f"  -> Tabelas: {len(tables)} | Colunas totais: {len(cols_raw)}")

    cols_by_table = defaultdict(list)
    for c in cols_raw:
        c_name = str(c["nome_coluna"]).upper().strip()
        c_desc = str(c.get("descricao_coluna", "")).strip()
        cat = classify_column(c_name, c_desc)
        
        col_obj = ColumnInfo(name=c_name, description=c_desc, category=cat)
        cols_by_table[c["id_tabela"]].append(col_obj)

    catalog = {}
    for t in tables:
        t_name = str(t["nome_tabela"]).upper().strip()
        t_desc = str(t["descricao_tabela"] or "").strip()
        catalog[t_name] = TableInfo(
            table_id=t["id_tabela"],
            table_name=t_name,
            table_desc=t_desc,
            columns=cols_by_table.get(t["id_tabela"], [])
        )
    
    return catalog


# ----------------------------
# SYNONYMS & ENRICHMENT
# ----------------------------

def generate_enrichment(
    client: OpenAI,
    model: str,
    table_name: str,
    columns: List[ColumnInfo],
    batch_size: int,
    skip_llm: bool = False
) -> None:
    """
    Gera sinônimos, termos de negócio e padrões de pergunta.
    Popula os objetos ColumnInfo in-place.
    """
    if not columns:
        return

    # Se skip_llm, usar heurística simples (Lite Mode)
    if skip_llm:
        print(f"  [Lite Mode] Gerando termos simples para {table_name}...")
        for col in columns:
            # Heurística: quebrar descrição em palavras chaves
            words = set(re.findall(r"\w{4,}", col.description.lower()))
            col.business_terms = list(words)[:5]
        return

    # Modo LLM
    batches = [columns[i:i + batch_size] for i in range(0, len(columns), batch_size)]
    
    system_prompt = (
        "Você é um especialista em Business Intelligence. "
        "Para cada coluna, extraia metadados de negócio para RAG. "
        "Retorne APENAS JSON válido com o schema solicitado."
    )

    for idx, batch in enumerate(batches, start=1):
        print(f"  -> Batch {idx}/{len(batches)} ({len(batch)} cols) - chamando LLM...")
        
        cols_payload = [{"col": c.name, "desc": c.description} for c in batch]
        
        user_prompt = {
            "tabela": table_name,
            "tarefa": "Gerar metadados de busca para estas colunas.",
            "colunas": cols_payload,
            "schema_esperado": {
                "COLUNA_X": {
                    "synonyms": ["sinonimo1", "sinonimo2"],
                    "business_terms": ["termo_negocio1", "termo_negocio2"],
                    "question_patterns": ["Pergunta exemplo 1?", "Pergunta exemplo 2?"]
                }
            },
            "regras": [
                "synonyms: 3-5 variações do nome técnico.",
                "business_terms: 3-5 termos que usuários de negócio usariam (ex: 'folha', 'rescisão').",
                "question_patterns: 2-3 perguntas curtas que seriam respondidas por esta coluna."
            ]
        }

        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            content = resp.choices[0].message.content
            data = json.loads(content)
            
            # Aplicar ao batch
            for col in batch:
                if col.name in data:
                    meta = data[col.name]
                    col.synonyms = meta.get("synonyms", [])[:5]
                    col.business_terms = meta.get("business_terms", [])[:5]
                    col.question_patterns = meta.get("question_patterns", [])[:3]
                else:
                    # Fallback se LLM esquecer a coluna
                    col.business_terms = ["(auto) " + w for w in col.description.split() if len(w)>4][:3]

        except Exception as e:
            print(f"  [ERRO] Falha no batch {idx}: {e}")
            # Não quebramos o processo, metadados ficam vazios


# ----------------------------
# MARKDOWN GENERATOR
# ----------------------------

def make_search_line(tname: str, col: ColumnInfo) -> str:
    """Padroniza a linha de busca da coluna para alta recuperação."""
    # Coletar keywords
    keywords = set()
    keywords.update(col.synonyms)
    keywords.update(col.business_terms)
    # Limpar keywords
    clean_kwd = [k.lower().strip() for k in keywords if k]
    kwd_str = ", ".join(clean_kwd)
    
    return f"`{tname}` | `{col.name}` | {kwd_str} | tipo:{col.category.lower()}"

def get_common_questions(table: TableInfo, top_cols: List[ColumnInfo]) -> List[str]:
    """Gera perguntas comuns baseado nas colunas principais (sem LLM)."""
    questions = []
    for c in top_cols[:5]:
        if c.question_patterns:
            questions.append(c.question_patterns[0])
        else:
            questions.append(f"Qual é o {c.name} ({c.description})?")
    return questions[:5]

def generate_markdown_content(
    table: TableInfo, 
    section_name: str, 
    columns: List[ColumnInfo],
    is_overview: bool = False
) -> str:
    """Gera o conteúdo de string do Markdown com Âncoras e SEO."""
    lines = []
    tname = table.table_name
    
    # 1. HEADER & ANCHORS
    lines.append(f"# {tname} - {section_name}")
    lines.append("")
    lines.append("<!-- RAG OPTIMIZATION ANCHORS -->")
    lines.append(f"TABLE: {tname}")
    lines.append("DOC_TYPE: table_schema")
    lines.append(f"SECTION: {section_name}")
    
    # Coletar keywords globais da seção
    all_terms = set()
    for c in columns:
        all_terms.update(c.business_terms)
    
    # Keywords limitadas a 20 para não poluir header
    kwh_list = list(all_terms)[:20]
    lines.append(f"KEYWORDS: {', '.join(kwh_list)}")
    lines.append("---")
    lines.append("")

    # 2. OVERVIEW BLOCK (Apenas no arquivo Overview)
    if is_overview:
        lines.append("## 📋 Business Mapping")
        lines.append(f"**Descrição da Tabela**: {table.table_desc}")
        lines.append("")
        
        lines.append("**✅ What this table answers:**")
        # Heurística simples
        lines.append(f"- Dados cadastrais e movimentações de `{tname}`.")
        lines.append(f"- Histórico de registros relacionados a {table.table_desc.lower()}.")
        lines.append("- Informações detalhadas para relatórios operacionais.")
        lines.append("")
        
        lines.append("**❓ Common Questions:**")
        # Pegar perguntas de colunas importantes (Status, Dates, Identifiers)
        important_cols = [c for c in columns if c.category in ("STATUS", "DATES", "IDENTIFIERS")]
        qs = get_common_questions(table, important_cols)
        for q in qs:
            lines.append(f"- {q}")
        lines.append("")
        
        lines.append("**📅 Important Date Fields:**")
        dates = [c.name for c in columns if c.category == "DATES"][:10]
        lines.append(", ".join(dates) if dates else "_None identified_")
        lines.append("")

        lines.append("**🚦 Status Fields:**")
        status = [c.name for c in columns if c.category == "STATUS"][:10]
        lines.append(", ".join(status) if status else "_None identified_")
        lines.append("---")
        lines.append("")

    # 3. COLUMNS LIST
    lines.append(f"## 🏛️ Colunas ({section_name})")
    lines.append("Use esta lista para identificar a coluna correta para sua query SQL.")
    lines.append("")
    
    for col in columns:
        # Search Line Otimizada
        lines.append(make_search_line(tname, col))
        
        # Descrição Detalhada
        desc_line = f"- **Desc**: {col.description}"
        if col.business_terms:
            desc_line += f" | **Termos**: {', '.join(col.business_terms)}"
        lines.append(desc_line)
        lines.append("")

    return "\n".join(lines)


def write_md_files(output_dir: str, catalog: Dict[str, TableInfo]) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    generated_files = []

    for tname, tinfo in catalog.items():
        total_cols = len(tinfo.columns)
        
        # Estratégia de Chunking
        # Se > THRESHOLD, quebra por categoria. Senão, Overview + All Columns.
        
        chunks = {} # {suffix: columns_list}
        
        # Sempre gera Overview com Top Columns (Identifiers + Status + algumas Dates)
        # Overview deve ser leve, para perguntas gerais "O que tem na tabela X?"
        overview_cols = [c for c in tinfo.columns if c.category in ("IDENTIFIERS", "STATUS")]
        # Adicionar algumas datas se não tiver muitas
        date_cols = [c for c in tinfo.columns if c.category == "DATES"]
        overview_cols.extend(date_cols[:5])
        chunks["00_OVERVIEW"] = overview_cols

        if total_cols > COLUMN_SPLIT_THRESHOLD:
            # Estratégia Granular
            by_cat = defaultdict(list)
            for c in tinfo.columns:
                by_cat[c.category].append(c)
            
            if by_cat["DATES"]: chunks["10_DATES"] = by_cat["DATES"]
            if by_cat["STATUS"]: chunks["20_STATUS"] = by_cat["STATUS"]
            if by_cat["VALUES"]: chunks["30_VALUES"] = by_cat["VALUES"]
            
            # Agrupar Identifiers e Other se forem muitos
            others = by_cat["IDENTIFIERS"] + by_cat["OTHER"]
            chunks["90_DETAILS"] = others
            
        else:
            # Estratégia Simples
            chunks["90_ALL_COLUMNS"] = tinfo.columns

        # Gerar Arquivos
        print(f"[{tname}] Gerando {len(chunks)} arquivos (Total Cols: {total_cols})...")
        
        for suffix, cols in chunks.items():
            if not cols and suffix != "00_OVERVIEW": 
                continue # Pula chunks vazios que não sejam overview
            
            filename = slugify_filename(f"{tname}__{suffix}.md")
            filepath = os.path.join(output_dir, filename)
            
            is_ov = (suffix == "00_OVERVIEW")
            # Para o nome da seção, removemos o número (00_OVERVIEW -> OVERVIEW)
            section_name = suffix.split("_", 1)[1] if "_" in suffix else suffix
            
            content = generate_markdown_content(tinfo, section_name, cols, is_overview=is_ov)
            
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            
            generated_files.append(filepath)

    return generated_files


# ----------------------------
# QUALITY VALIDATION
# ----------------------------

def validate_output(output_dir: str, catalog: Dict[str, TableInfo]) -> None:
    """ Gera relatório de qualidade e consistência. """
    print("\n[VALIDATOR] Iniciando verificação de qualidade...")
    
    files = [f for f in os.listdir(output_dir) if f.endswith(".md")]
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_files": len(files),
        "tables_processed": len(catalog),
        "details": {}
    }
    
    # Check consistency per table
    for tname, tinfo in catalog.items():
        t_files = [f for f in files if f.startswith(slugify_filename(tname))]
        
        # Check col coverage
        found_cols = set()
        for f in t_files:
            with open(os.path.join(output_dir, f), "r", encoding="utf-8") as fh:
                content = fh.read()
                # Procurar padrão da search line: `NAMETABLE` | `NAMECOL`
                matches = re.findall(rf"`{re.escape(tname)}`\s*\|\s*`([^`]+)`", content)
                found_cols.update(matches)
        
        # Overview file sempre tem redundancia, então found_cols pode ser > total_cols se contarmos duplicados em arquivos diferentes
        # Mas queremos garantir que TODAS colunas do catálogo apareçam em pelo menos UM arquivo.
        
        missing = [c.name for c in tinfo.columns if c.name not in found_cols]
        
        report["details"][tname] = {
            "files_count": len(t_files),
            "total_columns_catalog": len(tinfo.columns),
            "found_columns_in_md": len(found_cols),
            "missing_columns": missing,
            "status": "OK" if not missing else "WARNING"
        }

    report_path = os.path.join(output_dir, "__report__.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"[VALIDATOR] Relatório salvo em: {report_path}")


# ----------------------------
# UPLOAD
# ----------------------------

def upload_to_vector_store(client: OpenAI, vs_id: str, file_paths: List[str]) -> None:
    if not file_paths:
        return
    
    print(f"\n[UPLOAD] Enviando {len(file_paths)} arquivos para Vector Store {vs_id}...")
    
    # OpenAI recomenda batches de até 100 arquivos, mas a API lida bem com list streams now.
    # Vamos usar o helper upload_and_poll que é robusto.
    
    file_streams = [open(path, "rb") for path in file_paths]
    
    try:
        batch = client.vector_stores.file_batches.upload_and_poll(
            vector_store_id=vs_id,
            files=file_streams
        )
        print(f"[UPLOAD] Status: {batch.status}")
        print(f"[UPLOAD] File Counts: {batch.file_counts}")
    finally:
        for fs in file_streams:
            fs.close()


# ----------------------------
# MAIN
# ----------------------------

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Sync RAG Catalog")
    parser.add_argument("--out-dir", default="./out_md_rag", help="Output directory")
    parser.add_argument("--vector-store-id", default=os.getenv("OPENAI_VECTOR_STORE_ID"), help="ID da Vector Store")
    parser.add_argument("--skip-llm", action="store_true", help="Pula geração de sinônimos com LLM")
    parser.add_argument("--skip-upload", action="store_true", help="Pula upload para OpenAI")
    parser.add_argument("--limit-tables", type=int, default=0, help="Limita qtd tabelas para teste")
    
    args = parser.parse_args()
    
    print(">>> INICIANDO SYNC RAG (REFATORADO) <<<")
    
    # 1. DB Connect
    conn = pg_connect()
    
    # 2. Fetch
    try:
        catalog = fetch_catalog(conn)
    finally:
        conn.close()
        
    # Limit tables if dev mode
    if args.limit_tables > 0:
        limited_keys = list(catalog.keys())[:args.limit_tables]
        catalog = {k: catalog[k] for k in limited_keys}
        print(f"[DEV] Limitando a {args.limit_tables} tabelas.")

    # 3. Enrich (Synonyms via LLM)
    if not args.skip_llm:
        if not os.getenv("OPENAI_API_KEY"):
            print("[ERRO] OPENAI_API_KEY não encontrada. Use --skip-llm ou configure o .env")
            return
        
        client = OpenAI()
        for i, (tname, tinfo) in enumerate(catalog.items(), 1):
            print(f"[{i}/{len(catalog)}] Enriquecendo: {tname}")
            generate_enrichment(client, DEFAULT_SYNONYM_MODEL, tname, tinfo.columns, DEFAULT_BATCH_SIZE)
    else:
        print("[INFO] Pulando LLM Enrichment (Lite Mode). Gerando termos básicos.")
        # Ainda rodamos o loop para garantir fallback do Lite Mode
        for tname, tinfo in catalog.items():
            generate_enrichment(None, "", tname, tinfo.columns, 0, skip_llm=True)

    # 4. Generate Files
    all_files = write_md_files(args.out_dir, catalog)
    
    # 5. Validate
    validate_output(args.out_dir, catalog)
    
    # 6. Upload
    if not args.skip_upload and args.vector_store_id:
        if not os.getenv("OPENAI_API_KEY"):
            print("[ERRO] OPENAI_API_KEY ausente para upload.")
        else:
            client = OpenAI()
            upload_to_vector_store(client, args.vector_store_id, all_files)
    else:
        print("[INFO] Upload pulado ou Vector Store ID não informado.")

    print("\n>>> PROCESSO CONCLUÍDO <<<")

if __name__ == "__main__":
    main()
