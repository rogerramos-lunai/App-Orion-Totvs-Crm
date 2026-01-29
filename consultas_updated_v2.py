import streamlit as st
import psycopg2
import pandas as pd
import json
import io
import os
from datetime import datetime, date, timedelta
from psycopg2.extras import RealDictCursor
from psycopg2 import sql  # para compor identificadores com segurança
import base64
import requests
import re
import time
import secrets
from datetime import datetime, timedelta
import hashlib

def reset_app_state():
    # limpa TUDO do session_state
    for key in list(st.session_state.keys()):
        del st.session_state[key]

    # limpa caches novos
    try:
        st.cache_data.clear()
    except Exception:
        pass

    try:
        st.cache_resource.clear()
    except Exception:
        pass

# ====== GERENCIAMENTO DE SESSÃO COM QUERY PARAMS ======
def gerar_token_sessao():
    """Gera token único para sessão"""
    return secrets.token_urlsafe(32)

def salvar_sessao_db(token: str, login: str, is_admin: bool, nome_exibicao: str):
    """Salva sessão no banco de dados"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Remove sessões antigas (mais de 30 minutos)
        cur.execute("""
            DELETE FROM sessoes_portal 
            WHERE criado_em < NOW() - INTERVAL '30 minutes'
        """)
        
        # Salva nova sessão
        cur.execute("""
            INSERT INTO sessoes_portal (token, login, is_admin, nome_exibicao, criado_em)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (token) DO UPDATE
            SET criado_em = NOW()
        """, (token, login, is_admin, nome_exibicao))
        
        conn.commit()
        return token
    except Exception as e:
        conn.rollback()
        # Se a tabela não existir, cria
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessoes_portal (
                token VARCHAR(255) PRIMARY KEY,
                login VARCHAR(255) NOT NULL,
                is_admin BOOLEAN NOT NULL,
                nome_exibicao VARCHAR(255) NOT NULL,
                criado_em TIMESTAMP NOT NULL
            )
        """)
        conn.commit()
        # Tenta novamente
        cur.execute("""
            INSERT INTO sessoes_portal (token, login, is_admin, nome_exibicao, criado_em)
            VALUES (%s, %s, %s, %s, NOW())
        """, (token, login, is_admin, nome_exibicao))
        conn.commit()
        return token
    finally:
        cur.close()
        conn.close()

def carregar_sessao_db(token: str):
    """Carrega sessão do banco se ainda válida"""
    if not token:
        return None
    
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Busca sessão que não expirou (30 minutos)
        cur.execute("""
            SELECT login, is_admin, nome_exibicao, criado_em
            FROM sessoes_portal
            WHERE token = %s
            AND criado_em > NOW() - INTERVAL '30 minutes'
        """, (token,))
        
        row = cur.fetchone()
        if not row:
            return None
        
        login, is_admin, nome_exibicao, criado_em = row
        
        # Valida se usuário ainda existe e está ativo
        info = carregar_portal_info_por_nome(login)
        if not info or not info.get("ativo"):
            limpar_sessao_db(token)
            return None
        
        return {
            "id_usuario": info["id_portal_usuario"],
            "nome": nome_exibicao,
            "is_admin": bool(is_admin),
            "perfil_desc": "Administrador" if is_admin else "Usuário",
            "id_perfil": None,
            "id_empresa": None,
            "token": token
        }
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()

def limpar_sessao_db(token: str):
    """Remove sessão do banco"""
    if not token:
        return
    
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM sessoes_portal WHERE token = %s", (token,))
        conn.commit()
    except Exception:
        pass
    finally:
        cur.close()
        conn.close()

# ====== GERENCIAMENTO DE SESSÃO COM COOKIES ======
def get_cookie_manager():
    """Retorna o gerenciador de cookies (singleton via session_state)"""
    if "cookie_manager" not in st.session_state:
        st.session_state.cookie_manager = EncryptedCookieManager(
            prefix="lunai_portal_",
            password="lunai_secret_key_2024_secure"
        )
    return st.session_state.cookie_manager

def salvar_sessao_cookie(login: str, is_admin: bool, nome_exibicao: str):
    """Salva a sessão no cookie (expira em 30 minutos)"""
    try:
        cookies = get_cookie_manager()
        
        if not cookies.ready():
            return None
        
        # Salva cada campo separadamente
        cookies["session_login"] = login
        cookies["session_is_admin"] = str(is_admin)
        cookies["session_nome"] = nome_exibicao
        cookies["session_timestamp"] = datetime.now().isoformat()
        cookies.save()
        
        return True
    except Exception as e:
        print(f"Erro ao salvar cookie: {e}")
        return None

def carregar_sessao_cookie():
    """Carrega a sessão do cookie se ainda válida"""
    try:
        cookies = get_cookie_manager()
        
        # IMPORTANTE: Aguarda cookies estarem prontos
        if not cookies.ready():
            st.info("⏳ Carregando sessão...")
            st.stop()  # Para aqui até cookies carregarem
        
        # Verifica se os cookies existem
        login = cookies.get("session_login")
        if not login:
            return None
        
        timestamp_str = cookies.get("session_timestamp")
        if not timestamp_str:
            return None
        
        # Verifica se a sessão expirou (30 minutos)
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            tempo_decorrido = datetime.now() - timestamp
            
            if tempo_decorrido > timedelta(minutes=30):
                limpar_sessao_cookie()
                return None
        except Exception as e:
            print(f"Erro ao verificar timestamp: {e}")
            return None
        
        # Valida se o usuário ainda existe e está ativo
        info = carregar_portal_info_por_nome(login)
        if not info or not info.get("ativo"):
            limpar_sessao_cookie()
            return None
        
        # Sessão válida - retorna dados
        return {
            "id_usuario": info["id_portal_usuario"],
            "nome": info["nome_exibicao"],
            "is_admin": bool(info["is_admin"]),
            "perfil_desc": "Administrador" if info["is_admin"] else "Usuário",
            "id_perfil": None,
            "id_empresa": None,
        }
    except Exception as e:
        print(f"Erro ao carregar cookie: {e}")
        return None

def limpar_sessao_cookie():
    """Remove o cookie de sessão (logout)"""
    try:
        cookies = get_cookie_manager()
        
        if not cookies.ready():
            return
        
        cookies["session_login"] = ""
        cookies["session_is_admin"] = ""
        cookies["session_nome"] = ""
        cookies["session_timestamp"] = ""
        cookies.save()
    except Exception as e:
        print(f"Erro ao limpar cookie: {e}")

def only_digits(s: str) -> str:
    return re.sub(r'\D', '', s or '')

def format_cnpj(raw: str) -> str:
    """
    Normaliza e formata CNPJ como 00.000.000/0000-00.
    Se não tiver 14 dígitos, devolve o próprio texto (pra exibir erro depois).
    """
    digits = only_digits(raw)
    if len(digits) != 14:
        return raw  # deixa como está, pra gente avisar no submit
    return f"{digits[0:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}"

def mask_cnpj(field_key: str):
    """
    Máscara automática de CNPJ:
    - mantém só dígitos
    - limita em 14
    - ao chegar em 14, formata 00.000.000/0000-00
    """
    raw = st.session_state.get(field_key, "") or ""
    digits = only_digits(raw)[:14]

    # Nada digitado
    if not digits:
        st.session_state[field_key] = ""
        return

    # Se ainda não chegou em 14, mostra só os dígitos
    if len(digits) < 14:
        st.session_state[field_key] = digits
    else:
        # Chegou nos 14 -> aplica máscara
        st.session_state[field_key] = format_cnpj(digits)

# =========================
# Configurações do banco
# =========================
DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

# =========================
# Conexão
# =========================
def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )

import base64
import requests

# ========================
# TOTVS RM API - Helpers
# ========================
def get_or_create_tabela_id(nome_tabela: str) -> int:
    """
    Resolve o id_tabela da TABELA LEGADA (tabela) a partir do nome.
    Se não existir, cria (mantemos a sincronização mínima com o catálogo).
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT id_tabela FROM tabela WHERE LOWER(nome_tabela)=LOWER(%s)", (nome_tabela,))
        row = cur.fetchone()
        if row:
            return int(row["id_tabela"])
        # cria registro básico
        cur.execute(
            "INSERT INTO tabela (nome_tabela, descricao) VALUES (%s, %s) RETURNING id_tabela",
            (nome_tabela, f"Criado automaticamente a partir do Catálogo")
        )
        new_id = cur.fetchone()["id_tabela"]
        conn.commit()
        return int(new_id)
    finally:
        cur.close()
        conn.close()

def rm_build_auth_headers():
    """
    Monta headers c/ auth:
      - Basic (RM_USER/RM_PASS) OU
      - Bearer (RM_TOKEN)
    """
    headers = {"Content-Type": "application/json"}
    token = os.getenv("RM_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
        return headers
    user = os.getenv("RM_USER", "lunai").strip()
    pwd  = os.getenv("RM_PASS", "hictoW-fyxvyz-4qamse").strip()
    if user and pwd:
        b = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        headers["Authorization"] = f"Basic {b}"
    return headers

def rm_edit_query(sentenca_sql: str,
                  codsentenca: str,
                  codcoligada: int = 0,
                  aplicacao: str = "G"):
    """
    PUT para editar/atualizar a sentença no RM.
    Requer:
      RM_BASE_URL (ex: http://56.125.120.179:8051)
    Endpoint:
      {RM_BASE_URL}/RMSRestDataServer/rest/GlbConsSqlData/{codcoligada}$_${aplicacao}$_${codsentenca}
    Payload:
      {
        "CODCOLIGADA": ...,
        "APLICACAO": "...",
        "CODSENTENCA": "...",
        "TITULO": "... (opcional)",
        "SENTENCA": "SELECT ...",
      }
    """
    base = os.getenv("RM_BASE_URL", "http://56.125.120.179:8051").rstrip("/")
    if not base:
        raise RuntimeError("Configure RM_BASE_URL")
    url = f"{base}/RMSRestDataServer/rest/GlbConsSqlData/{codcoligada}$_${aplicacao}$_${codsentenca}"
    payload = {
        "CODCOLIGADA": codcoligada,
        "APLICACAO": aplicacao,
        "CODSENTENCA": codsentenca,
        "TITULO": f"{codsentenca} - SENTENCA AUTO",
        "SENTENCA": sentenca_sql
    }
    resp = requests.put(url, headers=rm_build_auth_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

def rm_execute_query(codsentenca: str,
                     codcoligada: int = 0,
                     aplicacao: str = "G"):
    """
    Executa a sentença já salva (GET/POST conforme instância).
    Endpoint típico:
      {RM_BASE_URL}/api/framework/v1/consultaSQLServer/RealizaConsulta/{codsentenca}/{codcoligada}/{aplicacao}
    Retorna lista de dicts.
    """
    base = os.getenv("RM_BASE_URL", "http://56.125.120.179:8051").rstrip("/")
    if not base:
        raise RuntimeError("Configure RM_BASE_URL")
    url = f"{base}/api/framework/v1/consultaSQLServer/RealizaConsulta/{codsentenca}/{codcoligada}/{aplicacao}"
    resp = requests.get(url, headers=rm_build_auth_headers(), timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # Normaliza para lista de dicts
    if isinstance(data, dict) and "data" in data:
        # alguns ambientes retornam {"data":[...]}
        return data.get("data") or []
    if isinstance(data, list):
        return data
    return []

def distinct_from_db(table_code: str, column_name: str, limit: int = 500):
    """
    DISTINCT ao vivo (sem N8N). Usa o mesmo get_connection() do portal.
    Segurança: os nomes vêm do Catálogo (UI só exibe colunas daquela tabela).
    """
    # saneamento leve por garantia: só letras, números e underscore
    if not table_code or not column_name:
        return []
    if not table_code.replace("_","").isalnum() or not column_name.replace("_","").isalnum():
        return []

    conn = get_connection()
    cur = conn.cursor()
    try:
        q = sql.SQL("SELECT DISTINCT {col}::text AS v FROM {tbl} ORDER BY 1 LIMIT %s").format(
            col=sql.Identifier(column_name),
            tbl=sql.Identifier(table_code)
        )
        cur.execute(q, (int(limit),))
        rows = cur.fetchall()
        return [r[0] for r in rows if r and r[0] is not None]
    finally:
        cur.close()
        conn.close()


# =========================
# Utilitários / Logs
# =========================

def exec_sql(sql, params=None, fetch=False):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor) if fetch else conn.cursor()
    cur.execute(sql, params or ())
    rows = cur.fetchall() if fetch else None
    conn.commit()
    cur.close(); conn.close()
    return rows

@st.cache_data(ttl=300)
def carregar_modulos(id_grupo_empresa: int | None = None):
    try:
        conn = get_connection()
        if id_grupo_empresa:
            df = pd.read_sql("""
                SELECT id_modulo, id_grupo_empresa, codigo, nome, descricao, ativo, criado_em
                FROM modulo
                WHERE id_grupo_empresa = %s
                ORDER BY nome
            """, conn, params=(int(id_grupo_empresa),))
        else:
            df = pd.read_sql("""
                SELECT id_modulo, id_grupo_empresa, codigo, nome, descricao, ativo, criado_em
                FROM modulo
                ORDER BY nome
            """, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def carregar_tabelas_catalogo(id_grupo_empresa: int | None = None):
    """
    Agora filtra pelo grupo via módulo.
    """
    try:
        conn = get_connection()
        if id_grupo_empresa:
            df = pd.read_sql("""
                SELECT t.*, m.nome AS modulo_nome, m.codigo AS modulo_codigo, m.id_grupo_empresa
                FROM tabela_catalogo t
                JOIN modulo m ON m.id_modulo = t.id_modulo
                WHERE m.id_grupo_empresa = %s
                ORDER BY m.nome, t.tabela_codigo
            """, conn, params=(int(id_grupo_empresa),))
        else:
            df = pd.read_sql("""
                SELECT t.*, m.nome AS modulo_nome, m.codigo AS modulo_codigo, m.id_grupo_empresa
                FROM tabela_catalogo t
                JOIN modulo m ON m.id_modulo = t.id_modulo
                ORDER BY m.nome, t.tabela_codigo
            """, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def carregar_tabelas_por_modulo(id_modulo: int) -> pd.DataFrame:
    """
    Lista as tabelas do Catálogo para um módulo específico.
    """
    try:
        conn = get_connection()
        df = pd.read_sql("""
            SELECT 
                t.id_tabela,
                t.id_modulo,
                t.tabela_codigo,
                t.titulo,
                t.descricao,
                t.sistema_origem,
                m.nome  AS modulo_nome,
                m.codigo AS modulo_codigo
            FROM tabela_catalogo t
            JOIN modulo m ON m.id_modulo = t.id_modulo
            WHERE t.id_modulo = %s
            ORDER BY t.tabela_codigo
        """, conn, params=(int(id_modulo),))
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def carregar_usuarios_portal() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql("""
        SELECT 
            id_portal_usuario,
            login,
            nome_exibicao,
            is_admin,
            ativo,
            criado_em,
            atualizado_em
        FROM portal_usuario
        ORDER BY login
    """, conn)
    conn.close()
    return df

def excluir_tabelas_catalogo(id_modulo: int, codigos: list[str]):
    """
    Exclui múltiplas tabelas do Catálogo para um mesmo módulo.
    - Busca os ids (id_tabela) pelo (id_modulo, tabela_codigo)
    - Apaga primeiro em coluna_catalogo (FK), depois em tabela_catalogo
    - Limpa caches ao final
    """
    if not codigos:
        return

    # segurança leve: só aceitar alfanumérico + underscore
    safe = []
    for c in codigos:
        c2 = (c or "").strip().upper()
        if c2 and c2.replace("_","").isalnum():
            safe.append(c2)
    if not safe:
        return

    conn = get_connection()
    cur  = conn.cursor()

    try:
        # 1) Descobrir ids
        q_ids = """
            SELECT id_tabela
              FROM tabela_catalogo
             WHERE id_modulo = %s
               AND UPPER(tabela_codigo) = ANY(%s)
        """
        cur.execute(q_ids, (int(id_modulo), safe))
        ids = [r[0] for r in cur.fetchall()]

        if not ids:
            conn.rollback()
            return

        # 2) Deletar colunas (FK)
        q_del_cols = "DELETE FROM coluna_catalogo WHERE id_tabela = ANY(%s)"
        cur.execute(q_del_cols, (ids,))

        # 3) Deletar as tabelas do catálogo
        q_del_tabs = "DELETE FROM tabela_catalogo WHERE id_tabela = ANY(%s)"
        cur.execute(q_del_tabs, (ids,))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

    st.cache_data.clear()

@st.cache_data(ttl=300)
def carregar_colunas_catalogo(id_tabela:int):
    try:
        conn = get_connection()
        df = pd.read_sql("""
            SELECT coluna_nome, titulo, descricao, tipo_dado, eh_sensivel, nivel_pii, eh_pk, eh_nk, ordem
            FROM coluna_catalogo WHERE id_tabela=%s ORDER BY ordem, coluna_nome
        """, conn, params=(int(id_tabela),))
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def log_action(usuario_id, usuario_nome, perfil_desc, acao, entidade, registro_id=None,
               valores_anteriores=None, valores_novos=None, status='SUCESSO', mensagem_extra=None,
               ip_origem='0.0.0.0', user_agent='streamlit'):
    """
    Insere um log na tabela logs
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO logs (
            usuario_id, usuario_nome, perfil, acao, entidade, registro_id,
            valores_anteriores, valores_novos, ip_origem, user_agent, status, mensagem_extra
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s)
    """, (
        usuario_id, usuario_nome, perfil_desc, acao, entidade, str(registro_id) if registro_id else None,
        json.dumps(valores_anteriores) if valores_anteriores is not None else None,
        json.dumps(valores_novos) if valores_novos is not None else None,
        ip_origem, user_agent, status, mensagem_extra
    ))
    conn.commit()
    cur.close()
    conn.close()

def get_session_user_info():
    """Usa o usuário logado no PORTAL (tabela secundária)."""
    u = st.session_state.get("usuario_portal")
    if not u:
        return {
            "usuario_id": 0,
            "usuario_nome": "deslogado",
            "perfil_desc": "—",
            "id_perfil": None
        }
    return {
        "usuario_id": int(u.get("id_usuario") or 0),
        "usuario_nome": str(u.get("nome") or "—"),
        "perfil_desc": str(u.get("perfil_desc") or "Usuário"),
        "id_perfil": None  # portal não amarra perfil/empresa
    }

@st.cache_data(ttl=300)
def carregar_perfis():
    """
    Perfil possui FK para empresa.
    Retornamos também o id_grupo_empresa para poder filtrar por grupo.
    """
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            p.id_perfil,
            p.descricao,
            p.id_empresa,
            e.nome_empresa,
            e.id_grupo_empresa
        FROM perfil p
        JOIN empresa e ON p.id_empresa = e.id_empresa
        ORDER BY e.nome_empresa, p.descricao
    """, conn)
    conn.close()
    return df


# =========================
# Cache de dados básicos
# =========================
@st.cache_data(ttl=300)
def carregar_empresas():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT e.id_empresa, e.nome_empresa, e.cnpj, g.nome_grupo_empresa, e.id_grupo_empresa
        FROM empresa e
        JOIN grupo_empresa g ON e.id_grupo_empresa = g.id_grupo_empresa
        ORDER BY e.nome_empresa
    """, conn)
    conn.close()
    return df

def deletar_perfil(id_perfil: int):
    """
    Exclui o perfil e suas permissões associadas.
    NÃO permite excluir se houver usuários usando este perfil.
    """
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    # descobrir id_grupo_empresa do perfil / empresa
    cur.execute("""
        SELECT e.id_grupo_empresa
        FROM perfil p
        JOIN empresa e ON e.id_empresa = p.id_empresa
        WHERE p.id_perfil = %s
    """, (id_perfil,))
    row = cur.fetchone()
    id_grupo = row[0] if row else None

    if not st.session_state.get("usuario_portal", {}).get("is_admin"):
        grupos_ids = st.session_state.get("grupos_usuario_ids", [])
        if id_grupo not in grupos_ids:
            raise PermissionError("Você não pode alterar perfis de outro grupo de empresas.")

    try:
        # Verifica se há usuários com este perfil
        cur.execute("SELECT COUNT(*) FROM usuario WHERE id_perfil = %s", (id_perfil,))
        qt_usuarios = cur.fetchone()[0]
        
        if qt_usuarios > 0:
            raise RuntimeError(
                f"Não é possível excluir o perfil: existem {qt_usuarios} usuário(s) vinculado(s). "
                f"Reatribua os usuários a outro perfil antes de excluir."
            )
        
        # Busca dados do perfil antes de excluir (para log)
        cur.execute(
            "SELECT id_empresa, descricao FROM perfil WHERE id_perfil = %s",
            (id_perfil,)
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Perfil não encontrado.")
        
        before = {
            "id_perfil": id_perfil,
            "id_empresa": row[0],
            "descricao": row[1],
        }
        
        # 1) Exclui permissões associadas
        cur.execute("DELETE FROM permissao WHERE id_perfil = %s", (id_perfil,))
        qt_perms = cur.rowcount or 0
        
        # 2) Exclui o perfil
        cur.execute("DELETE FROM perfil WHERE id_perfil = %s", (id_perfil,))
        
        conn.commit()
        
        # Log da ação
        log_action(
            u["usuario_id"], u["usuario_nome"], u["perfil_desc"],
            "DELETE", "perfil",
            registro_id=id_perfil,
            valores_anteriores=before,
            valores_novos=None,
            mensagem_extra=f"{qt_perms} permissão(ões) excluída(s) junto com o perfil."
        )
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
    
    st.cache_data.clear()

@st.cache_data(ttl=300)
def carregar_tabelas():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT id_tabela, nome_tabela, descricao
        FROM tabela
        ORDER BY nome_tabela
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def carregar_usuarios():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            u.id_usuario,
            u.nome,
            u.senha,
            u.is_admin,
            p.id_perfil,
            p.descricao AS perfil,
            e.id_empresa,
            e.nome_empresa,
            e.id_grupo_empresa
        FROM usuario u
        JOIN perfil p ON u.id_perfil = p.id_perfil
        JOIN empresa e ON p.id_empresa = e.id_empresa
        ORDER BY u.nome
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def carregar_usuarios_portal() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT
            id_portal_usuario,
            login,
            nome_exibicao,
            is_admin,
            ativo,
            criado_em,
            atualizado_em
        FROM portal_usuario
        ORDER BY login
        """,
        conn,
    )
    conn.close()
    return df


def carregar_portal_info_por_nome(nome: str):
    """
    Retorna info básica do portal_usuario para um nome (login),
    ou None se não existir.
    """
    login = (nome or "").strip().lower()
    if not login:
        return None

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id_portal_usuario, login, nome_exibicao, is_admin, ativo
              FROM portal_usuario
             WHERE LOWER(TRIM(login)) = %s
             LIMIT 1
            """,
            (login,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = ["id_portal_usuario", "login", "nome_exibicao", "is_admin", "ativo"]
        return dict(zip(cols, row))
    finally:
        cur.close()
        conn.close()


def sync_portal_usuario_from_usuario(
    nome: str,
    senha: str,
    portal_is_admin: bool,
    ativo: bool = True,
):
    """
    Garante que exista (ou atualiza) um registro em portal_usuario
    para o usuário desta aplicação.
    """
    login = (nome or "").strip()
    if not login:
        return

    login_norm = login.strip().lower()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id_portal_usuario
              FROM portal_usuario
             WHERE LOWER(TRIM(login)) = %s
             LIMIT 1
            """,
            (login_norm,),
        )
        row = cur.fetchone()

        if row:
            cur.execute(
                """
                UPDATE portal_usuario
                   SET senha         = %s,
                       nome_exibicao = %s,
                       is_admin      = %s,
                       ativo         = %s,
                       atualizado_em = NOW()
                 WHERE id_portal_usuario = %s
                """,
                (senha, login, portal_is_admin, ativo, row[0]),
            )
        else:
            cur.execute(
                """
                INSERT INTO portal_usuario (login, senha, nome_exibicao, is_admin, ativo)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (login, senha, login, portal_is_admin, ativo),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def salvar_usuario_portal(
    id_portal_usuario: int,
    login: str,
    nome_exibicao: str,
    senha_nova: str | None,
    is_admin_portal: bool,
    ativo: bool,
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        if senha_nova:
            cur.execute(
                """
                UPDATE portal_usuario
                   SET login         = %s,
                       nome_exibicao = %s,
                       senha         = %s,
                       is_admin      = %s,
                       ativo         = %s,
                       atualizado_em = NOW()
                 WHERE id_portal_usuario = %s
                """,
                (login, nome_exibicao, senha_nova, is_admin_portal, ativo, id_portal_usuario),
            )
        else:
            cur.execute(
                """
                UPDATE portal_usuario
                   SET login         = %s,
                       nome_exibicao = %s,
                       is_admin      = %s,
                       ativo         = %s,
                       atualizado_em = NOW()
                 WHERE id_portal_usuario = %s
                """,
                (login, nome_exibicao, is_admin_portal, ativo, id_portal_usuario),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    st.cache_data.clear()

@st.cache_data(ttl=300)
def carregar_grupos():
    """
    Retorna todos os grupos de empresas cadastrados.
    """
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT 
            id_grupo_empresa,
            nome_grupo_empresa,
            banco_dados,
            versao,
            cnpj_matriz
        FROM grupo_empresa
        ORDER BY nome_grupo_empresa;
        """,
        conn
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def carregar_grupos_usuario_portal(id_portal_usuario: int) -> pd.DataFrame:
    """
    Retorna os grupos de empresas aos quais o USUÁRIO DO PORTAL está ligado,
    via:
      portal_usuario -> usuario (nome = login) -> usuario_empresa -> empresa -> grupo_empresa
    """
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT DISTINCT
               ge.id_grupo_empresa,
               ge.nome_grupo_empresa
        FROM portal_usuario pu
        JOIN usuario u
          ON LOWER(TRIM(u.nome)) = LOWER(TRIM(pu.login))
        JOIN usuario_empresa ue
          ON ue.id_usuario = u.id_usuario
        JOIN empresa e
          ON e.id_empresa = ue.id_empresa
        JOIN grupo_empresa ge
          ON ge.id_grupo_empresa = e.id_grupo_empresa
        WHERE pu.id_portal_usuario = %s
        ORDER BY ge.nome_grupo_empresa
        """,
        conn,
        params=(int(id_portal_usuario),),
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def carregar_empresas_usuario(id_usuario: int) -> pd.DataFrame:
    """
    Empresas às quais o usuário está vinculado (tabela usuario_empresa).
    """
    conn = get_connection()
    df = pd.read_sql("""
        SELECT ue.id_empresa, e.nome_empresa
          FROM usuario_empresa ue
          JOIN empresa e ON e.id_empresa = ue.id_empresa
         WHERE ue.id_usuario = %s
         ORDER BY e.nome_empresa
    """, conn, params=(int(id_usuario),))
    conn.close()
    return df

@st.cache_data(ttl=300)
def carregar_permissoes_perfil(id_perfil: int):
    conn = get_connection()
    df = pd.read_sql("""
        SELECT p.id_permissao, p.id_perfil, p.id_tabela, t.nome_tabela, p.campos_nao_permitidos
        FROM permissao p
        JOIN tabela t ON p.id_tabela = t.id_tabela
        WHERE p.id_perfil = %s
        ORDER BY t.nome_tabela
    """, conn, params=(int(id_perfil),))
    conn.close()
    return df

# =========================
# CRUD + auxiliares
# =========================
def salvar_grupo(id_grupo, nome, banco_dados, versao, cnpj_matriz):
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None
    if id_grupo is None:
        cur.execute("""
            INSERT INTO grupo_empresa (nome_grupo_empresa, banco_dados, versao, cnpj_matriz)
            VALUES (%s, %s, %s, %s) RETURNING id_grupo_empresa
        """, (nome, banco_dados, versao, cnpj_matriz))
        new_id = cur.fetchone()[0]
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "CREATE", "grupo_empresa",
                   registro_id=new_id, valores_anteriores=before, valores_novos={"nome": nome, "banco_dados": banco_dados, "versao": versao, "cnpj_matriz": cnpj_matriz})
    else:
        cur.execute("SELECT nome_grupo_empresa, banco_dados, versao, cnpj_matriz FROM grupo_empresa WHERE id_grupo_empresa=%s", (id_grupo,))
        row = cur.fetchone()
        before = {"nome": row[0], "banco_dados": row[1], "versao": row[2], "cnpj_matriz": row[3]} if row else None
        cur.execute("""
            UPDATE grupo_empresa SET nome_grupo_empresa=%s, banco_dados=%s, versao=%s, cnpj_matriz=%s
            WHERE id_grupo_empresa=%s
        """, (nome, banco_dados, versao, cnpj_matriz, id_grupo))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "UPDATE", "grupo_empresa",
                   registro_id=id_grupo, valores_anteriores=before, valores_novos={"nome": nome, "banco_dados": banco_dados, "versao": versao, "cnpj_matriz": cnpj_matriz})
    cur.close()
    conn.close()
    st.cache_data.clear()

def salvar_empresa(id_empresa, id_grupo_empresa, nome_empresa, cnpj):
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None
    if id_empresa is None:
        cur.execute("""
            INSERT INTO empresa (id_grupo_empresa, nome_empresa, cnpj)
            VALUES (%s, %s, %s) RETURNING id_empresa
        """, (id_grupo_empresa, nome_empresa, cnpj))
        new_id = cur.fetchone()[0]
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "CREATE", "empresa",
                   registro_id=new_id, valores_anteriores=before, valores_novos={"id_grupo_empresa": id_grupo_empresa, "nome_empresa": nome_empresa, "cnpj": cnpj})
    else:
        cur.execute("SELECT id_grupo_empresa, nome_empresa, cnpj FROM empresa WHERE id_empresa=%s", (id_empresa,))
        row = cur.fetchone()
        before = {"id_grupo_empresa": row[0], "nome_empresa": row[1], "cnpj": row[2]} if row else None
        cur.execute("""
            UPDATE empresa SET id_grupo_empresa=%s, nome_empresa=%s, cnpj=%s
            WHERE id_empresa=%s
        """, (id_grupo_empresa, nome_empresa, cnpj, id_empresa))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "UPDATE", "empresa",
                   registro_id=id_empresa, valores_anteriores=before, valores_novos={"id_grupo_empresa": id_grupo_empresa, "nome_empresa": nome_empresa, "cnpj": cnpj})
    cur.close()
    conn.close()
    st.cache_data.clear()

def salvar_modulo(id_modulo, id_grupo_empresa, codigo, nome, descricao, ativo: bool):
    """Se id_modulo vier, atualiza por ID. Senão, cria. Evita criar módulo novo ao trocar 'codigo'."""
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None
    # SEGURANÇA: não-admin só pode mexer no seu grupo
    grupos_ids = st.session_state.get("grupos_usuario_ids", [])
    if not st.session_state.get("usuario_portal", {}).get("is_admin"):
        if int(id_grupo_empresa) not in grupos_ids:
            raise PermissionError("Você não pode alterar módulos de outro grupo de empresas.")
    try:
        if id_modulo is None:
            cur.execute("""
                INSERT INTO modulo (id_grupo_empresa, codigo, nome, descricao, ativo)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id_modulo
            """, (id_grupo_empresa, codigo.strip(), nome.strip(), descricao.strip(), ativo))
            new_id = cur.fetchone()[0]
            conn.commit()
            log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "CREATE", "modulo",
                       registro_id=new_id, valores_anteriores=None,
                       valores_novos={"id_grupo_empresa": id_grupo_empresa, "codigo": codigo, "nome": nome, "descricao": descricao, "ativo": ativo})
        else:
            cur.execute("SELECT id_grupo_empresa, codigo, nome, descricao, ativo FROM modulo WHERE id_modulo=%s", (id_modulo,))
            r = cur.fetchone()
            before = {"id_grupo_empresa": r[0], "codigo": r[1], "nome": r[2], "descricao": r[3], "ativo": r[4]} if r else None
            cur.execute("""
                UPDATE modulo
                   SET id_grupo_empresa=%s, codigo=%s, nome=%s, descricao=%s, ativo=%s
                 WHERE id_modulo=%s
            """, (id_grupo_empresa, codigo.strip(), nome.strip(), descricao.strip(), ativo, id_modulo))
            conn.commit()
            log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "UPDATE", "modulo",
                       registro_id=id_modulo, valores_anteriores=before,
                       valores_novos={"id_grupo_empresa": id_grupo_empresa, "codigo": codigo, "nome": nome, "descricao": descricao, "ativo": ativo})
    finally:
        cur.close(); conn.close()
    st.cache_data.clear()


def deletar_modulo_cascata(id_modulo: int):
    
    """
    Exclui o MÓDULO e TUDO que estiver pendurado no Catálogo:
      - coluna_catalogo das tabelas do módulo
      - tabela_catalogo do módulo
      - o próprio modulo
    Obs: não mexe na tabela 'tabela' (legado) nem em 'permissao', pois são entidades separadas do catálogo.
    """
    u = get_session_user_info()

    conn = get_connection()
    cur  = conn.cursor()
    # SEGURANÇA: não-admin só pode mexer no seu grupo
    grupos_ids = st.session_state.get("grupos_usuario_ids", [])
    if not st.session_state.get("usuario_portal", {}).get("is_admin"):
        if int(id_grupo_empresa) not in grupos_ids:
            raise PermissionError("Você não pode alterar módulos de outro grupo de empresas.")
    try:
        # 1) Coletar ids de tabela_catalogo do módulo
        cur.execute("SELECT id_tabela FROM tabela_catalogo WHERE id_modulo=%s", (id_modulo,))
        ids = [r[0] for r in cur.fetchall()]

        qt_cols_del = 0
        qt_tabs_del = 0

        if ids:
            # 2) Deletar colunas do catálogo
            cur.execute("DELETE FROM coluna_catalogo WHERE id_tabela = ANY(%s)", (ids,))
            qt_cols_del = cur.rowcount or 0

            # 3) Deletar tabelas do catálogo
            cur.execute("DELETE FROM tabela_catalogo WHERE id_tabela = ANY(%s)", (ids,))
            qt_tabs_del = cur.rowcount or 0

        # 4) Deletar o módulo
        cur.execute("DELETE FROM modulo WHERE id_modulo=%s", (id_modulo,))
        qt_mod = cur.rowcount or 0

        conn.commit()

        # Log
        log_action(
            u["usuario_id"], u["usuario_nome"], u["perfil_desc"],
            "DELETE", "modulo", registro_id=id_modulo,
            valores_anteriores={"id_modulo": id_modulo},
            valores_novos=None,
            mensagem_extra=f"Exclusão em cascata: {qt_tabs_del} tabela(s) e {qt_cols_del} coluna(s) do Catálogo removidas antes do módulo."
        )

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

    st.cache_data.clear()

def deletar_empresa_cascata(id_empresa: int):
    """
    Exclui TUDO relacionado a UMA EMPRESA:
      - permissao (de perfis da empresa)
      - usuario_empresa (todos os vínculos de usuários impactados)
      - usuario (cujos id_perfil pertencem à empresa)
      - perfil (da empresa)
      - empresa (a própria)
    Observação: módulos vivem no nível do GRUPO; não são apagados aqui.
    """
    u = get_session_user_info()
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # Perfis da empresa
        cur.execute("SELECT id_perfil FROM perfil WHERE id_empresa=%s", (id_empresa,))
        perfis = [r[0] for r in cur.fetchall()]  # [] se vazio

        # Usuários que usam esses perfis
        if perfis:
            cur.execute("SELECT id_usuario FROM usuario WHERE id_perfil = ANY(%s)", (perfis,))
            usuarios = [r[0] for r in cur.fetchall()]
        else:
            usuarios = []

        # 1) permissao desses perfis
        if perfis:
            cur.execute("DELETE FROM permissao WHERE id_perfil = ANY(%s)", (perfis,))

        # 2) vínculos em usuario_empresa (para os usuários impactados)
        if usuarios:
            cur.execute("DELETE FROM usuario_empresa WHERE id_usuario = ANY(%s)", (usuarios,))

        # 3) usuários cujo perfil será removido
        if usuarios:
            cur.execute("DELETE FROM usuario WHERE id_usuario = ANY(%s)", (usuarios,))

        # 4) perfis da empresa
        if perfis:
            cur.execute("DELETE FROM perfil WHERE id_perfil = ANY(%s)", (perfis,))

        # 5) empresa
        cur.execute("DELETE FROM empresa WHERE id_empresa=%s", (id_empresa,))

        conn.commit()

        log_action(
            u["usuario_id"], u["usuario_nome"], u["perfil_desc"],
            "DELETE", "empresa",
            registro_id=id_empresa,
            valores_anteriores={"id_empresa": id_empresa},
            valores_novos=None,
            mensagem_extra="Exclusão em cascata: permissões, vínculos, usuários e perfis apagados antes da empresa."
        )
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close(); conn.close()
    st.cache_data.clear()

def deletar_grupo_cascata(id_grupo: int):
    """
    Exclui TUDO relacionado a um GRUPO:
      - módulos do grupo (via deletar_modulo_cascata → apaga Catálogo)
      - empresas do grupo (via deletar_empresa_cascata → apaga perfis/usuarios/permissões)
      - o próprio grupo
    """
    u = get_session_user_info()
    conn = get_connection(); cur = conn.cursor()

    try:
        # 1) Módulos do grupo (apagam Catálogo)
        cur.execute("SELECT id_modulo FROM modulo WHERE id_grupo_empresa=%s", (id_grupo,))
        mod_ids = [r[0] for r in cur.fetchall()]
        for mid in mod_ids:
            # usa transação separada dentro da função
            deletar_modulo_cascata(mid)

        # 2) Empresas do grupo (apagam perfis/usuarios/permissões)
        cur.execute("SELECT id_empresa FROM empresa WHERE id_grupo_empresa=%s", (id_grupo,))
        emp_ids = [r[0] for r in cur.fetchall()]
        for eid in emp_ids:
            deletar_empresa_cascata(eid)

        # 3) Grupo
        cur.execute("DELETE FROM grupo_empresa WHERE id_grupo_empresa=%s", (id_grupo,))

        conn.commit()

        log_action(
            u["usuario_id"], u["usuario_nome"], u["perfil_desc"],
            "DELETE", "grupo_empresa",
            registro_id=id_grupo,
            valores_anteriores={"id_grupo_empresa": id_grupo},
            valores_novos=None,
            mensagem_extra=f"Exclusão em cascata: {len(mod_ids)} módulo(s) e {len(emp_ids)} empresa(s) removidos antes do grupo."
        )

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close(); conn.close()
    st.cache_data.clear()

def deletar_grupo(id_grupo: int):
    """Impede exclusão de grupo se houver empresas ou módulos vinculados."""
    u = get_session_user_info()
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM empresa WHERE id_grupo_empresa=%s", (id_grupo,))
        (qt_empresas,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM modulo WHERE id_grupo_empresa=%s", (id_grupo,))
        (qt_modulos,) = cur.fetchone()
        msgs = []
        if qt_empresas: msgs.append(f"{qt_empresas} empresa(s)")
        if qt_modulos:  msgs.append(f"{qt_modulos} módulo(s)")
        if msgs:
            raise RuntimeError("Não é possível excluir o Grupo: existem " + " e ".join(msgs) + " vinculadas.")
        cur.execute("DELETE FROM grupo_empresa WHERE id_grupo_empresa=%s", (id_grupo,))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "DELETE", "grupo_empresa",
                   registro_id=id_grupo, valores_anteriores=None, valores_novos=None)
    finally:
        cur.close(); conn.close()
    st.cache_data.clear()

def deletar_usuario(id_usuario: int):
    """
    Exclui o usuário e seus vínculos em usuario_empresa.
    NÃO mexe em permissao, pois ela é por perfil.
    """
    u = get_session_user_info()
    conn = get_connection(); cur = conn.cursor()

    try:
        cur.execute("SELECT id_perfil, nome, is_admin FROM usuario WHERE id_usuario=%s", (id_usuario,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Usuário não encontrado.")

        before = {
            "id_perfil": row[0],
            "nome": row[1],
            "is_admin": row[2],
        }

        # Remove vínculos em usuario_empresa
        cur.execute("DELETE FROM usuario_empresa WHERE id_usuario=%s", (id_usuario,))

        # Remove o usuário
        cur.execute("DELETE FROM usuario WHERE id_usuario=%s", (id_usuario,))
        conn.commit()

        log_action(
            u["usuario_id"], u["usuario_nome"], u["perfil_desc"],
            "DELETE", "usuario",
            registro_id=id_usuario,
            valores_anteriores=before,
            valores_novos=None
        )
    finally:
        cur.close(); conn.close()

    st.cache_data.clear()

def deletar_empresa(id_empresa: int):
    """Impede exclusão de empresa se houver perfis/usuários vinculados."""
    u = get_session_user_info()
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM perfil WHERE id_empresa=%s", (id_empresa,))
        (qt_perfis,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM usuario_empresa WHERE id_empresa=%s", (id_empresa,))
        (qt_usuarios_map,) = cur.fetchone()
        msgs = []
        if qt_perfis:       msgs.append(f"{qt_perfis} perfil(s)")
        if qt_usuarios_map: msgs.append(f"{qt_usuarios_map} vínculo(s) em usuario_empresa")
        if msgs:
            raise RuntimeError("Não é possível excluir a Empresa: existem " + " e ".join(msgs) + " vinculados.")
        cur.execute("DELETE FROM empresa WHERE id_empresa=%s", (id_empresa,))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "DELETE", "empresa",
                   registro_id=id_empresa, valores_anteriores=None, valores_novos=None)
    finally:
        cur.close(); conn.close()
    st.cache_data.clear()

def salvar_perfil(id_perfil, id_empresa, descricao):
    """
    Schema novo: perfil precisa de id_empresa.
    """
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None
    # descobrir id_grupo_empresa do perfil / empresa
    cur.execute("""
        SELECT e.id_grupo_empresa
        FROM perfil p
        JOIN empresa e ON e.id_empresa = p.id_empresa
        WHERE p.id_perfil = %s
    """, (id_perfil,))
    row = cur.fetchone()
    id_grupo = row[0] if row else None

    if not st.session_state.get("usuario_portal", {}).get("is_admin"):
        grupos_ids = st.session_state.get("grupos_usuario_ids", [])
        if id_grupo not in grupos_ids:
            raise PermissionError("Você não pode alterar perfis de outro grupo de empresas.")

    if id_perfil is None:
        cur.execute("INSERT INTO perfil (id_empresa, descricao) VALUES (%s, %s) RETURNING id_perfil", (id_empresa, descricao))
        new_id = cur.fetchone()[0]
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "CREATE", "perfil",
                   registro_id=new_id, valores_anteriores=before, valores_novos={"id_empresa": id_empresa, "descricao": descricao})
    else:
        cur.execute("SELECT id_empresa, descricao FROM perfil WHERE id_perfil=%s", (id_perfil,))
        row = cur.fetchone()
        before = {"id_empresa": row[0], "descricao": row[1]} if row else None
        cur.execute("UPDATE perfil SET id_empresa=%s, descricao=%s WHERE id_perfil=%s", (id_empresa, descricao, id_perfil))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "UPDATE", "perfil",
                   registro_id=id_perfil, valores_anteriores=before, valores_novos={"id_empresa": id_empresa, "descricao": descricao})
    cur.close()
    conn.close()
    st.cache_data.clear()

def salvar_tabela(id_tabela, nome_tabela, descricao):
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None
    if id_tabela is None:
        cur.execute("INSERT INTO tabela (nome_tabela, descricao) VALUES (%s, %s) RETURNING id_tabela", (nome_tabela, descricao))
        new_id = cur.fetchone()[0]
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "CREATE", "tabela",
                   registro_id=new_id, valores_anteriores=before, valores_novos={"nome_tabela": nome_tabela, "descricao": descricao})
    else:
        cur.execute("SELECT nome_tabela, descricao FROM tabela WHERE id_tabela=%s", (id_tabela,))
        row = cur.fetchone()
        before = {"nome_tabela": row[0], "descricao": row[1]} if row else None
        cur.execute("UPDATE tabela SET nome_tabela=%s, descricao=%s WHERE id_tabela=%s", (nome_tabela, descricao, id_tabela))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "UPDATE", "tabela",
                   registro_id=id_tabela, valores_anteriores=before, valores_novos={"nome_tabela": nome_tabela, "descricao": descricao})
    cur.close()
    conn.close()
    st.cache_data.clear()

def upsert_usuario_empresa(id_usuario, id_empresa, id_perfil):
    """
    Garante a associação em usuario_empresa (não remove associações existentes).
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO usuario_empresa (id_usuario, id_empresa, id_perfil)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_usuario, id_empresa, id_perfil) DO NOTHING
    """, (id_usuario, id_empresa, id_perfil))
    conn.commit()
    cur.close()
    conn.close()
def salvar_usuario(
    id_usuario,
    nome,
    senha,
    id_perfil,
    is_admin,
    ids_empresas_para_mapear=None,
    tem_acesso_portal: bool = False,
    portal_is_admin: bool = False,
):
    """
    Schema novo: usuario tem somente id_perfil (empresa vem de perfil).
    Agora, controle de acesso às empresas é feito via usuario_empresa,
    aceitando múltiplas empresas dentro do grupo.

    tem_acesso_portal / portal_is_admin controlam sincronização com portal_usuario.
    """
    u = get_session_user_info()

    # Normaliza lista de empresas
    ids_empresas_para_mapear = ids_empresas_para_mapear or []
    ids_empresas = sorted({int(e) for e in ids_empresas_para_mapear if e})

    conn = get_connection()
    cur = conn.cursor()
    before = None
    id_usuario_final = id_usuario

    try:
        if id_usuario is None:
            # CREATE
            cur.execute("""
                INSERT INTO usuario (id_perfil, senha, nome, is_admin)
                VALUES (%s, %s, %s, %s)
                RETURNING id_usuario
            """, (id_perfil, senha, nome, is_admin))
            id_usuario_final = cur.fetchone()[0]
            conn.commit()

            log_action(
                u["usuario_id"], u["usuario_nome"], u["perfil_desc"],
                "CREATE", "usuario",
                registro_id=id_usuario_final,
                valores_anteriores=None,
                valores_novos={
                    "nome": nome,
                    "id_perfil": id_perfil,
                    "is_admin": is_admin,
                    "empresas": ids_empresas,
                },
            )
        else:
            # UPDATE
            cur.execute(
                "SELECT id_perfil, senha, nome, is_admin FROM usuario WHERE id_usuario=%s",
                (id_usuario,),
            )
            row = cur.fetchone()
            before = {
                "id_perfil": row[0],
                "senha": "***",
                "nome": row[2],
                "is_admin": row[3],
            } if row else None

            cur.execute(
                """
                UPDATE usuario
                   SET id_perfil=%s,
                       senha=%s,
                       nome=%s,
                       is_admin=%s
                 WHERE id_usuario=%s
                """,
                (id_perfil, senha, nome, is_admin, id_usuario),
            )
            conn.commit()

            log_action(
                u["usuario_id"], u["usuario_nome"], u["perfil_desc"],
                "UPDATE", "usuario",
                registro_id=id_usuario,
                valores_anteriores=before,
                valores_novos={
                    "id_perfil": id_perfil,
                    "senha": "***",
                    "nome": nome,
                    "is_admin": is_admin,
                    "empresas": ids_empresas,
                },
            )
            id_usuario_final = id_usuario
    finally:
        cur.close()
        conn.close()

    # Atualiza vínculos em usuario_empresa
    conn_map = get_connection()
    cur_map = conn_map.cursor()
    try:
        # Remove vínculos anteriores do usuário
        cur_map.execute(
            "DELETE FROM usuario_empresa WHERE id_usuario=%s",
            (id_usuario_final,),
        )
        # Recria vínculos
        for id_emp in ids_empresas:
            cur_map.execute(
                """
                INSERT INTO usuario_empresa (id_usuario, id_empresa, id_perfil)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_usuario, id_empresa, id_perfil) DO NOTHING
                """,
                (id_usuario_final, id_emp, id_perfil),
            )
        conn_map.commit()
    finally:
        cur_map.close()
        conn_map.close()

    # Se for para ter acesso ao portal, sincroniza com portal_usuario.
    # Se tem_acesso_portal=False, não mexemos em portal_usuario aqui.
    if tem_acesso_portal:
        sync_portal_usuario_from_usuario(
            nome=nome,
            senha=senha,
            is_admin=portal_is_admin,  # CORRIGIDO: era portal_is_admin, agora é is_admin no parâmetro
            ativo=True,
        )

    st.cache_data.clear()

def salvar_permissao_perfil(id_permissao, id_perfil, id_tabela, json_rules):
    """
    Persistência do JSON de regras por tabela (column_blocks/row_filters).
    """
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None

    table_name = json_rules.get("_table_name")
    table_payload = json_rules.get("tabelas", {}).get(table_name, {})

    payload_to_save = {"tabelas": {table_name: table_payload}, "metadata": {
        "escopo": "perfil", "id_perfil": id_perfil,
        "ultima_atualizacao": datetime.utcnow().isoformat() + "Z",
        "autor": u["usuario_nome"]
    }}

    if id_permissao is None:
        cur.execute("""
            INSERT INTO permissao (id_perfil, id_tabela, campos_nao_permitidos)
            VALUES (%s, %s, %s::jsonb)
            RETURNING id_permissao
        """, (id_perfil, id_tabela, json.dumps(payload_to_save)))
        new_id = cur.fetchone()[0]
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "PERMISSION_CREATE", "permissao",
                   registro_id=new_id, valores_anteriores=before, valores_novos=payload_to_save)
    else:
        cur.execute("SELECT campos_nao_permitidos FROM permissao WHERE id_permissao=%s", (id_permissao,))
        row = cur.fetchone()
        before = row[0] if row else None
        cur.execute("""
            UPDATE permissao
               SET campos_nao_permitidos = %s::jsonb
             WHERE id_permissao = %s
        """, (json.dumps(payload_to_save), id_permissao))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "PERMISSION_UPDATE", "permissao",
                   registro_id=id_permissao, valores_anteriores=before, valores_novos=payload_to_save)

    cur.close()
    conn.close()
    st.cache_data.clear()

def deletar_permissao(id_permissao):
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT campos_nao_permitidos FROM permissao WHERE id_permissao=%s", (id_permissao,))
    row = cur.fetchone()
    before = row[0] if row else None
    cur.execute("DELETE FROM permissao WHERE id_permissao = %s", (id_permissao,))
    conn.commit()
    cur.close()
    conn.close()
    log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "PERMISSION_DELETE", "permissao",
               registro_id=id_permissao, valores_anteriores=before, valores_novos=None)
    st.cache_data.clear()


# Login simples
# ========= Auth helpers do PORTAL (usando tabela SECUNDÁRIA) =========
def autenticar_portal_usuario(login: str, senha: str):
    """Autentica contra a TABELA SECUNDÁRIA portal_usuario (independente do chat/n8n)."""
    user_norm = (login or "").strip().lower()
    pwd_norm  = (senha or "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_portal_usuario, login, nome_exibicao, is_admin, ativo
              FROM portal_usuario
             WHERE LOWER(TRIM(login)) = %s
               AND senha = %s
               AND ativo = TRUE
             LIMIT 1
        """, (user_norm, pwd_norm))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id_usuario": int(row["id_portal_usuario"]),   # mantém a chave 'id_usuario' para o resto do portal
            "nome": row["nome_exibicao"],                  # exibir bonito na UI
            "is_admin": bool(row["is_admin"]),
            "perfil_desc": "Administrador" if row["is_admin"] else "Usuário",
            # Estes campos não existem na tabela secundária; deixamos None:
            "id_perfil": None,
            "id_empresa": None,
        }
    finally:
        cur.close()
        conn.close()

def usuario_tem_acesso_portal(nome: str) -> bool:
    """
    Verifica se o 'nome' do usuário existe como login ativo na tabela portal_usuario.
    """
    login = (nome or "").strip().lower()
    if not login:
        return False

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 1
              FROM portal_usuario
             WHERE LOWER(TRIM(login)) = %s
               AND ativo = TRUE
             LIMIT 1
        """, (login,))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def sync_portal_usuario_from_usuario(nome: str, senha: str, is_admin: bool, ativo: bool = True):
    """
    Garante que exista (ou seja atualizado) um registro em portal_usuario
    para o usuário desta aplicação de permissionamento.

    login       = nome (normalizado)
    senha       = mesma senha do cadastro de usuário
    nome_exibicao = nome
    is_admin    = mesmo flag
    ativo       = controla se o usuário pode logar ou não
    """
    login = (nome or "").strip()
    if not login:
        return

    login_norm = login.strip().lower()

    conn = get_connection()
    cur = conn.cursor()
    try:
        # Verifica se já existe um portal_usuario com esse login
        cur.execute("""
            SELECT id_portal_usuario
              FROM portal_usuario
             WHERE LOWER(TRIM(login)) = %s
             LIMIT 1
        """, (login_norm,))
        row = cur.fetchone()

        if row:
            # Atualiza dados básicos e marca ativo/inativo
            cur.execute("""
                UPDATE portal_usuario
                   SET senha         = %s,
                       nome_exibicao = %s,
                       is_admin      = %s,
                       ativo         = %s,
                       atualizado_em = NOW()
                 WHERE id_portal_usuario = %s
            """, (senha, login, is_admin, ativo, row[0]))
        else:
            # Cria novo usuário do portal
            cur.execute("""
                INSERT INTO portal_usuario (login, senha, nome_exibicao, is_admin, ativo)
                VALUES (%s, %s, %s, %s, %s)
            """, (login, senha, login, is_admin, ativo))

        conn.commit()
    finally:
        cur.close()
        conn.close()


def login_portal():
    """Tela de login do PORTAL"""
    st.subheader("Login do Portal")
    
    with st.form("form_login_portal", clear_on_submit=False):
        usuario = st.text_input("Usuário (login)")
        senha = st.text_input("Senha", type="password")
        lembrar = st.checkbox("Manter conectado (30 minutos)", value=True)
        ok = st.form_submit_button("Entrar")
        
        if ok:
            if not usuario or not senha:
                st.error("Preencha usuário e senha.")
            else:
                u = autenticar_portal_usuario(usuario, senha)
                
                if u:
                    st.session_state.logado = True
                    st.session_state.usuario_portal = u
                    
                    # Salva sessão no banco se solicitado
                    if lembrar:
                        token = gerar_token_sessao()
                        salvar_sessao_db(
                            token=token,
                            login=usuario,
                            is_admin=u["is_admin"],
                            nome_exibicao=u["nome"]
                        )
                        # coloca o token dentro do usuário logado
                        u["token"] = token
                        # coloca o token na URL
                        st.query_params["session"] = token
                        st.success("✅ Login realizado! Sessão salva.")
                    else:
                        st.success("✅ Login realizado!")
                    
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Usuário ou senha inválidos, ou usuário inativo.")
                    

# =========================
# Interface
# =========================
st.set_page_config(page_title="Gestão ERP - Usuários e Permissões", layout="wide")

st.title("Gestão de Usuários e Permissões - ERP (TOTVS RM)")

# ========= Estado inicial =========
if 'logado' not in st.session_state:
    st.session_state.logado = False
if 'usuario_portal' not in st.session_state:
    st.session_state.usuario_portal = None


# ========= TENTA RECUPERAR SESSÃO DA URL =========
if not st.session_state.logado:
    # Pega token da URL
    token = st.query_params.get("session")
    
    if token:
        sessao = carregar_sessao_db(token)
        
        if sessao:
            # Sessão válida!
            st.session_state.logado = True
            st.session_state.usuario_portal = sessao
            st.success("✅ Bem-vindo de volta!")
            time.sleep(0.5)
            st.rerun()
        else:
            # Sessão inválida - remove da URL
            st.query_params.clear()

# ========= Gate de login do PORTAL =========
if not st.session_state.logado:
    login_portal()
    st.stop()

# --- a partir daqui, SEMPRE logado no portal ---
u = st.session_state.usuario_portal
st.sidebar.subheader(f"Olá, {u['nome']} ({u['perfil_desc']})")

# Mostra tempo restante
if "token" in u:
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (NOW() - criado_em))/60 as minutos
            FROM sessoes_portal WHERE token = %s
        """, (u["token"],))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            minutos_passados = int(row[0])
            minutos_restantes = 30 - minutos_passados
            if minutos_restantes > 0:
                st.sidebar.caption(f"⏱️ Sessão: {minutos_restantes} min restantes")
    except:
        pass

# Botão de logout
if st.sidebar.button("Logout"):
    if "token" in u:
        limpar_sessao_db(u["token"])
    st.query_params.clear()  # Remove token da URL
    st.session_state.logado = False
    st.session_state.usuario_portal = None
    st.success("Logout realizado!")
    time.sleep(1)
    st.rerun()

# Menus diferentes para admin do portal x admin de empresa
menu_admin = [
    "Dashboard",
    "Grupos de Empresas",
    "Empresas",
    "Catálogo de Módulos",
    "Tabelas",
    "Perfis",
    "Permissões (por Perfil)",
    "Usuários - Aplicação",
    "Usuários - Portal",
    "Logs",
    "Logs de Interações",
    "Logout",
]

menu_empresa_admin = [
    "Dashboard",
    "Catálogo de Módulos",
    "Tabelas",
    "Perfis",
    "Permissões (por Perfil)",
    "Usuários - Aplicação",
    "Logs",
    "Logs de Interações",
    "Logout",
]

u = st.session_state.get("usuario_portal")

grupos_usuario_df = None
grupos_usuario_ids = []
if u and not u.get("is_admin"):
    grupos_usuario_df = carregar_grupos_usuario_portal(u["id_usuario"])
    grupos_usuario_ids = grupos_usuario_df["id_grupo_empresa"].astype(int).tolist()
    st.session_state["grupos_usuario_ids"] = grupos_usuario_ids


opcoes_menu = menu_admin if u.get("is_admin") else menu_empresa_admin

menu = st.sidebar.selectbox("Menu", opcoes_menu)



if menu == "Logout":
    st.session_state.logado = False
    st.session_state.usuario_portal = None
    reset_app_state()
    st.rerun()

# Dashboard
if menu == "Dashboard":
    st.subheader("Dashboard")
    st.write("Bem-vindo ao sistema de gestão de usuários e permissões para o agente de IA conectado ao TOTVS RM.")
    st.info("Aqui você gerencia perfis, usuários, empresas e as permissões por tabela (coluna e linha).")


elif menu == "Catálogo de Módulos":
    st.markdown("### Módulos (por Grupo de Empresas)")

    if u.get("is_admin"):
        grupos = carregar_grupos()
    else:
        grupos = carregar_grupos_usuario_portal(u["id_usuario"])
        if grupos.empty:
            st.error("Você não está vinculado a nenhum Grupo de Empresas. Fale com o administrador.")
            st.stop()

    grupo_label = st.selectbox(
        "Selecione o Grupo de Empresas",
        options=[f"{g['nome_grupo_empresa']} (id={g['id_grupo_empresa']})" for _, g in grupos.iterrows()]
    )
    id_grupo_sel = int(grupo_label.split("id=")[-1].rstrip(")"))

    df_mod = carregar_modulos(id_grupo_sel)

    # label: "Nome [CODIGO] (id=123)"
    mod_opts = ["<novo>"] + [f"{m['nome']} [{m['codigo']}] (id={m['id_modulo']})" for _, m in df_mod.iterrows()]
    mod_choice = st.selectbox("Selecione um módulo para editar (ou <novo>)", mod_opts)

    if mod_choice != "<novo>":
        id_modulo_atual = int(mod_choice.split("id=")[-1].rstrip(")"))
        mod_row = df_mod[df_mod["id_modulo"] == id_modulo_atual].iloc[0]
        mod_codigo = st.text_input("Código (único por grupo)", value=str(mod_row["codigo"]))
        mod_nome   = st.text_input("Nome", value=str(mod_row["nome"]))
        mod_desc   = st.text_area("Descrição", value=str(mod_row.get("descricao","") or ""))
        mod_ativo  = st.checkbox("Ativo", value=bool(mod_row.get("ativo", True)))
        c1, c2 = st.columns([1,1])
        if c1.button("Salvar alterações"):
            try:
                salvar_modulo(id_modulo_atual, id_grupo_sel, mod_codigo, mod_nome, mod_desc, mod_ativo)
                st.success("Módulo atualizado.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")
        # --- EXCLUSÃO DO MÓDULO (sem st.confirm) ---
        st.markdown("##### Excluir este módulo")

        del_col1, del_col2 = st.columns([1, 3])

        # Abre a etapa de confirmação
        if del_col1.button("🗑️ Excluir módulo", key=f"ask_del_mod_{id_modulo_atual}"):
            st.session_state[f"_ask_del_mod_{id_modulo_atual}"] = True
            st.rerun()

        # Renderiza confirmação
        if st.session_state.get(f"_ask_del_mod_{id_modulo_atual}", False):
            # preview antes de apagar
            conn = get_connection()
            df_prev = pd.read_sql("""
                SELECT 
                (SELECT COUNT(*) FROM tabela_catalogo WHERE id_modulo=%s) AS qt_tab,
                (SELECT COUNT(*) FROM coluna_catalogo WHERE id_tabela IN (SELECT id_tabela FROM tabela_catalogo WHERE id_modulo=%s)) AS qt_col
            """, conn, params=(id_modulo_atual, id_modulo_atual))
            conn.close()
            st.caption(f"Isso irá remover {int(df_prev.iloc[0]['qt_tab'])} tabela(s) e {int(df_prev.iloc[0]['qt_col'])} coluna(s) do Catálogo antes de excluir o módulo.")

            st.error("Essa ação é irreversível. Confirme para continuar.")
            confirm_txt = st.text_input(
                "Digite o CÓDIGO do módulo para confirmar a exclusão",
                key=f"confirm_mod_code_{id_modulo_atual}",
                placeholder=str(mod_codigo)
            )
            col_ok, col_cancel = st.columns([1,1])

            if col_ok.button("Confirmar exclusão", key=f"do_del_mod_{id_modulo_atual}"):
                if confirm_txt.strip().upper() == str(mod_codigo).strip().upper():
                    try:
                        deletar_modulo_cascata(id_modulo_atual)
                        st.session_state.pop(f"_ask_del_mod_{id_modulo_atual}", None)
                        st.success("Módulo excluído.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Não foi possível excluir: {e}")
                else:
                    st.error("Confirmação inválida. Digite exatamente o CÓDIGO do módulo.")

            if col_cancel.button("Cancelar", key=f"cancel_del_mod_{id_modulo_atual}"):
                st.session_state.pop(f"_ask_del_mod_{id_modulo_atual}", None)
                st.info("Exclusão cancelada.")
                st.rerun()
        # --- fim exclusão do módulo ---


        # === NOVO: Listar e excluir tabelas do Catálogo para o módulo selecionado ===
        st.markdown("#### Tabelas do Catálogo deste Módulo")
        df_tabs_mod = carregar_tabelas_por_modulo(id_modulo_atual)
        if df_tabs_mod.empty:
            st.info("Este módulo ainda não possui tabelas no Catálogo.")
        else:
            st.dataframe(
                df_tabs_mod[["tabela_codigo", "titulo", "descricao", "sistema_origem"]],
                use_container_width=True,
                hide_index=True
            )

            st.markdown("##### Excluir múltiplas tabelas deste módulo")
            # opções (tabela_codigo)
            opts = df_tabs_mod["tabela_codigo"].tolist()
            tabs_sel = st.multiselect(
                "Selecione as tabelas para excluir",
                options=opts,
                help="A exclusão remove as colunas (coluna_catalogo) e a própria entrada em tabela_catalogo."
            )

            # Confirmação textual (sem st.confirm)
            col_c1, col_c2 = st.columns([1.2, 1])
            confirm_txt = col_c1.text_input(
                "Digite o CÓDIGO do módulo para confirmar",
                placeholder=f"{mod_row['codigo']}"
            )
            confirmar = (confirm_txt.strip().upper() == str(mod_row["codigo"]).strip().upper())

            if col_c2.button("🗑️ Excluir selecionadas", type="secondary", disabled=(not tabs_sel)):
                if not confirmar:
                    st.error("Confirmação inválida. Digite corretamente o CÓDIGO do módulo.")
                else:
                    try:
                        excluir_tabelas_catalogo(id_modulo_atual, tabs_sel)
                        st.success(f"{len(tabs_sel)} tabela(s) excluída(s) do módulo.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao excluir: {e}")
    else:
        # Criar novo
        mod_codigo = st.text_input("Código (único por grupo)", value="")
        mod_nome   = st.text_input("Nome", value="")
        mod_desc   = st.text_area("Descrição", value="")
        mod_ativo  = st.checkbox("Ativo", value=True)
        if st.button("Criar módulo"):
            try:
                salvar_modulo(None, id_grupo_sel, mod_codigo, mod_nome, mod_desc, mod_ativo)
                st.success("Módulo criado.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro: {e}")

    st.markdown("#### Módulos deste Grupo") 
    df_mod = carregar_modulos(id_grupo_sel)
    st.dataframe(df_mod, use_container_width=True)

    st.markdown("---")
    # ====== (mantém) Importar Tabelas do Catálogo (Excel) ======
    st.markdown("### Importar Tabelas do Catálogo (Excel)")
    mods_grupo = carregar_modulos(id_grupo_sel)
    if mods_grupo.empty:
        st.info("Este grupo ainda não possui módulos. Crie um módulo acima para poder importar tabelas.")
    else:
        mod_label = st.selectbox(
            "Módulo do Catálogo",
            options=[f"{m['nome']} [{m['codigo']}] (id={m['id_modulo']})" for _, m in mods_grupo.iterrows()],
            help="As tabelas importadas neste passo ficarão vinculadas ao módulo selecionado."
        )
        id_modulo_sel = int(mod_label.split("id=")[-1].rstrip(")"))

        st.caption("Formato do Excel: colunas **TABELA | COLUNA | DESCRICAO**. "
                   "Inclua uma linha com **COLUNA = #** para descrever a tabela.")
        # ... (mantenha exatamente o teu bloco de parsing e import aqui, trocando id_modulo_sel se necessário)
        # DICA: no teu INSERT de tabela_catalogo, garanta usar 'id_modulo_sel' (selecionado acima).
        # O resto do teu código permanece igual.

        st.markdown("#### Tabelas já existentes neste módulo (Catálogo)")
        df_tabs_mod2 = carregar_tabelas_por_modulo(id_modulo_sel)
        if df_tabs_mod2.empty:
            st.caption("Nenhuma tabela cadastrada ainda para este módulo.")
        else:
            st.dataframe(
                df_tabs_mod2[["tabela_codigo", "titulo", "descricao", "sistema_origem"]],
                use_container_width=True,
                hide_index=True
            )

            tabs_del2 = st.multiselect(
                "Excluir tabelas (múltiplas) deste módulo",
                options=df_tabs_mod2["tabela_codigo"].tolist(),
                key="del_tabs_mod_import"
            )
            col_d1, col_d2 = st.columns([1.2, 1])
            confirm2 = col_d1.text_input(
                "Confirme digitando o CÓDIGO do módulo",
                placeholder=str(df_tabs_mod2.iloc[0]["modulo_codigo"])
            )
            ok2 = confirm2.strip().upper() == str(df_tabs_mod2.iloc[0]["modulo_codigo"]).strip().upper()

            if col_d2.button("🗑️ Excluir selecionadas (nesta seção)", type="secondary", disabled=(not tabs_del2)):
                if not ok2:
                    st.error("Confirmação inválida. Digite corretamente o CÓDIGO do módulo.")
                else:
                    try:
                        excluir_tabelas_catalogo(id_modulo_sel, tabs_del2)
                        st.success(f"{len(tabs_del2)} tabela(s) excluída(s).")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao excluir: {e}")


        up_cat = st.file_uploader("Selecione o Excel do Catálogo", type=["xlsx"], key="xls_cat_import")

        parsed = {}  # { TABELA: {desc: str, cols: [(col, desc), ...]} }

        if up_cat is not None:
            try:
                import pandas as _pd, io as _io, numpy as _np
                df_raw = _pd.read_excel(_io.BytesIO(up_cat.read()), header=None)
                # garante 3 colunas, expandindo se vier a mais
                df_raw = df_raw.iloc[:, :3].copy()
                df_raw.columns = ["TABELA", "COLUNA", "DESCRICAO"]

                # normalizações: strip e tratar NaN
                for c in ["TABELA","COLUNA","DESCRICAO"]:
                    df_raw[c] = df_raw[c].astype(str).fillna("").str.strip()

                # 1) Remover linhas totalmente vazias
                df_raw = df_raw[~((df_raw["TABELA"]=="") & (df_raw["COLUNA"]=="") & (df_raw["DESCRICAO"]==""))]

                # 2) Detectar e remover CABEÇALHO padrão (primeira linha = TABELA|COLUNA|DESCRICAO)
                if not df_raw.empty:
                    first = df_raw.iloc[0].astype(str).str.strip().str.upper()
                    if first.get("TABELA") == "TABELA" and first.get("COLUNA") == "COLUNA":
                        # é cabeçalho – remove
                        df_raw = df_raw.iloc[1:].copy()

                # 3) Segurança leve: tabela só alfa-num e underscore
                def is_valid_table(s: str) -> bool:
                    s = (s or "").strip()
                    return bool(s) and s.replace("_","").isalnum()

                # 4) Agrupar por TABELA
                for tab, g in df_raw.groupby(df_raw["TABELA"].astype(str).str.strip().str.upper()):
                    # ignorar “TABELA” (resquício de cabeçalho mal formado) e nomes inválidos
                    if tab in ("", "NAN", "NONE", "TABELA"):
                        continue
                    if not is_valid_table(tab):
                        continue

                    table_desc = ""
                    cols = []
                    for _, r in g.iterrows():
                        col = (r["COLUNA"] or "").strip()
                        desc = (r["DESCRICAO"] or "").strip()
                        # linha com '#': descrição da tabela
                        if col.startswith("#"):
                            if desc:
                                table_desc = desc
                            continue
                        # pular a palavra 'COLUNA' por segurança
                        if col.upper() in {"", "COLUNA", "#"}:
                            continue
                        cols.append((col.upper(), desc))

                    # só registra se existir ao menos 1 coluna
                    if cols:
                        parsed[tab] = {"desc": table_desc, "cols": cols}

                if not parsed:
                    st.warning("Nenhuma tabela válida detectada. Confira se o arquivo tem as colunas TABELA|COLUNA|DESCRICAO e se a primeira linha de fato é o cabeçalho.")
                else:
                    st.success(f"Detectadas {len(parsed)} tabela(s) no arquivo.")
                    for t, info in parsed.items():
                        with st.expander(f"Tabela: {t}  —  {len(info['cols'])} coluna(s)"):
                            st.write(f"**Descrição:** {info['desc'] or '(vazia)'}")
                            _dfc = _pd.DataFrame(info["cols"], columns=["COLUNA", "DESCRICAO"])
                            st.dataframe(_dfc, use_container_width=True)

            except Exception as e:
                st.error(f"Erro ao ler Excel do Catálogo: {e}")
                parsed = {}

                
        if parsed:
            if st.button("Importar/Atualizar no Catálogo"):
                try:
                    # Para cada TABELA do arquivo: UPSERT em tabela_catalogo e coluna_catalogo
                    for idx_tab, (tab_code, info) in enumerate(parsed.items(), start=1):
                        # 1) UPSERT da TABELA
                        exec_sql("""
                            INSERT INTO tabela_catalogo (id_modulo, tabela_codigo, titulo, descricao, sistema_origem)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (id_modulo, tabela_codigo) DO UPDATE
                              SET titulo=EXCLUDED.titulo,
                                  descricao=EXCLUDED.descricao,
                                  sistema_origem=EXCLUDED.sistema_origem
                        """, (id_modulo_sel, tab_code, tab_code, info["desc"] or "", "TOTVS"))

                        # 2) Descobrir id_tabela recém (ou já) existente
                        row = exec_sql("""
                            SELECT id_tabela FROM tabela_catalogo
                             WHERE id_modulo=%s AND tabela_codigo=%s
                        """, (id_modulo_sel, tab_code), fetch=True)
                        id_tabela_cat = int(row[0]["id_tabela"])

                        # 3) Colunas (ordem incremental a partir do arquivo)
                        for ordem, (col, desc) in enumerate(info["cols"], start=1):
                            exec_sql("""
                                INSERT INTO coluna_catalogo
                                    (id_tabela, coluna_nome, titulo, descricao, tipo_dado, eh_sensivel, nivel_pii, eh_pk, eh_nk, ordem)
                                VALUES (%s, %s, %s, %s, NULL, FALSE, NULL, FALSE, FALSE, %s)
                                ON CONFLICT (id_tabela, coluna_nome) DO UPDATE
                                   SET titulo=EXCLUDED.titulo,
                                       descricao=EXCLUDED.descricao,
                                       ordem=EXCLUDED.ordem
                            """, (id_tabela_cat, col, col, desc, ordem))

                    st.cache_data.clear()
                    st.success("Catálogo importado/atualizado com sucesso.")
                    time.sleep(3)
                    st.rerun() 
                except Exception as e:
                    st.error(f"Erro ao importar para o Catálogo: {e}")

elif menu == "Grupos de Empresas":
    if not u.get("is_admin"):
        st.error("Apenas administradores da aplicação podem gerenciar grupos de empresas.")
        st.stop()

    st.subheader("Grupos de Empresas")

    grupos = carregar_grupos()

    # ---------- ESTADO INICIAL ----------
    if "grupo_reset" not in st.session_state:
        st.session_state["grupo_reset"] = False
    if "grupo_id" not in st.session_state:
        st.session_state["grupo_id"] = None
    if "grupo_nome" not in st.session_state:
        st.session_state["grupo_nome"] = ""
    if "grupo_banco_dados" not in st.session_state:
        st.session_state["grupo_banco_dados"] = ""
    if "grupo_versao" not in st.session_state:
        st.session_state["grupo_versao"] = ""
    if "grupo_cnpj" not in st.session_state:
        st.session_state["grupo_cnpj"] = ""
    # seleção atual + última seleção (para detectar troca)
    if "grupo_select_current" not in st.session_state:
        st.session_state["grupo_select_current"] = ""
    if "grupo_select_prev" not in st.session_state:
        st.session_state["grupo_select_prev"] = ""
    # cópia original p/ detecção de alteração
    if "grupo_original" not in st.session_state:
        st.session_state["grupo_original"] = None

    with st.expander("Cadastrar / Editar Grupo de Empresa", expanded=True):

        # ---------- APLICA RESET ANTES DE CRIAR WIDGETS ----------
        if st.session_state["grupo_reset"]:
            st.session_state["grupo_id"] = None
            st.session_state["grupo_nome"] = ""
            st.session_state["grupo_banco_dados"] = ""
            st.session_state["grupo_versao"] = ""
            st.session_state["grupo_cnpj"] = ""
            st.session_state["grupo_select_current"] = ""
            st.session_state["grupo_select_prev"] = ""
            st.session_state["grupo_original"] = None
            st.session_state["grupo_reset"] = False

        # ---------- SELECT DO GRUPO (FORA DO FORM) ----------
        opcoes_grupo = [""] + grupos["nome_grupo_empresa"].tolist()

        # garante que o valor atual está entre as opções
        if st.session_state["grupo_select_current"] not in opcoes_grupo:
            st.session_state["grupo_select_current"] = ""

        grupo_selecionado = st.selectbox(
            "Selecione grupo para editar (ou deixe em branco para novo)",
            opcoes_grupo,
            key="grupo_select_current"  # quem manda é o session_state
        )

        # ---------- SE SELEÇÃO MUDOU, CARREGA CAMPOS ----------
        if st.session_state["grupo_select_prev"] != grupo_selecionado:
            if grupo_selecionado:
                # modo edição
                grupo_data = grupos[grupos["nome_grupo_empresa"] == grupo_selecionado].iloc[0]
                st.session_state["grupo_id"] = int(grupo_data["id_grupo_empresa"])
                st.session_state["grupo_nome"] = str(grupo_data["nome_grupo_empresa"])
                st.session_state["grupo_banco_dados"] = str(grupo_data.get("banco_dados", "") or "")
                st.session_state["grupo_versao"] = str(grupo_data.get("versao", "") or "")
                st.session_state["grupo_cnpj"] = str(grupo_data.get("cnpj_matriz", "") or "")

                st.session_state["grupo_original"] = {
                    "nome_grupo_empresa": st.session_state["grupo_nome"],
                    "banco_dados": st.session_state["grupo_banco_dados"],
                    "versao": st.session_state["grupo_versao"],
                    "cnpj_matriz": st.session_state["grupo_cnpj"],
                }
            else:
                # novo grupo
                st.session_state["grupo_id"] = None
                st.session_state["grupo_nome"] = ""
                st.session_state["grupo_banco_dados"] = ""
                st.session_state["grupo_versao"] = ""
                st.session_state["grupo_cnpj"] = ""
                st.session_state["grupo_original"] = None

            st.session_state["grupo_select_prev"] = grupo_selecionado

        # ---------- FORM (sem st.form, para permitir máscara) ----------
        nome = st.text_input("Nome do Grupo de Empresa", key="grupo_nome")
        banco_dados = st.text_input("Banco de Dados", key="grupo_banco_dados")
        versao = st.text_input("Versão", key="grupo_versao")

        cnpj_raw = st.text_input(
            "CNPJ Matriz",
            key="grupo_cnpj",
            on_change=mask_cnpj,
            args=("grupo_cnpj",),
        )

        submit_grupo = st.button("Salvar Grupo")

        # ---------- DETECÇÃO DE ALTERAÇÃO ----------
        id_grupo = st.session_state["grupo_id"]
        original = st.session_state.get("grupo_original")
        is_edit_mode = id_grupo is not None

        changed = True  # novo sempre pode salvar
        if is_edit_mode and original is not None:
            changed = any([
                (nome or "") != (original.get("nome_grupo_empresa") or ""),
                (banco_dados or "") != (original.get("banco_dados") or ""),
                (versao or "") != (original.get("versao") or ""),
                (cnpj_raw or "") != (original.get("cnpj_matriz") or ""),
            ])

        # ---------- LÓGICA DE SALVAR ----------
        if submit_grupo:
            if is_edit_mode and not changed:
                st.info("Nenhuma alteração detectada no grupo. Nada foi salvo.")
            else:
                if not nome:
                    st.error("Nome do grupo é obrigatório.")
                else:
                    cnpj_digits = only_digits(cnpj_raw)

                    if not cnpj_raw:
                        st.error("CNPJ da matriz é obrigatório.")
                    elif len(cnpj_digits) != 14:
                        st.error("CNPJ inválido. Informe um CNPJ com 14 dígitos.")
                    else:
                        nome_norm = nome.strip().lower()
                        conflitos = grupos[
                            grupos["nome_grupo_empresa"].str.strip().str.lower() == nome_norm
                        ]

                        if not conflitos.empty:
                            if id_grupo is None or int(conflitos.iloc[0]["id_grupo_empresa"]) != id_grupo:
                                st.error("Já existe um Grupo de Empresas com este nome.")
                                conflitos = pd.DataFrame()

                        if conflitos.empty:
                            try:
                                cnpj_to_save = format_cnpj(cnpj_raw)
                                salvar_grupo(id_grupo, nome, banco_dados, versao, cnpj_to_save)

                                st.session_state["grupo_reset"] = True
                                st.success("Grupo salvo com sucesso!")
                                st.rerun()
                            except Exception as e:
                                st.error("Não foi possível salvar o grupo. Verifique os dados e tente novamente.")
                                st.exception(e)

        # ---------- EXCLUSÃO EM CASCATA ----------
        id_grupo_atual = st.session_state["grupo_id"]
        nome_atual = st.session_state["grupo_nome"]

        if id_grupo_atual is not None:
            st.divider()
            st.error(
                "Excluir este GRUPO vai remover TODAS as EMPRESAS e MÓDULOS dele, "
                "além de perfis, usuários, permissões e Catálogo."
            )

            try:
                prev_conn = get_connection()
                prev = pd.read_sql("""
                    SELECT 
                      (SELECT COUNT(*) FROM empresa WHERE id_grupo_empresa=%s) AS qt_emp,
                      (SELECT COUNT(*) FROM modulo  WHERE id_grupo_empresa=%s) AS qt_mod
                """, prev_conn, params=(id_grupo_atual, id_grupo_atual))
                prev_conn.close()
                st.caption(
                    f"Prévia: {int(prev.iloc[0]['qt_emp'])} empresa(s) e "
                    f"{int(prev.iloc[0]['qt_mod'])} módulo(s) serão removidos."
                )
            except Exception:
                st.caption("Não foi possível carregar a prévia de dependências.")

            confirm_g = st.text_input(
                "Digite exatamente o NOME do grupo para confirmar a exclusão",
                value=""
            )
            if st.button("🗑️ Excluir Grupo (cascata)", type="secondary"):
                if confirm_g.strip() == nome_atual:
                    try:
                        deletar_grupo_cascata(id_grupo_atual)
                        st.success("Grupo excluído em cascata.")
                        st.session_state["grupo_reset"] = True
                        st.rerun()
                    except Exception:
                        st.error("Não foi possível excluir o grupo. Verifique dependências e tente novamente.")
                else:
                    st.error("Confirmação inválida. Digite o NOME do grupo exatamente igual.")

    st.markdown("---")
    st.dataframe(grupos, use_container_width=True)

elif menu == "Empresas":
    if not u.get("is_admin"):
        st.error("Apenas administradores da aplicação podem gerenciar empresas.")
        st.stop()

    st.subheader("Empresas")

    # carrega dados iniciais
    empresas = carregar_empresas()
    grupos = carregar_grupos()

    # =========================
    # Cadastrar / Editar Empresa
    # =========================
    with st.expander("Cadastrar / Editar Empresa", expanded=True):
        # estado inicial
        if "empresa_form_submitted" not in st.session_state:
            st.session_state["empresa_form_submitted"] = False
        
        id_empresa = None
        nome_default = ""
        cnpj_default = ""
        grupo_default = ""

        # Monta label com CNPJ para facilitar visual na EDIÇÃO
        empresas_label = empresas.copy()
        if not empresas_label.empty:
            empresas_label["label"] = empresas_label.apply(
                lambda r: (
                    f"{r['nome_empresa']} - {format_cnpj(str(r['cnpj']) if r['cnpj'] else '')}"
                    if r.get("cnpj")
                    else str(r["nome_empresa"])
                ),
                axis=1,
            )
        else:
            empresas_label["label"] = []

        # Seleção FORA do form
        opcoes_emp = [""] + empresas_label["label"].tolist()
        empresa_selecionada_label = st.selectbox(
            "Selecione empresa para editar (ou deixe em branco para novo)",
            opcoes_emp,
            key="empresa_select_outer"
        )

        if empresa_selecionada_label:
            # encontra o registro original pela label
            empresa_row = empresas_label[
                empresas_label["label"] == empresa_selecionada_label
            ].iloc[0]
            id_empresa = int(empresa_row["id_empresa"])
            nome_default = str(empresa_row["nome_empresa"])
            cnpj_default = str(empresa_row["cnpj"] or "")
            grupo_default = str(empresa_row["nome_grupo_empresa"])

        # Controle do CNPJ de empresa via session_state para permitir máscara
        if "empresa_cnpj" not in st.session_state:
            st.session_state["empresa_cnpj"] = cnpj_default or ""

        if "empresa_selected_label" not in st.session_state:
            st.session_state["empresa_selected_label"] = empresa_selecionada_label
        elif empresa_selecionada_label != st.session_state["empresa_selected_label"]:
            # mudou a empresa selecionada -> atualiza o CNPJ da sessão com o do registro
            st.session_state["empresa_selected_label"] = empresa_selecionada_label
            st.session_state["empresa_cnpj"] = cnpj_default or ""

        # ---------- ESTADO / NORMALIZAÇÃO DO CNPJ DA EMPRESA ----------
        # garante que sempre exista a chave no session_state
        if "empresa_cnpj" not in st.session_state:
            st.session_state["empresa_cnpj"] = cnpj_default or ""
        # se nenhuma empresa estiver selecionada (novo cadastro) e ainda não tiver valor, garante vazio
        if not empresa_selecionada_label and not st.session_state.get("empresa_cnpj"):
            st.session_state["empresa_cnpj"] = cnpj_default or ""

        # -------- FORM (sem st.form, para permitir máscara) --------
        nome_empresa = st.text_input("Nome da Empresa", value=nome_default)

        # Campo CNPJ com máscara automática
        cnpj_raw = st.text_input(
            "CNPJ",
            key="empresa_cnpj",
            on_change=mask_cnpj,
            args=("empresa_cnpj",),
        )

        # ---------- SELECTBOX DE GRUPO COM PLACEHOLDER ----------
        if not grupos.empty:
            lista_grupos = grupos["nome_grupo_empresa"].tolist()
            opcoes_grupos = ["Selecione um Grupo de Empresas"] + lista_grupos

            # Se estiver editando e houver grupo_default, deixa ele selecionado
            if grupo_default and grupo_default in lista_grupos:
                grupo_index = opcoes_grupos.index(grupo_default)
            else:
                # novo cadastro -> começa sempre no placeholder
                grupo_index = 0
        else:
            opcoes_grupos = ["Selecione um Grupo de Empresas"]
            grupo_index = 0

        grupo_nome = st.selectbox(
            "Grupo de Empresa",
            opcoes_grupos,
            index=grupo_index,
        )

        # Só define id_grupo_empresa se NÃO estiver no placeholder
        if not grupos.empty and grupo_nome != "Selecione um Grupo de Empresas":
            id_grupo_empresa = int(
                grupos[grupos["nome_grupo_empresa"] == grupo_nome]["id_grupo_empresa"].values[0]
            )
        else:
            id_grupo_empresa = None

        submit_empresa = st.button("Salvar Empresa")

        # -------- LÓGICA DE SALVAR (FORA DO FORM) --------
        if submit_empresa:
            if not nome_empresa:
                st.error("Nome da empresa é obrigatório.")
            elif id_grupo_empresa is None:
                st.error("Selecione um Grupo de Empresas válido.")
            else:
                # valida CNPJ se informado
                cnpj_digits = only_digits(cnpj_raw)
                if cnpj_raw and len(cnpj_digits) != 14:
                    st.error("CNPJ inválido. Informe um CNPJ com 14 dígitos.")
                else:
                    # checar duplicidade de nome (case-insensitive)
                    empresas_all = carregar_empresas()
                    nome_norm = nome_empresa.strip().lower()
                    conflitos = empresas_all[
                        empresas_all["nome_empresa"].str.strip().str.lower() == nome_norm
                    ]

                    if not conflitos.empty:
                        if id_empresa is None or int(conflitos.iloc[0]["id_empresa"]) != id_empresa:
                            st.error("Já existe uma Empresa com este nome cadastrada.")
                            conflitos = pd.DataFrame()  # força a não salvar
                        else:
                            conflitos = pd.DataFrame()  # permite atualizar a mesma empresa

                    if conflitos.empty and (not conflitos.empty or id_empresa is not None or nome_norm not in [e.lower() for e in empresas_all["nome_empresa"].str.strip()]):
                        cnpj_to_save = format_cnpj(cnpj_raw) if cnpj_digits else None
                        
                        try:
                            salvar_empresa(id_empresa, id_grupo_empresa, nome_empresa, cnpj_to_save)
                            st.success("✅ Empresa salva com sucesso!")
                            st.cache_data.clear()
                            time.sleep(3)
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Erro ao salvar: {str(e)}")

        # --- Exclusão em CASCATA da Empresa ---
        if id_empresa is not None:
            st.divider()
            st.error("⚠️ Excluir esta EMPRESA vai remover perfis, usuários, permissões e vínculos relacionados a ela.")
            
            try:
                prev_conn = get_connection()
                prev = pd.read_sql("""
                    SELECT
                      (SELECT COUNT(*) FROM perfil WHERE id_empresa=%s) AS qt_perfis,
                      (SELECT COUNT(*) FROM usuario u JOIN perfil p ON u.id_perfil = p.id_perfil WHERE p.id_empresa=%s) AS qt_usuarios,
                      (SELECT COUNT(*) FROM permissao pr JOIN perfil p2 ON pr.id_perfil = p2.id_perfil WHERE p2.id_empresa=%s) AS qt_perms
                """, prev_conn, params=(id_empresa, id_empresa, id_empresa))
                prev_conn.close()
                
                st.caption(
                    f"Prévia: {int(prev.iloc[0]['qt_perfis'])} perfil(s), "
                    f"{int(prev.iloc[0]['qt_usuarios'])} usuário(s) e "
                    f"{int(prev.iloc[0]['qt_perms'])} permissão(ões) serão removidos."
                )
            except Exception:
                st.caption("Não foi possível carregar a prévia de dependências.")

            with st.form("form_delete_empresa"):
                confirm_e = st.text_input(
                    "Digite exatamente o NOME da empresa para confirmar a exclusão",
                    value=""
                )
                delete_btn = st.form_submit_button("🗑️ Excluir Empresa (cascata)", type="secondary")
                
            if delete_btn:
                if confirm_e.strip() == nome_default:
                    try:
                        deletar_empresa_cascata(id_empresa)
                        st.success("✅ Empresa excluída em cascata.")
                        st.cache_data.clear()
                        time.sleep(3)
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro ao excluir: {str(e)}")
                else:
                    st.error("❌ Confirmação inválida. Digite o NOME da empresa exatamente igual.")

    # =========================
    # Filtro + Tabela de Empresas
    # =========================
    st.markdown("---")

    if empresas.empty:
        st.info("Nenhuma empresa cadastrada ainda.")
    else:
        opcoes_filtro = ["Todas as empresas"] + grupos["nome_grupo_empresa"].tolist()
        filtro_grupo = st.selectbox(
            "Filtrar tabela por Grupo de Empresas", opcoes_filtro
        )

        if filtro_grupo == "Todas as empresas":
            empresas_view = empresas.copy()
        else:
            empresas_view = empresas[empresas["nome_grupo_empresa"] == filtro_grupo].copy()

        st.dataframe(empresas_view, use_container_width=True)

elif menu == "Perfis":
    st.subheader("Perfis de Usuário")

    u = st.session_state["usuario_portal"]
    grupos_ids = st.session_state.get("grupos_usuario_ids", [])

    # Carrega perfis e empresas
    perfis = carregar_perfis()          # já traz id_grupo_empresa (via JOIN empresa)
    empresas = carregar_empresas()      # também tem id_grupo_empresa

    # 🔒 RLS: se não for admin, só vê coisas dos seus grupos
    if not u.get("is_admin"):
        if grupos_ids:
            perfis = perfis[perfis["id_grupo_empresa"].isin(grupos_ids)]
            empresas = empresas[empresas["id_grupo_empresa"].isin(grupos_ids)]
        else:
            st.warning("Você não está vinculado a nenhum Grupo de Empresas.")
            st.stop()

    # Monta label com CNPJ para as empresas (Ex: "Minha Empresa LTDA - 00.000.000/0000-00")
    empresas_pf = empresas.copy()
    empresas_pf["label"] = empresas_pf.apply(
        lambda r: (
            f"{r['nome_empresa']} - {format_cnpj(str(r['cnpj']) if r['cnpj'] else '')}"
            if r.get("cnpj") else str(r["nome_empresa"])
        ),
        axis=1
    )
    # alias usado depois
    empresas_us = empresas_pf

    # Estado do formulário de Perfil
    if "perfil_select" not in st.session_state:
        st.session_state["perfil_select"] = ""
    if "perfil_descricao" not in st.session_state:
        st.session_state["perfil_descricao"] = ""
    if "perfil_empresa_label" not in st.session_state:
        st.session_state["perfil_empresa_label"] = ""
    if "perfil_reset" not in st.session_state:
        st.session_state["perfil_reset"] = False

    # se no ciclo anterior mandamos resetar, limpa AGORA (antes dos widgets)
    if st.session_state["perfil_reset"]:
        st.session_state["perfil_descricao"] = ""
        st.session_state["perfil_empresa_label"] = ""
        st.session_state["perfil_reset"] = False

    with st.expander("Cadastrar / Editar Perfil", expanded=True):
        id_perfil = None
        perfil_descricao_atual = ""

        # SELECTBOX FORA DO FORM (selecionar perfil existente)
        perfis_fresh = perfis.copy()  # ⚠️ usa a versão JÁ FILTRADA
        if not perfis_fresh.empty:
            perfis_fresh["label_perfil"] = perfis_fresh["nome_empresa"] + " - " + perfis_fresh["descricao"]
            perfil_opcoes = [""] + perfis_fresh["label_perfil"].tolist()
        else:
            perfis_fresh["label_perfil"] = []
            perfil_opcoes = [""]

        perfil_selecionado = st.selectbox(
            "Selecione perfil para editar (ou deixe em branco para novo)",
            perfil_opcoes,
            index=0
        )

        if perfil_selecionado:
            perfil_data = perfis_fresh[perfis_fresh["label_perfil"] == perfil_selecionado].iloc[0]
            id_perfil = int(perfil_data["id_perfil"])
            perfil_descricao_atual = str(perfil_data["descricao"])
            st.session_state["perfil_descricao"] = perfil_descricao_atual

            # empresa atual do perfil -> converte para label com CNPJ
            emp_atual_nome = str(perfil_data["nome_empresa"])
            match_emp = empresas_pf[empresas_pf["nome_empresa"] == emp_atual_nome]
            if not match_emp.empty:
                st.session_state["perfil_empresa_label"] = match_emp["label"].iloc[0]
        else:
            id_perfil = None

        # FORM DE CADASTRO/EDIÇÃO
        with st.form("form_perfil"):
            # Escolha da empresa (com nome + CNPJ) + placeholder
            empresa_labels = empresas_pf["label"].tolist()
            empresa_row = None
            id_empresa_sel = None
            id_grupo_empresa = None

            if empresa_labels:
                opcoes_empresas = ["Selecione uma empresa"] + empresa_labels

                current_label = st.session_state.get("_usuario_empresa_label", "")
                if current_label in empresa_labels:
                    idx_empresa = opcoes_empresas.index(current_label)
                else:
                    idx_empresa = 0  # placeholder

                empresa_sel_op = st.selectbox(
                    "Empresa Principal",
                    opcoes_empresas,
                    index=idx_empresa,
                    key="_usuario_empresa_label"
                )

                if empresa_sel_op != "Selecione uma empresa":
                    empresa_row = empresas_us[empresas_us["label"] == empresa_sel_op].iloc[0]
                    id_empresa_sel = int(empresa_row["id_empresa"])
                    empresa_nome_sel = str(empresa_row["nome_empresa"])
                    id_grupo_empresa = int(empresa_row["id_grupo_empresa"])
                else:
                    empresa_row = None
                    id_empresa_sel = None
                    empresa_nome_sel = None
                    id_grupo_empresa = None
            else:
                st.warning("Não há empresas cadastradas. Cadastre uma empresa primeiro.")

            descricao = st.text_input("Descrição do Perfil", key="perfil_descricao")

            submit_perfil = st.form_submit_button("Salvar Perfil")

            if submit_perfil:
                if not descricao:
                    st.error("❌ Descrição do perfil é obrigatória.")
                elif id_empresa_sel is None:
                    st.error("❌ Selecione uma empresa válida.")
                else:
                    try:
                        perfis_all = carregar_perfis()

                        # 🔒 RLS também na validação de conflito:
                        if not u.get("is_admin") and grupos_ids:
                            perfis_all = perfis_all[perfis_all["id_grupo_empresa"].isin(grupos_ids)]

                        desc_norm = descricao.strip().lower()
                        conflitos = perfis_all[
                            (perfis_all["id_empresa"] == id_empresa_sel) &
                            (perfis_all["descricao"].str.strip().str.lower() == desc_norm)
                        ]

                        if not conflitos.empty:
                            if id_perfil is None or int(conflitos.iloc[0]["id_perfil"]) != id_perfil:
                                st.error(f"❌ Já existe um perfil '{descricao}' para esta empresa.")
                            else:
                                conflitos = pd.DataFrame()

                        if conflitos.empty:
                            # (opcional) segurança extra: não-admin não pode salvar perfil de outro grupo
                            if (not u.get("is_admin")) and (id_grupo_empresa not in grupos_ids):
                                st.error("❌ Você não pode criar/alterar perfil de outra empresa/grupo.")
                            else:
                                salvar_perfil(id_perfil, id_empresa_sel, descricao)

                                st.session_state["perfil_select"] = ""
                                st.session_state["perfil_reset"] = True

                                if id_perfil is None:
                                    st.success(f"✅ Perfil '{descricao}' criado com sucesso!")
                                else:
                                    st.success(f"✅ Perfil '{descricao}' atualizado com sucesso!")

                                time.sleep(1)
                                st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro ao salvar perfil: {e}")

        # -------- ZONA DE PERIGO: EXCLUIR PERFIL --------
        if id_perfil is not None:
            st.divider()
            st.error(f"⚠️ **Zona de perigo:** Excluir o perfil '{perfil_descricao_atual}'")
            
            try:
                conn = get_connection()
                cur = conn.cursor()
                
                cur.execute("SELECT COUNT(*) FROM usuario WHERE id_perfil = %s", (id_perfil,))
                qt_usuarios = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM permissao WHERE id_perfil = %s", (id_perfil,))
                qt_permissoes = cur.fetchone()[0]
                
                cur.close()
                conn.close()
                
                st.caption(
                    f"⚠️ Este perfil possui **{qt_usuarios} usuário(s)** e **{qt_permissoes} permissão(ões)** vinculadas."
                )
                
                if qt_usuarios > 0:
                    st.warning(
                        f"❌ **Não é possível excluir este perfil!** "
                        f"Existem {qt_usuarios} usuário(s) usando este perfil. "
                        f"Reatribua os usuários a outro perfil antes de excluir."
                    )
                else:
                    with st.form("form_delete_perfil"):
                        st.warning(
                            f"⚠️ **Atenção:** Esta ação irá excluir:\n"
                            f"- O perfil '{perfil_descricao_atual}'\n"
                            f"- {qt_permissoes} permissão(ões) associadas\n\n"
                            f"**Esta ação é irreversível!**"
                        )
                        
                        confirm_text = st.text_input(
                            "Digite exatamente a DESCRIÇÃO do perfil para confirmar",
                            placeholder=perfil_descricao_atual
                        )
                        
                        delete_btn = st.form_submit_button("🗑️ Excluir Perfil", type="secondary")
                        
                    if delete_btn:
                        if confirm_text.strip() == perfil_descricao_atual.strip():
                            try:
                                # (opcional) segurança extra: se quiser, aqui você também pode bloquear não-admin
                                deletar_perfil(id_perfil)
                                st.success(f"✅ Perfil '{perfil_descricao_atual}' excluído com sucesso!")

                                st.session_state["perfil_select"] = ""
                                st.session_state["perfil_reset"] = True

                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Erro ao excluir perfil: {e}")
                        else:
                            st.error("❌ Confirmação inválida. Digite a DESCRIÇÃO exatamente igual.")
            except Exception as e:
                st.error(f"❌ Erro ao verificar dependências: {e}")

    st.markdown("---")
    # Exibe a lista de perfis (respeitando o filtro por grupo)
    perfis_display = perfis.copy()
    if "label_perfil" in perfis_display.columns:
        perfis_display = perfis_display.drop(columns=["label_perfil"])
    st.dataframe(perfis_display, use_container_width=True)

# Tabelas
elif menu == "Tabelas":
    st.subheader("Catálogo de Tabelas por Grupo / Módulo")

    if u.get("is_admin"):
        grupos = carregar_grupos()
    else:
        grupos = carregar_grupos_usuario_portal(u["id_usuario"])
        if grupos.empty:
            st.error("Você não está vinculado a nenhum Grupo de Empresas.")
            st.stop()

    grupo_label = st.selectbox(
        "Selecione o Grupo de Empresas",
        options=[f"{g['nome_grupo_empresa']} (id={g['id_grupo_empresa']})" for _, g in grupos.iterrows()]
    )
    id_grupo_sel = int(grupo_label.split("id=")[-1].rstrip(")"))

    df_tabs = carregar_tabelas_catalogo(id_grupo_sel)
    if grupos.empty:
        st.info("Cadastre um Grupo de Empresas.")
        st.stop()

    grupo_label = st.selectbox(
        "Grupo de Empresas",
        options=[f"{g['nome_grupo_empresa']} (id={g['id_grupo_empresa']})" for _, g in grupos.iterrows()]
    )
    id_grupo_sel = int(grupo_label.split("id=")[-1].rstrip(")"))

    df_tabs_cat = carregar_tabelas_catalogo(id_grupo_sel)
    if df_tabs_cat.empty:
        st.warning("Este grupo ainda não possui tabelas no Catálogo.")
    else:
        tabela_sel = st.selectbox(
            "Selecione a Tabela (Catálogo)",
            options=sorted(df_tabs_cat["tabela_codigo"].unique().tolist())
        )
        id_tabela_cat = int(df_tabs_cat[df_tabs_cat["tabela_codigo"] == tabela_sel].iloc[0]["id_tabela"])

        df_cols = carregar_colunas_catalogo(id_tabela_cat)
                
        if df_cols.empty:
            st.info("Esta tabela ainda não tem colunas no Catálogo.")
        else:
            st.markdown(f"**Colunas da tabela {tabela_sel}**")

            # ---- MOSTRAR SÓ O QUE INTERESSA ----
            # mantém: coluna_nome, titulo, descricao, tipo_dado, eh_sensivel, ordem
            # oculta: eh_pk, eh_nk, nivel_pii (e quaisquer outras)
            cols_to_show = ["coluna_nome", "titulo", "descricao", "tipo_dado", "eh_sensivel", "ordem"]
            cols_existentes = [c for c in cols_to_show if c in df_cols.columns]

            df_view = df_cols.loc[:, cols_existentes].copy()

            # booleano mais amigável
            if "eh_sensivel" in df_view.columns:
                df_view["eh_sensivel"] = df_view["eh_sensivel"].map({True: "Sim", False: "Não"})

            # rótulos legíveis
            df_view = df_view.rename(columns={
                "coluna_nome": "Coluna",
                "titulo": "Título",
                "descricao": "Descrição",
                "tipo_dado": "Tipo de dado",
                "eh_sensivel": "É sensível?",
                "ordem": "Ordem"
            })

            st.dataframe(df_view, use_container_width=True, hide_index=True)
            with st.expander("✏️ Editar metadados das colunas desta tabela"):
                # Usamos df_cols original para preservar tipos
                cols_edit = ["coluna_nome", "titulo", "descricao", "tipo_dado", "eh_sensivel", "ordem"]
                cols_edit_exist = [c for c in cols_edit if c in df_cols.columns]
                df_edit = df_cols.loc[:, cols_edit_exist].copy()

                st.write("Altere os campos abaixo e clique em **Salvar alterações** para atualizar o Catálogo.")
                df_editado = st.data_editor(
                    df_edit,
                    num_rows="fixed",
                    use_container_width=True,
                    hide_index=True,
                    disabled=["coluna_nome"],  # preserva o nome técnico da coluna
                    key=f"edit_cols_{id_tabela_cat}"
                )

                if st.button("Salvar alterações das colunas", key=f"btn_salvar_cols_{id_tabela_cat}"):
                    try:
                        conn_ed = get_connection()
                        cur_ed = conn_ed.cursor()

                        for _, row_ed in df_editado.iterrows():
                            cur_ed.execute("""
                                UPDATE coluna_catalogo
                                   SET titulo      = %s,
                                       descricao   = %s,
                                       tipo_dado   = %s,
                                       eh_sensivel = %s,
                                       ordem       = %s
                                 WHERE id_tabela   = %s
                                   AND coluna_nome = %s
                            """, (
                                row_ed.get("titulo"),
                                row_ed.get("descricao"),
                                row_ed.get("tipo_dado"),
                                bool(row_ed.get("eh_sensivel")) if "eh_sensivel" in row_ed else False,
                                int(row_ed.get("ordem")) if not pd.isna(row_ed.get("ordem")) else None,
                                id_tabela_cat,
                                row_ed.get("coluna_nome")
                            ))

                        conn_ed.commit()
                        cur_ed.close(); conn_ed.close()

                        st.cache_data.clear()
                        st.success("Metadados das colunas atualizados com sucesso.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao atualizar colunas: {e}")

    st.caption("Edição de Tabelas/Colunas deve ser feita no fluxo de Catálogo (fora desta tela).")

elif menu == "Usuários - Aplicação":
    st.subheader("Usuários")

    usuarios = carregar_usuarios()
    empresas = carregar_empresas()
    perfis = carregar_perfis()
    grupos = carregar_grupos()

    usuarios = carregar_usuarios()

    if not u.get("is_admin"):
        grupos_ids = st.session_state.get("grupos_usuario_ids", [])
        usuarios = usuarios[usuarios["id_grupo_empresa"].isin(grupos_ids)]


    # Monta label com CNPJ para empresas
    empresas_us = empresas.copy()
    if not empresas_us.empty:
        empresas_us["label"] = empresas_us.apply(
            lambda r: (
                f"{r['nome_empresa']} - {format_cnpj(str(r['cnpj']) if r['cnpj'] else '')}"
                if r.get("cnpj") else str(r["nome_empresa"])
            ),
            axis=1,
        )
    else:
        empresas_us["label"] = []

    # Estado do formulário de usuário
    if "_usuario_id_editing" not in st.session_state:
        st.session_state["_usuario_id_editing"] = None
    if "_usuario_nome" not in st.session_state:
        st.session_state["_usuario_nome"] = ""
    if "_usuario_senha" not in st.session_state:
        st.session_state["_usuario_senha"] = ""
    if "_usuario_is_admin" not in st.session_state:
        st.session_state["_usuario_is_admin"] = False
    # usamos isso só como memória nossa (NÃO é key de widget)
    if "_usuario_empresa_label" not in st.session_state:
        st.session_state["_usuario_empresa_label"] = ""
    if "_usuario_perfil" not in st.session_state:
        st.session_state["_usuario_perfil"] = ""
    if "_usuario_portal_acesso" not in st.session_state:
        st.session_state["_usuario_portal_acesso"] = False
    if "_usuario_portal_is_admin" not in st.session_state:
        st.session_state["_usuario_portal_is_admin"] = False
    if "_usuario_empresas_multiselect" not in st.session_state:
        st.session_state["_usuario_empresas_multiselect"] = []

    with st.expander("Cadastrar / Editar Usuário", expanded=True):
        id_usuario = None

        # Monta opções com ID para evitar ambiguidade
        usuarios_opts = usuarios.copy()
        if not usuarios_opts.empty:
            usuarios_opts["label"] = usuarios_opts.apply(
                lambda r: f"{r['id_usuario']} - {r['nome']}",
                axis=1,
            )
            usuario_opcoes = [""] + usuarios_opts["label"].tolist()
        else:
            usuarios_opts["label"] = []
            usuario_opcoes = [""]

        usuario_selecionado = st.selectbox(
            "Selecione usuário para editar (ou deixe em branco para novo)",
            usuario_opcoes,
            index=0
        )

        # ====== CARREGA DADOS DO USUÁRIO SELECIONADO ======
        if usuario_selecionado:
            try:
                id_usuario = int(usuario_selecionado.split(" - ", 1)[0])
            except Exception:
                id_usuario = None

            if id_usuario is not None and id_usuario != st.session_state["_usuario_id_editing"]:
                usuario_data = usuarios[usuarios["id_usuario"] == id_usuario].iloc[0]

                st.session_state["_usuario_id_editing"] = id_usuario
                st.session_state["_usuario_nome"] = str(usuario_data["nome"])
                st.session_state["_usuario_senha"] = str(usuario_data["senha"])
                st.session_state["_usuario_is_admin"] = bool(usuario_data["is_admin"])

                # empresa atual do usuário -> label com CNPJ
                emp_nome_atual = str(usuario_data["nome_empresa"])
                match_emp = empresas_us[empresas_us["nome_empresa"] == emp_nome_atual]
                if not match_emp.empty:
                    st.session_state["_usuario_empresa_label"] = match_emp["label"].iloc[0]
                else:
                    st.session_state["_usuario_empresa_label"] = ""

                # perfil atual
                st.session_state["_usuario_perfil"] = str(usuario_data["perfil"])

                # empresas vinculadas
                df_emp_usr = carregar_empresas_usuario(id_usuario)
                if not df_emp_usr.empty:
                    empresas_usr_labels = []
                    for _, emp_usr_row in df_emp_usr.iterrows():
                        match = empresas_us[empresas_us["id_empresa"] == emp_usr_row["id_empresa"]]
                        if not match.empty:
                            empresas_usr_labels.append(match["label"].iloc[0])
                    st.session_state["_usuario_empresas_multiselect"] = empresas_usr_labels
                else:
                    st.session_state["_usuario_empresas_multiselect"] = []

                # info portal
                info_portal = carregar_portal_info_por_nome(str(usuario_data["nome"]))
                if info_portal:
                    st.session_state["_usuario_portal_acesso"] = bool(info_portal.get("ativo", False))
                    st.session_state["_usuario_portal_is_admin"] = bool(info_portal.get("is_admin", False))
                else:
                    st.session_state["_usuario_portal_acesso"] = False
                    st.session_state["_usuario_portal_is_admin"] = False
        else:
            # novo usuário - limpa campos
            if st.session_state["_usuario_id_editing"] is not None:
                st.session_state["_usuario_id_editing"] = None
                st.session_state["_usuario_nome"] = ""
                st.session_state["_usuario_senha"] = ""
                st.session_state["_usuario_is_admin"] = False
                st.session_state["_usuario_empresa_label"] = ""
                st.session_state["_usuario_perfil"] = ""
                st.session_state["_usuario_portal_acesso"] = False
                st.session_state["_usuario_portal_is_admin"] = False
                st.session_state["_usuario_empresas_multiselect"] = []

        id_usuario = st.session_state["_usuario_id_editing"]

        # ====== EMPRESA PRINCIPAL (FORA DO FORM) ======
        empresa_labels = empresas_us["label"].tolist()
        id_empresa_sel = None
        empresa_nome_sel = None
        id_grupo_empresa = None
        empresa_row = None

        if empresa_labels:
            opcoes_empresas = ["Selecione uma empresa"] + empresa_labels

            current_label = st.session_state.get("_usuario_empresa_label", "")
            if current_label in empresa_labels:
                idx_empresa = opcoes_empresas.index(current_label)
            else:
                idx_empresa = 0  # placeholder

            # SEM key aqui → _usuario_empresa_label não é key de widget
            empresa_sel_op = st.selectbox(
                "Empresa Principal",
                opcoes_empresas,
                index=idx_empresa,
            )

            # atualiza nosso estado manualmente (agora pode)
            st.session_state["_usuario_empresa_label"] = (
                empresa_sel_op if empresa_sel_op != "Selecione uma empresa" else ""
            )

            if empresa_sel_op != "Selecione uma empresa":
                empresa_row_df = empresas_us[empresas_us["label"] == empresa_sel_op]
                if not empresa_row_df.empty:
                    empresa_row = empresa_row_df.iloc[0]
                    id_empresa_sel = int(empresa_row["id_empresa"])
                    empresa_nome_sel = str(empresa_row["nome_empresa"])
                    id_grupo_empresa = int(empresa_row["id_grupo_empresa"])
            else:
                empresa_row = None
                id_empresa_sel = None
                empresa_nome_sel = None
                id_grupo_empresa = None
        else:
            st.warning("Não há empresas cadastradas. Cadastre uma empresa primeiro.")

        # ====== FORM USUÁRIO ======
        with st.form("form_usuario", clear_on_submit=False):
            nome = st.text_input("Nome", value=st.session_state["_usuario_nome"])
            senha = st.text_input("Senha", type="password", value=st.session_state["_usuario_senha"])

            if id_empresa_sel is None:
                st.warning("Selecione uma empresa principal acima.")
            else:
                st.caption(f"Empresa principal selecionada: **{empresa_nome_sel}**")

            # PERFIL (filtrado pela empresa)
            id_perfil_sel = None
            perfil_desc = None
            if id_empresa_sel is not None:
                perfis_empresa = perfis[perfis["id_empresa"] == id_empresa_sel].copy()
                if not perfis_empresa.empty:
                    perfil_opcoes = perfis_empresa["descricao"].tolist()
                    perfil_index = 0
                    if st.session_state["_usuario_perfil"] in perfil_opcoes:
                        perfil_index = perfil_opcoes.index(st.session_state["_usuario_perfil"])
                    
                    perfil_desc = st.selectbox(
                        "Perfil",
                        perfil_opcoes,
                        index=perfil_index
                    )
                    
                    perfil_match = perfis_empresa[perfis_empresa["descricao"] == perfil_desc]
                    if not perfil_match.empty:
                        id_perfil_sel = int(perfil_match["id_perfil"].iloc[0])
                else:
                    st.warning("Esta empresa não possui perfis cadastrados.")
            else:
                st.info("Selecione uma empresa para escolher o perfil.")

            # MÚLTIPLAS EMPRESAS (do mesmo grupo)
            empresas_multiselect = []
            if id_grupo_empresa is not None:
                empresas_grupo = empresas_us[empresas_us["id_grupo_empresa"] == id_grupo_empresa]
                empresas_grupo_labels = empresas_grupo["label"].tolist()
                
                empresas_multiselect = st.multiselect(
                    "Empresas vinculadas (mesmo grupo)",
                    options=empresas_grupo_labels,
                    default=st.session_state["_usuario_empresas_multiselect"],
                    help="Selecione as empresas às quais este usuário terá acesso"
                )

            # ADMINISTRADOR
            is_admin = st.checkbox(
                "Administrador da aplicação",
                value=st.session_state["_usuario_is_admin"]
            )

            # ACESSO AO PORTAL
            st.markdown("---")
            st.markdown("**Acesso ao Portal de Permissionamento**")
            
            tem_acesso_portal = st.checkbox(
                "Permitir acesso ao portal",
                value=st.session_state["_usuario_portal_acesso"],
                help="Permite que o usuário faça login no portal de permissionamento"
            )
            
            portal_is_admin = False
            if tem_acesso_portal:
                portal_is_admin = st.checkbox(
                    "Administrador do portal",
                    value=st.session_state["_usuario_portal_is_admin"],
                    help="Concede privilégios administrativos: gerenciar grupos, empresas, módulos, etc."
                )
                
                if portal_is_admin:
                    st.info("🔑 Este usuário terá **privilégios administrativos totais** no portal.")
                else:
                    st.info("👤 Este usuário terá acesso **restrito** ao portal (apenas consultas permitidas pelo perfil).")
            else:
                st.caption("ℹ️ Marque 'Permitir acesso ao portal' para definir privilégios administrativos.")

            submit_usuario = st.form_submit_button("Salvar Usuário")

            # ====== LÓGICA DE SALVAR ======
            if submit_usuario:
                if not nome or not senha:
                    st.error("❌ **Erro:** Nome e senha são obrigatórios.")
                elif id_empresa_sel is None:
                    st.error("❌ **Erro:** Selecione uma empresa válida.")
                elif id_perfil_sel is None:
                    st.error("❌ **Erro:** Selecione um perfil válido.")
                else:
                    nome_norm = (nome or "").strip().lower()
                    duplicado = usuarios[usuarios["nome"].str.strip().str.lower() == nome_norm]
                    if id_usuario is not None:
                        duplicado = duplicado[duplicado["id_usuario"] != id_usuario]

                    if not duplicado.empty:
                        st.error(f"❌ **Erro:** Já existe um usuário com o nome '{nome}'. Escolha outro nome.")
                    else:
                        try:
                            ids_empresas_mapear = []
                            for lab in empresas_multiselect:
                                row_emp = empresas_us[empresas_us["label"] == lab]
                                if not row_emp.empty:
                                    ids_empresas_mapear.append(int(row_emp.iloc[0]["id_empresa"]))

                            if not ids_empresas_mapear and id_empresa_sel is not None:
                                ids_empresas_mapear.append(int(id_empresa_sel))

                            salvar_usuario(
                                id_usuario,
                                nome,
                                senha,
                                id_perfil_sel,
                                is_admin,
                                ids_empresas_para_mapear=ids_empresas_mapear,
                                tem_acesso_portal=tem_acesso_portal,
                                portal_is_admin=portal_is_admin
                            )

                            if id_usuario is None:
                                st.success(f"✅ **Sucesso!** Usuário '{nome}' cadastrado com sucesso!")
                            else:
                                st.success(f"✅ **Sucesso!** Usuário '{nome}' atualizado com sucesso!")
                            
                            if tem_acesso_portal:
                                if portal_is_admin:
                                    st.info(
                                        "🔑 **Acesso ao portal:** ADMINISTRADOR com privilégios totais "
                                        "(pode gerenciar grupos, empresas, módulos, usuários, etc.)"
                                    )
                                else:
                                    st.info(
                                        f"👤 **Acesso ao portal:** Usuário comum com permissões restritas ao perfil '{perfil_desc}'"
                                        if perfil_desc else
                                        "👤 **Acesso ao portal:** Usuário comum com permissões restritas."
                                    )
                            else:
                                st.warning(
                                    "🚫 **Sem acesso ao portal:** Este usuário não poderá fazer login no portal de permissionamento"
                                )

                            # Reset total
                            st.session_state["_usuario_id_editing"] = None
                            st.session_state["_usuario_nome"] = ""
                            st.session_state["_usuario_senha"] = ""
                            st.session_state["_usuario_is_admin"] = False
                            st.session_state["_usuario_empresa_label"] = ""
                            st.session_state["_usuario_perfil"] = ""
                            st.session_state["_usuario_portal_acesso"] = False
                            st.session_state["_usuario_portal_is_admin"] = False
                            st.session_state["_usuario_empresas_multiselect"] = []
                            
                            time.sleep(3)
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ **Erro ao salvar usuário:** {str(e)}")
                            st.exception(e)

        # -------- Zona de perigo: excluir usuário selecionado --------
        if id_usuario is not None:
            st.divider()
            st.error(f"⚠️ **Zona de perigo:** Excluir o usuário '{st.session_state['_usuario_nome']}'")

            with st.form("form_delete_usuario"):
                confirm_text = st.text_input(
                    "Digite exatamente o NOME do usuário para confirmar a exclusão"
                )
                delete_btn = st.form_submit_button("🗑️ Excluir Usuário", type="secondary")
                
            if delete_btn:
                if confirm_text.strip() == st.session_state["_usuario_nome"].strip():
                    try:
                        nome_excluido = st.session_state["_usuario_nome"]
                        deletar_usuario(id_usuario)
                        
                        st.session_state["_usuario_id_editing"] = None
                        st.session_state["_usuario_nome"] = ""
                        st.session_state["_usuario_senha"] = ""
                        st.session_state["_usuario_is_admin"] = False
                        st.session_state["_usuario_empresa_label"] = ""
                        st.session_state["_usuario_perfil"] = ""
                        st.session_state["_usuario_portal_acesso"] = False
                        st.session_state["_usuario_portal_is_admin"] = False
                        st.session_state["_usuario_empresas_multiselect"] = []
                        
                        st.success(f"✅ Usuário '{nome_excluido}' excluído com sucesso.")
                        time.sleep(3)
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro ao excluir usuário: {str(e)}")
                        st.exception(e)
                else:
                    st.error("❌ Confirmação inválida. Digite o NOME exatamente igual.")

    # ====== FILTROS E TABELA DE USUÁRIOS ======
    st.markdown("---")
    st.markdown("### 📋 Lista de Usuários")
    
    usuarios_view = usuarios.copy()
    usuarios_view = usuarios_view.merge(
        empresas[['id_empresa', 'cnpj', 'id_grupo_empresa']], 
        on='id_empresa', 
        how='left'
    )
    
    usuarios_view['empresa_cnpj'] = usuarios_view.apply(
        lambda r: f"{r['nome_empresa']} - {format_cnpj(str(r['cnpj']) if r['cnpj'] else '')}" 
        if r.get('cnpj') else str(r['nome_empresa']),
        axis=1
    )
    
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        if not grupos.empty:
            opcoes_grupo = ["Todos os grupos"] + grupos['nome_grupo_empresa'].tolist()
            filtro_grupo = st.selectbox("🏢 Filtrar por Grupo", opcoes_grupo)
            
            if filtro_grupo != "Todos os grupos":
                id_grupo_filtro = int(grupos[grupos['nome_grupo_empresa'] == filtro_grupo]['id_grupo_empresa'].iloc[0])
                usuarios_view = usuarios_view[usuarios_view['id_grupo_empresa'] == id_grupo_filtro]
    
    with col_f2:
        empresas_disponiveis = usuarios_view['empresa_cnpj'].unique().tolist()
        opcoes_empresa = ["Todas as empresas"] + sorted(empresas_disponiveis)
        filtro_empresa = st.selectbox("🏭 Filtrar por Empresa", opcoes_empresa)
        
        if filtro_empresa != "Todas as empresas":
            usuarios_view = usuarios_view[usuarios_view['empresa_cnpj'] == filtro_empresa]
    
    with col_f3:
        perfis_disponiveis = usuarios_view['perfil'].unique().tolist()
        opcoes_perfil = ["Todos os perfis"] + sorted(perfis_disponiveis)
        filtro_perfil = st.selectbox("👤 Filtrar por Perfil", opcoes_perfil)
        
        if filtro_perfil != "Todos os perfis":
            usuarios_view = usuarios_view[usuarios_view['perfil'] == filtro_perfil]
    
    colunas_exibir = [
        'id_usuario', 
        'nome', 
        'empresa_cnpj',
        'perfil', 
        'is_admin'
    ]
    
    usuarios_display = usuarios_view[colunas_exibir].copy()
    usuarios_display = usuarios_display.rename(columns={
        'id_usuario': 'ID',
        'nome': 'Nome',
        'empresa_cnpj': 'Empresa (CNPJ)',
        'perfil': 'Perfil',
        'is_admin': 'Admin?'
    })
    
    col_info1, col_info2, col_info3 = st.columns(3)
    col_info1.metric("Total de usuários", len(usuarios_display))
    col_info2.metric("Administradores", len(usuarios_display[usuarios_display['Admin?'] == True]))
    col_info3.metric("Usuários comuns", len(usuarios_display[usuarios_display['Admin?'] == False]))
    
    st.dataframe(
        usuarios_display, 
        use_container_width=True,
        hide_index=True,
        column_config={
            "Admin?": st.column_config.CheckboxColumn("Admin?")
        }
    )


elif menu == "Usuários - Portal":
    if not u.get("is_admin"):
        st.error("Apenas administradores da aplicação podem gerenciar usuários do portal.")
        st.stop()

    st.subheader("Usuários do Portal de Permissionamento")

    df_portal = carregar_usuarios_portal()

    if df_portal.empty:
        st.info("Nenhum usuário de portal cadastrado ainda.")
    else:
        df_view = df_portal[["login", "nome_exibicao", "is_admin", "ativo", "criado_em", "atualizado_em"]].copy()
        df_view = df_view.rename(columns={
            "login": "Login",
            "nome_exibicao": "Nome exibido",
            "is_admin": "É admin do portal?",
            "ativo": "Ativo?",
            "criado_em": "Criado em",
            "atualizado_em": "Atualizado em",
        })
        st.dataframe(df_view, use_container_width=True, hide_index=True)

    st.caption("Nesta tela você vê quem pode acessar o portal de permissionamento. A criação/edição destes usuários é feita a partir do cadastro de Usuários - Aplicação (marcando o acesso ao portal).")

elif menu == "Usuários - Portal":
    if not u.get("is_admin"):
        st.error("Apenas administradores da aplicação podem gerenciar usuários do portal.")
        st.stop()

    st.subheader("Usuários do Portal de Permissionamento")

    df_portal = carregar_usuarios_portal()

    if df_portal.empty:
        st.info("Nenhum usuário de portal cadastrado ainda.")
    else:
        df_view = df_portal[["login", "nome_exibicao", "is_admin", "ativo", "criado_em", "atualizado_em"]].copy()
        df_view = df_view.rename(columns={
            "login": "Login",
            "nome_exibicao": "Nome exibido",
            "is_admin": "Admin do portal?",
            "ativo": "Ativo?",
            "criado_em": "Criado em",
            "atualizado_em": "Atualizado em",
        })
        st.dataframe(df_view, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Editar usuário do portal")

    if df_portal.empty:
        st.info("Não há usuários para editar.")
    else:
        df_portal["label"] = df_portal.apply(
            lambda r: f"{r['id_portal_usuario']} - {r['login']}",
            axis=1
        )
        opc = st.selectbox(
            "Selecione o usuário do portal para editar",
            [""] + df_portal["label"].tolist(),
            key="portal_usuario_select"
        )

        if opc:
            id_portal_usuario = int(opc.split(" - ", 1)[0])
            row = df_portal[df_portal["id_portal_usuario"] == id_portal_usuario].iloc[0]

            with st.form(f"form_portal_usuario_{id_portal_usuario}"):
                login = st.text_input("Login (não altere se estiver em uso)", value=row["login"])
                nome_exibicao = st.text_input("Nome exibido", value=row["nome_exibicao"])
                is_admin_portal = st.checkbox("Administrador do portal?", value=bool(row["is_admin"]))
                ativo = st.checkbox("Ativo?", value=bool(row["ativo"]))
                senha_nova = st.text_input("Nova senha (deixe em branco para manter a atual)", type="password")

                submit_portal = st.form_submit_button("Salvar usuário do portal")

            if submit_portal:
                try:
                    salvar_usuario_portal(
                        id_portal_usuario=id_portal_usuario,
                        login=login,
                        nome_exibicao=nome_exibicao,
                        senha_nova=senha_nova if senha_nova.strip() else None,
                        is_admin_portal=is_admin_portal,
                        ativo=ativo,
                    )
                    st.success("Usuário do portal atualizado com sucesso.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao atualizar usuário do portal: {e}")

# =========================
# Permissões por Perfil (EDITOR COMPLETO)
# =========================
elif menu == "Permissões (por Perfil)":
    st.subheader("Permissões por Perfil")

    u = st.session_state["usuario_portal"]
    grupos_ids = st.session_state.get("grupos_usuario_ids", [])

    # Helpers de estado para o editor
    def _ensure_editor_state(table_key: str):
        """Garante estrutura estável em st.session_state para a tabela selecionada (sempre lowercase)."""
        key = (table_key or "").lower()
        if "perm_editor" not in st.session_state:
            st.session_state["perm_editor"] = {}
        if key not in st.session_state["perm_editor"]:
            st.session_state["perm_editor"][key] = {
                "column_blocks": [],
                "row_filters": [],        # [{field, op, values}]
                "distinct_cache": {},     # {field: [values]}
                "loaded_from_db": False,
                "id_permissao": None,
            }

    def _get_editor(table_key: str) -> dict:
        """Retorna o editor (criando se necessário) para a tabela (sempre lowercase)."""
        key = (table_key or "").lower()
        _ensure_editor_state(key)
        return st.session_state["perm_editor"][key]

    def _load_existing_payload_to_editor(table_key: str, id_perfil: int):
        """Carrega do BD a permissão existente da tabela para o perfil, e injeta no editor."""
        key = (table_key or "").lower()
        perm_df = carregar_permissoes_perfil(id_perfil)
        match = perm_df[perm_df["nome_tabela"].str.lower() == key]
        ed = _get_editor(key)

        if match.empty:
            ed["id_permissao"] = None
            ed["column_blocks"] = []
            ed["row_filters"] = []
            ed["loaded_from_db"] = True
            return

        row = match.iloc[0]
        ed["id_permissao"] = int(row["id_permissao"])
        try:
            payload = row["campos_nao_permitidos"]
            payload = payload if isinstance(payload, dict) else (json.loads(payload) if payload else {})
        except Exception:
            payload = {}

        bloco = (payload.get("tabelas", {}) or {}).get(key, {})
        ed["column_blocks"] = list(bloco.get("column_blocks", []) or [])
        ed["row_filters"] = list(bloco.get("row_filters", []) or [])
        ed["loaded_from_db"] = True

    def _fetch_distinct(table_code: str, field: str) -> list[str]:
        """Busca DISTINCT via RM e cacheia (normaliza table_code p/ lowercase)."""
        key = (table_code or "").lower()
        ed = _get_editor(key)

        if field in ed["distinct_cache"]:
            return ed["distinct_cache"][field]

        RM_CODSENTENCA = os.getenv("RM_CODSENTENCA", "LUNA0077").strip()
        RM_CODCOLIGADA = int(os.getenv("RM_CODCOLIGADA", "0"))
        RM_APLICACAO   = os.getenv("RM_APLICACAO", "G").strip()

        try:
            sentenca = f"SELECT DISTINCT TOP 500 {field} AS VALOR FROM {table_code} WHERE {field} IS NOT NULL ORDER BY 1"
            _ = rm_edit_query(sentenca, RM_CODSENTENCA, RM_CODCOLIGADA, RM_APLICACAO)
            rows = rm_execute_query(RM_CODSENTENCA, RM_CODCOLIGADA, RM_APLICACAO)
            vals = []
            for r in rows:
                if isinstance(r, dict):
                    if "VALOR" in r:
                        vals.append(str(r["VALOR"]) if r["VALOR"] is not None else None)
                    else:
                        v = next(iter(r.values()))
                        vals.append(str(v) if v is not None else None)
            vals = sorted({v for v in vals if v})
        except Exception as e:
            st.warning(f"Falha no DISTINCT via RM para {field}: {e}")
            vals = []

        ed["distinct_cache"][field] = vals
        return vals

    def _rule_base_condition(r):
        f = r.get("field","")
        op = (r.get("op","=") or "=").upper()
        vals = r.get("values") or []
        if op in ("IS NULL", "IS NOT NULL"):
            return f"{f} {op}"
        if op == "IN":
            inside = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals) or "''"
            return f"{f} IN ({inside})"
        if op == "BETWEEN":
            a = "'" + str(vals[0]).replace("'", "''") + "'" if len(vals) >= 1 else "''"
            b = "'" + str(vals[1]).replace("'", "''") + "'" if len(vals) >= 2 else "''"
            return f"{f} BETWEEN {a} AND {b}"
        if op in (">", ">=", "<", "<=", "=", "LIKE"):
            v = "'" + str(vals[0]).replace("'", "''") + "'" if vals else "''"
            return f"{f} {op} {v}"
        # fallback seguro
        v = "'" + str(vals[0]).replace("'", "''") + "'" if vals else "''"
        return f"{f} = {v}"

    def _render_rule_sqllike_deny(r):
        return f"NOT ({_rule_base_condition(r)})"

    # -------- Perfis filtrados por grupo (RLS) --------
    df_perfis = carregar_perfis()

    if not u.get("is_admin"):
        df_perfis = df_perfis[df_perfis["id_grupo_empresa"].isin(grupos_ids)]

    if df_perfis.empty:
        st.warning("Nenhum perfil disponível para o seu grupo de empresas.")
        st.stop()

    # -------- Seleções de Perfil e Tabela (Catálogo restrito ao grupo do perfil) --------
    empresas = carregar_empresas()
    if df_perfis.empty:
        st.info("Cadastre perfis primeiro.")
        st.stop()

    col1, col2 = st.columns(2)
    with col1:
        perfis = df_perfis.copy()
        perfis['label'] = perfis['nome_empresa'] + " - " + perfis['descricao']
        perfil_desc_sel = st.selectbox("Selecione o Perfil", perfis['label'])
        perfil_row = perfis[perfis['label'] == perfil_desc_sel].iloc[0]
        id_perfil = int(perfil_row['id_perfil'])
        id_empresa_do_perfil = int(perfil_row['id_empresa'])
        emp_row = empresas[empresas['id_empresa'] == id_empresa_do_perfil].iloc[0]
        id_grupo_do_perfil = int(emp_row['id_grupo_empresa'])

        # segurança extra: não-admin não consegue escolher perfil de outro grupo
        if (not u.get("is_admin")) and (id_grupo_do_perfil not in grupos_ids):
            st.error("Você não pode gerenciar permissões de perfis de outro grupo de empresas.")
            st.stop()

    with col2:
        df_tabs_cat_group = carregar_tabelas_catalogo(id_grupo_do_perfil)
        if df_tabs_cat_group.empty:
            st.warning("O grupo da empresa deste perfil não tem Tabelas no Catálogo.")
            st.stop()
        tabela_sel = st.selectbox(
            "Selecione a Tabela (Catálogo)",
            sorted(df_tabs_cat_group["tabela_codigo"].unique().tolist())
        )
        id_tabela = get_or_create_tabela_id(tabela_sel)

    # Carrega colunas da tabela do Catálogo
    colunas_disp = []
    try:
        tab_row = df_tabs_cat_group[df_tabs_cat_group["tabela_codigo"].str.lower()==tabela_sel.lower()].head(1)
        if not tab_row.empty:
            id_tab_cat = int(tab_row.iloc[0]["id_tabela"])
            df_cols_cat = carregar_colunas_catalogo(id_tab_cat)
            colunas_disp = sorted(
                df_cols_cat["coluna_nome"].tolist(),
                key=lambda x: str(x).upper()
            )
    except Exception:
        pass
    if not colunas_disp:
        st.warning("Esta tabela ainda não tem colunas no Catálogo.")
        st.stop()

    # Prepara editor para a tabela atual e carrega permissão existente (uma vez por seleção)
    _ensure_editor_state(tabela_sel.lower())
    if not st.session_state["perm_editor"][tabela_sel.lower()]["loaded_from_db"]:
        _load_existing_payload_to_editor(tabela_sel.lower(), id_perfil)

    editor = _get_editor(tabela_sel)
    if not editor["loaded_from_db"]:
        _load_existing_payload_to_editor(tabela_sel, id_perfil)

    # =========================
    # BLOQUEIO POR COLUNAS
    # =========================
    st.markdown("### Bloqueio por Colunas")
    columns_block = st.multiselect(
        "Selecione as colunas a bloquear",
        options=colunas_disp,
        default=[c for c in editor["column_blocks"] if c in colunas_disp],
        key=f"cb_{tabela_sel.lower()}",
        help="As colunas selecionadas ficarão ocultas para usuários deste perfil ao consultar esta tabela."
    )
    editor["column_blocks"] = columns_block

    # =========================
    # FILTROS POR LINHA
    # =========================
    st.markdown("### Bloqueio a Nível de Linha (Row-level)")

    operadores = ["=", ">", ">=", "<", "<=", "IN", "LIKE", "BETWEEN", "IS NULL", "IS NOT NULL"]

    # Botões de controle do editor
    cadd, creset = st.columns([1,1])
    if cadd.button("➕ Adicionar Regra"):
        editor["row_filters"].append({"field": colunas_disp[0], "op": "=", "values": []})
    if creset.button("↩️ Recarregar do Banco"):
        editor["loaded_from_db"] = False
        _load_existing_payload_to_editor(tabela_sel.lower(), id_perfil)
        st.success("Permissão recarregada do banco.")

    # Renderiza cada regra com estado estável
    new_rules = []
    for i, r in enumerate(editor["row_filters"]):
        st.markdown(f"**Regra {i+1}**")
        colA, colB, colC = st.columns([1.4, 0.8, 2.2])

        # Campo (com auto DISTINCT ao trocar)
        field_key = f"rf_field_{tabela_sel.lower()}_{i}"
        old_field = st.session_state.get(field_key + "_old")
        field_sel = colA.selectbox(
            "Campo",
            options=colunas_disp,
            index=(colunas_disp.index(r.get("field")) if r.get("field") in colunas_disp else 0),
            key=field_key
        )
        if old_field is None or old_field != field_sel:
            _fetch_distinct(tabela_sel, field_sel)
            st.session_state[field_key + "_old"] = field_sel

        # Operador
        op_sel = colB.selectbox(
            "Operador",
            operadores,
            index=(operadores.index(r.get("op","=")) if r.get("op","=") in operadores else 0),
            key=f"rf_op_{tabela_sel.lower()}_{i}"
        )

        # Valores (auto-carrega opções se houver DISTINCT)
        valores_distintos = editor["distinct_cache"].get(field_sel, [])
        valores_default = r.get("values", []) or []

        if op_sel in ["IN", "NOT IN"]:
            val = colC.multiselect(
                "Valores",
                options=(valores_distintos or valores_default),
                default=valores_default,
                key=f"rf_vals_multi_{tabela_sel.lower()}_{i}"
            )
            values_sel = val
        elif op_sel == "BETWEEN":
            v1 = colC.text_input(
                "Valor inicial",
                value=(valores_default[0] if len(valores_default) >= 1 else ""),
                key=f"rf_v1_{tabela_sel.lower()}_{i}"
            )
            v2 = colC.text_input(
                "Valor final",
                value=(valores_default[1] if len(valores_default) >= 2 else ""),
                key=f"rf_v2_{tabela_sel.lower()}_{i}"
            )
            values_sel = [v1, v2]
        elif op_sel in ["IS NULL", "IS NOT NULL"]:
            values_sel = []
            colC.caption("Sem valores para este operador.")
        else:
            if valores_distintos:
                val = colC.selectbox(
                    "Valor",
                    options=valores_distintos,
                    index=(valores_distintos.index(valores_default[0]) if (valores_default and valores_default[0] in valores_distintos) else 0),
                    key=f"rf_val_one_sel_{tabela_sel.lower()}_{i}"
                )
                values_sel = [val]
            else:
                val = colC.text_input(
                    "Valor",
                    value=(valores_default[0] if valores_default else ""),
                    key=f"rf_val_one_txt_{tabela_sel.lower()}_{i}"
                )
                values_sel = [val] if val != "" else []

        colP, colR = st.columns([2.6, 0.4])
        colP.caption("Prévia (SQL-like)")
        colP.code(_render_rule_sqllike_deny({"field": field_sel, "op": op_sel, "values": values_sel}), language="sql")
        if colR.button("🗑️", key=f"rf_del_{tabela_sel.lower()}_{i}", help="Remover esta regra"):
            pass
        else:
            new_rules.append({"field": field_sel, "op": op_sel, "values": values_sel})

        st.divider()

    editor["row_filters"] = new_rules

    # Prévia combinada
    if editor["row_filters"]:
        base = " OR ".join(_rule_base_condition(r) for r in editor["row_filters"])
        st.markdown("**Como será aplicado (deny):**")
        st.code(f"NOT ({base})", language="sql")

    # =========================
    # Ações de Persistência
    # =========================
    colsave1, colsave2, colsave3 = st.columns([1,1,1])
    if colsave1.button("💾 Salvar/Atualizar Permissão"):
        payload_rules = {
            "_table_name": tabela_sel.lower(),
            "tabelas": {
                tabela_sel.lower(): {
                    "column_blocks": editor["column_blocks"],
                    "row_filters": editor["row_filters"]
                }
            }
        }
        try:
            salvar_permissao_perfil(
                id_permissao = editor["id_permissao"],
                id_perfil    = id_perfil,
                id_tabela    = id_tabela,
                json_rules   = payload_rules
            )
            editor["loaded_from_db"] = False
            _load_existing_payload_to_editor(tabela_sel.lower(), id_perfil)
            st.success("Permissão salva/atualizada com sucesso.")
        except Exception as e:
            st.error(f"Erro ao salvar permissão: {e}")

    if editor["id_permissao"] is not None and colsave2.button("🗑️ Excluir Permissão"):
        try:
            deletar_permissao(editor["id_permissao"])
            editor["id_permissao"] = None
            editor["column_blocks"] = []
            editor["row_filters"] = []
            st.success("Permissão excluída.")
        except Exception as e:
            st.error(f"Erro ao excluir permissão: {e}")

    if colsave3.button("🧹 Limpar rascunho (não salva)"):
        editor["column_blocks"] = []
        editor["row_filters"] = []
        editor["distinct_cache"] = {}
        st.info("Rascunho limpo. (A permissão no banco permanece a mesma)")

    # =========================
    # Visualização das Permissões atuais (read-only, amigável)
    # =========================
    st.markdown("---")
    st.markdown("### Permissões atuais do Perfil")
    perm_all = carregar_permissoes_perfil(id_perfil)

    if perm_all.empty:
        st.info("Nenhuma permissão cadastrada para este perfil.")
    else:
        for _, row in perm_all.iterrows():
            with st.expander(f"Tabela: {row['nome_tabela']} (id_permissao={row['id_permissao']})", expanded=False):
                try:
                    payload = row['campos_nao_permitidos']
                    payload = payload if isinstance(payload, dict) else (json.loads(payload) if payload else {})
                except Exception:
                    payload = {}

                meta     = payload.get("metadata", {})
                tabelas  = payload.get("tabelas", {})
                tablekey = next(iter(tabelas.keys()), None)
                bloco    = tabelas.get(tablekey or "", {})

                col_blocks = bloco.get("column_blocks", []) or []
                rules      = bloco.get("row_filters", []) or []

                c1, c2, c3 = st.columns([1.3, 1, 1])
                c1.markdown(f"**Tabela:** `{row['nome_tabela']}`")
                c2.markdown(f"**Atualizado por:** {meta.get('autor','—')}")
                c3.markdown(f"**Quando:** {meta.get('ultima_atualizacao','—')}")

                st.divider()

                st.markdown("**Colunas bloqueadas**")
                if col_blocks:
                    st.write(", ".join(f"`{c}`" for c in col_blocks))
                else:
                    st.write("— Nenhuma coluna bloqueada.")

                st.markdown("**Regras de linha**")
                if rules:
                    table_rows = []
                    for i2, r in enumerate(rules, start=1):
                        vals = r.get("values") or []
                        show_vals = ", ".join(map(str, vals)) if vals else "—"
                        table_rows.append({
                            "#": i2,
                            "Campo": r.get("field",""),
                            "Operador": r.get("op",""),
                            "Valores": show_vals,
                            "Prévia (SQL)": _render_rule_sqllike_deny(r)
                        })
                    st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)
                else:
                    st.write("— Nenhuma regra de linha.")

                if st.button("✏️ Editar esta permissão", key=f"edit_perm_{row['id_permissao']}"):
                    editor["loaded_from_db"] = False
                    _load_existing_payload_to_editor(tabela_sel.lower(), id_perfil)
                    st.rerun()

# Logs
elif menu == "Logs":
    st.subheader("Logs do Sistema")
    colf1, colf2, colf3, colf4 = st.columns([1,1,1,1.2])
    data_ini = colf1.date_input("Data início", value=(date.today() - timedelta(days=7)))
    data_fim = colf2.date_input("Data fim", value=date.today())
    filtro_usuario = colf3.text_input("Usuário (contém)")
    filtro_acao = colf4.text_input("Ação (contém)")

    query = """
        SELECT id, usuario_id, usuario_nome, perfil, acao, entidade, registro_id,
               status, mensagem_extra, data_hora
        FROM logs
        WHERE data_hora >= %s AND data_hora < %s
    """
    params = [datetime.combine(data_ini, datetime.min.time()),
              datetime.combine(data_fim + timedelta(days=1), datetime.min.time())]

    if filtro_usuario:
        query += " AND usuario_nome ILIKE %s"
        params.append(f"%{filtro_usuario}%")
    if filtro_acao:
        query += " AND acao ILIKE %s"
        params.append(f"%{filtro_acao}%")

    query += " ORDER BY data_hora DESC LIMIT 2000"

    conn = get_connection()
    df_logs = pd.read_sql(query, conn, params=params)
    conn.close()

    st.dataframe(df_logs, use_container_width=True)

    st.markdown("#### Detalhe do Log")
    selected_id = st.text_input("ID do log para detalhar")
    if st.button("Ver Detalhe"):
        if selected_id:
            conn = get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT id, usuario_id, usuario_nome, perfil, acao, entidade, registro_id,
                       valores_anteriores, valores_novos, ip_origem, user_agent, status, mensagem_extra, data_hora
                FROM logs
                WHERE id = %s
            """, (int(selected_id),))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                st.write(f"**Usuário:** {row['usuario_nome']}  |  **Perfil:** {row['perfil']}  |  **Ação:** {row['acao']}")
                st.write(f"**Entidade:** {row['entidade']}  |  **Registro ID:** {row['registro_id']}  |  **Status:** {row['status']}")
                st.write(f"**Data/Hora:** {row['data_hora']}")
                st.write(f"**IP Origem:** {row['ip_origem']}  |  **User-Agent:** {row['user_agent']}")
                st.write("**Valores Anteriores:**")
                st.json(row['valores_anteriores'] or {})
                st.write("**Valores Novos:**")
                st.json(row['valores_novos'] or {})
                if row.get("mensagem_extra"):
                    st.write("**Mensagem Extra:**")
                    st.write(row['mensagem_extra'])
            else:
                st.warning("Log não encontrado.")

# =========================
# Nova Tela para Logs de Interações
# =========================
elif menu == "Logs de Interações":
    st.subheader("Logs de Interações")
    
    # Filtros para pesquisa
    colf1, colf2, colf3 = st.columns([1,1,1])
    data_ini = colf1.date_input("Data início", value=(date.today() - timedelta(days=7)))
    data_fim = colf2.date_input("Data fim", value=date.today())
    filtro_usuario = colf3.text_input("Usuário (contém)")
    
    query = """
        SELECT id, usuario_id, usuario_nome, perfil, prompt_chat, retorno_chat, status, data_hora
        FROM logs_interacao
        WHERE data_hora >= %s AND data_hora < %s
    """
    params = [datetime.combine(data_ini, datetime.min.time()), datetime.combine(data_fim + timedelta(days=1), datetime.min.time())]
    
    if filtro_usuario:
        query += " AND usuario_nome ILIKE %s"
        params.append(f"%{filtro_usuario}%")
    
    query += " ORDER BY data_hora DESC LIMIT 2000"
    
    conn = get_connection()
    df_logs_interacao = pd.read_sql(query, conn, params=params)
    conn.close()

    # Exibir os logs
    st.dataframe(df_logs_interacao, use_container_width=True)

    # Detalhamento do log
    st.markdown("#### Detalhe do Log")
    selected_id = st.text_input("ID do log para detalhar")
    if st.button("Ver Detalhe"):
        if selected_id:
            conn = get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT id, usuario_id, usuario_nome, perfil, prompt_chat, retorno_chat, status, data_hora
                FROM logs_interacao
                WHERE id = %s
            """, (int(selected_id),))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                st.write(f"**Usuário:** {row['usuario_nome']}  |  **Perfil:** {row['perfil']}  |  **Status:** {row['status']}")
                st.write(f"**Prompt Chat:** {row['prompt_chat']}")
                st.write(f"**Retorno Chat:** {row['retorno_chat']}")
                st.write(f"**Data/Hora:** {row['data_hora']}")
            else:
                st.warning("Log não encontrado.")
