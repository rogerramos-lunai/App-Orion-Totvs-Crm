import os
import streamlit as st
import requests
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import json
import time
from dotenv import load_dotenv

load_dotenv()

# Configurações de IA (Sincronizado com att_vectorstore.py)
GPT_MODEL = "gpt-4o-mini"

# ========= Streamlit config (primeira chamada de UI) =========
st.set_page_config(page_title="Portal de Chat com IA", layout="wide")

# ========= Estado inicial =========
if 'logado' not in st.session_state:
    st.session_state.logado = False
if 'usuario' not in st.session_state:
    st.session_state.usuario = None
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# ========= Configurações =========
WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "http://56.125.69.27:5678/webhook/test-webhook")

DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

# ========= Conexão com banco =========
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def extrair_texto_n8n(payload):
    KEYS_PRIORIDADE = ("output", "resposta", "sql", "text", "message")
    def _pick_from_dict(d):
        for k in KEYS_PRIORIDADE:
            if k in d and d[k] is not None:
                v = d[k]
                if isinstance(v, dict):
                    for kk in KEYS_PRIORIDADE + ("value","content"):
                        if kk in v and v[kk] is not None:
                            return str(v[kk]).strip()
                    return json.dumps(v, ensure_ascii=False)
                return str(v).strip()
        for v in d.values():
            if isinstance(v, str):
                return v.strip()
        return json.dumps(d, ensure_ascii=False)

    if isinstance(payload, list):
        if len(payload) == 1:
            item = payload[0]
            return _pick_from_dict(item) if isinstance(item, dict) else str(item).strip()
        partes = []
        for item in payload:
            partes.append(_pick_from_dict(item) if isinstance(item, dict) else str(item).strip())
        return "\n".join([p for p in partes if p])

    if isinstance(payload, dict):
        return _pick_from_dict(payload)

    return str(payload).strip()

def log_interacao(usuario_id, usuario_nome, perfil_desc, prompt, retorno, status="SUCESSO"):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO logs_interacao (usuario_id, usuario_nome, perfil, prompt_chat, retorno_chat, status, data_hora)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (usuario_id, usuario_nome, perfil_desc, prompt, retorno, status, datetime.utcnow()))
    conn.commit()
    cur.close()
    conn.close()

def extrair_sql_da_resposta(resposta):
    """
    Extrai apenas a query SQL da resposta da IA.
    Procura por padrões SQL comuns e retorna apenas a query.
    """
    import re

    # 1. Tenta encontrar SQL entre tags personalizadas <SQL_QUERY> (Mais robusto para JSON)
    sql_tag = re.search(r'<SQL_QUERY>([\s\S]+?)</SQL_QUERY>', resposta, re.IGNORECASE)
    if sql_tag:
        return sql_tag.group(1).strip()
    
    # 2. Tenta encontrar SQL entre blocos de código markdown
    sql_block = re.search(r'```sql\s*([\s\S]+?)```', resposta, re.IGNORECASE)
    if sql_block:
        return sql_block.group(1).strip()
    
    # 3. Tenta encontrar SQL entre blocos de código genéricos
    code_block = re.search(r'```\s*([\s\S]+?)```', resposta)
    if code_block:
        content = code_block.group(1).strip()
        if any(keyword in content.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE']):
            return content
    
    # 4. Procura padrão específico encontrado no log do n8n: "Query |SQL: ..."
    # Exemplo: "... SALARIO: 3192.8 - Query |SQL: SELECT ... "
    sql_n8n_pattern = re.search(r'Query \|SQL:\s*(.+)$', resposta, re.IGNORECASE | re.DOTALL)
    if sql_n8n_pattern:
        return sql_n8n_pattern.group(1).strip()

    # 5. Procura por linhas que começam com SELECT, INSERT, UPDATE, etc (Fallback)
    lines = resposta.split('\n')
    sql_lines = []
    capturing = False
    
    for line in lines:
        line_upper = line.strip().upper()
        # Começa a capturar quando encontra início de SQL se não estivermos capturando block tags
        if any(line_upper.startswith(kw) for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']):
            capturing = True
            sql_lines.append(line.strip())
        elif capturing:
            # Continua capturando até encontrar ponto e vírgula ou linha vazia
            if line.strip():
                sql_lines.append(line.strip())
                if ';' in line:
                    break
            else:
                break
    
    if sql_lines:
        return '\n'.join(sql_lines)
    
    # Se não encontrou nada específico, retorna None para evitar salvar texto como SQL
    return None

def log_ai_query(pergunta_usuario, resposta_completa):
    """
    Salva a query gerada pela IA na tabela ai_query_learning para validação posterior.
    
    Args:
        pergunta_usuario: A pergunta original feita pelo usuário
        resposta_completa: A resposta completa da IA (será extraída apenas a query SQL)
    
    Returns:
        int: ID do registro inserido, ou None se houver erro
    """
    try:
        # Extrai apenas a query SQL da resposta
        query_sql = extrair_sql_da_resposta(resposta_completa)
        
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_query_learning (pergunta_usuario, query_gerada, status)
            VALUES (%s, %s, 'PENDENTE')
            RETURNING id
        """, (pergunta_usuario, query_sql))
        
        query_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        print(f"DEBUG DB SUCESSO. ID: {query_id}")
        return query_id
    except Exception as e:
        # Não queremos que erros de logging quebrem o chat
        print(f"Erro ao salvar query de IA: {e}")
        return None

def marcar_query_correta(query_id, usuario_nome, observacao=""):
    """
    Marca uma query como correta (aprovada).
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE ai_query_learning 
            SET status = 'APROVADA',
                validado_por = %s,
                observacoes = %s
            WHERE id = %s
        """, (usuario_nome, observacao, query_id))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Erro ao marcar query como correta: {e}")
        return False

def marcar_query_incorreta(query_id, query_correta, usuario_nome, observacao=""):
    """
    Marca uma query como incorreta e salva a query corrigida.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        # Se forneceu uma correção, salva em query_sugerida
        cur.execute("""
            UPDATE ai_query_learning 
            SET status = 'REJEITADA',
                query_sugerida = %s,
                validado_por = %s,
                observacoes = %s
            WHERE id = %s
        """, (query_correta, usuario_nome, observacao, query_id))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Erro ao marcar query como incorreta: {e}")
        return False

def salvar_query_sugerida(query_id, query_sugerida, usuario_nome, observacao=""):
    """
    Salva uma sugestão de query alternativa.
    NÃO marca como validada (aguarda revisão posterior).
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE ai_query_learning 
            SET status = 'REGULAR',
                query_sugerida = %s,
                observacoes = %s,
                validado_por = %s
            WHERE id = %s
        """, (query_sugerida, observacao, usuario_nome, query_id))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Erro ao salvar query sugerida: {e}")
        return False

def excluir_query(query_id):
    """
    Remove uma query do banco (usado quando o usuário descarta a query anterior sem validar).
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM ai_query_learning WHERE id = %s", (query_id,))
        conn.commit()
        cur.close()
        conn.close()
        print(f"DEBUG: Query {query_id} excluída por descarte do usuário.")
        return True
    except Exception as e:
        print(f"Erro ao excluir query: {e}")
        return False

def consolidar_pergunta(chat_history):
    """
    Usa o GPT para consolidar o histórico de conversa em uma única pergunta técnica
    para facilitar a geração de SQL e o entendimento futuro.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
        
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        
        msgs = [{
            "role": "system", 
            "content": "Você é um especialista em SQL e análise de dados. Sua tarefa é analisar o histórico e a ÚLTIMA pergunta do usuário. Reescreva a última pergunta para ser autossuficiente e técnica (pronta para gerar SQL). IMPORTANTE: Se a última mensagem for uma correção (ex: 'esquece'), ignore o anterior. A saída deve ser APENAS a pergunta reescrita. NUNCA responda a pergunta, apenas reformule-a. NUNCA inclua o SQL ou a resposta, apenas a pergunta textual."
        }]
        
        # Pega as últimas 10 mensagens para dar contexto suficiente
        ultimas = chat_history[-10:] if len(chat_history) > 0 else []
        for m in ultimas:
            role = "assistant" if m.get('autor') == 'IA' else "user"
            content = m.get('mensagem', '')
            if content:
                msgs.append({"role": role, "content": content})
        
        print(f"DEBUG GPT INPUT: {msgs}") # VER LOG

        # Usando modelo padronizado (gpt-4o-mini)
        response = client.chat.completions.create(
            model=GPT_MODEL, 
            messages=msgs,
            temperature=0.3,
            max_tokens=200
        )
        output = response.choices[0].message.content.strip()
        print(f"DEBUG GPT OUTPUT: {output}") # VER LOG
        return output
    except Exception as e:
        print(f"DEBUG GPT ERROR: {e}")
        return None

# ========= Auth helpers =========
def autenticar_usuario(username: str, password: str):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                u.id_usuario,
                u.nome,
                u.senha,
                u.is_admin,
                p.id_perfil,
                p.descricao AS perfil_desc,
                p.id_empresa
            FROM usuario u
            JOIN perfil p ON p.id_perfil = u.id_perfil
            WHERE u.nome = %s AND u.senha = %s
            LIMIT 1
        """, (username, password))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id_usuario": int(row["id_usuario"]),
            "nome": row["nome"],
            "is_admin": bool(row["is_admin"]),
            "id_perfil": int(row["id_perfil"]),
            "perfil_desc": row["perfil_desc"],
            "id_empresa": int(row["id_empresa"]) if row["id_empresa"] is not None else None
        }
    finally:
        cur.close()
        conn.close()

# ========= Login (form + callback com rerun) =========
def _efetivar_login():
    user = (st.session_state.get("_login_user") or "").strip()
    pwd  = (st.session_state.get("_login_pass") or "")
    if not user or not pwd:
        st.session_state.logado = False
        st.warning("Usuário ou senha inválidos")
        return

    u = autenticar_usuario(user, pwd)
    if u:
        st.session_state.logado = True
        st.session_state.usuario = u
        # NÃO chame st.rerun() aqui; o form já vai rerodar a página
    else:
        st.session_state.logado = False
        st.error("Usuário ou senha inválidos")

def login():
    st.subheader("Login")
    with st.form("form_login", clear_on_submit=False):
        st.text_input("Usuário", key="_login_user")
        st.text_input("Senha", type="password", key="_login_pass")
        st.form_submit_button("Entrar", on_click=_efetivar_login)

# ========= Gate de login =========
if not st.session_state.logado:
    login()
    st.stop()
 
# --- a partir daqui, SEMPRE logado ---
u = st.session_state.usuario  # <-- agora 'u' existe antes de usar
st.sidebar.subheader(f"Olá, {u['nome']} ({u['perfil_desc']})")
if st.sidebar.button("Logout"):
    st.session_state.logado = False
    st.session_state.usuario = None
    st.session_state.chat_history = []
    st.rerun()

st.title("Chat Interno com IA")

# ========= Histórico =========
for msg in st.session_state.chat_history:
    author = msg.get('autor', 'Usuário')
    st.markdown(f"**{author}**: {msg['mensagem']}")



# ========= Área de Input (Formulário Anti-Duplicação) =========
with st.form(key="chat_input_form", clear_on_submit=True):
    # O input de texto fica dentro do form.
    # clear_on_submit=True garante limpeza após envio com sucesso.
    user_input_widget = st.text_input("Digite sua mensagem aqui", key="input_msg_field")
    submitted = st.form_submit_button("Enviar")

if submitted and user_input_widget:
    user_input = user_input_widget.strip()
    if user_input:
        # 0. Limpa validação anterior para garantir que a nova query seja salva (sem excluir a anterior)
        st.session_state.last_query_id = None
        st.session_state.last_query_sql = None
        st.session_state.corrigindo_query = None
        st.session_state.sugerindo_query = None

        # 1. Adiciona user msg ao histórico
        st.session_state.chat_history.append({"autor": u['nome'], "mensagem": user_input})
        
        # 2. Define flags e recarrega para mostrar msg no histórico imediatamente
        st.session_state.aguardando_resposta = True
        st.session_state.last_user_prompt = user_input
        st.rerun()


# ========= Processamento da IA (Renderizado APÓS o Form) =========
if st.session_state.get("aguardando_resposta"):
    
    # Recupera o prompt salvo
    user_prompt = st.session_state.get("last_user_prompt", "")
    
    # Prepara payload
    # Voltando ao original para não quebrar o n8n
    payload = {
        "mensagem": user_prompt,
        "usuario": {
            "id": u["id_usuario"],
            "nome": u["nome"],
            "perfil_id": u["id_perfil"],
            "perfil_desc": u["perfil_desc"],
            "id_empresa": u["id_empresa"],
            "is_admin": u["is_admin"]
        },
        "meta": {
            "enviado_em": datetime.utcnow().isoformat() + "Z",
            "fonte": "chat.py"
        }
    }

    resposta_ia = ""

    # Container de Status com Auto-Scroll "Targeted"
    with st.status("🧠 A IA está analisando sua pergunta...", expanded=True) as status:
        
        # Script JS Agressivo (Intervalo Contínuo)
        # Força rolagem repetidamente por 5 segundos para garantir visibilidade durante renderização e animação
        st.markdown(
            """
            <script>
                function forceScroll() {
                    const container = window.parent.document.querySelector('[data-testid="stAppViewContainer"]');
                    if (container) {
                        container.scrollTop = container.scrollHeight;
                    } else {
                        window.parent.scrollTo(0, window.parent.document.body.scrollHeight);
                    }
                }
                
                // Rola a cada 100ms por 5 segundos
                const scrollInterval = setInterval(forceScroll, 100);
                setTimeout(() => clearInterval(scrollInterval), 5000);
            </script>
            """,
            unsafe_allow_html=True
        )
        
        st.write("🔍 Conectando ao assistente inteligente...")
        try:
            resp = requests.post(WEBHOOK_URL, json=payload, timeout=60)
            resp.raise_for_status()
            
            st.write("⚙️ Processando resposta...")
            try:
                data = resp.json()
                print("DEBUG N8N RESPONSE:", json.dumps(data, indent=2, ensure_ascii=False)) # VERIFICAR ONDE ESTÁ O SQL
                resposta_completa = extrair_texto_n8n(data)
                
                # Tenta extrair SQL de forma robusta
                sql_extraido = extrair_sql_da_resposta(resposta_completa)
                print(f"DEBUG SQL EXTRAIDO: {sql_extraido}") # VER LOG
                
                # Se achou SQL, mantemos a resposta original na tela (o usuário reclama se sumir)
                # E usamos o SQL extraído apenas para a caixa de validação no final
                resposta_ia = resposta_completa
                
                if sql_extraido:
                    try:
                        # Só salva se ainda não tivermos um ID salvo nesta sessão para esta query
                        if not st.session_state.get('last_query_id'):
                            # Constrói contexto com TODO o histórico para salvar no banco (Pedido do usuário: desde a primeira pergunta)
                            contexto_msgs = st.session_state.chat_history # Pega tudo
                            contexto_texto = "\n".join([f"{m.get('autor', 'User')}: {m.get('mensagem')}" for m in contexto_msgs])
                            
                            # O histórico já contém a pergunta atual (adicionada antes do rerun)
                            # Tenta consolidar a pergunta com GPT se houver chave
                            pergunta_consolidada = consolidar_pergunta(st.session_state.chat_history)
                            
                            if pergunta_consolidada:
                                prompt_para_banco = pergunta_consolidada
                                print(f"Pergunta consolidada pelo GPT: {prompt_para_banco}")
                            else:
                                # Fallback: usa apenas a última mensagem do usuário (limpo) em vez de todo histórico
                                try:
                                    prompt_para_banco = st.session_state.chat_history[-1]['mensagem']
                                    print("Usando última pergunta do usuário (Fallback Limpo)")
                                except:
                                    prompt_para_banco = user_input # Garantia final
                                    print("Usando user_input (Fallback Emergência)")
                            
                            print(f"Salvando query com contexto....")
                            # CORREÇÃO: Passar sql_extraido (limpo) em vez de resposta_ia (texto completo)
                            query_id = log_ai_query(prompt_para_banco, sql_extraido)
                            print(f"DEBUG QUERY ID SALVO: {query_id}") # VER LOG
                            
                            if query_id:
                                # Salva ID e SQL extraído para interface de validação
                                st.session_state.last_query_id = query_id
                                st.session_state.last_query_sql = sql_extraido
                                st.rerun() # Força rerun para mostrar a interface
                            
                    except Exception as e:
                        print(f"ERRO log_ai_query: {e}")  # Log discreto no console
                        
            except Exception:
                resposta_ia = resp.text.strip()
            
            st.write("✅ Resposta gerada!")
            status.update(label="Processo concluído!", state="complete", expanded=False)

        except requests.exceptions.RequestException as e:
            error_msg = f"Erro de comunicação com IA: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg += f" | Status: {e.response.status_code} | Body: {e.response.text}"
            resposta_ia = error_msg
            status.update(label="Erro na comunicação", state="error", expanded=False)
        except Exception as e:
            resposta_ia = f"Erro inesperado: {str(e)}"
            status.update(label="Erro inesperado", state="error", expanded=False)

    # Adiciona ao histórico
    st.session_state.chat_history.append({"autor": "IA", "mensagem": resposta_ia})
    
    try:
        log_interacao(
            u['id_usuario'],
            u['nome'],
            u['perfil_desc'],
            user_prompt,
            resposta_ia,
            status="SUCESSO" if not str(resposta_ia).startswith("Erro") else "ERRO"
        )
    except Exception:
        pass

    # Limpa flag e recarrega
    st.session_state.aguardando_resposta = False
    st.rerun()

# ========= Área de Validação (Renderizada no FINAL, limpa e única) =========
if st.session_state.get('last_query_id') and st.session_state.get('last_query_sql'):
    # Container para isolar visualmente no final da página
    with st.container():
        st.markdown("---")
        st.info("💾 **Validação de Query**: A IA gerou um código SQL. Por favor, valide se está correto.")
        
        col_sql, col_action = st.columns([2, 1])
        
        with col_sql:
            st.code(st.session_state.last_query_sql, language="sql")
        
        with col_action:
            st.write("**Ação:**")
            query_id = st.session_state.last_query_id
            
            # Campo de observação rápida
            obs_rapida = st.text_input("Observação (opcional):", key=f"obs_main_{query_id}", placeholder="Ex: Query otimizada...")

            # Botões de ação rápida
            if st.button("✅ Correta", key=f"btn_ok_{query_id}", use_container_width=True):
                if marcar_query_correta(query_id, u['nome'], obs_rapida):
                    st.toast("✅ Query validada com sucesso!")
                    st.session_state.last_query_id = None
                    st.session_state.last_query_sql = None
                    time.sleep(1) # Delay visual
                    st.rerun()

            if st.button("❌ Incorreta", key=f"btn_bad_{query_id}", use_container_width=True):
                st.session_state.corrigindo_query = query_id
            
            if st.button("💡 Sugerir", key=f"btn_sug_{query_id}", use_container_width=True):
                st.session_state.sugerindo_query = query_id
            
            if st.button("⏭️ Pular", key=f"btn_skip_{query_id}", use_container_width=True):
                st.session_state.last_query_id = None
                st.session_state.last_query_sql = None
                st.rerun()

        # Áreas expandidas para correção/sugestão (só aparecem se clicado)
        if st.session_state.get('corrigindo_query') == query_id:
            with st.expander("Corrigir Query", expanded=True):
                novo_sql = st.text_area("Digite o SQL correto:", value=st.session_state.last_query_sql, height=150)
                # obs_rapida já capturou a observação lá em cima, usamos ela aqui
                if st.button("💾 Rejeitar e Salvar Correção"):
                    if marcar_query_incorreta(query_id, novo_sql, u['nome'], obs_rapida):
                        st.toast("📝 Query rejeitada e correção salva!")
                        st.session_state.corrigindo_query = None
                        st.session_state.last_query_id = None
                        st.session_state.last_query_sql = None
                        time.sleep(1)
                        st.rerun()
        
        if st.session_state.get('sugerindo_query') == query_id:
            with st.expander("Sugerir Alternativa", expanded=True):
                novo_sql = st.text_area("Digite sua sugestão:", value=st.session_state.last_query_sql, height=150)
                # obs_rapida já capturou a observação lá em cima, usamos ela aqui
                if st.button("💾 Salvar como Regular"):
                    if salvar_query_sugerida(query_id, novo_sql, u['nome'], obs_rapida):
                        st.toast("💡 Sugestão salva como Regular!")
                        st.session_state.sugerindo_query = None
                        st.session_state.last_query_id = None
                        st.session_state.last_query_sql = None
                        time.sleep(1)
                        st.rerun()
