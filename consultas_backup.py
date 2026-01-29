import streamlit as st
import psycopg2
import pandas as pd
import json
from datetime import datetime, date, timedelta
from psycopg2.extras import RealDictCursor

# =========================
# Configurações do banco
# =========================
DB_CONFIG = {
    'host': '18.229.163.5',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass'
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

# =========================
# Utilitários / Logs
# =========================
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
    """
    Recupera informações do usuário 'logado' no portal (mock simples).
    """
    u = st.session_state.get("usuario_portal") or "admin"
    try:
        conn = get_connection()
        df = pd.read_sql(
            "SELECT id_usuario, nome, is_admin, id_perfil FROM usuario WHERE nome=%s LIMIT 1",
            conn, params=(u,)
        )
        conn.close()
        if not df.empty:
            return {
                "usuario_id": int(df.iloc[0]["id_usuario"]),
                "usuario_nome": str(df.iloc[0]["nome"]),
                "perfil_desc": "Administrador" if bool(df.iloc[0]["is_admin"]) else "Usuário",
                "id_perfil": int(df.iloc[0]["id_perfil"])
            }
    except Exception:
        pass
    return {
        "usuario_id": 0,
        "usuario_nome": u,
        "perfil_desc": "Administrador",
        "id_perfil": None
    }

# =========================
# Cache de dados básicos
# =========================
@st.cache_data(ttl=300)
def carregar_grupos():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT id_grupo_empresa, nome_grupo_empresa, banco_dados, versao, cnpj_matriz
        FROM grupo_empresa
        ORDER BY nome_grupo_empresa
    """, conn)
    conn.close()
    return df

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

@st.cache_data(ttl=300)
def carregar_perfis():
    """
    Agora perfil possui FK para empresa. Retornamos também dados da empresa
    para facilitar as telas.
    """
    conn = get_connection()
    df = pd.read_sql("""
        SELECT p.id_perfil, p.descricao, p.id_empresa, e.nome_empresa
        FROM perfil p
        JOIN empresa e ON p.id_empresa = e.id_empresa
        ORDER BY e.nome_empresa, p.descricao
    """, conn)
    conn.close()
    return df

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
    """
    Atenção: schema novo não tem u.id_empresa. Precisamos juntar via perfil -> empresa.
    """
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
            e.nome_empresa
        FROM usuario u
        JOIN perfil p ON u.id_perfil = p.id_perfil
        JOIN empresa e ON p.id_empresa = e.id_empresa
        ORDER BY u.nome
    """, conn)
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

def salvar_perfil(id_perfil, id_empresa, descricao):
    """
    Schema novo: perfil precisa de id_empresa.
    """
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None
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

def salvar_usuario(id_usuario, nome, senha, id_perfil, is_admin, id_empresa_para_mapear=None):
    """
    Schema novo: usuario tem somente id_perfil (empresa vem de perfil).
    Opcionalmente, garantimos mapeamento em usuario_empresa.
    """
    u = get_session_user_info()
    conn = get_connection()
    cur = conn.cursor()
    before = None

    if id_usuario is None:
        cur.execute("""
            INSERT INTO usuario (id_perfil, senha, nome, is_admin)
            VALUES (%s, %s, %s, %s)
            RETURNING id_usuario
        """, (id_perfil, senha, nome, is_admin))
        new_id = cur.fetchone()[0]
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "CREATE", "usuario",
                   registro_id=new_id, valores_anteriores=before,
                   valores_novos={"nome": nome, "id_perfil": id_perfil, "is_admin": is_admin})
        # Upsert em usuario_empresa (se empresa for fornecida)
        if id_empresa_para_mapear:
            upsert_usuario_empresa(new_id, id_empresa_para_mapear, id_perfil)
    else:
        cur.execute("SELECT id_perfil, senha, nome, is_admin FROM usuario WHERE id_usuario=%s", (id_usuario,))
        row = cur.fetchone()
        before = {"id_perfil": row[0], "senha": "***", "nome": row[2], "is_admin": row[3]} if row else None
        cur.execute("""
            UPDATE usuario SET id_perfil=%s, senha=%s, nome=%s, is_admin=%s
            WHERE id_usuario=%s
        """, (id_perfil, senha, nome, is_admin, id_usuario))
        conn.commit()
        log_action(u["usuario_id"], u["usuario_nome"], u["perfil_desc"], "UPDATE", "usuario",
                   registro_id=id_usuario, valores_anteriores=before,
                   valores_novos={"id_perfil": id_perfil, "senha": "***", "nome": nome, "is_admin": is_admin})
        if id_empresa_para_mapear:
            upsert_usuario_empresa(id_usuario, id_empresa_para_mapear, id_perfil)

    cur.close()
    conn.close()
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

# =========================
# Interface
# =========================
st.set_page_config(page_title="Gestão ERP - Usuários e Permissões", layout="wide")

st.title("Gestão de Usuários e Permissões - ERP (TOTVS RM)")

if 'logado' not in st.session_state:
    st.session_state.logado = False

if 'usuario_portal' not in st.session_state:
    st.session_state.usuario_portal = None

# Login simples
def login_portal():
    st.subheader("Login Portal")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if usuario and senha:
            st.session_state.logado = True
            st.session_state.usuario_portal = usuario
            st.success("Login realizado com sucesso!")
        else:
            st.error("Usuário ou senha inválidos")

if not st.session_state.logado:
    login_portal()
    st.stop()

menu = st.sidebar.selectbox(
    "Menu",
    [
        "Dashboard",
        "Grupos de Empresas",
        "Empresas",
        "Perfis",
        "Tabelas",
        "Usuários",
        "Permissões (por Perfil)",
        "Logs",
        "Logs de Interações",
        "Logout"
    ]
)

if menu == "Logout":
    st.session_state.logado = False
    st.session_state.usuario_portal = None
    st.rerun()

# Dashboard
if menu == "Dashboard":
    st.subheader("Dashboard")
    st.write("Bem-vindo ao sistema de gestão de usuários e permissões para o agente de IA conectado ao TOTVS RM.")
    st.info("Aqui você gerencia perfis, usuários, empresas e as permissões por tabela (coluna e linha).")

# Grupos
elif menu == "Grupos de Empresas":
    st.subheader("Grupos de Empresas")
    grupos = carregar_grupos()
    with st.expander("Cadastrar / Editar Grupo de Empresa"):
        id_grupo = None
        grupo_selecionado = st.selectbox("Selecione grupo para editar (ou deixe em branco para novo)", [""] + grupos['nome_grupo_empresa'].tolist())
        if grupo_selecionado:
            grupo_data = grupos[grupos['nome_grupo_empresa'] == grupo_selecionado].iloc[0]
            id_grupo = int(grupo_data['id_grupo_empresa'])
            nome = str(grupo_data['nome_grupo_empresa'])
            banco_dados = st.text_input("Banco de Dados", value=str(grupo_data.get('banco_dados', 'PostgreSQL')))
            versao = st.text_input("Versão", value=str(grupo_data.get('versao', '')))
            cnpj_matriz = st.text_input("CNPJ Matriz", value=str(grupo_data.get('cnpj_matriz', '')))
        else:
            nome = st.text_input("Nome do Grupo de Empresa")
            banco_dados = st.text_input("Banco de Dados", value="PostgreSQL")
            versao = st.text_input("Versão")
            cnpj_matriz = st.text_input("CNPJ Matriz")
        if st.button("Salvar Grupo"):
            if not nome:
                st.error("Nome do grupo é obrigatório")
            else:
                salvar_grupo(id_grupo, nome, banco_dados, versao, cnpj_matriz)
                st.success("Grupo salvo com sucesso!")
                st.rerun()
    st.markdown("---")
    st.dataframe(grupos, use_container_width=True)

# Empresas
elif menu == "Empresas":
    st.subheader("Empresas")
    empresas = carregar_empresas()
    grupos = carregar_grupos()
    with st.expander("Cadastrar / Editar Empresa"):
        id_empresa = None
        empresa_selecionada = st.selectbox("Selecione empresa para editar (ou deixe em branco para novo)", [""] + empresas['nome_empresa'].tolist())
        if empresa_selecionada:
            empresa_data = empresas[empresas['nome_empresa'] == empresa_selecionada].iloc[0]
            id_empresa = int(empresa_data['id_empresa'])
            nome_empresa = str(empresa_data['nome_empresa'])
            cnpj = str(empresa_data['cnpj'] or "")
            grupo_sel = str(empresa_data['nome_grupo_empresa'])
        else:
            nome_empresa = ""
            cnpj = ""
            grupo_sel = grupos['nome_grupo_empresa'].iloc[0] if not grupos.empty else None

        nome_empresa = st.text_input("Nome da Empresa", value=nome_empresa)
        cnpj = st.text_input("CNPJ", value=cnpj)
        grupo_nome = st.selectbox(
            "Grupo de Empresa",
            grupos['nome_grupo_empresa'],
            index=int(grupos[grupos['nome_grupo_empresa'] == grupo_sel].index[0]) if (grupo_sel and grupo_sel in grupos['nome_grupo_empresa'].values) else 0
        )
        id_grupo_empresa = int(grupos[grupos['nome_grupo_empresa'] == grupo_nome]['id_grupo_empresa'].values[0])

        if st.button("Salvar Empresa"):
            if not nome_empresa:
                st.error("Nome da empresa é obrigatório")
            else:
                salvar_empresa(id_empresa, id_grupo_empresa, nome_empresa, cnpj)
                st.success("Empresa salva com sucesso!")
                st.rerun()
    st.markdown("---")
    st.dataframe(empresas, use_container_width=True)

# Perfis
elif menu == "Perfis":
    st.subheader("Perfis de Usuário")
    perfis = carregar_perfis()
    empresas = carregar_empresas()

    with st.expander("Cadastrar / Editar Perfil"):
        id_perfil = None
        # Para editar: escolher pelo label Empresa - Perfil
        perfis['label'] = perfis['nome_empresa'] + " - " + perfis['descricao']
        perfil_opcoes = [""] + perfis['label'].tolist()
        perfil_selecionado = st.selectbox("Selecione perfil para editar (ou deixe em branco para novo)", perfil_opcoes)

        if perfil_selecionado:
            perfil_data = perfis[perfis['label'] == perfil_selecionado].iloc[0]
            id_perfil = int(perfil_data['id_perfil'])
            descricao = str(perfil_data['descricao'])
            empresa_nome_atual = str(perfil_data['nome_empresa'])
        else:
            descricao = ""
            empresa_nome_atual = empresas['nome_empresa'].iloc[0] if not empresas.empty else None

        empresa_nome_sel = st.selectbox(
            "Empresa do Perfil",
            empresas['nome_empresa'],
            index=int(empresas[empresas['nome_empresa'] == empresa_nome_atual].index[0]) if (empresa_nome_atual and empresa_nome_atual in empresas['nome_empresa'].values) else 0
        )
        id_empresa_sel = int(empresas[empresas['nome_empresa'] == empresa_nome_sel]['id_empresa'].values[0])

        descricao = st.text_input("Descrição do Perfil", value=descricao)

        if st.button("Salvar Perfil"):
            if not descricao:
                st.error("Descrição é obrigatória")
            else:
                salvar_perfil(id_perfil, id_empresa_sel, descricao)
                st.success("Perfil salvo com sucesso!")
                st.rerun()

    st.markdown("---")
    st.dataframe(perfis.drop(columns=['label']), use_container_width=True)

# Tabelas
elif menu == "Tabelas":
    st.subheader("Tabelas")
    tabelas = carregar_tabelas()
    with st.expander("Cadastrar / Editar Tabela"):
        id_tabela = None
        tabela_selecionada = st.selectbox("Selecione tabela para editar (ou deixe em branco para novo)", [""] + tabelas['nome_tabela'].tolist())
        if tabela_selecionada:
            tabela_data = tabelas[tabelas['nome_tabela'] == tabela_selecionada].iloc[0]
            id_tabela = int(tabela_data['id_tabela'])
            nome_tabela = str(tabela_data['nome_tabela'])
            descricao = str(tabela_data['descricao'] or "")
        else:
            nome_tabela = ""
            descricao = ""
        nome_tabela = st.text_input("Nome da Tabela", value=nome_tabela)
        descricao = st.text_input("Descrição", value=descricao)
        if st.button("Salvar Tabela"):
            if not nome_tabela:
                st.error("Nome da tabela é obrigatório")
            else:
                salvar_tabela(id_tabela, nome_tabela, descricao)
                st.success("Tabela salva com sucesso!")
                st.rerun()
    st.markdown("---")
    st.dataframe(tabelas, use_container_width=True)

# Usuários
elif menu == "Usuários":
    st.subheader("Usuários")
    usuarios = carregar_usuarios()
    empresas = carregar_empresas()
    perfis = carregar_perfis()

    with st.expander("Cadastrar / Editar Usuário"):
        id_usuario = None
        usuario_opcoes = [""] + usuarios['nome'].tolist()
        usuario_selecionado = st.selectbox("Selecione usuário para editar (ou deixe em branco para novo)", usuario_opcoes)

        if usuario_selecionado:
            usuario_data = usuarios[usuarios['nome'] == usuario_selecionado].iloc[0]
            id_usuario = int(usuario_data['id_usuario'])
            nome = str(usuario_data['nome'])
            senha = str(usuario_data['senha'])
            is_admin = bool(usuario_data['is_admin'])
            empresa_nome_atual = str(usuario_data['nome_empresa'])
            perfil_desc_atual = str(usuario_data['perfil'])
        else:
            nome = ""
            senha = ""
            is_admin = False
            empresa_nome_atual = empresas['nome_empresa'].iloc[0] if not empresas.empty else None
            # Se novo, tentamos pegar um perfil da empresa default
            perfis_da_emp = perfis[perfis['nome_empresa'] == empresa_nome_atual]
            perfil_desc_atual = perfis_da_emp['descricao'].iloc[0] if not perfis_da_emp.empty else (perfis['descricao'].iloc[0] if not perfis.empty else "")

        nome = st.text_input("Nome", value=nome)
        senha = st.text_input("Senha", type="password", value=senha)
        empresa_nome_sel = st.selectbox(
            "Empresa",
            empresas['nome_empresa'],
            index=int(empresas[empresas['nome_empresa'] == empresa_nome_atual].index[0]) if (empresa_nome_atual and empresa_nome_atual in empresas['nome_empresa'].values) else 0
        )
        # Perfis filtrados pela empresa escolhida
        perfis_emp = perfis[perfis['nome_empresa'] == empresa_nome_sel]
        perfis_labels = perfis_emp['descricao'].tolist()
        if not perfis_labels:
            st.warning("Esta empresa ainda não tem perfis. Cadastre um perfil antes.")
            st.stop()

        perfil_desc_sel = st.selectbox(
            "Perfil",
            perfis_labels,
            index=(perfis_labels.index(perfil_desc_atual) if perfil_desc_atual in perfis_labels else 0)
        )
        is_admin = st.checkbox("Administrador", value=is_admin)

        id_empresa_sel = int(empresas[empresas['nome_empresa'] == empresa_nome_sel]['id_empresa'].values[0])
        id_perfil_sel = int(perfis_emp[perfis_emp['descricao'] == perfil_desc_sel]['id_perfil'].values[0])

        if st.button("Salvar Usuário"):
            if not nome or not senha:
                st.error("Nome e senha são obrigatórios")
            else:
                salvar_usuario(id_usuario, nome, senha, id_perfil_sel, is_admin, id_empresa_para_mapear=id_empresa_sel)
                st.success("Usuário salvo com sucesso!")
                st.rerun()

    st.markdown("---")
    st.dataframe(usuarios, use_container_width=True)

# Permissões por Perfil
elif menu == "Permissões (por Perfil)":
    st.subheader("Permissões por Perfil")
    perfis = carregar_perfis()
    tabelas = carregar_tabelas()

    if perfis.empty or tabelas.empty:
        st.info("Cadastre perfis e tabelas antes de definir permissões.")
        st.stop()

    col1, col2 = st.columns(2)
    with col1:
        perfis['label'] = perfis['nome_empresa'] + " - " + perfis['descricao']
        perfil_desc_sel = st.selectbox("Selecione o Perfil", perfis['label'])
        id_perfil = int(perfis[perfis['label'] == perfil_desc_sel]['id_perfil'].values[0])
    with col2:
        tabela_sel = st.selectbox("Selecione a Tabela", tabelas['nome_tabela'])
        id_tabela = int(tabelas[tabelas['nome_tabela'] == tabela_sel]['id_tabela'].values[0])

    perm_df = carregar_permissoes_perfil(id_perfil)
    perm_existente = perm_df[perm_df['nome_tabela'] == tabela_sel]
    id_permissao = int(perm_existente.iloc[0]['id_permissao']) if not perm_existente.empty else None
    payload_atual = {}
    if not perm_existente.empty and pd.notna(perm_existente.iloc[0]['campos_nao_permitidos']):
        try:
            payload_atual = json.loads(perm_existente.iloc[0]['campos_nao_permitidos'])
        except Exception:
            payload_atual = {}

    # ====== BLOQUEIO DE COLUNAS ======
    st.markdown("### Bloqueio por Colunas")
    colunas_fake_por_tabela = {
        "clientes": ["id_cliente", "nome", "email", "telefone", "empresa_id", "data_cadastro"],
        "produtos": ["id_produto", "descricao", "preco", "estoque", "categoria"],
        "vendas":   ["id_venda", "cliente_id", "valor_total", "filial", "situacao", "data"]
    }
    colunas_disp = colunas_fake_por_tabela.get(tabela_sel.lower(), ["id", "col1", "col2", "col3"])
    initial_blocks = []
    try:
        initial_blocks = payload_atual.get("tabelas", {}).get(tabela_sel.lower(), {}).get("column_blocks", [])
    except Exception:
        initial_blocks = []

    columns_block = st.multiselect("Selecione as colunas a bloquear", colunas_disp, default=initial_blocks)

    # ====== FILTROS POR LINHA ======
    st.markdown("### Permissão a Nível de Linha (Row-level)")
    st.caption("Defina regras de filtro: campo, operador e valores. (Por ora com DISTINCT simulado)")

    operadores = ["=", "!=", ">", ">=", "<", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE", "BETWEEN"]

    distintos_fake = {
        "empresa_id": ["1", "2", "3"],
        "filial": ["Filial1", "Filial2", "Filial3"],
        "situacao": ["ABERTA", "FECHADA", "CANCELADA"],
        "categoria": ["A", "B", "C"]
    }

    row_filters_state = []
    try:
        row_filters_state = payload_atual.get("tabelas", {}).get(tabela_sel.lower(), {}).get("row_filters", [])
        if not isinstance(row_filters_state, list):
            row_filters_state = []
    except Exception:
        row_filters_state = []

    st.markdown("#### Regras")
    max_regras = st.number_input(
        "Quantidade de regras",
        min_value=0, max_value=20,
        value=max(1, len(row_filters_state)) if row_filters_state else 1, step=1
    )

    novas_regras = []
    for i in range(int(max_regras)):
        st.write(f"**Regra {i+1}**")
        colA, colB, colC = st.columns([1.3, 0.7, 2.0])

        campo_default = row_filters_state[i]["field"] if i < len(row_filters_state) else None
        campo_sel = colA.selectbox(
            "Campo",
            options=colunas_disp,
            index=(colunas_disp.index(campo_default) if (campo_default in colunas_disp) else 0),
            key=f"rowf_campo_{i}"
        )

        op_default = row_filters_state[i]["op"] if i < len(row_filters_state) else "="
        op_sel = colB.selectbox("Operador", operadores,
                                index=(operadores.index(op_default) if op_default in operadores else 0),
                                key=f"rowf_op_{i}")

        valores_default = row_filters_state[i].get("values", []) if i < len(row_filters_state) else []
        valores_distintos = distintos_fake.get(campo_sel, [])

        if op_sel in ["IN", "NOT IN"]:
            valores_sel = colC.multiselect(
                "Valores", options=valores_distintos or valores_default or ["A","B","C"],
                default=valores_default, key=f"rowf_vals_{i}"
            )
        elif op_sel == "BETWEEN":
            v1 = colC.text_input("Valor inicial", value=(valores_default[0] if len(valores_default) >= 1 else ""), key=f"rowf_v1_{i}")
            v2 = colC.text_input("Valor final", value=(valores_default[1] if len(valores_default) >= 2 else ""), key=f"rowf_v2_{i}")
            valores_sel = [v1, v2]
        else:
            valor_unico = colC.text_input("Valor", value=(valores_default[0] if valores_default else ""), key=f"rowf_val_{i}")
            valores_sel = [valor_unico] if valor_unico != "" else []

        novas_regras.append({"field": campo_sel, "op": op_sel, "values": valores_sel})

    payload_rules = {
        "_table_name": tabela_sel.lower(),
        "tabelas": {
            tabela_sel.lower(): {
                "column_blocks": columns_block,
                "row_filters": [r for r in novas_regras if r["values"] or r["op"] in ("IS NULL","IS NOT NULL")]
            }
        }
    }

    colsave1, colsave2 = st.columns([1,1])
    if colsave1.button("Salvar Permissão"):
        salvar_permissao_perfil(id_permissao, id_perfil, id_tabela, payload_rules)
        st.success("Permissão salva!")
        st.rerun()

    if id_permissao is not None and colsave2.button("Excluir Permissão"):
        deletar_permissao(id_permissao)
        st.success("Permissão excluída!")
        st.rerun()

    st.markdown("---")
    st.markdown("### Permissões atuais do Perfil")
    perm_all = carregar_permissoes_perfil(id_perfil)
    if perm_all.empty:
        st.info("Perfil não possui permissões definidas.")
    else:
        for _, row in perm_all.iterrows():
            with st.expander(f"Tabela: {row['nome_tabela']} (id_permissao={row['id_permissao']})"):
                try:
                    p = json.loads(row['campos_nao_permitidos'])
                except Exception:
                    p = {}
                st.json(p)

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
