import psycopg2
import os

# Configurações do Banco (Do chat.py)
DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def recriar_tabela():
    print("🧨 Iniciando recriação da tabela ai_query_learning...")
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # 1. Dropar tabela existente
        print("🗑️ Apagando tabela antiga...")
        cur.execute("DROP TABLE IF EXISTS ai_query_learning;")
        
        # 2. Criar nova tabela com a ORDEM solicitada
        print("🔨 Criando nova tabela com ordem correta de colunas...")
        cur.execute("""
            CREATE TABLE ai_query_learning (
                id SERIAL PRIMARY KEY,
                pergunta_usuario TEXT,
                query_gerada TEXT,
                status VARCHAR(20) DEFAULT 'PENDENTE', -- LOGO APÓS QUERY_GERADA
                query_sugerida TEXT,
                observacoes TEXT,
                validado_por VARCHAR(100)
            );
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Tabela recriada com sucesso! Tabela está limpa e ordenada.")
        
    except Exception as e:
        print(f"❌ Erro ao recriar tabela: {e}")

if __name__ == "__main__":
    recriar_tabela()
