import psycopg2
import os
from dotenv import load_dotenv

# Configurações do Banco (copiadas do chat.py)
DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def migrar_tabela():
    print("🚀 Iniciando migração da tabela ai_query_learning...")
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # 1. Adicionar coluna STATUS
        print("Adicionando coluna 'status'...")
        cur.execute("ALTER TABLE ai_query_learning ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'PENDENTE';")
        
        # 2. Remover colunas antigas (query_correta, validada, aprovada)
        print("Removendo colunas antigas (query_correta, validada, aprovada)...")
        # Usamos DROP COLUMN IF EXISTS para evitar erros se rodar 2x
        cur.execute("ALTER TABLE ai_query_learning DROP COLUMN IF EXISTS query_correta;")
        cur.execute("ALTER TABLE ai_query_learning DROP COLUMN IF EXISTS validada;")
        cur.execute("ALTER TABLE ai_query_learning DROP COLUMN IF EXISTS aprovada;")
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Migração concluída com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro na migração: {e}")

if __name__ == "__main__":
    migrar_tabela()
