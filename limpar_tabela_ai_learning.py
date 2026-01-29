import psycopg2

# Configurações do banco
DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

def limpar_tabela():
    """
    Limpa todos os registros da tabela ai_query_learning
    """
    try:
        print("🔌 Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Conta quantos registros existem
        cur.execute("SELECT COUNT(*) FROM ai_query_learning")
        count_antes = cur.fetchone()[0]
        print(f"📊 Registros encontrados: {count_antes}")
        
        if count_antes > 0:
            # Limpa a tabela
            print("🗑️ Limpando tabela...")
            cur.execute("DELETE FROM ai_query_learning")
            conn.commit()
            
            # Reseta o contador de ID
            cur.execute("ALTER SEQUENCE ai_query_learning_id_seq RESTART WITH 1")
            conn.commit()
            
            print(f"✅ {count_antes} registros deletados com sucesso!")
            print("✅ Contador de ID resetado para 1")
        else:
            print("ℹ️ Tabela já está vazia")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERRO: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🗑️ LIMPEZA DA TABELA AI_QUERY_LEARNING")
    print("=" * 60)
    limpar_tabela()
    print("=" * 60)
