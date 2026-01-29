import psycopg2

# Configurações do banco
DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

def adicionar_coluna_query_sugerida():
    """
    Adiciona a coluna query_sugerida na tabela ai_query_learning
    """
    try:
        print("🔌 Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Verifica se a coluna já existe
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'ai_query_learning' 
            AND column_name = 'query_sugerida'
        """)
        
        if cur.fetchone():
            print("ℹ️ Coluna 'query_sugerida' já existe!")
        else:
            # Adiciona a coluna
            print("➕ Adicionando coluna 'query_sugerida'...")
            cur.execute("""
                ALTER TABLE ai_query_learning 
                ADD COLUMN query_sugerida TEXT
            """)
            
            # Adiciona comentário
            cur.execute("""
                COMMENT ON COLUMN ai_query_learning.query_sugerida IS 
                'Query sugerida pelo usuário como alternativa'
            """)
            
            conn.commit()
            print("✅ Coluna 'query_sugerida' adicionada com sucesso!")
        
        # Mostra estrutura atualizada
        print("\n📊 Estrutura da tabela atualizada:")
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'ai_query_learning'
            ORDER BY ordinal_position
        """)
        
        for row in cur.fetchall():
            nullable = "NULL" if row[2] == 'YES' else "NOT NULL"
            print(f"   - {row[0]}: {row[1]} ({nullable})")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERRO: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🔧 ADICIONANDO COLUNA QUERY_SUGERIDA")
    print("=" * 60)
    adicionar_coluna_query_sugerida()
    print("=" * 60)
