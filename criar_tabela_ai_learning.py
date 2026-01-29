import psycopg2
from psycopg2 import sql

# Configurações do banco (mesmas do chat.py)
DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

def criar_tabela_ai_learning():
    """
    Cria a tabela ai_query_learning no PostgreSQL para armazenar
    queries geradas pela IA e permitir validação manual pela equipe.
    """
    
    # SQL de criação da tabela
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ai_query_learning (
        id SERIAL PRIMARY KEY,
        pergunta_usuario TEXT NOT NULL,
        query_gerada TEXT NOT NULL,
        query_correta TEXT,
        validada BOOLEAN DEFAULT FALSE,
        aprovada BOOLEAN,
        observacoes TEXT,
        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_validacao TIMESTAMP,
        validado_por VARCHAR(255)
    );
    """
    
    # SQL para criar índices
    create_indexes_sql = """
    CREATE INDEX IF NOT EXISTS idx_ai_query_learning_validada 
        ON ai_query_learning(validada);
    
    CREATE INDEX IF NOT EXISTS idx_ai_query_learning_data_criacao 
        ON ai_query_learning(data_criacao);
    """
    
    # SQL para adicionar comentários (documentação)
    create_comments_sql = """
    COMMENT ON TABLE ai_query_learning IS 
        'Tabela para armazenar queries geradas pela IA para validação e aprendizado';
    
    COMMENT ON COLUMN ai_query_learning.pergunta_usuario IS 
        'Pergunta original feita pelo usuário';
    
    COMMENT ON COLUMN ai_query_learning.query_gerada IS 
        'Query SQL gerada pela IA';
    
    COMMENT ON COLUMN ai_query_learning.query_correta IS 
        'Query SQL correta após validação manual';
    
    COMMENT ON COLUMN ai_query_learning.validada IS 
        'Indica se o registro foi revisado pela equipe';
    
    COMMENT ON COLUMN ai_query_learning.aprovada IS 
        'TRUE se query_gerada estava correta, FALSE se precisou correção';
    """
    
    try:
        # Conecta ao banco
        print("🔌 Conectando ao PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Cria a tabela
        print("📋 Criando tabela ai_query_learning...")
        cur.execute(create_table_sql)
        
        # Cria os índices
        print("🔍 Criando índices...")
        cur.execute(create_indexes_sql)
        
        # Adiciona comentários
        print("📝 Adicionando documentação...")
        cur.execute(create_comments_sql)
        
        # Commit das alterações
        conn.commit()
        
        print("\n✅ SUCESSO! Tabela ai_query_learning criada com sucesso!")
        print("\n📊 Estrutura da tabela:")
        print("   - id (SERIAL PRIMARY KEY)")
        print("   - pergunta_usuario (TEXT NOT NULL)")
        print("   - query_gerada (TEXT NOT NULL)")
        print("   - query_correta (TEXT)")
        print("   - validada (BOOLEAN DEFAULT FALSE)")
        print("   - aprovada (BOOLEAN)")
        print("   - observacoes (TEXT)")
        print("   - data_criacao (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        print("   - data_validacao (TIMESTAMP)")
        print("   - validado_por (VARCHAR(255))")
        
        # Verifica se a tabela foi criada
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'ai_query_learning'
        """)
        count = cur.fetchone()[0]
        
        if count > 0:
            print(f"\n✅ Verificação: Tabela encontrada no banco de dados!")
        
        cur.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"\n❌ ERRO ao criar tabela: {e}")
        print(f"   Código do erro: {e.pgcode}")
        print(f"   Detalhes: {e.pgerror}")
        
    except Exception as e:
        print(f"\n❌ ERRO inesperado: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CRIAÇÃO DA TABELA AI_QUERY_LEARNING")
    print("=" * 60)
    criar_tabela_ai_learning()
    print("=" * 60)
