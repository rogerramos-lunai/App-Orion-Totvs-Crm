import psycopg2
from datetime import datetime

# Configurações do banco
DB_CONFIG = {
    'host': '56.125.69.27',
    'port': 5432,
    'dbname': 'n8n_db',
    'user': 'n8n',
    'password': 'n8n_pass_2024'
}

def testar_extracao_sql():
    """
    Testa a função de extração de SQL
    """
    import re
    
    def extrair_sql_da_resposta(resposta):
        # Tenta encontrar SQL entre blocos de código markdown
        sql_block = re.search(r'```sql\s*([\s\S]+?)```', resposta, re.IGNORECASE)
        if sql_block:
            return sql_block.group(1).strip()
        
        # Tenta encontrar SQL entre blocos de código genéricos
        code_block = re.search(r'```\s*([\s\S]+?)```', resposta)
        if code_block:
            content = code_block.group(1).strip()
            if any(keyword in content.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE']):
                return content
        
        # Procura por linhas que começam com SELECT, INSERT, UPDATE, etc
        lines = resposta.split('\n')
        sql_lines = []
        capturing = False
        
        for line in lines:
            line_upper = line.strip().upper()
            if any(line_upper.startswith(kw) for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']):
                capturing = True
                sql_lines.append(line.strip())
            elif capturing:
                if line.strip():
                    sql_lines.append(line.strip())
                    if ';' in line:
                        break
                else:
                    break
        
        if sql_lines:
            return '\n'.join(sql_lines)
        
        return resposta.strip()
    
    # Testes
    print("=" * 60)
    print("🧪 TESTANDO EXTRAÇÃO DE SQL")
    print("=" * 60)
    
    # Teste 1: SQL em bloco markdown
    resposta1 = """
    Aqui está a consulta:
    
    ```sql
    SELECT * FROM usuarios WHERE ativo = true;
    ```
    
    Isso retorna todos os usuários ativos.
    """
    
    sql1 = extrair_sql_da_resposta(resposta1)
    print("\n📝 Teste 1 - SQL em bloco markdown:")
    print(f"Extraído: {sql1}")
    print(f"✅ Correto!" if "SELECT * FROM usuarios" in sql1 else "❌ Erro!")
    
    # Teste 2: SQL direto
    resposta2 = """
    SELECT COUNT(*) as total
    FROM funcionarios
    WHERE departamento = 'TI';
    """
    
    sql2 = extrair_sql_da_resposta(resposta2)
    print("\n📝 Teste 2 - SQL direto:")
    print(f"Extraído: {sql2}")
    print(f"✅ Correto!" if "SELECT COUNT(*)" in sql2 else "❌ Erro!")
    
    # Teste 3: SQL em bloco genérico
    resposta3 = """
    ```
    INSERT INTO logs (mensagem, data) VALUES ('teste', NOW());
    ```
    """
    
    sql3 = extrair_sql_da_resposta(resposta3)
    print("\n📝 Teste 3 - SQL em bloco genérico:")
    print(f"Extraído: {sql3}")
    print(f"✅ Correto!" if "INSERT INTO logs" in sql3 else "❌ Erro!")

def testar_insercao_banco():
    """
    Testa inserção direta no banco
    """
    print("\n" + "=" * 60)
    print("💾 TESTANDO INSERÇÃO NO BANCO")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Insere um registro de teste
        pergunta_teste = "Teste: Quantos usuários existem?"
        query_teste = "SELECT COUNT(*) FROM usuarios;"
        
        cur.execute("""
            INSERT INTO ai_query_learning (pergunta_usuario, query_gerada, validada, data_criacao)
            VALUES (%s, %s, FALSE, %s)
        """, (pergunta_teste, query_teste, datetime.utcnow()))
        
        conn.commit()
        print("✅ Registro de teste inserido com sucesso!")
        
        # Verifica se foi inserido
        cur.execute("SELECT COUNT(*) FROM ai_query_learning")
        count = cur.fetchone()[0]
        print(f"📊 Total de registros na tabela: {count}")
        
        # Mostra o último registro
        cur.execute("""
            SELECT id, pergunta_usuario, query_gerada, data_criacao 
            FROM ai_query_learning 
            ORDER BY data_criacao DESC 
            LIMIT 1
        """)
        
        row = cur.fetchone()
        if row:
            print(f"\n📋 Último registro:")
            print(f"   ID: {row[0]}")
            print(f"   Pergunta: {row[1]}")
            print(f"   Query: {row[2]}")
            print(f"   Data: {row[3]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERRO: {e}")

if __name__ == "__main__":
    testar_extracao_sql()
    testar_insercao_banco()
    print("\n" + "=" * 60)
    print("✅ TESTES CONCLUÍDOS!")
    print("=" * 60)
