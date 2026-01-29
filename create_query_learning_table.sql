-- =====================================================
-- Script de Criação da Tabela de Aprendizado de Queries
-- =====================================================
-- Objetivo: Armazenar queries SQL geradas pela IA para
-- permitir validação e aprendizado contínuo
-- =====================================================

CREATE TABLE IF NOT EXISTS query_learning (
    -- Identificador único
    id SERIAL PRIMARY KEY,
    
    -- Pergunta original do usuário
    pergunta_usuario TEXT NOT NULL,
    
    -- Query SQL gerada pela IA (a ser validada)
    query_gerada TEXT NOT NULL,
    
    -- Query SQL correta (preenchida após revisão)
    query_correta TEXT NULL,
    
    -- Status da validação
    status_validacao VARCHAR(20) DEFAULT 'pendente' 
        CHECK (status_validacao IN ('pendente', 'aprovada', 'corrigida')),
    
    -- Timestamps
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_validacao TIMESTAMP NULL,
    
    -- Rastreabilidade
    validado_por VARCHAR(100) NULL,
    
    -- Observações da equipe de revisão
    observacoes TEXT NULL
);

-- Criar índices para melhorar performance de consultas
CREATE INDEX idx_query_learning_status ON query_learning(status_validacao);
CREATE INDEX idx_query_learning_data_criacao ON query_learning(data_criacao);

-- Comentários na tabela e colunas para documentação
COMMENT ON TABLE query_learning IS 'Tabela para armazenar queries SQL geradas pela IA e suas validações para aprendizado contínuo';
COMMENT ON COLUMN query_learning.id IS 'Identificador único auto-incrementado';
COMMENT ON COLUMN query_learning.pergunta_usuario IS 'Pergunta original feita pelo usuário';
COMMENT ON COLUMN query_learning.query_gerada IS 'Query SQL gerada pela IA (a ser validada)';
COMMENT ON COLUMN query_learning.query_correta IS 'Query SQL correta após revisão (NULL se ainda não revisada)';
COMMENT ON COLUMN query_learning.status_validacao IS 'Status: pendente, aprovada ou corrigida';
COMMENT ON COLUMN query_learning.data_criacao IS 'Data e hora de criação do registro';
COMMENT ON COLUMN query_learning.data_validacao IS 'Data e hora da validação';
COMMENT ON COLUMN query_learning.validado_por IS 'Usuário que realizou a validação';
COMMENT ON COLUMN query_learning.observacoes IS 'Comentários sobre a validação';

-- =====================================================
-- Exemplo de Uso
-- =====================================================

-- 1. Inserir uma query gerada pela IA
-- INSERT INTO query_learning (pergunta_usuario, query_gerada)
-- VALUES ('Quantos funcionários tem?', 'SELECT COUNT(*) FROM funcionarios');

-- 2. Aprovar uma query (quando está correta)
-- UPDATE query_learning 
-- SET status_validacao = 'aprovada',
--     query_correta = query_gerada,
--     data_validacao = CURRENT_TIMESTAMP,
--     validado_por = 'Nome do Revisor'
-- WHERE id = 1;

-- 3. Corrigir uma query (quando está incorreta)
-- UPDATE query_learning 
-- SET status_validacao = 'corrigida',
--     query_correta = 'SELECT COUNT(*) FROM employees',
--     data_validacao = CURRENT_TIMESTAMP,
--     validado_por = 'Nome do Revisor',
--     observacoes = 'Nome da tabela estava incorreto'
-- WHERE id = 1;

-- 4. Buscar queries validadas para treinamento da IA
-- SELECT pergunta_usuario, query_correta 
-- FROM query_learning 
-- WHERE status_validacao IN ('aprovada', 'corrigida')
-- ORDER BY data_validacao DESC;
