# Guia de QA e Testes - Portal de Gestão (Admin)

Este documento detalha os cenários de teste para o **Portal de Gestão de Usuários e Permissões** (`consultas_updated_v2.py`). O sistema é crítico para garantir a segurança e a correta distribuição de acesso aos dados do ERP.

## 1. Visão Geral e Escopo

O portal é dividido em 12 funcionalidades principais. O QA deve validar cada uma delas conforme os cenários abaixo.

**URL Padrão:** `http://localhost:8501`
**Usuário Admin Padrão:** (Verificar credenciais no código ou banco)

---

## 2. Cenários de Teste por Funcionalidade

### 2.1. Autenticação e Sessão
**Objetivo:** Garantir que apenas usuários autorizados acessem o portal e que a sessão seja segura.

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **AUTH-01** | Login com Sucesso | 1. Acessar portal.<br>2. Inserir usuário/senha válidos.<br>3. Marcar "Manter conectado".<br>4. Clicar em Entrar. | Redirecionar para Dashboard. Token de sessão gerado na URL. |
| **AUTH-02** | Login Inválido | 1. Tentar login com senha errada. | Exibir mensagem de erro "Usuário ou senha inválidos". |
| **AUTH-03** | Logout | 1. Estando logado, clicar em "Logout" na sidebar. | Sessão encerrada, token removido da URL, redirecionar para login. |
| **AUTH-04** | Timeout de Sessão | 1. Logar.<br>2. Aguardar 30 min (ou simular alterando banco).<br>3. Tentar navegar. | Deslogar automaticamente e pedir login novamente. |

### 2.2. Gestão de Grupos de Empresas
**Objetivo:** Validar o cadastro da estrutura hierárquica superior (Holding/Grupo).

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **GRP-01** | Criar Grupo | 1. Menu "Grupos de Empresas".<br>2. Preencher Nome, Banco, Versão, CNPJ Matriz.<br>3. Salvar. | Mensagem de sucesso. Grupo aparece na lista. |
| **GRP-02** | Validação CNPJ | 1. Tentar criar grupo com CNPJ incompleto. | Erro "CNPJ inválido". Não salvar. |
| **GRP-03** | Exclusão em Cascata | 1. Selecionar grupo existente.<br>2. Digitar nome para confirmar exclusão.<br>3. Clicar em Excluir. | **CRÍTICO:** Grupo e TODAS as empresas/módulos vinculados devem ser excluídos. |

### 2.3. Gestão de Empresas
**Objetivo:** Validar o cadastro de empresas filiais vinculadas a um grupo.

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **EMP-01** | Criar Empresa | 1. Menu "Empresas".<br>2. Preencher Nome, CNPJ.<br>3. Vincular a um Grupo existente.<br>4. Salvar. | Mensagem de sucesso. Empresa listada no filtro do grupo. |
| **EMP-02** | Duplicidade | 1. Tentar criar empresa com nome já existente. | Erro "Já existe uma Empresa com este nome". |

### 2.4. Catálogo de Módulos (e Importação Excel)
**Objetivo:** Validar a definição dos módulos do sistema e a importação de metadados.

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **MOD-01** | Criar Módulo | 1. Menu "Catálogo de Módulos".<br>2. Selecionar Grupo.<br>3. Preencher Código e Nome.<br>4. Criar. | Módulo criado com sucesso. |
| **MOD-02** | Importar Excel | 1. Selecionar Módulo criado.<br>2. Upload de Excel (colunas: TABELA, COLUNA, DESCRICAO).<br>3. Clicar "Importar". | Sistema lê o Excel, exibe prévia das tabelas e salva no banco. |
| **MOD-03** | Excluir Tabela | 1. Na lista de tabelas do módulo, selecionar uma.<br>2. Confirmar exclusão. | Tabela removida do catálogo. |

### 2.5. Tabelas (Metadados)
**Objetivo:** Validar a visualização e edição de metadados das colunas.

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **TAB-01** | Editar Coluna | 1. Menu "Tabelas".<br>2. Selecionar Tabela.<br>3. Editar descrição de uma coluna no grid.<br>4. Salvar. | Alteração persistida no banco. |

### 2.6. Perfis de Usuário
**Objetivo:** Validar a criação de perfis de acesso (papéis).

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **PRF-01** | Criar Perfil | 1. Menu "Perfis".<br>2. Selecionar Empresa.<br>3. Definir nome (ex: "Vendedor").<br>4. Salvar. | Perfil criado. |
| **PRF-02** | Exclusão Bloqueada | 1. Tentar excluir perfil que tem usuários vinculados. | Erro "Não é possível excluir... existem usuários vinculados". |

### 2.7. Permissões (Editor Avançado)
**Objetivo:** Validar o motor de regras de acesso (Row-Level Security e Column Security). **Funcionalidade mais complexa.**

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **PERM-01** | Bloqueio de Coluna | 1. Menu "Permissões".<br>2. Selecionar Perfil e Tabela.<br>3. Em "Bloqueio por Colunas", selecionar `salario`.<br>4. Salvar. | Coluna `salario` salva na lista de bloqueios. |
| **PERM-02** | Filtro de Linha (UI) | 1. Adicionar Regra.<br>2. Campo: `uf`. Operador: `IN`. Valores: `SP`, `RJ`.<br>3. Verificar Prévia. | Prévia SQL deve mostrar: `NOT (uf IN ('SP', 'RJ'))` (se for lógica de deny) ou filtro correspondente. |
| **PERM-03** | Persistência | 1. Salvar permissão.<br>2. Recarregar página.<br>3. Voltar na mesma tabela. | As regras configuradas devem reaparecer exatamente como estavam. |

### 2.8. Gestão de Usuários (Aplicação e Portal)
**Objetivo:** Validar o cadastro de usuários e seus acessos.

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **USR-01** | Criar Usuário App | 1. Menu "Usuários - Aplicação".<br>2. Preencher Nome, Senha.<br>3. Vincular Empresa e Perfil.<br>4. Salvar. | Usuário criado. |
| **USR-02** | Acesso ao Portal | 1. No cadastro de usuário, marcar "Permitir acesso ao portal".<br>2. Salvar.<br>3. Ir em "Usuários - Portal". | Usuário deve aparecer na lista de usuários do portal. |
| **USR-03** | Login Portal | 1. Deslogar do admin.<br>2. Tentar logar com o usuário criado no passo anterior. | Login deve funcionar (se não for admin, menu será restrito). |

### 2.9. Logs e Auditoria
**Objetivo:** Garantir rastreabilidade.

| ID | Cenário | Passos | Resultado Esperado |
| :--- | :--- | :--- | :--- |
| **LOG-01** | Verificar Log | 1. Realizar uma alteração (ex: criar perfil).<br>2. Ir no menu "Logs".<br>3. Filtrar pela data de hoje. | Deve existir um registro do tipo `CREATE` na entidade `perfil` feito pelo seu usuário. |
| **LOG-02** | Log de Interação | 1. Menu "Logs de Interações". | Deve listar conversas do chat (se houver uso do `chat.py`). |

---

## 3. Automação Recomendada (Playwright)

Para a automação, foque nos fluxos **GRP-01**, **EMP-01**, **USR-01** e **AUTH-01**, pois são a base para todo o resto.

**Exemplo de Teste de Permissão (PERM-01):**

```python
def test_criar_bloqueio_coluna(page: Page):
    # ... login ...
    page.get_by_role("button", name="Permissões (por Perfil)").click()
    
    # Selecionar Perfil e Tabela (pode exigir interação com selectbox do Streamlit)
    # Dica: Streamlit usa 'role=combobox' e é chato de selecionar. 
    # Use page.get_by_label("Selecione o Perfil").click() e depois clique na opção.
    
    # Selecionar coluna para bloquear
    page.get_by_text("Selecione as colunas a bloquear").click()
    page.get_by_text("SALARIO").click() # Exemplo de coluna
    page.keyboard.press("Escape") # Fechar dropdown
    
    page.get_by_role("button", name="Salvar/Atualizar Permissão").click()
    
    expect(page.get_by_text("Permissão salva/atualizada com sucesso")).to_be_visible()
```

## 4. Dicas para o QA

*   **Cuidado com Exclusões:** As funções de "Exclusão em Cascata" são destrutivas. Teste sempre em ambiente de homologação/local.
*   **Distinct Cache:** O editor de permissões faz queries no banco (RM) para buscar valores distintos (`SELECT DISTINCT`). Se o banco estiver lento, a UI pode travar. Isso é um ponto de atenção para testes de performance.
*   **Validação de JSON:** As permissões são salvas como JSON no banco. Vale a pena verificar no banco (`SELECT * FROM permissao`) se o JSON gerado está válido após salvar na UI.
