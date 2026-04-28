# Análise de Código - Módulo Analytics

Este documento detalha o funcionamento interno do módulo `analytics` da plataforma GER, mapeando suas responsabilidades, algoritmos e fluxos de dados sob a ótica da Arquitetura Hexagonal e do Domain-Driven Design (DDD).

## 1. Arquitetura e Componentes

O módulo `analytics` é estruturado seguindo o padrão de Portas e Adaptadores:

*   **Presentation (Adapters Primários)**
    *   `app_analytics.py`: Interface do usuário construída em Streamlit. Atua como um *Humble Object*, delegando toda a lógica de negócio.
    *   `presentation.di_container`: Resolve as dependências de infraestrutura e casos de uso.
*   **Application (Casos de Uso)**
    *   `analytics_use_case.py`: Orquestrador das operações analíticas. Transforma objetos de domínio em respostas estruturadas para a UI.
*   **Domain (Regras de Negócio)**
    *   `FiltroAvancadoSpec`, `PacienteUrgenteSpec`, `PacienteVencidoSpec`: Especificações de filtragem baseadas no *Specification Pattern*.
*   **Infrastructure (Adapters Secundários)**
    *   `DuckDBAnalyticsRepository`: Implementação concreta do repositório analítico baseado em OLAP (Parquet).
    *   `DuckDBCriteriaTranslator`: Tradutor puro de Especificações para o dialeto SQL do DuckDB.

## 2. Análise Detalhada das Classes e Métodos

### 2.1. `AnalyticsUseCase`

Classe core do processamento de regras analíticas.

*   `verify_data_readiness()`: Verifica se o arquivo Parquet de leitura está disponível.
*   `get_clinical_audit_heatmap(spec, user)`: Computa uma matriz de risco de atores clínicos (Médicos vs Diagnósticos) utilizando Z-Score.
    *   *Complexidade Algorítmica*: Alta. Utiliza CTEs (`TopAtores`, `TopDiags`) e operações de vetorização em Pandas (`df_math.sub(...).div(...)`).
*   `get_executive_summary(spec, user)`: Consolida as principais métricas da plataforma.

### 2.2. `DuckDBAnalyticsRepository`

Gerencia o estado OLAP e a persistência otimizada.

*   *Mecanismo de Cache*: Implementa cache via PyArrow IPC (Feather) serializado e persistido no Redis.
*   *Segurança e Isolamento (RLS)*: Aplica filtros de acesso utilizando expressões CTE dinâmicas (`_get_rls_cte`).

### 2.3. `DuckDBCriteriaTranslator`

Mapeia especificações para predicados SQL.

*   Suporta operações booleanas (`AND`, `OR`, `NOT`), faixas de data (`BETWEEN`), limites numéricos e pesquisas avançadas tolerantes a acentos (`strip_accents`).

## 3. Máquinas de Estado e Fluxos de Controle

A interface Streamlit opera em um ciclo reativo padrão, dividindo-se em 3 fases:
1.  **Boot Infra/DX**: Configuração de Sentry, estilos CSS customizados e limites de renderização.
2.  **Identity Gatekeeper**: Autenticação validada através do Identity Aware Proxy.
3.  **Domain Execution**: Resolução dos KPIs analíticos a partir dos critérios de filtragem ativos.

## 4. Algoritmos Críticos e Salvaguardas

*   **Proteção OOM**: Limitação explícita de RAM no DuckDB (`PRAGMA memory_limit`).
*   **Fallback de Falhas**: Em caso de desconexão do Redis, o sistema degrada suavemente para leitura direta do Parquet sem interromper o fluxo de atendimento.
