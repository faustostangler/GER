# Análise de Código - GER

Este documento detalha o funcionamento interno da plataforma GER sob a ótica da Arquitetura Hexagonal e do Domain-Driven Design (DDD).

---

## Módulo 1: Analytics

### 1. Arquitetura e Componentes
O módulo `analytics` é estruturado seguindo o padrão de Portas e Adaptadores:
*   **Presentation**: `app_analytics.py`, `presentation.di_container`.
*   **Application**: `analytics_use_case.py`.
*   **Domain**: `FiltroAvancadoSpec`, `PacienteUrgenteSpec`, `PacienteVencidoSpec`.
*   **Infrastructure**: `DuckDBAnalyticsRepository`, `DuckDBCriteriaTranslator`.

### 2. Análise Detalhada das Classes
*   **AnalyticsUseCase**: Orquestra o read model. Destaque para `get_clinical_audit_heatmap` (Z-Score em Pandas).
*   **DuckDBAnalyticsRepository**: Modo OLAP colunar. Cache PyArrow Feather + Redis. RLS em nível de linha.

---

## Módulo 2: Scraper (Ingestão Assíncrona)

### 1. Arquitetura e Componentes
*   **Application**: `ScraperUseCase` (Orquestrador principal de fluxos, paginação e segurança).
*   **Domain Contract (ACL)**: `GerconPayloadContract` (Garante que pílulas venenosas externas não quebrem o domínio).
*   **Infrastructure Adapters**: 
    *   `PlaywrightGerconAdapter`: Automação e navegação JS no frontend Angular legado do Vendor.
    *   `SQLiteRawRepository`: Camada de buffer e auditoria técnica Post-Mortem.
    *   `ParquetDataRepository`: Persistência em formato colunar otimizado para escrita e leitura DuckDB (S3/Local).

### 2. Máquina de Estados e Circuit Breaker
A ingestão implementa salvaguardas rigorosas contra APIs instáveis:
*   **DLQ Fail-Fast**: Validações falhas no Pydantic isolam o payload quebrado na tabela `dead_letter_queue`.
*   **Circuit Breaker**: Taxa máxima de 5% de falhas sobre o volume processado (mínimo de 100 iterações). Caso ultrapassado, aborta preventivamente o processamento para barrar custos de armazenamento e CPU.

### 3. Mecanismos de Resiliência I/O
*   Utilização estratégica do Tenacity para controle de conexões intermitentes.
*   Gravações atômicas com POSIX Sync (`os.replace`) para arquivos de checkpoint.

