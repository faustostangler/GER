# Dicionário de Dados - Módulo Analytics & Ingestão

Este documento detalha o modelo físico e analítico de dados do sistema GER, cobrindo as tabelas operacionais (SQLite) e o modelo OLAP (DuckDB).

## 1. Modelo Analítico (DuckDB / Parquet)

O repositório analítico (`DuckDBAnalyticsRepository`) lê a visão `gercon` gerada a partir do arquivo Parquet consolidado.

### Tabela/Visão: `gercon`

| Campo | Tipo DuckDB | Descrição | Restrição |
| :--- | :--- | :--- | :--- |
| `numeroCMCE` | `VARCHAR` | Identificador único da solicitação clínica | Chave Primária Lógica |
| `dataSolicitacao` | `VARCHAR`/`DATE` | Data em que a solicitação foi realizada | Formato: YYYY-MM-DD |
| `dataCadastro` | `VARCHAR`/`TIMESTAMP` | Data de registro da solicitação no sistema | Formato: ISO-8601 |
| `origem_lista` | `VARCHAR` | Identificador da lista de regulação de origem | |
| `medicoSolicitante` | `VARCHAR` | Nome ou CRM do profissional requisitante | |
| `entidade_especialidade_descricao` | `VARCHAR` | Descrição da sub-especialidade médica | |
| `entidade_especialidade_especialidadeMae_descricao` | `VARCHAR` | Descrição da especialidade médica mãe | |
| `entidade_cidPrincipal_descricao` | `VARCHAR` | Descrição do diagnóstico (CID-10) | |
| `entidade_classificacaoRisco_cor` | `VARCHAR` | Cor atribuída pela triagem de risco (Ex: VERMELHO) | Data Contract Guard |

## 2. Modelo Operacional (SQLite: `gercon_raw_data.db`)

Utilizado para armazenar os dados em formato bruto capturados pelo scraper, auditorias e tratamento de falhas.

### Tabela: `solicitacoes_raw`

| Campo | Tipo SQLite | Descrição | Restrição |
| :--- | :--- | :--- | :--- |
| `protocolo` | `TEXT` | Chave primária e identificador do protocolo CMCE | PRIMARY KEY |
| `data_captura` | `TIMESTAMP` | Momento da execução da captura pelo scraper | DEFAULT CURRENT_TIMESTAMP |
| `data_alteracao` | `INTEGER` | Timestamp Unix do último estado conhecido | |
| `conteudo_json` | `TEXT` | Payload JSON bruto capturado via vendor API | |
| `origem_lista` | `TEXT` | Nome da fila de atendimento | |

### Tabela: `ingestion_logs`

Auditoria técnica estruturada para Post-Mortem e Golden Signals do pipeline.

| Campo | Tipo SQLite | Descrição |
| :--- | :--- | :--- |
| `id` | `INTEGER` | Chave primária autoincrementada |
| `timestamp` | `REAL` | Timestamp Unix do início do ciclo |
| `duration_seconds` | `REAL` | Tempo total de execução do scraping |
| `status` | `TEXT` | Estado final (`SUCCESS`, `PARTIAL`, `FAILURE`, `CIRCUIT_BREAKER`) |
| `items_ingested` | `INTEGER` | Quantidade de registros processados com sucesso |
| `items_failed` | `INTEGER` | Quantidade de registros problemáticos |
| `bytes_processed` | `INTEGER` | Tamanho total dos payloads trafegados |
| `target_lists` | `TEXT` | JSON array das listas mapeadas |
| `error_message` | `TEXT` | Trace de exceção para status de erro |

### Tabela: `dead_letter_queue` (DLQ)

Destino isolado para "poison pills" e dados que violam o contrato do domínio.

| Campo | Tipo SQLite | Descrição |
| :--- | :--- | :--- |
| `id` | `INTEGER` | Chave primária autoincrementada |
| `timestamp` | `DATETIME` | Horário do incidente |
| `target_list` | `TEXT` | Fila associada |
| `payload` | `TEXT` | O JSON bruto causador da violação |
| `error_message` | `TEXT` | Detalhes do erro de validação Pydantic |
