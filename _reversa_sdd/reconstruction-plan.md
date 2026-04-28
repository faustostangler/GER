# Plano de Reconstrução — GER

> Criado pelo Reconstructor em 2026-04-28
> Projeto: GER
> Nível: Detalhado

## Alertas de Pré-Voo ⚠️
*Não há gaps críticos ou lacunas que impeçam o início da reconstrução.*

---

## Tarefas de Reconstrução (Bottom-Up)

### 1. Schema do Banco de Dados (OLAP/DuckDB & ODS/SQLite)
- **Status**: `pending`
- **Lê**: `_reversa_sdd/database/erd.md`, `_reversa_sdd/database/data-dictionary.md`
- **Pronto quando**: Schemas das tabelas `solicitacoes_raw`, `ingestion_logs`, `dead_letter_queue` (SQLite) e a view virtual `gercon` (DuckDB) estiverem implementados com os tipos corretos.

### 2. Entidades de Domínio e Value Objects
- **Status**: `pending`
- **Lê**: `_reversa_sdd/sdd/domain-models.md`
- **Pronto quando**: Entidades `DoctorProfile`, `IngestionLogEntry` e Value Objects `ClinicaPolicy` estiverem implementados usando Pydantic V2.

### 3. Especificações de Domínio (Filtros)
- **Status**: `pending`
- **Lê**: `_reversa_sdd/sdd/specifications.md`
- **Pronto quando**: Padrão Specification implementado para filtros avançados de pacientes e lead time.

### 4. Infraestrutura: Repositório DuckDB (OLAP)
- **Status**: `pending`
- **Lê**: `_reversa_sdd/sdd/duckdb-repository.md`, `_reversa_sdd/design-system/design-system.md`
- **Pronto quando**: Tradutor de especificações para SQL DuckDB e cache Redis via PyArrow Feather implementados com fallback automático.

### 5. Infraestrutura: Scraper Playwright
- **Status**: `pending`
- **Lê**: `_reversa_sdd/sdd/playwright-scraper.md`
- **Pronto quando**: Navegação headless autenticada e extração de dados via seletores CSS funcionando sem CAPTCHAs frequentes.

### 6. Aplicação: Scraper Use Case (Orquestração)
- **Status**: `pending`
- **Lê**: `_reversa_sdd/sdd/scraper-use-case.md`
- **Pronto quando**: Fluxo de ingestão com Circuit Breaker (5% erro/min 100 hits) e Dead Letter Queue para payloads inválidos operando.

### 7. Aplicação: Analytics Use Case
- **Status**: `pending`
- **Lê**: `_reversa_sdd/sdd/analytics-use-case.md`
- **Pronto quando**: Agregações de dados clínicos prontas para alimentar o BFF/Streamlit.

---

## Como Prosseguir
Diga **INICIAR** ou **execute a tarefa 1** para começar a implementação do zero.
