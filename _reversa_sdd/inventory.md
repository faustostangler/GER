# Inventário do Projeto — GER

Este documento contém o mapeamento da estrutura e componentes do projeto **GER**, realizado pelo agente **Scout**.

## Estrutura de Pastas Principal

```
GER/
├── .github/workflows/   # Pipelines de CI/CD
├── docs/                # Documentação (ADRs, Glossário)
├── env/                 # Configurações de ambiente (.env)
├── infra/               # Infraestrutura (Keycloak, Router)
├── monitoring/          # Observabilidade (Grafana, Prometheus)
├── src/                 # Código-fonte (Hexagonal Architecture)
│   ├── domain/          # Entidades, Modelos e Especificações
│   ├── application/     # Casos de Uso e Interfaces
│   ├── infrastructure/  # Adaptadores, Bancos de Dados, Repositórios
│   └── presentation/    # API e Interface Streamlit
├── tests/               # Suíte de testes (Unitários, Integração, E2E)
├── Dockerfile           # Containerização da aplicação
├── docker-compose.yml   # Orquestração dos serviços
├── Makefile             # Atalhos de automação
└── pyproject.toml       # Gerenciamento de dependências (uv)
```

## Tecnologias e Frameworks

- **Linguagem Principal:** Python 3.12
- **Interface do Usuário:** Streamlit
- **Banco de Dados:** DuckDB (OLAP), SQLite (Raw Data), PostgreSQL (Keycloak)
- **Processamento de Dados:** Pandas, PyArrow
- **Comunicação/Mensageria:** AIOConsumer (Kafka/Redpanda)
- **Fila/Cache:** Redis, Arq
- **Observabilidade:** Prometheus, Grafana, Loki, Sentry
- **Infraestrutura:** Docker, Nginx, Keycloak (OAuth2/OIDC)

## Pontos de Entrada (Entry Points)

- `src/presentation/app_analytics.py` — Dashboard Streamlit
- `src/presentation/api/` — Endpoints FastAPI
- `worker.py` — Arq Worker para tarefas em background
- `dom_scraper.py` / `master_scraper.py` — Scripts de Scraping

## Banco de Dados e Persistência

- `gercon_raw_data.db` — Banco SQLite com dados brutos.
- `src/infrastructure/repositories/duckdb_repository.py` — Implementação de persistência analítica.

## Cobertura de Testes

- **Framework:** Pytest
- **Estratégia:** TDD e Testes de Mutação (Mutmut)
- **Total de Arquivos de Teste:** 29
