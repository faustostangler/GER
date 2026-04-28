# Dependências do Projeto — GER

Este documento lista as dependências do projeto identificadas pelo agente **Scout**.

## Gerenciador de Pacotes
- **uv** (detectado via `uv.lock` e `pyproject.toml`)

## Dependências de Produção (via `pyproject.toml`)

| Dependência | Versão | Descrição/Uso |
| :--- | :--- | :--- |
| `duckdb` | `>=1.0.0` | Banco de dados OLAP |
| `streamlit` | `>=1.37.0` | Interface do Usuário (Analytics) |
| `pandas` | `>=2.2.0` | Manipulação de dados |
| `plotly` | `>=5.19.0` | Gráficos e visualizações |
| `pydantic` | `>=2.6.0` | Validação de dados |
| `pydantic-settings` | `>=2.2.0` | Configurações via variáveis de ambiente |
| `python-dotenv` | `>=1.0.1` | Carregamento de arquivo `.env` |
| `playwright` | `>=1.42.0` | Automação/Scraping |
| `httpx` | `>=0.27.0` | Cliente HTTP assíncrono |
| `pyarrow` | `>=15.0.0` | Processamento de dados eficiente |
| `fastapi` | `>=0.110.0` | API Backend |
| `uvicorn` | `>=0.29.0` | Servidor ASGI |
| `PyJWT` | `>=2.8.0` | Autenticação JWT |
| `cryptography` | `>=46.0.7` | Criptografia |
| `aiokafka` | `>=0.10.0` | Integração com Kafka/Redpanda |
| `arq` | `>=0.25.0` | Fila de tarefas assíncronas (Redis) |
| `redis` | `>=5.0.0` | Cache e Fila |
| `s3fs` | `>=2026.3.0` | Integração com S3 |
| `python-json-logger` | `>=4.1.0` | Logs estruturados em JSON |
| `prometheus-client` | `>=0.24.1` | Métricas para monitoramento |
| `rq` | `>=2.7.0` | Fila de tarefas (alternativa) |
| `sentry-sdk` | `>=2.0.0` | Rastreamento de erros |
| `opentelemetry-api` | `>=1.40.0` | Telemetria |
| `opentelemetry-sdk` | `>=1.40.0` | Telemetria |
| `sqlalchemy` | `>=2.0.49` | ORM |
| `psycopg` | `>=3.3.3` | Driver PostgreSQL |

## Dependências de Desenvolvimento

| Dependência | Versão | Descrição/Uso |
| :--- | :--- | :--- |
| `pytest` | `>=8.0.0` | Framework de testes |
| `ruff` | `>=0.3.0` | Linter e Formatador |
| `mypy` | `>=1.9.0` | Verificador de tipos estáticos |
| `mutmut` | `>=2.4.4` | Testes de mutação |
| `pytest-playwright` | `>=0.7.2` | Testes de integração com Playwright |
| `tenacity` | `>=9.1.4` | Resiliência e retentativas |
| `pytest-cov` | `>=7.1.0` | Cobertura de testes |
| `pytest-asyncio` | `>=1.3.0` | Suporte a testes assíncronos |
