# ADR-001: Redis como Cache Distribuído para Queries OLAP

## Status
**Accepted** — 2026-04-03

## Contexto
O Streamlit utiliza `@st.cache_data` (cache in-memory) para evitar reprocessamento de queries DuckDB. No entanto, em deploy escalado no Google Cloud Run (multi-instância serverless), cada instância mantém seu próprio cache local, resultando em:
- **Cache Miss Amplificado:** N instâncias × M queries = N×M execuções idênticas no Parquet.
- **Inconsistência de Estado:** Instância A mostra dados diferentes da B durante a janela de invalidação.
- **Cold Start Penalty:** Cada nova instância escalada começa do zero.

## Decisão
Migrar o cache de queries OLAP do `@st.cache_data` (process-local) para **Redis** (cache distribuído), utilizando serialização **PyArrow IPC** (Feather) ao invés de `pickle`.

### Alternativas Consideradas
| Opção | Prós | Contras |
|---|---|---|
| `@st.cache_data` | Zero infra adicional | Não escala horizontalmente |
| Redis + `pickle` | Simples de implementar | CPU-bound em DataFrames grandes; não é language-agnostic |
| Redis + PyArrow IPC | ~40-60% mais rápido; interoperável Arrow | Dependência de `pyarrow` (já existente via DuckDB) |
| Memcached | Mais simples que Redis | Sem TTL nativo; sem persistência |

### Degradação Graciosa
Se o Redis estiver indisponível:
1. O construtor do `DuckDBAnalyticsRepository` tenta `ping()` com timeout de 2s.
2. Se falhar, define `self.redis_client = None` e loga `CRITICAL`.
3. `_query()` verifica `if self.redis_client:` antes de cada operação de cache.
4. O fluxo continua normalmente via query direta no Parquet.

## Consequências
- **Positivas:** Consistência de cache cross-instância; redução de latência P90 em 40-60%; fallback transparente.
- **Negativas:** Dependência adicional de infraestrutura (Redis); necessidade de monitorar Redis via Prometheus.
- **Riscos Mitigados:** Chaos test `test_chaos_resilience.py::TestRedisChaos` valida a degradação graciosa automaticamente.
