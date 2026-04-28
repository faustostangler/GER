# ADR-002: Circuit Breaker no Scraper Use Case

## Status
**Accepted** — 2026-04-03

## Contexto
O Scraper consome dados de uma API Angular legada (Gercon/Procempa) que frequentemente retorna payloads corrompidos, schemas alterados sem aviso, ou falhas intermitentes de rede. Sem proteção, o Worker:
- Pode popular o Parquet com dados inválidos (efeito "Garbage In, Garbage Out" no Dashboard).
- Pode estourar o custo de DLQ (Dead Letter Queue) em S3 ao processar milhares de registros inválidos.
- Pode travar indefinidamente tentando processar um payload "envenenado".

## Decisão
Implementar um **Circuit Breaker baseado em threshold de erro** no `ScraperUseCase`:

```python
CB_THRESHOLD_RATIO = 0.05   # 5% de falhas
CB_MIN_HITS = 100            # Mínimo de registros processados antes de avaliar
```

### Fluxo de Decisão
1. Cada registro processado incrementa `cb_total_processed`.
2. Cada falha de validação (Pydantic `ValidationError`) ou payload corrupto incrementa `cb_error_count`.
3. O Circuit Breaker só é avaliado após `CB_MIN_HITS` (evitar falso positivo em amostras pequenas).
4. Se `cb_error_count / cb_total_processed > CB_THRESHOLD_RATIO`, dispara `DomainContractViolationException` e marca `IngestionStatus.CIRCUIT_BREAKER` no audit log.

### Status Granulares no Audit Log
| Status | Significado |
|---|---|
| `SUCCESS` | Ciclo completo sem falhas |
| `PARTIAL` | Ciclo completo com poison pills na DLQ |
| `FAILURE` | Falha de login, rede ou exceção não tratada |
| `CIRCUIT_BREAKER` | API do vendor radicalmente quebrada (>5% schema violations) |

## Consequências
- **Positivas:** Protege a integridade do Data Lake; limita o blast radius de falhas do vendor; gera audit trail para Post-Mortem.
- **Negativas:** Pode abortar prematuramente se o threshold for muito agressivo (mitigado pelo `CB_MIN_HITS`).
- **Telemetria:** `scraper_failure_total` e `SCRAPER_ERRORS_TOTAL` (com label `SCHEMA_VIOLATION`) alimentam alertas no Grafana.
