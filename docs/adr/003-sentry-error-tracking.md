# ADR-003: Sentry para Error Tracking em Produção

## Status
**Accepted** — 2026-04-03

## Contexto
O sistema possui uma malha de testes rigorosa (Mutation Testing, Chaos Engineering, CDC Contracts) que valida o que **sabemos que pode falhar**. No entanto, em produção:
- Exceções inesperadas (unknown unknowns) podem ocorrer em combinações de dados/estado não previstas nos testes.
- O Streamlit captura exceções silenciosamente em alguns componentes (`try/except` interno), dificultando a detecção via logs.
- Sem rastreamento de erros em tempo real, a equipe só descobre problemas quando o médico reporta.

## Decisão
Integrar o **Sentry SDK** (versão `sentry-sdk[streamlit]`) para captura automática de exceções em produção.

### Release Tracking
- `GIT_SHA` é injetado como build-arg no Dockerfile e como env_var no Cloud Run.
- O Sentry recebe o release `gercon-analytics@{GIT_SHA}` via `sentry-cli` no CI/CD.
- Cada erro é mapeado diretamente ao commit que o introduziu.

### LGPD Compliance
- `send_default_pii=False` — não envia dados pessoais.
- `before_breadcrumb` filtra SQL queries que podem conter dados de pacientes.
- Breadcrumbs de categoria `query` são substituídos por `[REDACTED - LGPD]`.

### Degradação
- Se `SENTRY_DSN` não estiver configurado (env local), `init_sentry` é no-op.
- Se o SDK falhar durante init, loga `warning` e continua normalmente.

## Consequências
- **Positivas:** Detecção de erros em <5min; correlação direta erro→commit; coverage de unknown unknowns.
- **Negativas:** Dependência de serviço SaaS externo; overhead marginal de 10% nas traces.
- **Mitigação LGPD:** Breadcrumb filtering implementado e testado.
