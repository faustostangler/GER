# ADR-005: Business Policy Extraction from Infrastructure Config

**Status:** Accepted  
**Date:** 2026-04-17  
**Deciders:** Principal Architect  
**Ref:** GER Onboarding Guide §6 (Design Patterns & Strict Rules)

---

## Contexto

O `AppSettings` (pydantic-settings) era o repositório único para **toda** configuração
do sistema. Isso criava um problema de contaminação de camadas:

```python
# .env e AppSettings misturavam dois tipos distintos de variáveis:
REDIS__HOST = "cache"          # ← Infra: onde está o Redis?
KAFKA_URL = "redpanda-0:9092"  # ← Infra: onde está o Kafka?
AGE_MIN = 0                    # ← Negócio: qual é a idade mínima de um paciente?
SLA_DIAS_VENCIMENTO = 180      # ← Negócio: quanto tempo até uma solicitação vencer?
CORES_URGENCIA = VERMELHO,...  # ← Negócio: o que define urgência clínica?
```

**Sintomas do problema:**
1. `AnalyticsUseCase` importava `from infrastructure.config import settings` — violação direta da Regra da Dependência (Clean Architecture). A camada Application não pode depender da Infrastructure.
2. Regras de negócio mudavam silenciosamente em deploy (via CI/CD), sem cobertura de testes unitários e sem rastreamento GitOps nas invariantes.
3. Impossível testar o Use Case sem configurar o ambiente de infra completo.

---

## Decisão

Criar `ClinicaPolicy` como **Value Object imutável no Core Domain** (`src/domain/policies.py`).

```
┌─────────────────────────────────────────────────────────────────┐
│  ANTES                          DEPOIS                          │
│                                                                  │
│  AppSettings ─────────────────► ClinicaPolicy (domain)          │
│  (infra+negócio misturados)    (invariantes puras)               │
│                                    ↑                            │
│  AnalyticsUseCase                  │ DI                         │
│  └── import settings ──────────────┘                            │
│       (violação)           get_use_case() [presentation]        │
│                             constrói ClinicaPolicy              │
│                             a partir dos settings               │
└─────────────────────────────────────────────────────────────────┘
```

### Arquitetura de Composição

```
[app_analytics.py: get_use_case()]          ← camada de composição
  ├── lê AppSettings (infra override)        ← infra
  ├── constrói ClinicaPolicy(...)            ← domain
  └── injeta em AnalyticsUseCase(policy=X)  ← application
```

O `AppSettings` mantém os campos `AGE_MIN`, `SLA_DIAS_VENCIMENTO` etc. como **mecanismo de override de ambiente**, mas são claramente documentados como pontes para `ClinicaPolicy`, não como fonte de verdade.

---

## Consequências

### Positivas

- **Clean Architecture restaurada:** `AnalyticsUseCase` não importa mais `infrastructure.config`. O teste `test_use_case_nao_importa_settings_diretamente` protege essa fronteira via AST parse.
- **Testabilidade:** Use Case testável via `AnalyticsUseCase(repo, policy=ClinicaPolicy(sla_dias_vencimento=90))` sem qualquer `.env` ou mock de settings.
- **GitOps de Negócio:** Alterações nas invariantes de negócio (e.g., mudar SLA de 180 para 120 dias) agora são mudanças de código versionadas no Git que passam pelo CI gate de mutmut e testes unitários — não são mais silenciosas via `.env`.
- **Ubiquitous Language:** `ClinicaPolicy`, `sla_dias_vencimento`, `cores_urgencia` substituem `SLA_DIAS_VENCIMENTO` (SCREAMING_SNAKE_CASE de infra) por vocabulário clínico.
- **Value Object imutável:** `frozen=True` garante consistência em sessions concorrentes do Streamlit.

### Negativas / Trade-offs

- **Override via .env ainda possível:** O mecanismo de sobrescrever via ambiente persiste para staging/produção. Isso é deliberado (12-Factor App), mas o time deve saber que é um override, não a definição.
- **Campo `data_sla_threshold_horas` renomeado:** Era `DATA_SLA_THRESHOLD` no settings. Compatibilidade mantida via mapeamento explícito em `get_use_case()`.

---

## Alternativas Consideradas

| Alternativa | Motivo para Rejeitar |
|------------|----------------------|
| Remover `AGE_MIN` etc. do AppSettings completamente | Quebraria configurabilidade via .env para staging |
| Usar `@dataclass(frozen=True)` em vez de Pydantic | Perderia validação automática dos invariantes |
| Policy como singleton global no domain | Impediria testes com políticas customizadas (DI) |

---

## Referências

- [Clean Architecture - Regra da Dependência](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
- [DDD - Value Objects](https://martinfowler.com/bliki/ValueObject.html)
- `src/domain/policies.py` — implementação
- `tests/domain/test_clinica_policy.py` — 18 testes com invariantes
- `tests/application/test_analytics_use_case.py::TestAnalyticsUseCasePolicyInjection`
