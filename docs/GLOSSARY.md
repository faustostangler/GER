# Glossário de Linguagem Ubíqua — Subdomínio Clínico Gercon

> Este glossário garante que **Stakeholders Clínicos** e **Engenheiros** usem terminologia idêntica.
> Todo código, teste, métrica e documentação deve usar estes termos sem tradução.

---

## Entidades do Domínio

| Termo Ubíquo | Código/Coluna | Definição Clínica |
|---|---|---|
| **Protocolo** | `numeroCMCE` | Identificador único de uma solicitação de consulta ou procedimento no sistema Gercon. Equivalente a um "ticket" no contexto de filas clínicas. |
| **Paciente** | `usuarioSUS_*` | Pessoa que aguarda atendimento no sistema de regulação. Identificado por CPF (hash PII) e dados demográficos. |
| **Especialidade Mãe** | `entidade_especialidade_descricao` | Categoria principal médica (ex: Cardiologia, Neurologia). |
| **Sub-Especialidade** | `entidade_subespecialidade_descricao` | Especialização dentro da Mãe (ex: Ecocardiograma dentro de Cardiologia). |
| **Cor de Risco** | `entidade_classificacaoRisco_cor` | Classificação de urgência clínica por cores: VERMELHO (emergência) > LARANJA (urgente) > AMARELO (pouco urgente) > VERDE (não urgente) > AZUL (eletivo) > BRANCO (não classificado). |
| **Origem da Lista** | `origem_lista` | Tipo de fila de espera no Gercon (ex: "Aguardando Vaga", "Agendados"). |
| **CID** | `cid_descricao` | Classificação Internacional de Doenças — código diagnóstico associado à solicitação. |

## Métricas Temporais

| Termo Ubíquo | Cálculo | Definição |
|---|---|---|
| **Lead Time** | `DATEDIFF(dataSolicitacao, CURRENT_DATE)` | Tempo total em dias que um paciente aguarda na fila desde a solicitação. É a métrica primária de SLA do sistema regulatório. |
| **Esquecido** | `Lead Time > SLA_DIAS_VENCIMENTO` | Paciente cujo Lead Time ultrapassou o limite aceitável (default: 180 dias). Indica abandono governamental ou falha sistêmica de regulação. |
| **Vencido** | Sinônimo de **Esquecido** | Usado intercambiavelmente nos filtros da UI. Representa pacientes que violaram o SLA temporal. |
| **P90 Lead Time** | `PERCENTILE_CONT(0.9)` sobre Lead Time | Latência de cauda: 90% dos pacientes esperam menos que este valor. Métrica SRE para detecção de outliers. |
| **Span (Janela)** | `MAX(dataSolicitacao) - MIN(dataSolicitacao)` | Amplitude temporal do dataset filtrado. Usado para normalizar métricas derivadas como "Cadastros por Mês". |

## Métricas Derivadas

| Termo Ubíquo | Fórmula | Interpretação |
|---|---|---|
| **Evolução por Paciente** | `eventos / pacientes` | Quantos eventos (consultas, procedimentos) cada paciente gerou em média. Valores altos indicam tratamento contínuo ou erro de cadastro. |
| **Taxa de Urgência** | `(pac_urgentes / pacientes) × 100` | Percentual de pacientes classificados como risco alto. Indica pressão sobre o sistema de regulação. |
| **Taxa de Vencidos** | `(pac_vencidos / pacientes) × 100` | Percentual de pacientes que ultrapassaram o SLA. Indicador direto de falha operacional. |
| **Cadastros por Mês** | `pacientes / (span_dias / MES_COMERCIAL)` | Throughput normalizado de entrada de pacientes na fila. |

## Conceitos de Infraestrutura

| Termo | Definição |
|---|---|
| **Data Contract** | Validação do schema do Parquet na inicialização do repositório. Se colunas obrigatórias estiverem ausentes, o Circuit Breaker é acionado. |
| **Amber Alert** | Banner de alerta na UI quando o Parquet tem `mtime` mais antigo que `DATA_SLA_THRESHOLD` horas. Sinaliza "Silêncio dos Dados" — o Scraper pode ter parado. |
| **Poison Pill** | Payload JSON recebido do Gercon que falha na validação Pydantic (`GerconPayloadContract`). Redirecionado para a DLQ (Dead Letter Queue). |
| **DLQ (Dead Letter Queue)** | Lista de registros que falharam na validação de contrato e foram armazenados para reprocessamento futuro. |
| **Circuit Breaker** | Padrão de resiliência: se >5% dos registros forem Poison Pills, aborta o ciclo de ingestão para proteger a integridade do Data Lake. |
| **Degradação Graciosa** | Capacidade do sistema de continuar operando (com performance reduzida) quando uma dependência falha (ex: Redis indisponível → fallback para query direta). |
| **Watermark** | Timestamp do último registro processado para cada lista. Usado para evitar reprocessamento em ciclos incrementais. |
| **Cloud Run Auth Adapter** | Adaptador de autenticação para runtime serverless (ADR-004). Usa password gate temporário enquanto Firebase Auth não é configurado. Detectado via `K_SERVICE`. |
| **Password Gate** | Mecanismo de autenticação temporário para Cloud Run via `CLOUD_RUN_AUTH_PASSWORD`. Substitui Keycloak/oauth2-proxy em ambientes sem sidecar de identidade. |
| **Firebase Auth (Phase 2)** | Provedor de identidade gerenciado pelo GCP para substituir o Password Gate. Suporta email/senha, SSO Google e custom claims (CRM, roles). |
