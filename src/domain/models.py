from __future__ import annotations
import time

from enum import Enum
from typing import Annotated


from pydantic import BaseModel, Field, model_validator

from domain.specifications import FiltroAvancadoSpec


class IngestionStatus(str, Enum):
    """Status possíveis de uma execução de ingestão."""
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILURE = "FAILURE"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"


class IngestionLogEntry(BaseModel):
    """Value Object para auditoria de cada ciclo do Scraper/Worker."""
    timestamp: float = Field(description="Epoch UTC do início da execução")
    duration_seconds: float = Field(description="Duração total da sessão de ingestão")
    status: IngestionStatus
    items_ingested: int = Field(default=0, description="Registros novos/atualizados com sucesso")
    items_failed: int = Field(default=0, description="Poison pills enviadas para DLQ")
    bytes_processed: int = Field(default=0, description="Volume estimado de payload processado")
    target_lists: list[str] = Field(default_factory=list, description="Listas-alvo processadas neste ciclo")
    error_message: str = Field(default="", description="Mensagem de erro se status != SUCCESS")





class AnalyticKPIs(BaseModel):
    pacientes: int
    eventos: int
    esp_mae: int
    sub_esp: int
    medicos: int
    cids: int
    origens: int
    lead_time: float
    max_lead_time: int
    span_dias: int
    pac_urgentes: int
    pac_vencidos: int
    p90_lead_time: float
    p90_esquecido: float
    last_sync_at: float = Field(
        default=0.0, description="Timestamp de modificação do Parquet para checagem de SLA de dados"
    )
    mes_comercial: float = Field(
        default=30.416, description="Dias do mês comercial inserido por Use Case"
    )

    @property
    def evo_por_paciente(self) -> float:
        return round(self.eventos / self.pacientes, 1) if self.pacientes > 0 else 0.0

    @property
    def sub_por_esp(self) -> float:
        return round(self.sub_esp / self.esp_mae, 1) if self.esp_mae > 0 else 0.0

    @property
    def cid_por_medico(self) -> float:
        return round(self.cids / self.medicos, 1) if self.medicos > 0 else 0.0

    @property
    def evo_por_medico(self) -> float:
        return round(self.eventos / self.medicos, 1) if self.medicos > 0 else 0.0

    @property
    def cad_por_mes(self) -> float:
        meses_janela = max(self.span_dias / self.mes_comercial, 1.0)
        return round(self.pacientes / meses_janela, 1) if self.pacientes > 0 else 0.0

    @property
    def taxa_urgencia(self) -> float:
        return (
            round((self.pac_urgentes / self.pacientes) * 100, 1)
            if self.pacientes > 0
            else 0.0
        )

    @property
    def taxa_vencidos(self) -> float:
        return (
            round((self.pac_vencidos / self.pacientes) * 100, 1)
            if self.pacientes > 0
            else 0.0
        )

    @property
    def age_hours(self) -> float:
        """Age of the data in hours relative to wall-clock time.

        Returns 0.0 when last_sync_at is unknown (fresh install or read failure).
        WHY: Derived metric consumed by is_stale() and by Analytics UI for the
        human-readable label in the Amber Alert banner ("Data is X.X hours old").
        """
        if self.last_sync_at <= 0.0:
            return 0.0
        return (time.time() - self.last_sync_at) / 3600

    def is_stale(self, threshold_hours: float) -> bool:
        """Amber Alert predicate: True when data age strictly exceeds the SLA threshold.

        WHY (Domain Isolation): This encapsulates the Amber Alert business rule so
        the Presentation layer (app_analytics.py) remains a thin Humble Object
        with no arithmetic. Ref: docs/GLOSSARY.md — Amber Alert.

        Boundary contract (strict >): data exactly AT the threshold is considered fresh.
        last_sync_at == 0 returns False — 'unknown' is handled as a separate, more
        prominent banner in the UI, not as a staleness violation.

        Args:
            threshold_hours: Maximum acceptable data age in hours (e.g. 2.0).
                             Usually sourced from ClinicaPolicy.data_sla_threshold_horas.

        Returns:
            bool: True → Amber Alert must be displayed. False → data is within SLA.
        """
        if self.last_sync_at <= 0.0:
            # WHY: last_sync_at = 0 means "no data ever synced" (fresh install).
            # The UI renders a distinct critical banner for this case; we must NOT
            # conflate "never synced" with "synced but stale".
            return False
        return self.age_hours > threshold_hours



"""Política de Negócio Clínica — Value Object do Core Domain.

WHY: As invariantes de negócio do domínio hospitalar (faixas etárias, SLAs clínicos,
classificações de risco) pertenciam ao .env global (AppSettings), misturadas com
DSNs e portas de infra. Isso permitia mutação silenciosa dessas regras em tempo de
deploy sem cobertura de testes e sem rastreamento GitOps.

Solução DDD: ClinicaPolicy é um Value Object imutável (frozen) que encapsula
as invariantes do domínio clínico. Os defaults são as regras de negócio reais;
sobrescrita é possível via parâmetros explícitos para testes e multi-tenant futuro.

A camada de infraestrutura (AppSettings) pode opcionalmente sobrescrever esses
valores via variáveis de ambiente, mas o ponto de verdade é o domínio, não o .env.

Ref: ADR-005 — Business Policy Extraction from Infrastructure Config
"""



class ClinicaPolicy(BaseModel):
    """Value Object de Política de Negócio Clínica.

    Encapsula as invariantes do domínio hospitalar/analítico que regem:
    - Faixas etárias válidas para pacientes na fila de espera.
    - SLA de vencimento de solicitações clínicas (dias sem atendimento).
    - Cores de classificação de risco que caracterizam urgência.
    - Calendário comercial para cálculo de taxa de cadastro por mês.
    - Limiar de frescor dos dados para o Amber Alert de SLA de dados.

    WHY frozen=True: Value Objects têm identidade baseada no valor, não na
    referência. Imutabilidade garante consistência em toda a Bounded Context
    e previne corrupção de estado em ambientes concorrentes (Streamlit sessions).

    Attributes:
        idade_min: Menor idade válida para paciente na lista (anos).
        idade_max: Maior idade válida para paciente na lista (anos).
        sla_dias_vencimento: Dias sem atendimento que caracterizam solicitação vencida.
        mes_comercial_dias: Duração do mês comercial para cálculo de taxa de cadastro.
        data_sla_threshold_horas: Horas máximas sem atualização do Parquet antes do Amber Alert.
        cores_urgencia: Classificações de risco clínico que caracterizam paciente urgente.
    """

    model_config = {"frozen": True}  # Value Object: imutável após criação

    idade_min: Annotated[int, Field(ge=0, description="Menor idade válida para paciente (anos)")] = 0
    idade_max: Annotated[int, Field(le=150, description="Maior idade válida para paciente (anos)")] = 120
    sla_dias_vencimento: Annotated[
        int, Field(gt=0, description="Dias sem atendimento que caracterizam solicitação vencida")
    ] = 180
    mes_comercial_dias: Annotated[
        float, Field(gt=0.0, description="Duração do mês comercial em dias para cálculo de taxa")
    ] = 30.416
    data_sla_threshold_horas: Annotated[
        float, Field(gt=0.0, description="Horas máximas sem atualização do Parquet antes do Amber Alert")
    ] = 2.0
    cores_urgencia: tuple[str, ...] = Field(
        default=("VERMELHO", "LARANJA", "AMARELO"),
        description="Classificações de risco clínico que caracterizam paciente urgente",
    )

    @model_validator(mode="after")
    def validate_faixa_etaria(self) -> "ClinicaPolicy":
        """WHY: Invariante de domínio — faixa etária deve ser consistente.

        idade_min >= idade_max é um absurdo clínico; fail-fast previne dados corrompidos.
        """
        if self.idade_min >= self.idade_max:
            raise ValueError(
                f"idade_min ({self.idade_min}) deve ser menor que "
                f"idade_max ({self.idade_max}) — faixa etária inválida."
            )
        return self

    @model_validator(mode="after")
    def validate_cores_nao_vazia(self) -> "ClinicaPolicy":
        """WHY: Ao menos uma cor de urgência é obrigatória para operação clínica."""
        if not self.cores_urgencia:
            raise ValueError(
                "cores_urgencia não pode ser vazia — ao menos uma classificação de risco é obrigatória."
            )
        return self


# Instância padrão: singleton de conveniência para o Use Case.
# WHY: Permite uso como `from domain.models import DEFAULT_CLINICA_POLICY`
# sem acoplar ao settings de infraestrutura. O Use Case pode sobrescrever
# injetando uma instância customizada via Dependency Injection.
DEFAULT_CLINICA_POLICY = ClinicaPolicy()


class DashboardState(BaseModel):
    kpis: AnalyticKPIs
    policy: ClinicaPolicy
