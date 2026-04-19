import time

from enum import Enum

from pydantic import BaseModel, Field

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


# WHY: FilterCriteria foi refatorado para FiltroAvancadoSpec (domain/specifications.py).
# O SQL leak (list[str] clauses) foi removido do Core Domain. Este alias garante
# compatibilidade retroativa com app_analytics.py durante a migração gradual.
# TODO(#ADR-004): Remover este alias após migração completa da camada de apresentação.
# DEPRECATED: Use FiltroAvancadoSpec em vez de FilterCriteria.
FilterCriteria = FiltroAvancadoSpec


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

