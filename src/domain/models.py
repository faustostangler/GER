from pydantic import BaseModel, Field
from enum import Enum


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


class FilterCriteria(BaseModel):
    clauses: list[str] = Field(default_factory=list, description="Lista de cláusulas SQL injetadas de forma segura")


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
