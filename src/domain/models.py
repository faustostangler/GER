from pydantic import BaseModel, Field
from typing import List, Optional

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
        meses_janela = max(self.span_dias / 30.416, 1.0)
        return round(self.pacientes / meses_janela, 1) if self.pacientes > 0 else 0.0

    @property
    def taxa_urgencia(self) -> float:
        return round((self.pac_urgentes / self.pacientes) * 100, 1) if self.pacientes > 0 else 0.0

    @property
    def taxa_vencidos(self) -> float:
        return round((self.pac_vencidos / self.pacientes) * 100, 1) if self.pacientes > 0 else 0.0

class FilterCriteria(BaseModel):
    clauses: List[str] = Field(default_factory=lambda: ["1=1"])

    def get_where_clause(self) -> str:
        return " AND ".join(self.clauses)
