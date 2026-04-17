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
from __future__ import annotations

from typing import Annotated
from pydantic import BaseModel, Field, model_validator


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
# WHY: Permite uso como `from domain.policies import DEFAULT_CLINICA_POLICY`
# sem acoplar ao settings de infraestrutura. O Use Case pode sobrescrever
# injetando uma instância customizada via Dependency Injection.
DEFAULT_CLINICA_POLICY = ClinicaPolicy()
