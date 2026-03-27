from pydantic import BaseModel, ConfigDict, Field, field_validator
import re

class MedicalCouncilRegistration(BaseModel):
    model_config = ConfigDict(frozen=True)
    crm_numero: str = Field(..., description="O número do CRM contendo apenas dígitos")
    crm_uf: str = Field(..., description="A UF do CRM contendo exatamente 2 letras")

    @field_validator("crm_uf")
    @classmethod
    def validate_uf(cls, v: str) -> str:
        v = v.strip().upper()
        if not re.match(r"^[A-Z]{2}$", v):
            raise ValueError("CRM UF deve conter exatamente 2 letras.")
        return v

    @field_validator("crm_numero")
    @classmethod
    def validate_crm(cls, v: str) -> str:
        v = v.strip()
        if not re.match(r"^\d+$", v):
            raise ValueError("CRM Numero deve conter apenas digitos numéricos.")
        return v
