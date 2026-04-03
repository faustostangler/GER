from pydantic import BaseModel, ConfigDict
from typing import Optional, List


# ==========================================
# TESTES DE CONTRATO / ACL (Pydantic V2)
# ==========================================
class UsuarioSUS(BaseModel):
    model_config = ConfigDict(extra="ignore")
    nomeCompleto: str
    dataNascimento: Optional[int] = None
    cpf: Optional[str] = None


class ClassificacaoRisco(BaseModel):
    model_config = ConfigDict(extra="ignore")
    cor: Optional[str] = None
    pontosGravidade: Optional[float] = None
    pontosTempo: Optional[float] = None
    totalPontos: Optional[float] = None


class Evolucao(BaseModel):
    model_config = ConfigDict(extra="ignore")
    data: Optional[int] = None
    detalhes: Optional[str] = None


class GerconPayloadContract(BaseModel):
    """
    Garante que o payload externo (Vendor) mantém a estrutura
    esperada pelo nosso Domínio e Data Lake. Funciona como
    a nossa Anti-Corruption Layer (ACL).
    """

    model_config = ConfigDict(extra="ignore")

    numeroCMCE: int
    situacao: str
    dataSolicitacao: Optional[int] = None
    usuarioSUS: Optional[UsuarioSUS] = None
    classificacaoRisco: Optional[ClassificacaoRisco] = None
    evolucoes: Optional[List[Evolucao]] = None
