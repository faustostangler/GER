from abc import ABC, abstractmethod
from typing import Any
import datetime

class Specification(ABC):
    @abstractmethod
    def is_satisfied_by(self, candidate: Any) -> bool:
        pass

    def __and__(self, other: 'Specification') -> 'Specification':
        return AndSpecification(self, other)

    def __or__(self, other: 'Specification') -> 'Specification':
        return OrSpecification(self, other)

    def __invert__(self) -> 'Specification':
        return NotSpecification(self)

class AndSpecification(Specification):
    def __init__(self, left: Specification, right: Specification):
        self.left = left
        self.right = right

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self.left.is_satisfied_by(candidate) and self.right.is_satisfied_by(candidate)

class OrSpecification(Specification):
    def __init__(self, left: Specification, right: Specification):
        self.left = left
        self.right = right

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self.left.is_satisfied_by(candidate) or self.right.is_satisfied_by(candidate)

class NotSpecification(Specification):
    def __init__(self, spec: Specification):
        self.spec = spec

    def is_satisfied_by(self, candidate: Any) -> bool:
        return not self.spec.is_satisfied_by(candidate)

class PacienteUrgenteSpec(Specification):
    def __init__(self, cores_alvo: list[str]):
        self.cores_urgencia = cores_alvo

    def is_satisfied_by(self, candidate: Any) -> bool:
        if isinstance(candidate, dict):
            risk = candidate.get('entidade_classificacaoRisco_cor', '').upper()
            return risk in self.cores_urgencia
        return False

class PacienteVencidoSpec(Specification):
    def __init__(self, dias_tolerancia: int):
        self.dias_vencimento = dias_tolerancia

    def is_satisfied_by(self, candidate: Any) -> bool:
        if isinstance(candidate, dict):
            data_solicitacao = candidate.get('dataSolicitacao')
            if data_solicitacao:
                if isinstance(data_solicitacao, str):
                    data_solicitacao = datetime.datetime.strptime(data_solicitacao.split()[0], "%Y-%m-%d")
                dias = (datetime.datetime.now() - data_solicitacao).days
                return dias > self.dias_vencimento
        return False

class LeadTimeCriticoSpec(Specification):
    def __init__(self, max_dias: int):
        self.max_dias = max_dias

    def is_satisfied_by(self, candidate: Any) -> bool:
        if isinstance(candidate, dict):
            data_solicitacao = candidate.get('dataSolicitacao')
            if data_solicitacao:
                if isinstance(data_solicitacao, str):
                    data_solicitacao = datetime.datetime.strptime(data_solicitacao.split()[0], "%Y-%m-%d")
                dias = (datetime.datetime.now() - data_solicitacao).days
                return dias > self.max_dias
        return False
