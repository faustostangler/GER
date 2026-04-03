from src.domain.specifications import (
    Specification,
    PacienteUrgenteSpec,
    PacienteVencidoSpec,
    LeadTimeCriticoSpec,
)
import datetime


class MockTrueSpec(Specification):
    def is_satisfied_by(self, candidate) -> bool:
        return True


class MockFalseSpec(Specification):
    def is_satisfied_by(self, candidate) -> bool:
        return False


def test_and_specification_truth_table():
    assert (MockTrueSpec() & MockTrueSpec()).is_satisfied_by(None) is True
    assert (MockTrueSpec() & MockFalseSpec()).is_satisfied_by(None) is False
    assert (MockFalseSpec() & MockTrueSpec()).is_satisfied_by(None) is False
    assert (MockFalseSpec() & MockFalseSpec()).is_satisfied_by(None) is False


def test_or_specification_truth_table():
    assert (MockTrueSpec() | MockTrueSpec()).is_satisfied_by(None) is True
    assert (MockTrueSpec() | MockFalseSpec()).is_satisfied_by(None) is True
    assert (MockFalseSpec() | MockTrueSpec()).is_satisfied_by(None) is True
    assert (MockFalseSpec() | MockFalseSpec()).is_satisfied_by(None) is False


def test_not_specification_truth_table():
    assert (~MockTrueSpec()).is_satisfied_by(None) is False
    assert (~MockFalseSpec()).is_satisfied_by(None) is True


def test_paciente_urgente_spec():
    spec = PacienteUrgenteSpec(cores_alvo=["VERMELHO", "LARANJA", "AMARELO"])
    assert spec.is_satisfied_by({"entidade_classificacaoRisco_cor": "vermelho"}) is True
    assert spec.is_satisfied_by({"entidade_classificacaoRisco_cor": "Laranja"}) is True
    assert spec.is_satisfied_by({"entidade_classificacaoRisco_cor": "AMARELO"}) is True
    assert spec.is_satisfied_by({"entidade_classificacaoRisco_cor": "AZUL"}) is False
    assert spec.is_satisfied_by({}) is False


def test_paciente_vencido_spec():
    spec = PacienteVencidoSpec(dias_tolerancia=180)
    old_date = (datetime.datetime.now() - datetime.timedelta(days=181)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    recent_date = (datetime.datetime.now() - datetime.timedelta(days=179)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    assert spec.is_satisfied_by({"dataSolicitacao": old_date}) is True
    assert spec.is_satisfied_by({"dataSolicitacao": recent_date}) is False
    assert spec.is_satisfied_by({}) is False


def test_lead_time_critico_spec():
    spec = LeadTimeCriticoSpec(max_dias=100)
    old_date = (datetime.datetime.now() - datetime.timedelta(days=101)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    recent_date = (datetime.datetime.now() - datetime.timedelta(days=99)).strftime(
        "%Y-%m-%d"
    )
    assert spec.is_satisfied_by({"dataSolicitacao": old_date}) is True
    assert spec.is_satisfied_by({"dataSolicitacao": recent_date}) is False
    assert spec.is_satisfied_by({}) is False
