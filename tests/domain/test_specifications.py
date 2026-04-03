from domain.specifications import (
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


def test_paciente_vencido_spec_boundaries():
    """Garante que a Specification de vencimento respeita os limites exatos matemáticos."""
    dias_limite = 30
    spec = PacienteVencidoSpec(dias_tolerancia=dias_limite)
    hoje = datetime.datetime.now()

    def gerar_data_passada(dias_atras: int) -> str:
        # Usamos 00:00:00 para garantir consistência no cálculo de 'days' do timedelta
        return (hoje - datetime.timedelta(days=dias_atras)).strftime("%Y-%m-%d 00:00:00")

    # 1. Seguro (29 dias) -> Dentro do prazo
    assert spec.is_satisfied_by({"dataSolicitacao": gerar_data_passada(29)}) is False

    # 2. O LIMITE EXATO (30 dias) -> O TÚMULO DO MUTANTE ☠️
    # No código original (>), isso deve ser False (30 > 30 é Falso).
    # Se o mutante mudar para (>=), isso vira True e o assert abaixo MATA o mutante.
    assert spec.is_satisfied_by({"dataSolicitacao": gerar_data_passada(30)}) is False

    # 3. Vencido de fato (31 dias) -> Fora do prazo
    assert spec.is_satisfied_by({"dataSolicitacao": gerar_data_passada(31)}) is True

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
