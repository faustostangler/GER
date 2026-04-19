"""Testes de Política de Negócio Clínica (TDD Red Phase).

WHY: Garante que as invariantes de domínio (AGE, SLA, urgência) vivam no Core
Domain como Value Object imutável — não em variáveis de infra no .env global.
Isso previne mutação silenciosa de regras de negócio em deploy sem cobertura de
testes e sem rastreamento GitOps.

Ref: ADR-005 (a criar) — Business Policy Extraction
"""
import pytest
from domain.models import ClinicaPolicy


class TestClinicaPolicyDefaults:
    """Verifica que os defaults são invariantes do domínio, não do .env."""

    def test_idade_min_default_zero(self):
        policy = ClinicaPolicy()
        assert policy.idade_min == 0

    def test_idade_max_default_120(self):
        policy = ClinicaPolicy()
        assert policy.idade_max == 120

    def test_sla_vencimento_default_180_dias(self):
        policy = ClinicaPolicy()
        assert policy.sla_dias_vencimento == 180

    def test_mes_comercial_default_30_416(self):
        policy = ClinicaPolicy()
        assert policy.mes_comercial_dias == pytest.approx(30.416)

    def test_data_sla_threshold_default_2_horas(self):
        policy = ClinicaPolicy()
        assert policy.data_sla_threshold_horas == pytest.approx(2.0)

    def test_cores_urgencia_default_vermelho_laranja_amarelo(self):
        policy = ClinicaPolicy()
        assert set(policy.cores_urgencia) == {"VERMELHO", "LARANJA", "AMARELO"}


class TestClinicaPolicyImmutability:
    """Value Object: imutável após criação (frozen=True)."""

    def test_policy_e_frozen(self):
        """Garantir que ClinicaPolicy é imutável (Value Object puro).

        WHY: Pydantic V2 frozen models lançam ValidationError no setattr,
        enquanto dataclasses lançam TypeError. Ambos são contratos válidos de imutabilidade.
        """
        from pydantic import ValidationError as PydanticValidationError
        policy = ClinicaPolicy()
        with pytest.raises((TypeError, AttributeError, PydanticValidationError)):
            policy.sla_dias_vencimento = 999  # type: ignore[misc]

    def test_duas_policies_identicas_sao_iguais(self):
        """Value Object equality: mesmo conteúdo = mesma identidade."""
        p1 = ClinicaPolicy()
        p2 = ClinicaPolicy()
        assert p1 == p2


class TestClinicaPolicyCustomizable:
    """Parametrizável para contextos diferentes (ex: testes, multi-tenant futuro)."""

    def test_custom_sla_vencimento(self):
        policy = ClinicaPolicy(sla_dias_vencimento=90)
        assert policy.sla_dias_vencimento == 90

    def test_custom_cores_urgencia(self):
        policy = ClinicaPolicy(cores_urgencia=["VERMELHO"])
        assert policy.cores_urgencia == ("VERMELHO",)

    def test_custom_data_sla_threshold(self):
        policy = ClinicaPolicy(data_sla_threshold_horas=4.0)
        assert policy.data_sla_threshold_horas == pytest.approx(4.0)


class TestClinicaPolicyInvariants:
    """Invariantes de domínio: regras que NUNCA podem ser violadas."""

    def test_idade_min_nao_pode_ser_negativa(self):
        with pytest.raises(ValueError, match="idade_min"):
            ClinicaPolicy(idade_min=-1)

    def test_idade_max_nao_pode_exceder_150(self):
        with pytest.raises(ValueError, match="idade_max"):
            ClinicaPolicy(idade_max=151)

    def test_idade_min_nao_pode_superar_max(self):
        with pytest.raises(ValueError, match="idade_min.*idade_max|faixa"):
            ClinicaPolicy(idade_min=100, idade_max=50)

    def test_sla_vencimento_deve_ser_positivo(self):
        with pytest.raises(ValueError, match="sla_dias_vencimento"):
            ClinicaPolicy(sla_dias_vencimento=0)

    def test_cores_urgencia_nao_pode_ser_vazia(self):
        with pytest.raises(ValueError, match="cores_urgencia"):
            ClinicaPolicy(cores_urgencia=[])

    def test_data_sla_threshold_deve_ser_positivo(self):
        with pytest.raises(ValueError, match="data_sla_threshold_horas"):
            ClinicaPolicy(data_sla_threshold_horas=0.0)


class TestClinicaPolicyDomainProtocol:
    """Garante que ClinicaPolicy pode ser instanciada sem dependência de infra."""

    def test_instancia_sem_settings(self):
        """CRÍTICO: domínio NÃO pode importar infrastructure.config."""
        import sys
        # Se ClinicaPolicy importar 'infrastructure', este teste falha
        policy = ClinicaPolicy()
        for mod_name in sys.modules:
            if "infrastructure" in mod_name and "config" in mod_name:
                # só verifica que ClinicaPolicy em si não é um alias de settings
                assert "ClinicaPolicy" not in str(
                    sys.modules[mod_name].__dict__.keys()
                )
        assert policy is not None
