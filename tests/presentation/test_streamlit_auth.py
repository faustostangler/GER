"""Tests for the Streamlit IAM Adapter (presentation/adapters/streamlit_auth.py).

WHY: Valida o isolamento estrito do IAM. Toda lógica de autenticação deve
viver no adapter, não no app_analytics.py. Cada "path" de autenticação
(dev mock, Cloud Run, IAP Proxy) deve ser testável independentemente,
sem inicializar o Streamlit completo (sem AppTest).

Ref: ADR-006 — IAM Adapter Isolation (Fase 3).
"""
from __future__ import annotations

import hashlib
import time
from unittest.mock import patch

import pytest

from infrastructure.auth.token_acl import ValidatedUserToken


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_token(**kwargs) -> ValidatedUserToken:
    """Fábrica de token para testes — garante campos obrigatórios."""
    defaults = dict(
        sub="test-sub",
        email="test@gercon.com",
        preferred_username="test_user",
        roles=["diretor_medico"],
        exp=int(time.time() + 86400),
    )
    return ValidatedUserToken(**(defaults | kwargs))


# ---------------------------------------------------------------------------
# 1. Detecção de Runtime
# ---------------------------------------------------------------------------


class TestRuntimeDetection:
    """Testa detecção do runtime (Cloud Run vs Docker Compose vs Dev)."""

    def test_is_cloud_run_true_when_k_service_set(self):
        """WHY: K_SERVICE é a variável canônica que o Cloud Run injeta
        automaticamente. Sua presença é o critério de detecção."""
        from presentation.adapters.streamlit_auth import is_cloud_run

        with patch.dict("os.environ", {"K_SERVICE": "gercon-analytics"}):
            assert is_cloud_run() is True

    def test_is_cloud_run_false_when_k_service_absent(self):
        from presentation.adapters.streamlit_auth import is_cloud_run

        env = {k: v for k, v in __import__("os").environ.items() if k != "K_SERVICE"}
        with patch("os.environ", env):
            assert is_cloud_run() is False

    def test_dev_mock_requires_both_guards(self):
        """WHY: Guarda dupla — ENVIRONMENT=dev sozinho não basta (ADR-004).
        Ambas as variáveis devem estar presentes e corretas."""
        from presentation.adapters.streamlit_auth import is_dev_mock_allowed

        # Caso feliz: ambas as condições
        with patch.dict("os.environ", {"ENVIRONMENT": "dev", "ALLOW_UNAUTHENTICATED_DEV": "true"}):
            assert is_dev_mock_allowed() is True

        # Apenas ENVIRONMENT=dev → não é suficiente
        with patch.dict("os.environ", {"ENVIRONMENT": "dev", "ALLOW_UNAUTHENTICATED_DEV": "false"}):
            assert is_dev_mock_allowed() is False

        # Apenas ALLOW_UNAUTHENTICATED_DEV=true → não é suficiente
        with patch.dict("os.environ", {"ENVIRONMENT": "production", "ALLOW_UNAUTHENTICATED_DEV": "true"}):
            assert is_dev_mock_allowed() is False

    def test_dev_mock_accepts_local_environment(self):
        """WHY: accepts 'local' como alias de 'dev' para flexibilidade de DX."""
        from presentation.adapters.streamlit_auth import is_dev_mock_allowed

        with patch.dict("os.environ", {"ENVIRONMENT": "local", "ALLOW_UNAUTHENTICATED_DEV": "true"}):
            assert is_dev_mock_allowed() is True


# ---------------------------------------------------------------------------
# 2. Path de Autenticação: Dev Mock
# ---------------------------------------------------------------------------


class TestDevMockPath:
    """Testa o path do mock de desenvolvimento (sem IAP Proxy)."""

    def test_returns_mock_user_in_dev_mode(self):
        """WHY: Em ambiente dev, o adapter deve retornar um usuário mock
        com perfil clínico completo sem precisar de Keycloak."""
        from presentation.adapters.streamlit_auth import resolve_authenticated_user

        env = {"ENVIRONMENT": "dev", "ALLOW_UNAUTHENTICATED_DEV": "true"}
        with patch.dict("os.environ", env):
            user, token = resolve_authenticated_user(headers={})

        assert isinstance(user, ValidatedUserToken)
        assert user.email == "dev@gercon.com"
        assert "diretor_medico" in user.roles
        assert token == "mock-jwt-token"

    def test_mock_user_has_valid_exp(self):
        """WHY: O token mock deve ter exp futuro para não disparar aviso de sessão."""
        from presentation.adapters.streamlit_auth import resolve_authenticated_user

        env = {"ENVIRONMENT": "dev", "ALLOW_UNAUTHENTICATED_DEV": "true"}
        with patch.dict("os.environ", env):
            user, _ = resolve_authenticated_user(headers={})

        assert user.exp is not None
        assert user.exp > time.time()


# ---------------------------------------------------------------------------
# 3. Path de Autenticação: IAP Proxy (Docker Compose / K8s)
# ---------------------------------------------------------------------------


class TestIAPProxyPath:
    """Testa extração de token dos headers do OAuth2-Proxy."""

    def test_resolves_from_x_forwarded_access_token(self):
        """WHY: OAuth2-Proxy injeta o JWT Keycloak no header
        x-forwarded-access-token. É o path primário em Docker Compose."""
        from presentation.adapters.streamlit_auth import resolve_authenticated_user

        mock_user = _make_token(email="medico@gercon.com")

        # Sem Cloud Run, sem dev mock
        env = {"ENVIRONMENT": "production"}
        headers = {"x-forwarded-access-token": "valid-jwt-token"}

        with patch.dict("os.environ", env, clear=False):
            with patch("os.environ.get", side_effect=lambda k, d=None: env.get(k, d)):
                with patch(
                    "presentation.adapters.streamlit_auth.is_cloud_run",
                    return_value=False,
                ):
                    with patch(
                        "presentation.adapters.streamlit_auth.is_dev_mock_allowed",
                        return_value=False,
                    ):
                        with patch(
                            "infrastructure.auth.jwt_validator.verify_token",
                            return_value=mock_user,
                        ) as mock_verify:
                            user, token = resolve_authenticated_user(headers=headers)

        mock_verify.assert_called_once_with("valid-jwt-token")
        assert user.email == "medico@gercon.com"
        assert token == "valid-jwt-token"

    def test_resolves_from_authorization_bearer(self):
        """WHY: Fallback para header Authorization: Bearer quando o header
        específico do proxy não está presente (compatibilidade futura)."""
        from presentation.adapters.streamlit_auth import resolve_authenticated_user

        mock_user = _make_token()
        headers = {"authorization": "Bearer direct-jwt-token"}

        with patch("presentation.adapters.streamlit_auth.is_cloud_run", return_value=False):
            with patch("presentation.adapters.streamlit_auth.is_dev_mock_allowed", return_value=False):
                with patch(
                    "infrastructure.auth.jwt_validator.verify_token",
                    return_value=mock_user,
                ) as mock_verify:
                    user, token = resolve_authenticated_user(headers=headers)

        mock_verify.assert_called_once_with("direct-jwt-token")

    def test_raises_value_error_when_no_auth_header(self):
        """WHY: A ausência de headers IAP em Docker Compose é uma falha real
        de configuração de infraestrutura — deve falhar alto e claramente."""
        from presentation.adapters.streamlit_auth import resolve_authenticated_user

        with patch("presentation.adapters.streamlit_auth.is_cloud_run", return_value=False):
            with patch("presentation.adapters.streamlit_auth.is_dev_mock_allowed", return_value=False):
                with pytest.raises(ValueError, match="Missing Authentication Headers"):
                    resolve_authenticated_user(headers={})


# ---------------------------------------------------------------------------
# 4. Path de Autenticação: Cloud Run Password Gate
# ---------------------------------------------------------------------------


class TestCloudRunPasswordGate:
    """Testa verificação de senha do Cloud Run sem Streamlit."""

    def test_valid_hash_password_returns_true(self):
        """WHY: Validação de hash SHA-256 para CLOUD_RUN_AUTH_PASSWORD_HASH.
        Testamos a função de verificação isolada do Streamlit."""
        from presentation.adapters.streamlit_auth import verify_cloud_run_password

        secret = "minha-senha-secreta"
        hashed = hashlib.sha256(secret.encode()).hexdigest()

        is_valid = verify_cloud_run_password(
            password_input=secret,
            expected_hash=hashed,
            expected_plain="",
        )
        assert is_valid is True

    def test_invalid_password_returns_false(self):
        from presentation.adapters.streamlit_auth import verify_cloud_run_password

        hashed = hashlib.sha256(b"correct").hexdigest()
        assert verify_cloud_run_password("wrong", hashed, "") is False

    def test_fallback_plain_text_match(self):
        """WHY: Suporte a texto plano (sem hash) para ambientes onde o
        administrador não configurou o hash SHA-256 (fallback de DX)."""
        from presentation.adapters.streamlit_auth import verify_cloud_run_password

        assert verify_cloud_run_password("minha-senha", "", "minha-senha") is True
        assert verify_cloud_run_password("errada", "", "minha-senha") is False

    def test_both_empty_returns_false(self):
        """WHY: Se nenhuma senha está configurada, o sistema deve negar
        qualquer tentativa — fail-secure."""
        from presentation.adapters.streamlit_auth import verify_cloud_run_password

        assert verify_cloud_run_password("qualquer", "", "") is False

    def test_hash_takes_priority_over_plain(self):
        """WHY: Se o admin configurou o hash, a senha em texto plano não deve
        ser usada como bypass acidental."""
        from presentation.adapters.streamlit_auth import verify_cloud_run_password

        correct_hash = hashlib.sha256(b"correta").hexdigest()
        # Senha correta pelo hash, mas texto plano está "errado"
        assert verify_cloud_run_password("correta", correct_hash, "outra-senha") is True
        # Senha errada pelo hash, mesmo que corresponda ao texto plano
        assert verify_cloud_run_password("outra-senha", correct_hash, "outra-senha") is False


# ---------------------------------------------------------------------------
# 5. Logout URL Builder
# ---------------------------------------------------------------------------


class TestLogoutUrlBuilder:
    """Testa construção de URLs de logout para cada runtime."""

    def test_cloud_run_logout_is_none(self):
        """WHY: Cloud Run não usa Keycloak/OAuth2-Proxy. O logout é feito
        por limpeza de session_state. Nenhuma URL deve ser construída."""
        from presentation.adapters.streamlit_auth import build_logout_url

        url = build_logout_url(is_cloud_run_runtime=True)
        assert url is None

    def test_docker_compose_logout_builds_keycloak_chain(self):
        """WHY: Docker Compose requer cadeia de redirects: OAuth2-Proxy sign_out
        aponta para o logout do Keycloak com post_logout_redirect_uri."""
        from presentation.adapters.streamlit_auth import build_logout_url

        env = {
            "KEYCLOAK_SERVER_URL": "http://iam.127.0.0.1.nip.io:8080",
            "KEYCLOAK_REALM": "gercon-realm",
            "KEYCLOAK_CLIENT_ID": "gercon-analytics",
            "EXTERNAL_DOMAIN": "127.0.0.1.nip.io",
        }
        with patch.dict("os.environ", env):
            url = build_logout_url(is_cloud_run_runtime=False)

        assert url is not None
        assert "/oauth2/sign_out" in url
        assert "gercon-realm" in url
        assert "gercon-analytics" in url

    def test_docker_compose_logout_contains_post_redirect(self):
        """WHY: Sem o post_logout_redirect_uri, o browser fica na tela de logout
        do Keycloak em vez de voltar ao dashboard.

        A URL resultante é double-encoded: OAuth2-Proxy recebe a URL do Keycloak
        como parâmetro ``rd``, que por sua vez contém o ``post_logout_redirect_uri``
        — ambos são URL-encoded na composição final.
        """
        from urllib.parse import unquote
        from presentation.adapters.streamlit_auth import build_logout_url

        env = {
            "KEYCLOAK_SERVER_URL": "http://iam.127.0.0.1.nip.io:8080",
            "KEYCLOAK_REALM": "test-realm",
            "KEYCLOAK_CLIENT_ID": "test-client",
            "EXTERNAL_DOMAIN": "127.0.0.1.nip.io",
        }
        with patch.dict("os.environ", env):
            url = build_logout_url(is_cloud_run_runtime=False)

        # WHY: A URL é double-encoded por design (OAuth2-Proxy rd param → Keycloak logout URL).
        # Decodificamos uma vez para verificar que o endpoint Keycloak e o domínio estão presentes.
        decoded_once = unquote(url)
        assert "openid-connect/logout" in decoded_once
        assert "127.0.0.1.nip.io" in decoded_once


# ---------------------------------------------------------------------------
# 6. Contrato de Exportação do Módulo (CDC)
# ---------------------------------------------------------------------------


class TestModuleContract:
    """Consumer-Driven Contract: valida que o módulo exporta a API esperada.

    WHY: Two consumers depend on this adapter's public surface:
    1. ``app_analytics.py`` → ``require_authentication`` (session lifecycle facade)
    2. ``auth_middleware.py`` → ``build_logout_url``, ``is_cloud_run`` (widget rendering)

    Any symbol removal is a breaking change that must be caught before deployment.
    """

    def test_all_public_symbols_are_importable(self):
        """WHY: O app_analytics.py importa símbolos específicos deste adapter.
        Se a assinatura mudar, o import quebrará em produção antes do teste."""
        import presentation.adapters.streamlit_auth as auth_mod

        assert callable(getattr(auth_mod, "is_cloud_run", None)), "is_cloud_run missing"
        assert callable(getattr(auth_mod, "is_dev_mock_allowed", None)), "is_dev_mock_allowed missing"
        assert callable(getattr(auth_mod, "resolve_authenticated_user", None)), "resolve_authenticated_user missing"
        assert callable(getattr(auth_mod, "verify_cloud_run_password", None)), "verify_cloud_run_password missing"
        assert callable(getattr(auth_mod, "build_logout_url", None)), "build_logout_url missing"
        assert callable(getattr(auth_mod, "require_authentication", None)), "require_authentication missing"

    def test_render_user_widget_lives_in_middleware_not_adapter(self):
        """WHY: SRP boundary guard — render_user_widget was extracted FROM app_analytics.py
        INTO auth_middleware.py. It must NOT live in streamlit_auth (that adapter
        handles identity resolution only, not widget rendering).
        Ref: ADR-006 Phase 3 / SRP extraction."""
        import presentation.adapters.streamlit_auth as auth_mod
        from presentation.middlewares.auth_middleware import render_user_widget

        # Correct home: middleware
        assert callable(render_user_widget)
        # Wrong home: adapter (would violate SRP boundary)
        assert not hasattr(auth_mod, "render_user_widget"), (
            "render_user_widget leaked into streamlit_auth — SRP violated"
        )
