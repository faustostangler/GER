"""IAM Adapter para interface Streamlit — Middleware/Facade de Autenticação.

WHY: Concentra toda a lógica de Identity e Access Management (IAM) para o
runtime Streamlit em um único módulo testável e com único vetor de mudança.

Antes desta extração, `_is_cloud_run()`, `_cloud_run_login_gate()` e
`get_authenticated_user()` viviam misturadas com CSS e Sentry em app_analytics.py,
criando acoplamento entre layout e segurança.

Padrões implementados:
- **Facade**: `resolve_authenticated_user()` expõe um único ponto de entrada.
- **Strategy implícita**: seleciona o path de autenticação conforme o runtime.
- **Humble Object**: lógica pura (verify_cloud_run_password, build_logout_url)
  separada dos side-effects do Streamlit (cloud_run_login_gate).

Ref: ADR-006 — IAM Adapter Isolation (Fase 3).
"""
from __future__ import annotations

import hashlib
import os
import time
from typing import Optional
from urllib.parse import quote

from infrastructure.auth.token_acl import ValidatedUserToken


# ---------------------------------------------------------------------------
# 1. Runtime Detection (pura — sem side-effects)
# ---------------------------------------------------------------------------


def is_cloud_run() -> bool:
    """Detecta se o runtime é Google Cloud Run.

    WHY: ``K_SERVICE`` é a variável canônica que o Cloud Run injeta
    automaticamente em cada instância de container. Sua presença indica
    o ambiente serverless onde Keycloak e OAuth2-Proxy *não* estão disponíveis.
    """
    return bool(os.getenv("K_SERVICE"))


def is_dev_mock_allowed() -> bool:
    """Guarda dupla de segurança para bypass de autenticação em desenvolvimento.

    WHY: Requer *dois* opt-ins explícitos para habilitar o mock:
    1. ``ENVIRONMENT`` in (``local``, ``dev``) — indicador de contexto.
    2. ``ALLOW_UNAUTHENTICATED_DEV=true`` — opt-in explícito de segundo fator.

    Um deploy acidental com ``ENVIRONMENT=dev`` no Cloud Run *não* habilita
    acesso sem login, pois ``ALLOW_UNAUTHENTICATED_DEV`` nunca é setado em prod.

    Ref: ADR-004 — Cloud Run Auth Strategy.
    """
    environment = os.getenv("ENVIRONMENT", "production").lower()
    allow_dev = os.getenv("ALLOW_UNAUTHENTICATED_DEV", "false").lower() == "true"
    return environment in ("local", "dev") and allow_dev


# ---------------------------------------------------------------------------
# 2. Verificação de Senha (Cloud Run Gate — pura, sem Streamlit)
# ---------------------------------------------------------------------------


def verify_cloud_run_password(
    password_input: str,
    expected_hash: str,
    expected_plain: str,
) -> bool:
    """Verifica a senha do Cloud Run Password Gate sem side-effects de UI.

    Args:
        password_input: Senha digitada pelo usuário.
        expected_hash: Hash SHA-256 esperado (``CLOUD_RUN_AUTH_PASSWORD_HASH``).
        expected_plain: Senha em texto plano (``CLOUD_RUN_AUTH_PASSWORD``).

    Returns:
        ``True`` se a senha é válida, ``False`` caso contrário ou se nenhuma
        configuração de senha foi fornecida (fail-secure).

    WHY: O hash tem prioridade sobre o texto plano para evitar bypass acidental
    quando ambas as variáveis estão configuradas. Se nenhuma estiver configurada,
    retorna ``False`` (fail-secure) em vez de conceder acesso.
    """
    if not expected_hash and not expected_plain:
        # Nenhuma senha configurada → fail-secure
        return False

    input_sha256 = hashlib.sha256(password_input.encode()).hexdigest()

    if expected_hash:
        # Hash tem prioridade — valida via SHA-256
        return input_sha256 == expected_hash.lower()

    # Fallback: texto plano (apenas para DX; prod deve sempre usar hash)
    return password_input == expected_plain


# ---------------------------------------------------------------------------
# 3. Cloud Run Login Gate (side-effect: renderiza UI Streamlit)
# ---------------------------------------------------------------------------


def cloud_run_login_gate() -> bool:
    """ADR-004 Phase 1: Login gate para Cloud Run via senha compartilhada.

    WHY: Cloud Run serverless não executa Keycloak/oauth2-proxy como sidecar.
    Usa ``CLOUD_RUN_AUTH_PASSWORD_HASH`` (injetado via Cloud Run secrets) como
    gate temporário enquanto Firebase Auth (Phase 2) não é implementado.

    Returns:
        ``True`` se o usuário já está autenticado nesta sessão Streamlit.
        Chama ``st.stop()`` se não autenticado (nunca retorna ``False``).

    Note:
        Esta função tem side-effects de UI (Streamlit). Não deve ser testada
        diretamente — os primitivos de verificação (``verify_cloud_run_password``)
        são testados separadamente (Humble Object pattern).

    Ref: ADR-006 — IAM Adapter Isolation.
    """
    import streamlit as st  # ACL: import local — evita erro fora do runtime Streamlit

    # Sessão já ativa neste ciclo Streamlit
    if st.session_state.get("cloud_run_authenticated"):
        return True

    expected_hash = os.getenv("CLOUD_RUN_AUTH_PASSWORD_HASH", "")
    expected_plain = os.getenv("CLOUD_RUN_AUTH_PASSWORD", "")

    # Fail-fast: nenhuma senha configurada no Cloud Run
    if not expected_hash and not expected_plain:
        st.error(
            "🚨 **Configuração Ausente.** "
            "`CLOUD_RUN_AUTH_PASSWORD` não está definido no Cloud Run. "
            "Contate o administrador."
        )
        st.stop()
        return False  # pragma: no cover

    st.markdown(
        """
        <div style="text-align: center; margin-top: 100px; padding: 2rem;">
            <div style="display: inline-block; padding: 1.5rem; background: rgba(173, 198, 255, 0.05); border-radius: 50%; margin-bottom: 2rem;">
                <span style="font-size: 3rem;">🎯</span>
            </div>
            <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; color: #fff; font-size: 3.5rem; letter-spacing: -0.05em; margin-bottom: 0.5rem;">Gercon Analytics</h1>
            <p style="color: #adc6ff; font-size: 1rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.3em; opacity: 0.8;">Sistema de Regulação Clínica</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        with st.form("cloud_run_login", clear_on_submit=True):
            st.subheader("🔐 Login")
            password = st.text_input("Senha de Acesso", type="password", key="cr_pwd")
            submitted = st.form_submit_button(
                "Entrar", use_container_width=True, type="primary"
            )

        if submitted and password:
            if verify_cloud_run_password(password, expected_hash, expected_plain):
                st.session_state["cloud_run_authenticated"] = True
                st.rerun()
            else:
                st.error("❌ Senha incorreta.")

    st.stop()
    return False  # pragma: no cover


# ---------------------------------------------------------------------------
# 4. Resolve Authenticated User — Facade Principal
# ---------------------------------------------------------------------------


def resolve_authenticated_user(
    headers: dict[str, str],
) -> tuple[ValidatedUserToken, str]:
    """SRE BFF Pattern: Seleciona a estratégia de autenticação conforme o runtime.

    Executa a strategy correta na seguinte ordem de prioridade:
    1. **Dev Mock** (ENVIRONMENT=dev + ALLOW_UNAUTHENTICATED_DEV=true):
       Retorna usuário mock sem IAP Proxy (dois fatores obrigatórios).
    2. **Cloud Run** (K_SERVICE presente):
       Executa o password gate (ADR-004) e retorna perfil padrão clínico.
    3. **Docker Compose / K8s** (IAP Proxy headers):
       Extrai JWT do header ``x-forwarded-access-token`` e valida via Keycloak.

    Args:
        headers: Dict de headers HTTP do request (``st.context.headers``).

    Returns:
        Tuple ``(ValidatedUserToken, jwt_str)`` onde ``jwt_str`` é o token raw.

    Raises:
        ValueError: Se nenhum header de autenticação for encontrado no path
            Docker Compose/K8s (configuração de infraestrutura incorreta).
    """
    # === PATH 1: Desenvolvimento Local sem IAP Proxy ===
    if is_dev_mock_allowed():
        mock_user = ValidatedUserToken(
            sub="dev-id-123",
            email="dev@gercon.com",
            preferred_username="dev_user",
            roles=["diretor_medico"],
            crm_numero="99999",
            crm_uf="RS",
            exp=int(time.time() + 86400),
        )
        return mock_user, "mock-jwt-token"

    # === PATH 2: Cloud Run Serverless (sem Keycloak/oauth2-proxy) ===
    # ADR-004: Password gate → cria sessão com perfil clínico default.
    # TODO(ADR-004/Phase2): Substituir por Firebase Auth com Firestore user profiles.
    if is_cloud_run():
        cloud_run_login_gate()  # Bloqueia com st.stop() se não autenticado
        cloud_user = ValidatedUserToken(
            sub="cloud-run-user",
            email=os.getenv("CLOUD_RUN_DEFAULT_EMAIL", "clinico@gercon.com"),
            preferred_username=os.getenv("CLOUD_RUN_DEFAULT_USER", "clinico"),
            roles=[os.getenv("CLOUD_RUN_DEFAULT_ROLE", "diretor_medico")],
            crm_numero=os.getenv("CLOUD_RUN_CRM_NUMERO"),
            crm_uf=os.getenv("CLOUD_RUN_CRM_UF"),
            exp=int(time.time() + 86400),
        )
        return cloud_user, "cloud-run-session"

    # === PATH 3: Docker Compose / K8s com OAuth2-Proxy (Prod original) ===
    # Extrai o JWT do header injetado pelo OAuth2-Proxy.
    auth_header = (
        headers.get("x-forwarded-access-token")
        or headers.get("x-auth-request-access-token")
        or headers.get("authorization", "").replace("Bearer ", "")
    )

    if not auth_header:
        raise ValueError("Missing Authentication Headers (IAP Proxy)")

    from infrastructure.auth.jwt_validator import verify_token  # ACL: import local

    user = verify_token(auth_header)
    return user, auth_header


# ---------------------------------------------------------------------------
# 5. Build Logout URL (pura — sem side-effects)
# ---------------------------------------------------------------------------


def build_logout_url(is_cloud_run_runtime: bool) -> Optional[str]:
    """Constrói a URL de logout conforme o runtime.

    Args:
        is_cloud_run_runtime: ``True`` se o runtime é Cloud Run.

    Returns:
        ``None`` para Cloud Run (logout via session_state clear).
        URL completa de logout OAuth2-Proxy → Keycloak para Docker Compose.

    WHY: Docker Compose requer cadeia de dois passos para logout completo:
    1. OAuth2-Proxy limpa o cookie de sessão.
    2. Keycloak destrói a sessão SSO.
    Sem o ``post_logout_redirect_uri`` o browser fica na tela do Keycloak.
    """
    if is_cloud_run_runtime:
        # Cloud Run: sem proxy/Keycloak — logout é via limpeza de session_state
        return None

    keycloak_base = os.getenv("KEYCLOAK_SERVER_URL", "http://iam.127.0.0.1.nip.io:8080")
    realm = os.getenv("KEYCLOAK_REALM", "gercon-realm")
    client_id = os.getenv("KEYCLOAK_CLIENT_ID", "gercon-analytics")
    external_domain = os.getenv("EXTERNAL_DOMAIN", "127.0.0.1.nip.io")
    post_logout_uri = f"http://{external_domain}/dashboard/"

    keycloak_logout_url = (
        f"{keycloak_base}/realms/{realm}/protocol/openid-connect/logout"
        f"?client_id={client_id}"
        f"&post_logout_redirect_uri={quote(post_logout_uri, safe='')}"
    )

    # OAuth2-Proxy: limpa cookie E redireciona para logout do Keycloak
    return f"/oauth2/sign_out?rd={quote(keycloak_logout_url, safe='')}"
