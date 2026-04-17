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


# ---------------------------------------------------------------------------
# 6. require_authentication — Facade Único para main() (Session Lifecycle)
# ---------------------------------------------------------------------------


def require_authentication() -> ValidatedUserToken:
    """Facade principal de IAM: gere o ciclo de vida completo da sessão.

    Deve ser a ÚNICA chamada de autenticação em ``main()``. Encapsula:

    - **Camada 1 — Sessão ativa e válida**: retorno imediato sem I/O.
    - **Camada 2 — Token expirado**: renderiza CTA de renovação e para.
    - **Camada 3 — Primeira carga**: executa resolução de identidade (Dev Mock /
      Cloud Run Gate / IAP Header), persiste na session e redesenha.

    Returns:
        ``ValidatedUserToken`` com identidade clínica verificada.

    Note:
        Chama ``st.stop()`` nas Camadas 2 e 3 quando o usuário ainda não está
        autenticado ou o token expirou — o Streamlit não continua renderizando
        além do ``st.stop()``. O retorno de valor ocorre APENAS na Camada 1
        (rerun posterior com sessão já populada).

    Ref: ADR-006 — IAM Adapter Isolation (Fase 3).
    """
    import streamlit as st  # ACL: import local — evita erro fora do runtime Streamlit

    # === CAMADA 1: Sessão ativa e válida — zero fricção ===
    _user_in_state = "user" in st.session_state
    _token_exp = st.session_state.get("token_exp", 0)
    _token_valid = _token_exp > time.time()

    if _user_in_state and _token_valid:
        # Happy path: retorna o usuário já validado sem nenhuma verificação adicional
        return st.session_state.user  # type: ignore[return-value]

    # === CAMADA 2: Token expirado — CTA de renovação ===
    if _user_in_state and not _token_valid:
        st.warning(
            "⏱️ Sua sessão de 24h expirou. Clique em **Renovar Login** para continuar.",
            icon="🔒",
        )
        # WHY: build_logout_url() retorna None para Cloud Run (limpeza de sessão)
        # e URL completa de cadeia OAuth2-Proxy → Keycloak para Docker Compose.
        if is_cloud_run():
            if st.button("🔄 Renovar Login", type="primary"):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
        else:
            renewal_url = build_logout_url(is_cloud_run_runtime=False) or "/oauth2/sign_out?rd=/dashboard/"
            st.link_button("🔄 Renovar Login", renewal_url, type="primary")
        st.stop()

    # === CAMADA 3: Primeira carga — resolução de identidade ===
    try:
        import streamlit as st  # noqa: F811 — re-import para escopo local
        user_domain, jwt_str = resolve_authenticated_user(headers=dict(st.context.headers))
        st.session_state.user = user_domain
        st.session_state.raw_jwt = jwt_str
        # SRE: Sessão de 24h alinhada com a política de sessão clínica do domínio
        st.session_state.token_exp = (
            user_domain.exp if user_domain.exp else (time.time() + 86400)
        )
        st.rerun()  # Redesenha com sessão populada → entra na Camada 1 no próximo ciclo
    except Exception as _auth_err:
        # WHY: Falha real de autenticação — exibe UI de erro e para.
        # Cloud Run: gate já tratou via st.stop() em cloud_run_login_gate().
        # Docker Compose: ausência de headers IAP → infra mal configurada.
        _render_auth_error(is_cloud_run_runtime=is_cloud_run())
        if os.getenv("APP__DEBUG", "false").lower() == "true":
            _render_debug_headers()
        st.stop()

    # Nunca chegará aqui — st.rerun() e st.stop() interrompem o fluxo Streamlit.
    # Linha de segurança para satisfazer type-checkers.
    raise RuntimeError("require_authentication: fluxo inesperado pós-stop/rerun.")  # pragma: no cover


# ---------------------------------------------------------------------------
# 7. Helpers de UI de Erro (Humble Object — testáveis via injeção)
# ---------------------------------------------------------------------------


def _render_auth_error(is_cloud_run_runtime: bool) -> None:
    """Renderiza a UI de erro de autenticação conforme o runtime.

    WHY: Extraído como função pura de UI para facilitar testes (Humble Object).
    Não chama ``st.stop()`` — responsabilidade do chamador.
    """
    import streamlit as st  # ACL: import local

    if is_cloud_run_runtime:
        st.error(
            "🚨 **Erro inesperado de autenticação no Cloud Run.** Recarregue a página."
        )
    else:
        st.error(
            "🚨 **Acesso não autorizado.** Não foi possível verificar a sua identidade."
        )
        st.markdown(
            """
            <div style="display: flex; justify-content: center; margin-top: 20px;">
                <form action="/oauth2/start" method="GET">
                    <input type="hidden" name="rd" value="/dashboard/" />
                    <button type="submit" style="
                        background-color: #ef4444;
                        color: white;
                        padding: 12px 32px;
                        border-radius: 12px;
                        font-weight: 600;
                        font-size: 1.1rem;
                        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                        border: 2px solid #ef4444;
                        cursor: pointer;
                        font-family: 'Source Sans Pro', sans-serif;
                        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
                    ">
                        🔑 Realizar Login (Keycloak)
                    </button>
                </form>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_debug_headers() -> None:
    """Renderiza painel de debug de headers IAP (apenas quando APP__DEBUG=true).

    WHY: Isolado para evitar vazamento de headers em produção. O caller
    é responsável por checar a env var antes de invocar.
    """
    import streamlit as st  # ACL: import local

    with st.expander("🛠️ Debug Identity (Headers detectados)"):
        st.write("Headers detectados via st.context.headers:")
        st.json(
            {
                k: v
                for k, v in st.context.headers.items()
                if k.lower().startswith("x-")
            }
        )
