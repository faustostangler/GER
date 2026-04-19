import os
import time
import hashlib
from typing import Optional
from urllib.parse import quote
import streamlit as st

from application.use_cases.interfaces import IIdentityService
from infrastructure.auth.token_acl import ValidatedUserToken

class MockIdentityAdapter(IIdentityService):
    def get_current_user(self) -> ValidatedUserToken:
        if self.is_authenticated():
            return st.session_state.user

        mock_user = ValidatedUserToken(
            sub="dev-id-123",
            email="dev@gercon.com",
            preferred_username="dev_user",
            roles=["diretor_medico"],
            crm_numero="99999",
            crm_uf="RS",
            exp=int(time.time() + 86400),
        )
        st.session_state.user = mock_user
        st.session_state.raw_jwt = "mock-jwt-token"
        st.session_state.token_exp = mock_user.exp
        st.rerun()

    def get_logout_url(self) -> Optional[str]:
        return None

    def is_authenticated(self) -> bool:
        _user_in_state = "user" in st.session_state
        _token_exp = st.session_state.get("token_exp", 0)
        _token_valid = _token_exp > time.time()
        return _user_in_state and _token_valid


class CloudRunIdentityAdapter(IIdentityService):
    def __init__(self, settings):
        self.settings = settings

    def get_current_user(self) -> ValidatedUserToken:
        if self.is_authenticated():
            return st.session_state.user
            
        _user_in_state = "user" in st.session_state
        _token_exp = st.session_state.get("token_exp", 0)
        _token_valid = _token_exp > time.time()
        
        # token expired check
        if _user_in_state and not _token_valid:
            st.warning("⏱️ Sua sessão de 24h expirou. Clique em **Renovar Login** para continuar.", icon="🔒")
            if st.button("🔄 Renovar Login", type="primary"):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
            st.stop()
            
        # check cloud run login gate
        if st.session_state.get("cloud_run_authenticated"):
            # already passed password gate
            cloud_user = ValidatedUserToken(
                sub="cloud-run-user",
                email=os.getenv("CLOUD_RUN_DEFAULT_EMAIL", "clinico@gercon.com"),
                preferred_username=os.getenv("CLOUD_RUN_DEFAULT_USER", "clinico"),
                roles=[os.getenv("CLOUD_RUN_DEFAULT_ROLE", "diretor_medico")],
                crm_numero=os.getenv("CLOUD_RUN_CRM_NUMERO"),
                crm_uf=os.getenv("CLOUD_RUN_CRM_UF"),
                exp=int(time.time() + 86400),
            )
            st.session_state.user = cloud_user
            st.session_state.raw_jwt = "cloud-run-session"
            st.session_state.token_exp = cloud_user.exp
            st.rerun()

        # Show gate
        self._cloud_run_login_gate()
        
    def _cloud_run_login_gate(self):
        expected_hash = os.getenv("CLOUD_RUN_AUTH_PASSWORD_HASH", "")
        expected_plain = os.getenv("CLOUD_RUN_AUTH_PASSWORD", "")

        if not expected_hash and not expected_plain:
            st.error(
                "🚨 **Configuração Ausente.** "
                "`CLOUD_RUN_AUTH_PASSWORD` não está definido no Cloud Run. "
                "Contate o administrador."
            )
            st.stop()

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
                input_sha256 = hashlib.sha256(password.encode()).hexdigest()
                valid = False
                if expected_hash:
                    valid = input_sha256 == expected_hash.lower()
                else:
                    valid = password == expected_plain
                
                if valid:
                    st.session_state["cloud_run_authenticated"] = True
                    st.rerun()
                else:
                    st.error("❌ Senha incorreta.")

        st.stop()

    def get_logout_url(self) -> Optional[str]:
        return None

    def is_authenticated(self) -> bool:
        _user_in_state = "user" in st.session_state
        _token_exp = st.session_state.get("token_exp", 0)
        _token_valid = _token_exp > time.time()
        return _user_in_state and _token_valid


class IAPIdentityAdapter(IIdentityService):
    def __init__(self, settings):
        self.settings = settings

    def get_current_user(self) -> ValidatedUserToken:
        if self.is_authenticated():
            return st.session_state.user
            
        _user_in_state = "user" in st.session_state
        _token_exp = st.session_state.get("token_exp", 0)
        _token_valid = _token_exp > time.time()
        
        if _user_in_state and not _token_valid:
            st.warning(
                "⏱️ Sua sessão de 24h expirou. Clique em **Renovar Login** para continuar.",
                icon="🔒",
            )
            renewal_url = self.get_logout_url() or "/oauth2/sign_out?rd=/dashboard/"
            st.link_button("🔄 Renovar Login", renewal_url, type="primary")
            st.stop()

        try:
            headers = {k.lower(): v for k, v in st.context.headers.items()}
            auth_header = (
                headers.get("x-forwarded-access-token")
                or headers.get("x-auth-request-access-token")
                or headers.get("authorization", "").replace("Bearer ", "")
            )

            if not auth_header:
                raise ValueError("Missing Authentication Headers (IAP Proxy)")

            from infrastructure.auth.jwt_validator import verify_token
            user_domain = verify_token(auth_header)

            st.session_state.user = user_domain
            st.session_state.raw_jwt = auth_header
            st.session_state.token_exp = (
                user_domain.exp if user_domain.exp else (time.time() + 86400)
            )
            st.rerun()
        except Exception as _auth_err:
            st.error("🚨 **Acesso não autorizado.** Não foi possível verificar a sua identidade.")
            st.markdown(
                f'''
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
                ''',
                unsafe_allow_html=True,
            )
            if os.getenv("APP__DEBUG", "false").lower() == "true":
                with st.expander("🛠️ Debug Identity (Headers detectados)"):
                    st.write("Headers detectados via st.context.headers:")
                    st.json(
                        {
                            k: v
                            for k, v in st.context.headers.items()
                            if k.lower().startswith("x-")
                        }
                    )
                    st.error(f"❌ Erro de autenticação: {type(_auth_err).__name__}: {_auth_err}")
            st.stop()

    def get_logout_url(self) -> Optional[str]:
        keycloak_logout_url = (
            f"{self.settings.keycloak_issuer}/protocol/openid-connect/logout"
            f"?client_id={self.settings.KEYCLOAK_CLIENT_ID}"
            f"&post_logout_redirect_uri={quote(f'{self.settings.base_url}/dashboard/', safe='')}"
        )
        return f"/oauth2/sign_out?rd={quote(keycloak_logout_url, safe='')}"

    def is_authenticated(self) -> bool:
        _user_in_state = "user" in st.session_state
        _token_exp = st.session_state.get("token_exp", 0)
        _token_valid = _token_exp > time.time()
        return _user_in_state and _token_valid
