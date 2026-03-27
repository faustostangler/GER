from pydantic import Field, HttpUrl, SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class KeycloakSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("env/creds.env", "env/config.env"), 
        env_file_encoding="utf-8", 
        extra="ignore"
    )

    # General Settings (migrated from app_analytics.py)
    OUTPUT_FILE: str = Field(default="gercon_consolidado.parquet")

    # Keycloak OIDC Settings
    KEYCLOAK_SERVER_URL: HttpUrl
    KEYCLOAK_REALM: str
    KEYCLOAK_CLIENT_ID: str
    KEYCLOAK_CLIENT_SECRET: SecretStr

    # Sanitization to ensure URL paths aren't malformed
    @field_validator("KEYCLOAK_SERVER_URL", mode="before")
    @classmethod
    def strip_trailing_slash(cls, v):
        if isinstance(v, str):
            return v.rstrip("/")
        return v

    @computed_field
    @property
    def jwks_url(self) -> str:
        # Pydantic V2 HttpUrl casts require str() for plain string manipulation
        url_str = str(self.KEYCLOAK_SERVER_URL).rstrip("/")
        return f"{url_str}/realms/{self.KEYCLOAK_REALM}/protocol/openid-connect/certs"

    @computed_field
    @property
    def keycloak_issuer(self) -> str:
        url_str = str(self.KEYCLOAK_SERVER_URL).rstrip("/")
        return f"{url_str}/realms/{self.KEYCLOAK_REALM}"

settings = KeycloakSettings()
