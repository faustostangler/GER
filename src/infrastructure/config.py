from pydantic import Field, HttpUrl, SecretStr, computed_field, field_validator, BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class RDESettings(BaseModel):
    """Configurações para o ambiente de desenvolvimento remoto."""
    access_token: str = Field(default="00000000000000000000000000000000", alias="RDE_ACCESS_TOKEN")
    vnc_password: str = Field(default="flyai_secret")
    grpc_port: int = Field(default=50051)

    @model_validator(mode="after")
    def validate_token_security(self) -> 'RDESettings':
        if len(self.access_token) < 32:
            raise ValueError("RDE_ACCESS_TOKEN deve ter pelo menos 32 caracteres para segurança.")
        return self

class KeycloakSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("env/creds.env", "env/config.env"), 
        env_file_encoding="utf-8", 
        extra="ignore"
    )

    rde: RDESettings = Field(default_factory=RDESettings)

    # General Settings (migrated from app_analytics.py)
    OUTPUT_FILE: str = Field(default="gercon_consolidado.parquet")

    # Keycloak OIDC Settings
    KEYCLOAK_SERVER_URL: HttpUrl
    KEYCLOAK_REALM: str
    KEYCLOAK_CLIENT_ID: str
    KEYCLOAK_CLIENT_SECRET: SecretStr

    # --- SRE: Infraestrutura Centralizada (Pydantic) ---
    KAFKA_URL: str = Field(default="redpanda-0:9092")

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

class Settings(KeycloakSettings):
    pass

settings = Settings()
