from pydantic import Field, HttpUrl, SecretStr, computed_field, field_validator, BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class RDESettings(BaseModel):
    """Configurações para o ambiente de desenvolvimento remoto."""
    access_token: str = Field(default="00000000000000000000000000000000", alias="RDE_ACCESS_TOKEN")
    vnc_password: str = Field(default="flyai_secret", alias="VNC_PASSWORD")
    grpc_port: int = Field(default=50051, alias="GRPC_PORT")

    @model_validator(mode="after")
    def validate_token_security(self) -> 'RDESettings':
        if len(self.access_token) < 32:
            raise ValueError("RDE_ACCESS_TOKEN deve ter pelo menos 32 caracteres para segurança.")
        return self

class DatabaseSettings(BaseModel):
    user: str = Field(default="postgres", alias="DB__USER")
    password: str = Field(default="postgres", alias="DB__PASSWORD")
    name: str = Field(default="fly_ai_db", alias="DB__NAME")
    service_name: str = Field(default="db", alias="DB_SERVICE_NAME")
    internal_port: int = Field(default=5432, alias="DB_INTERNAL_PORT")
    memory_limit: str = Field(default="1.5GB", alias="DUCKDB_MEMORY_LIMIT")

class RedisSettings(BaseModel):
    host: str = Field(default="cache", alias="REDIS__HOST")
    port: int = Field(default=6379, alias="REDIS__PORT")

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("env/creds.env", "env/config.env"), 
        env_file_encoding="utf-8", 
        extra="ignore"
    )

    # Core App Config
    ENVIRONMENT: str = Field(default="local", alias="APP__ENVIRONMENT")
    DEBUG: bool = Field(default=True, alias="APP__DEBUG")
    LOG_LEVEL: str = Field(default="INFO", alias="LOG_LEVEL")
    
    # Nested Settings
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    rde: RDESettings = Field(default_factory=RDESettings)

    # Infrastructure & IO
    OUTPUT_FILE: str = Field(default="gercon_consolidado.parquet")
    KAFKA_URL: str = Field(default="redpanda-0:9092")
    EXTERNAL_DOMAIN: str = Field(default="127.0.0.1.nip.io")

    # Keycloak OIDC Settings
    # SOTA: Fallback para as variáveis do oauth2-proxy se não estiverem explicitamente definidas
    KEYCLOAK_SERVER_URL: Optional[HttpUrl] = Field(default=None)
    KEYCLOAK_REALM: str = Field(default="gercon-realm")
    KEYCLOAK_CLIENT_ID: str = Field(default="gercon-analytics")
    KEYCLOAK_CLIENT_SECRET: SecretStr = Field(default="change-me")

    # Business Rules
    AGE_MIN: int = Field(default=0)
    AGE_MAX: int = Field(default=120)
    SLA_DIAS_VENCIMENTO: int = Field(default=180)
    MES_COMERCIAL_DIAS: float = Field(default=30.416)
    CORES_URGENCIA: list[str] = Field(default=["VERMELHO", "LARANJA", "AMARELO"])
    GERCON_URL: HttpUrl = Field(default="https://gercon.procempa.com.br/gerconweb/")

    @field_validator("CORES_URGENCIA", mode="before")
    @classmethod
    def parse_cores(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(',')]
        return v

    @field_validator("KEYCLOAK_SERVER_URL", mode="before")
    @classmethod
    def strip_trailing_slash(cls, v):
        if isinstance(v, str):
            return v.rstrip("/")
        return v

    @computed_field
    @property
    def keycloak_issuer(self) -> str:
        if self.KEYCLOAK_SERVER_URL:
            url_str = str(self.KEYCLOAK_SERVER_URL).rstrip("/")
            return f"{url_str}/realms/{self.KEYCLOAK_REALM}"
        # Fallback para o domínio externo se o server_url não estiver setado
        return f"http://{self.EXTERNAL_DOMAIN}:8080/realms/{self.KEYCLOAK_REALM}"

    @computed_field
    @property
    def jwks_url(self) -> str:
        return f"{self.keycloak_issuer}/protocol/openid-connect/certs"

class Settings(AppSettings):
    pass

settings = Settings()
