from pydantic import (
    Field,
    HttpUrl,
    SecretStr,
    computed_field,
    field_validator,
    BaseModel,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class RDESettings(BaseModel):
    """Configurações para o ambiente de desenvolvimento remoto."""

    access_token: str = Field(
        default="00000000000000000000000000000000", alias="RDE_ACCESS_TOKEN"
    )
    vnc_password: str = Field(default="flyai_secret", alias="VNC_PASSWORD")
    grpc_port: int = Field(default=50051, alias="GRPC_PORT")

    @model_validator(mode="after")
    def validate_token_security(self) -> "RDESettings":
        if len(self.access_token) < 32:
            raise ValueError(
                "RDE_ACCESS_TOKEN deve ter pelo menos 32 caracteres para segurança."
            )
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
        extra="ignore",
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

    # Rede & Domínio — valores refletem env/config.env; mudam por contexto (local/remoto)
    EXTERNAL_DOMAIN: str = Field(default="127.0.0.1.nip.io")
    PROTOCOL: str = Field(default="http")

    # Observability & Error Tracking
    SENTRY_DSN: Optional[str] = Field(default=None, description="Sentry DSN for production error tracking")
    GIT_SHA: str = Field(default="local-dev", description="Git commit SHA injected at build time for release tracking")

    # Keycloak OIDC Settings — hostname público (browser-facing)
    # WHY: KEYCLOAK_SERVER_URL e KEYCLOAK_REALM definem o 'iss' claim dos JWTs
    # e a URL de redirect para o browser. Mudam por contexto (local/remoto).
    KEYCLOAK_SERVER_URL: Optional[HttpUrl] = Field(
        default=None,
        description="URL pública do Keycloak (browser-facing). Ex: http://iam.127.0.0.1.nip.io:8080",
    )
    KEYCLOAK_REALM: str = Field(default="gercon-realm")
    KEYCLOAK_CLIENT_ID: str = Field(default="gercon-analytics")
    KEYCLOAK_CLIENT_SECRET: SecretStr = Field(default="change-me")

    # Split-Horizon DNS — acesso interno Docker ao Keycloak
    # WHY (ADR-003 / Split-Horizon): Containers não conseguem resolver o hostname público
    # do Keycloak (IAM_SUBDOMAIN). Usam o service name do Docker Compose via mesh interno.
    # KEYCLOAK_INTERNAL_SERVICE = nome do serviço no docker-compose.yml (canônico: 'keycloak').
    # Mude APENAS se o service name mudar — não muda entre contextos local/remoto.
    KEYCLOAK_INTERNAL_SERVICE: str = Field(
        default="keycloak",
        description="Service name Docker do Keycloak (Split-Horizon DNS, mesh interno).",
    )
    KEYCLOAK_INTERNAL_PORT: int = Field(
        default=8080,
        description="Porta interna do Keycloak no mesh Docker (Split-Horizon DNS).",
    )

    # Infrastructure Overrides → ClinicaPolicy (domain.models)
    # WHY (ADR-005): Estas vars permitem sobrescrever os defaults do domínio via .env
    # para ambientes específicos (ex: staging, testes de carga). O Domain (ClinicaPolicy)
    # É sempre a fonte de verdade dos defaults; .env é apenas um mecanismo de override.
    # A camada de composição (get_use_case em app_analytics.py) é responsável por
    # construir ClinicaPolicy com esses valores.
    AGE_MIN: int = Field(default=0, description="Override: ClinicaPolicy.idade_min")
    AGE_MAX: int = Field(default=120, description="Override: ClinicaPolicy.idade_max")
    SLA_DIAS_VENCIMENTO: int = Field(default=180, description="Override: ClinicaPolicy.sla_dias_vencimento")
    DATA_SLA_THRESHOLD: float = Field(default=2.0, description="Override: ClinicaPolicy.data_sla_threshold_horas")
    MES_COMERCIAL_DIAS: float = Field(default=30.416, description="Override: ClinicaPolicy.mes_comercial_dias")
    CORES_URGENCIA: list[str] = Field(default=["VERMELHO", "LARANJA", "AMARELO"], description="Override: ClinicaPolicy.cores_urgencia")
    GERCON_URL: HttpUrl = Field(default="https://gercon.procempa.com.br/gerconweb/")

    @field_validator("CORES_URGENCIA", mode="before")
    @classmethod
    def parse_cores(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
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
        """Issuer claim conforme emitido pelo Keycloak (hostname público KC_HOSTNAME_URL).

        WHY (Split-Horizon DNS): O Keycloak emite JWTs com o claim ``iss`` igual ao
        KC_HOSTNAME_URL — o hostname público configurado via KEYCLOAK_SERVER_URL.
        O PyJWT valida o claim ``iss`` do token contra este valor.
        Deve corresponder exatamente ao que o Keycloak emite.

        Contexto local : http://iam.127.0.0.1.nip.io:8080/realms/gercon-realm
        Contexto remoto: https://iam.exemplo.com/realms/gercon-realm
        """
        if self.KEYCLOAK_SERVER_URL:
            url_str = str(self.KEYCLOAK_SERVER_URL).rstrip("/")
            return f"{url_str}/realms/{self.KEYCLOAK_REALM}"
        # Fallback derivado do domínio externo (porta 8080 convencional do Keycloak)
        return f"{self.PROTOCOL}://{self.EXTERNAL_DOMAIN}:8080/realms/{self.KEYCLOAK_REALM}"

    @computed_field
    @property
    def keycloak_internal_base(self) -> str:
        """Base URL interna Docker para o Keycloak (mesh-internal, sem passar pelo proxy).

        WHY (Split-Horizon DNS): O JWKS endpoint (certs) deve ser buscado via
        service name Docker interno para que containers consigam resolver o DNS.
        O hostname público (IAM_SUBDOMAIN) só é resolvível pelo browser do cliente.

        Usa KEYCLOAK_INTERNAL_SERVICE e KEYCLOAK_INTERNAL_PORT do config para
        eliminar o hardcode 'keycloak:8080' e permitir parametrização.
        """
        return f"http://{self.KEYCLOAK_INTERNAL_SERVICE}:{self.KEYCLOAK_INTERNAL_PORT}/realms/{self.KEYCLOAK_REALM}"

    @computed_field
    @property
    def jwks_url(self) -> str:
        """URL interna para busca de chaves JWKS (Split-Horizon: usa mesh interno).

        WHY: A PyJWKClient faz um HTTP GET para buscar as chaves públicas do Keycloak.
        Esta requisição parte de *dentro* do container — deve usar o hostname Docker
        interno, não o hostname público inacessível internamente.
        """
        return f"{self.keycloak_internal_base}/protocol/openid-connect/certs"

    @computed_field
    @property
    def base_url(self) -> str:
        """URL base pública da aplicação (sem trailing slash).

        WHY: Utilizada para construir URLs de redirect (ex: logout post_redirect_uri).
        Derivada automaticamente de PROTOCOL e EXTERNAL_DOMAIN para suportar
        tanto contexto local (http://127.0.0.1.nip.io) quanto remoto (https://exemplo.com).
        """
        return f"{self.PROTOCOL}://{self.EXTERNAL_DOMAIN}"


class Settings(AppSettings):
    pass


settings = Settings()
