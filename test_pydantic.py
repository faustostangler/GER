from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import os

os.environ["S3_ENDPOINT_URL"] = "http://127.0.0.1:9000"

class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    endpoint_url: str = Field(default="def", alias="S3_ENDPOINT_URL")

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    s3: S3Settings = Field(default_factory=S3Settings)

s = AppSettings()
print("S3:", s.s3.endpoint_url)
