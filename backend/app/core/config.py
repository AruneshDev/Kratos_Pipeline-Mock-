from pydantic_settings import BaseSettings, SettingsConfigDict
from decimal import Decimal


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://mybank:mybank_dev@localhost:5432/mybank"
    sync_database_url: str = "postgresql://mybank:mybank_dev@localhost:5432/mybank"

    # Application
    app_env: str = "development"
    log_level: str = "INFO"

    # FDIC: Standard Maximum Deposit Insurance Amount
    smdia: Decimal = Decimal("250000.00")

    # Seed data path
    seed_data_path: str = "app/seed/data"


settings = Settings()
