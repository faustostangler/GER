import logging
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from infrastructure.config import settings
from infrastructure.database.models import Base

logger = logging.getLogger(__name__)

db_url = (
    f"postgresql+psycopg://{settings.db.user}:{settings.db.password}@"
    f"{settings.db.service_name}:{settings.db.internal_port}/{settings.db.name}"
)

# WHY (SRE Fail-Fast constraint): Verify DB reachability on startup. If unreachable,
# the pod crashes immediately instead of failing silently later during a user request.
# We skip this check if ENVIRONMENT is 'testing' OR if we are running inside pytest.
# Robust detection: check sys.modules, sys.argv, and APP__ENVIRONMENT.
is_pytest = (
    "pytest" in sys.modules
    or "_pytest" in sys.modules
    or any("pytest" in arg for arg in sys.argv)
)

if settings.ENVIRONMENT != "testing" and not is_pytest:
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            logger.info("✅ Database connection established successfully.")

        # Create tables automatically for local dev environments
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        logger.critical(f"FATAL: Database is unreachable on startup: {e}")
        sys.exit(1)
else:
    # In testing, we don't fail-fast to allow manual mocking of the engine/session.
    # We use a lazy approach: engine is created but no connection is attempted here.
    logger.info("🧪 Testing environment detected. Using SQLite in-memory fallback.")
    engine = create_engine("sqlite:///:memory:")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
