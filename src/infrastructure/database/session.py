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
try:
    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.connect() as conn:
        logger.info("✅ Database connection established successfully.")

    # Create tables automatically for local dev environments
    Base.metadata.create_all(bind=engine)
except Exception as e:
    logger.critical(f"FATAL: Database is unreachable on startup: {e}")
    sys.exit(1)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
