import json
import logging
from typing import Optional

from sqlalchemy import create_engine, Column, String, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
import redis

from application.use_cases.interfaces import IDoctorProfileRepository
from domain.identity import DoctorProfile, MedicalCouncilRegistration
from infrastructure.config import settings

logger = logging.getLogger(__name__)

Base = declarative_base()

class DoctorProfileModel(Base):
    """PostgreSQL model for DoctorProfile persistence."""
    __tablename__ = "doctor_profiles"

    user_id = Column(String, primary_key=True)
    crm_numero = Column(String, nullable=False)
    crm_uf = Column(String, nullable=False, index=True)
    crm_verified = Column(Boolean, default=False, nullable=False)


class SQLDoctorProfileRepository(IDoctorProfileRepository):
    """PostgreSQL implementation of DoctorProfile repository with Redis write-through cache."""

    def __init__(self):
        # Configure PostgreSQL connection
        db_url = (
            f"postgresql://{settings.db.user}:{settings.db.password}@"
            f"{settings.db.service_name}:{settings.db.internal_port}/{settings.db.name}"
        )
        self.engine = create_engine(db_url, pool_pre_ping=True)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # SRE: Ensure table exists (Init-on-startup/Gaffer pattern)
        try:
            Base.metadata.create_all(self.engine)
        except Exception as e:
            logger.error("Failed to create doctor_profiles table: %s", e)

        # Configure Redis cache (Graceful Degradation)
        try:
            self.redis_client = redis.Redis(
                host=settings.redis.host,
                port=settings.redis.port,
                db=0,
                socket_connect_timeout=2,
                socket_timeout=2,
                decode_responses=True
            )
            self.redis_client.ping()
        except Exception:
            logger.warning("Redis cache unavailable for DoctorProfileRepository. Falling back to SQL only.")
            self.redis_client = None

        self.cache_ttl = 3600  # 1 hour

    def _get_cache_key(self, user_id: str) -> str:
        return f"doctor_profile:{user_id}"

    def find_by_user_id(self, user_id: str) -> Optional[DoctorProfile]:
        """Lookup profile with Redis Fast Path → PostgreSQL Source of Truth."""
        # 1. Redis Fast Path
        if self.redis_client:
            try:
                cached = self.redis_client.get(self._get_cache_key(user_id))
                if cached:
                    data = json.loads(cached)
                    return DoctorProfile(
                        user_id=data["user_id"],
                        crm=MedicalCouncilRegistration(
                            crm_numero=data["crm_numero"],
                            crm_uf=data["crm_uf"]
                        ),
                        crm_verified=data["crm_verified"]
                    )
            except Exception as e:
                logger.warning("Redis cache read failure for user %s: %s", user_id, e)

        # 2. PostgreSQL Source of Truth
        db = self.SessionLocal()
        try:
            model = db.query(DoctorProfileModel).filter(DoctorProfileModel.user_id == user_id).first()
            if not model:
                return None
            
            profile = DoctorProfile(
                user_id=model.user_id,
                crm=MedicalCouncilRegistration(
                    crm_numero=model.crm_numero,
                    crm_uf=model.crm_uf
                ),
                crm_verified=model.crm_verified
            )

            # 3. Cache Hydration
            if self.redis_client:
                try:
                    self.redis_client.setex(
                        self._get_cache_key(user_id),
                        self.cache_ttl,
                        json.dumps({
                            "user_id": profile.user_id,
                            "crm_numero": profile.crm.crm_numero,
                            "crm_uf": profile.crm.crm_uf,
                            "crm_verified": profile.crm_verified
                        })
                    )
                except Exception as e:
                    logger.warning("Redis cache write failure for user %s: %s", user_id, e)

            return profile
        except SQLAlchemyError as e:
            logger.error("PostgreSQL query failure for user %s: %s", user_id, e)
            return None
        finally:
            db.close()

    def save(self, profile: DoctorProfile) -> None:
        """Persist to PostgreSQL and invalidate/update Redis cache."""
        db = self.SessionLocal()
        try:
            # Atomic UPSERT equivalent
            model = db.query(DoctorProfileModel).filter(DoctorProfileModel.user_id == profile.user_id).first()
            if model:
                model.crm_numero = profile.crm.crm_numero
                model.crm_uf = profile.crm.crm_uf
                model.crm_verified = profile.crm_verified
            else:
                model = DoctorProfileModel(
                    user_id=profile.user_id,
                    crm_numero=profile.crm.crm_numero,
                    crm_uf=profile.crm.crm_uf,
                    crm_verified=profile.crm_verified
                )
                db.add(model)
            
            db.commit()

            # 2. Redis Invalidation (Write-through/Update)
            if self.redis_client:
                try:
                    self.redis_client.setex(
                        self._get_cache_key(profile.user_id),
                        self.cache_ttl,
                        json.dumps({
                            "user_id": profile.user_id,
                            "crm_numero": profile.crm.crm_numero,
                            "crm_uf": profile.crm.crm_uf,
                            "crm_verified": profile.crm_verified
                        })
                    )
                except Exception as e:
                    logger.warning("Redis cache update failure for user %s: %s", profile.user_id, e)
        except SQLAlchemyError as e:
            db.rollback()
            logger.error("PostgreSQL save failure for user %s: %s", profile.user_id, e)
            raise
        finally:
            db.close()
