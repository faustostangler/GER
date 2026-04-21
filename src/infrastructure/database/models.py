from sqlalchemy import Column, String, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class DoctorProfileModel(Base):
    """PostgreSQL model for DoctorProfile persistence.
    
    Maps the Bounded Context of Authorization (DoctorProfile) to the relational schema.
    """
    __tablename__ = "doctor_profiles"

    id = Column(String, primary_key=True)  # Keycloak UUID / 'sub' mapping
    crm_numero = Column(String, nullable=False, index=True)
    crm_uf = Column(String, nullable=False)
    crm_verified = Column(Boolean, default=False, nullable=False)
