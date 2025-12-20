from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from quality.pipelines.database.base import Base

ENGINE = create_engine("sqlite:///quality_metrics.db")
SessionLocal = sessionmaker(bind=ENGINE)

def init_db():
    Base.metadata.create_all(bind=ENGINE)
