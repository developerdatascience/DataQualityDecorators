from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from quality.pipelines.database.base import Base


class DataQualityMetric(Base):
    __tablename__ = "data_quality_metrics"

    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, index=True)
    expectation_name = Column(String)
    rule = Column(String)
    total_records = Column(Integer)
    failed_records = Column(Integer)
    success_ratio = Column(Float)
    status = Column(String)  # PASS / FAIL
    created_at = Column(DateTime, server_default=func.now())
