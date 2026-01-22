import uuid

from sqlalchemy import (
    Column,
    String,
    Float,
    Date,
    ForeignKey,
    UniqueConstraint,
    DateTime,
    func,
    Integer,
    Boolean,       
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID

from app.database import Base


class Operator(Base):
    __tablename__ = "operators"

    id = Column(Integer, primary_key=True)
    agent_id = Column(Integer, nullable=False)

    operator_id = Column(String, nullable=False, unique=True)
    full_name = Column(String, nullable=False)
    group_name = Column(String, nullable=False)
    avatar_url = Column(String)

    created_at = Column(DateTime, server_default=func.now())

    metrics = relationship(
        "OperatorMetric",
        back_populates="operator",
        cascade="all, delete-orphan"
    )

    monthly_metrics = relationship(
        "OperatorMonthlyMetric",
        back_populates="operator",
        cascade="all, delete-orphan"
    )


class OperatorMetric(Base):
    __tablename__ = "operator_metrics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    operator_uuid = Column(
        UUID(as_uuid=True),
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False
    )

    date = Column(Date, nullable=False)

    busy_duration = Column(String)
    call_count = Column(Float)
    distributed_call_count = Column(Float)
    full_duration = Column(String)
    hold_duration = Column(String)
    idle_duration = Column(String)
    lock_duration = Column(String)

    kpi = Column(Float)

    operator = relationship(
        "Operator",
        back_populates="metrics"
    )

    __table_args__ = (
        UniqueConstraint(
            "operator_uuid",
            "date",
            name="uq_operator_date"
        ),
    )


class OperatorMonthlyMetric(Base):
    __tablename__ = "operator_monthly_metrics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    operator_uuid = Column(
        UUID(as_uuid=True),
        ForeignKey("operators.id", ondelete="CASCADE"),
        nullable=False
    )

    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)

    call_count = Column(Integer, nullable=False)
    avg_busy_per_call = Column(Float, nullable=False)

    kpi = Column(Float, nullable=False)

    score = Column(Integer)
    rank = Column(Integer)

    is_top_1 = Column(
        Boolean,
        nullable=False,
        server_default="false"   # ✅ Postgres uchun to‘g‘ri
    )

    stars = Column(Integer)

    created_at = Column(DateTime, server_default=func.now())

    operator = relationship(
        "Operator",
        back_populates="monthly_metrics"
    )

    __table_args__ = (
        UniqueConstraint(
            "operator_uuid",
            "year",
            "month",
            name="uq_operator_month"
        ),
    )
