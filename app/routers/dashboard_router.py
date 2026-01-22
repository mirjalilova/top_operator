from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Operator, OperatorMonthlyMetric
import logging

router = APIRouter(prefix="/api/groups", tags=["Groups"])
logger = logging.getLogger(__name__)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/{group}/operators")
def get_group_operators(
    group: str,
    year: int = Query(..., example=2026),
    month: int = Query(..., example=1),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(
            Operator.operator_id.label("login"),
            Operator.full_name,
            Operator.avatar_url,
            Operator.group_name,

            OperatorMonthlyMetric.call_count,
            OperatorMonthlyMetric.avg_busy_per_call,
            OperatorMonthlyMetric.kpi,
            OperatorMonthlyMetric.score,
            OperatorMonthlyMetric.rank,
            OperatorMonthlyMetric.stars,
            OperatorMonthlyMetric.is_top_1,
        )
        .join(
            OperatorMonthlyMetric,
            OperatorMonthlyMetric.operator_uuid == Operator.id
        )
        .filter(
            Operator.group_name == group,
            OperatorMonthlyMetric.year == year,
            OperatorMonthlyMetric.month == month,
        )
        .order_by(
            OperatorMonthlyMetric.score.desc(),
            OperatorMonthlyMetric.kpi.desc()
        )
        .all()
    )

    return {
        "group": group,
        "year": year,
        "month": month,
        "total": len(rows),
        "operators": [
            {
                "rank": r.rank,
                "login": r.login,
                "full_name": r.full_name,
                "avatar_url": r.avatar_url,
                "call_count": r.call_count,
                "avg_busy_per_call": r.avg_busy_per_call,
                "kpi": r.kpi,
                "score": r.score,
                "stars": r.stars,
                "is_top_1": r.is_top_1,
            }
            for r in rows
        ],
    }
