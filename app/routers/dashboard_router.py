from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import SessionLocal
from app.models import Operator, OperatorMonthlyMetric, BonusDistribution
import logging

router = APIRouter(prefix="/api/groups", tags=["Groups"])
logger = logging.getLogger(__name__)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def seconds_to_hhmm(seconds: float | None) -> str:
    if not seconds:
        return "00:00"
    minutes = int(seconds // 60)
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}"


@router.get("/{group}/operators")
def get_group_operators(
    group: str,
    year: int = Query(...),
    month: int = Query(...),
    db: Session = Depends(get_db),
):
    operators_query = text("""
        SELECT
            m.operator_uuid,
            o.full_name,
            o.avatar_url,

            m.rank,
            m.stars,

            m.call_count,
            m.kpi,
            m.avg_busy_per_call,

            m.score,

            COALESCE(b.kie, 0) AS kie,

            COALESCE(
                m.score -
                LAG(m.score) OVER (
                    PARTITION BY m.operator_uuid
                    ORDER BY d.date
                ),
                0
            ) AS score_delta

        FROM operator_monthly_metrics m
        JOIN operators o ON o.id = m.operator_uuid

        LEFT JOIN bonus_distributions b
            ON b.operator_uuid = m.operator_uuid
            AND b.year = m.year
            AND b.month = m.month

        LEFT JOIN operator_daily_rank d
          ON d.operator_uuid = m.operator_uuid
         AND d.year = m.year
         AND d.month = m.month

        WHERE m.year = :year
          AND m.month = :month
          AND o.group_name = :group
          AND m.rank IS NOT NULL

        ORDER BY m.rank
        LIMIT 10
    """)


    rows = db.execute(
        operators_query,
        {"year": year, "month": month, "group": group}
    ).mappings().all()

    graph_query = text("""
        SELECT
            d.date,
            d.rank
        FROM operator_daily_rank d
        JOIN operator_metrics m
            ON m.operator_uuid = d.operator_uuid
            AND m.date = d.date
        WHERE d.operator_uuid = :operator_uuid
            AND d.year = :year
            AND d.month = :month
            AND m.full_duration <> '00:00:00'
        ORDER BY d.date
    """)

    operators = []

    for r in rows:
        graph_rows = db.execute(
            graph_query,
            {
                "operator_uuid": r["operator_uuid"],
                "year": year,
                "month": month,
            }
        ).mappings().all()

        operators.append({
            "operator_uuid": r["operator_uuid"],
            "full_name": r["full_name"],
            "avatar_url": r["avatar_url"],

            "rank": r["rank"],
            "stars": r["stars"],

            "call_count": r["call_count"],
            "kpi": r["kpi"],

            "avg_busy_per_call": seconds_to_hhmm(r["avg_busy_per_call"]),

            "score": r["score"],
            "score_delta": r["score_delta"],

            "graph": [
                {
                    "day": g["date"].isoformat(),
                    "rank": g["rank"]
                }
                for g in graph_rows
            ]
        })

    return {
        "year": year,
        "month": month,
        "group": group,
        "count": len(operators),
        "operators": operators
    }




@router.get("/operators/{operator_uuid}/profile")
def get_operator_profile(
    operator_uuid: str,
    year: int = Query(...),
    month: int = Query(...),
    db: Session = Depends(get_db),
):
    # 1️⃣ OPERATOR + OYLIK MAʼLUMOT
    profile_q = text("""
        SELECT
            o.id AS operator_uuid,
            o.full_name,
            o.avatar_url,
            o.group_name,

            m.rank,
            m.score,
            m.stars,

            m.call_count,
            m.kpi,
            m.avg_busy_per_call,

            COALESCE(b.kie, 0) AS kie,
            COALESCE(b.active_participation, 0) AS active_participation,
            COALESCE(b.monitoring, 0) AS monitoring
        FROM operators o
        JOIN operator_monthly_metrics m
          ON m.operator_uuid = o.id
        LEFT JOIN bonus_distributions b
          ON b.operator_uuid = o.id
         AND b.year = m.year
         AND b.month = m.month
        WHERE o.id = :operator_uuid
          AND m.year = :year
          AND m.month = :month
    """)

    profile = db.execute(
        profile_q,
        {
            "operator_uuid": operator_uuid,
            "year": year,
            "month": month
        }
    ).mappings().first()

    if not profile:
        return {"detail": "Operator not found"}

    # 2️⃣ GRAPH — faqat ishlagan kunlar
    graph_q = text("""
        SELECT
            d.date,
            d.rank
        FROM operator_daily_rank d
        JOIN operator_metrics m
          ON m.operator_uuid = d.operator_uuid
         AND m.date = d.date
        WHERE d.operator_uuid = :operator_uuid
          AND d.year = :year
          AND d.month = :month
          AND m.full_duration <> '00:00:00'
        ORDER BY d.date
    """)

    graph_rows = db.execute(
        graph_q,
        {
            "operator_uuid": operator_uuid,
            "year": year,
            "month": month
        }
    ).mappings().all()

    # 3️⃣ KECHAGI STATISTIKA (kunlikdan)
    yesterday_q = text("""
        SELECT
            call_count,
            CASE
                WHEN call_count > 0 THEN
                    EXTRACT(EPOCH FROM busy_duration::interval) / call_count
                ELSE 0
            END AS avg_busy_seconds,
            kpi
        FROM operator_metrics
        WHERE operator_uuid = :operator_uuid
        ORDER BY date DESC
        LIMIT 1
    """)

    yesterday = db.execute(
        yesterday_q,
        {"operator_uuid": operator_uuid}
    ).mappings().first()

    # 4️⃣ RESPONSE
    return {
        "operator": {
            "operator_uuid": profile["operator_uuid"],
            "full_name": profile["full_name"],
            "avatar_url": profile["avatar_url"],
            "group": profile["group_name"],
        },
        "monthly": {
            "rank": profile["rank"],
            "score": profile["score"],
            "stars": profile["stars"],
            "call_count": profile["call_count"],
            "avg_busy_per_call": seconds_to_hhmm(profile["avg_busy_per_call"]),
            "kpi": profile["kpi"],
            "kie": profile["kie"],
            "active_participation": profile["active_participation"],
            "monitoring": profile["monitoring"],
        },
        "graph": [
            {
                "day": g["date"].isoformat(),
                "rank": g["rank"]
            }
            for g in graph_rows
        ],
        "yesterday": {
            "call_count": yesterday["call_count"] if yesterday else 0,
            "avg_busy_per_call": seconds_to_hhmm(
                yesterday["avg_busy_seconds"] if yesterday else 0
            ),
            "kpi": yesterday["kpi"] if yesterday else None,
        }
    }
