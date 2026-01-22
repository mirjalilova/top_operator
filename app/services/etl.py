import requests
from datetime import date, timedelta
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Operator, OperatorMetric

API_URL = "http://csv.ccenter.uz:5000/csv-to-json/day_by"
COLUMNS = "1,2,3,4,5,8,9,10,11,14"


# ---------- DATE RANGE ----------
def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# ---------- FETCH API ----------
def fetch_day_data(day: date):
    params = {
        "columns": COLUMNS,
        "year": day.year,
        "month": f"{day.month:02d}",
        "day": f"{day.day:02d}",
    }

    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


# ---------- MAIN ETL ----------
def run_etl():
    db: Session = SessionLocal()

    start_date = date(2025, 12, 1)
    end_date = date(2026, 1, 18)

    for day in daterange(start_date, end_date):
        print(f"📅 Processing {day}")

        try:
            api_rows = fetch_day_data(day)

            for row in api_rows:
                agent_id = row.get("ID")

                if not agent_id or not str(agent_id).isdigit():
                    continue

                agent_id = int(agent_id)

                # 🔎 operatorni agent_id orqali topamiz
                operator = db.query(Operator).filter(
                    Operator.agent_id == agent_id
                ).first()

                if not operator:
                    continue

                # 🔥 eski metricni o‘chiramiz (overwrite)
                db.query(OperatorMetric).filter(
                    OperatorMetric.operator_uuid == operator.id,
                    OperatorMetric.date == day
                ).delete(synchronize_session=False)

                # ✅ yangi metric qo‘shamiz
                db.add(
                    OperatorMetric(
                        operator_uuid=operator.id,
                        date=day,
                        busy_duration=row.get("BusyDuration"),
                        call_count=float(row.get("CallCount", 0)),
                        distributed_call_count=float(row.get("DistributedCallCount", 0)),
                        full_duration=row.get("FullDuration"),
                        hold_duration=row.get("HoldDuration"),
                        idle_duration=row.get("IdleDuration"),
                        lock_duration=row.get("LockDuration"),
                        kpi=None,  # hozircha yo‘q
                    )
                )

            db.commit()

        except Exception as e:
            db.rollback()
            print(f"❌ Error on {day}: {e}")

    db.close()
    print("✅ operator_metrics ETL completed")


if __name__ == "__main__":
    run_etl()
