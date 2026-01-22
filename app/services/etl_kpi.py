import re
import requests
import gspread
from datetime import date, timedelta
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Operator, OperatorMetric
from collections import Counter



# ================= CONFIG =================
API_URL = "http://csv.ccenter.uz:5000/csv-to-json/day_by"
COLUMNS = "2,3,4,5,8,9,10,11,14"

KPI_SHEET_URL = "https://docs.google.com/spreadsheets/d/1yKRsDh0S1lmcfFthlxhUtScGA-FK2XSe_8snO5-i50A"
GOOGLE_CREDS = "genial-smoke-461106-e4-1ff74dbbfcd0.json"

START_DATE = date(2025, 12, 1)
END_DATE = date(2026, 1, 6)
# =========================================


def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# ---------- KPI LOAD ----------
def load_kpi_map():
    """
    return:
    {
        (operator_id, month): kpi
    }
    """
    gc = gspread.service_account(GOOGLE_CREDS)
    sheet = gc.open_by_url(KPI_SHEET_URL).sheet1
    rows = sheet.get_all_values()

    kpi_map = {}

    for row in rows[1:]:
        if len(row) < 3:
            continue

        fio, kpi, cycle = row[0], row[1], row[2]

        m = re.search(r"\((\d+)\)", fio)
        if not m:
            continue

        operator_id = m.group(1).strip()

        month = int(cycle)

        try:
            kpi_map[(operator_id, month)] = float(kpi.replace(",", "."))
        except:
            continue

    return kpi_map


# ---------- API LOAD ----------
def fetch_day_metrics(day: date):
    params = {
        "columns": COLUMNS,
        "year": day.year,
        "month": f"{day.month:02d}",
        "day": f"{day.day:02d}",
    }

    r = requests.get(API_URL, params=params, timeout=30)

    if r.status_code == 400:
        print(f"⚠️ No data for {day} (API returned 400)")
        return []

    r.raise_for_status()
    return r.json().get("data", [])


def resolve_cycle(d: date) -> int | None:
    """
    KPI cycle logic:
    - 20.11 – 20.12 -> 12
    - 21.12 – 20.01 -> 1
    """
    if date(2025, 11, 20) <= d <= date(2025, 12, 20):
        return 12
    if date(2025, 12, 21) <= d <= date(2026, 1, 20):
        return 1
    return None


# ---------- MAIN ETL ----------
def run_etl():
    db: Session = SessionLocal()

    print("📥 Loading KPI data...")
    kpi_map = load_kpi_map()
    print(f"✅ KPI loaded: {len(kpi_map)}")

    operators = {
        op.operator_id: op
        for op in db.query(Operator).all()
    }

    for day in daterange(START_DATE, END_DATE):
        cycle = resolve_cycle(day)
        if cycle is None:
            continue

        print(f"📅 Processing {day} (cycle={cycle})")

        # overwrite
        db.query(OperatorMetric).filter(
            OperatorMetric.date == day
        ).delete(synchronize_session=False)
        db.commit()

        api_data = fetch_day_metrics(day)

        for row in api_data:
            login = row.get("login")
            if not login:
                continue

            operator_id = login.strip()

            operator = operators.get(operator_id)
            if not operator:
                continue

            # 🔥 KPI shu cycle bo‘yicha olinadi
            kpi = kpi_map.get((operator_id, cycle))

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
                    kpi=kpi,
                )
            )

        db.commit()

    db.close()
    print("✅ operator_metrics fully refreshed with KPI")



if __name__ == "__main__":
    run_etl()

