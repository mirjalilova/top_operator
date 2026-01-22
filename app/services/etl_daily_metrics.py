from datetime import date, datetime, timedelta
import time
import requests
import logging
import re
import gspread
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Operator, OperatorMetric

# ================= CONFIG =================
API_URL = "http://csv.ccenter.uz:5000/csv-to-json/day_by"
COLUMNS = "1,2,3,4,5,8,9,10,11,14"

KPI_SHEET_URL = "https://docs.google.com/spreadsheets/d/1yKRsDh0S1lmcfFthlxhUtScGA-FK2XSe_8snO5-i50A"
GOOGLE_CREDS = "genial-smoke-461106-e4-1ff74dbbfcd0.json"
OPERATORS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1lOyz1d6iL6Ok0uzElqrn_KM8Im-MgrEslRHu2Hi8ZKE/edit?gid=0#gid=0"
ALLOWED_GROUPS = {"1009", "1000", "1242", "1170", "1093", "ДОП"}

MAX_RETRY_HOUR = 23
RETRY_INTERVAL = 3600  # 1 soat

LOG_FILE = "/home/feruza/git/mirjalilova/top_operator/logs/etl_daily.log"
# =========================================

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_operator_sheet() -> dict:
    logger.info("Loading operators sheet...")

    gc = gspread.service_account(GOOGLE_CREDS)
    sh = gc.open_by_url(OPERATORS_SHEET_URL)
    ws = sh.worksheet("Operators")

    values = ws.get_all_values()
    rows = values[1:]  # header skip

    sheet_map = {}

    for r in rows:
        # minimal ustunlar
        if len(r) < 3:
            continue

        login = r[1].strip()   # ID = login (STRING)
        group_name = r[2].strip()

        # ❌ faqat ruxsat berilgan guruhlar
        if group_name not in ALLOWED_GROUPS:
            continue

        # login tekshiruvi
        if not login.isdigit():
            continue

        full_name = r[0].replace("👤", "").strip()
        avatar_url = r[5].strip() if len(r) > 5 and r[5] else None

        sheet_map[login] = {
            "full_name": full_name,
            "group_name": group_name,
            "avatar_url": avatar_url,
        }

    logger.info(
        f"Operators loaded from sheet: {len(sheet_map)} | groups={ALLOWED_GROUPS}"
    )
    return sheet_map


# ---------- KPI ----------
def load_kpi_map() -> dict:
    logger.info("Loading KPI sheet...")
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

        try:
            agent_id = int(m.group(1))
            cycle = int(cycle)
            kpi_value = float(kpi.replace(",", "."))
        except Exception:
            continue

        kpi_map[(agent_id, cycle)] = kpi_value

    logger.info(f"KPI loaded: {len(kpi_map)} rows")
    return kpi_map


# ---------- KPI CYCLE (20 → 20) ----------
def resolve_cycle_for_date(d: date) -> int:
    """
    20 dan oldin bo‘lsa — oldingi oy
    20 va keyin bo‘lsa — shu oy
    """
    if d.day >= 20:
        return d.month
    return 12 if d.month == 1 else d.month - 1


# ---------- API ----------
def fetch_day_data(day: date):
    params = {
        "columns": COLUMNS,
        "year": day.year,
        "month": f"{day.month:02d}",
        "day": f"{day.day:02d}",
    }

    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()

    payload = r.json()

    if "error" in payload:
        logger.warning(f"API error for {day}: {payload['error']}")
        return None

    return payload.get("data", [])

def get_or_create_operator(
    db: Session,
    agent_id: int,
    login: str,
    sheet_map: dict
) -> Operator | None:

    # 1️⃣ Avval agent_id bo‘yicha tekshiramiz
    operator = db.query(Operator).filter(
        Operator.agent_id == agent_id
    ).first()

    if operator:
        return operator

    # 2️⃣ Sheetdan login (STRING) orqali qidiramiz
    sheet_row = sheet_map.get(login)
    if not sheet_row:
        logger.warning(
            f"Operator not found in sheet | login={login}, agent_id={agent_id}"
        )
        return None

    operator = Operator(
        agent_id=agent_id,
        operator_id=login,   # 👈 aynan string
        full_name=sheet_row["full_name"],
        group_name=sheet_row["group_name"],
        avatar_url=sheet_row["avatar_url"],
    )

    db.add(operator)
    db.flush()

    logger.info(
        f"New operator inserted | login={login}, agent_id={agent_id}"
    )
    return operator


# ---------- ETL ----------
def try_fetch_and_save(
    day: date,
    kpi_map: dict,
    cycle: int,
    sheet_map: dict
) -> bool:
    db: Session = SessionLocal()
    inserted = 0

    try:
        api_rows = fetch_day_data(day)

        # ❗ agar API hali bo‘sh bo‘lsa — retry
        if api_rows is None or len(api_rows) == 0:
            logger.warning(f"No API data yet for {day}")
            return False

        for row in api_rows:
            agent_id = row.get("ID")

            if not agent_id or not str(agent_id).isdigit():
                continue

            agent_id = int(agent_id)
            operator = get_or_create_operator(
                db=db,
                agent_id=agent_id,
                login=row.get("login"),
                sheet_map=sheet_map
            )

            if not operator:
                continue


            kpi = kpi_map.get((agent_id, cycle))

            # overwrite (idempotent)
            db.query(OperatorMetric).filter(
                OperatorMetric.operator_uuid == operator.id,
                OperatorMetric.date == day
            ).delete(synchronize_session=False)

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
            inserted += 1

        db.commit()
        logger.info(
            f"Saved metrics for {day} | inserted={inserted} | cycle={cycle}"
        )
        return True

    except Exception:
        db.rollback()
        logger.exception(f"ETL error on {day}")
        return False

    finally:
        db.close()


# ---------- RUNNER ----------
def run_daily_job():
    target_day = date.today() - timedelta(days=2)

    logger.info(f"Daily ETL started | target_day={target_day}")

    kpi_map = load_kpi_map()
    sheet_map = load_operator_sheet()
    cycle = resolve_cycle_for_date(target_day)

    logger.info(f"Resolved KPI cycle={cycle} for date={target_day}")

    while True:
        now = datetime.now()

        if now.hour > MAX_RETRY_HOUR:
            logger.error(f"23:00 bo‘ldi, data kelmadi: {target_day}")
            break

        if try_fetch_and_save(target_day, kpi_map, cycle, sheet_map):
            logger.info("ETL finished successfully")
            break

        logger.info("Retry after 1 hour...")
        time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    run_daily_job()
