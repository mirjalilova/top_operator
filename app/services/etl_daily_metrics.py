from datetime import date, datetime, timedelta
import time
import requests
import logging
import re
import gspread
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Operator, OperatorMetric

API_URL = "http://csv.ccenter.uz:5000/csv-to-json/day_by"
COLUMNS = "1,2,3,4,5,8,9,10,11,14"

KPI_SHEET_URL = "https://docs.google.com/spreadsheets/d/1yKRsDh0S1lmcfFthlxhUtScGA-FK2XSe_8snO5-i50A"
GOOGLE_CREDS = "genial-smoke-461106-e4-90a8532e00b8.json"
OPERATORS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1lOyz1d6iL6Ok0uzElqrn_KM8Im-MgrEslRHu2Hi8ZKE/edit?gid=0#gid=0"
ALLOWED_GROUPS = {"1009", "1000", "1242", "1170", "1093", "ДОП"}

MAX_RETRY_HOUR = 23
RETRY_INTERVAL = 3600  # 1 soat

LOG_FILE = "/home/user/Projects/top_operator/logs/etl_daily.log"
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
    rows = values[1:]

    sheet_map = {}

    for r in rows:
        if len(r) < 3:
            continue

        login = r[1].strip() 
        group_name = r[2].strip()

        if group_name not in ALLOWED_GROUPS:
            continue

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


def resolve_cycle_for_date(d: date) -> int:
    if d.day >= 20:
        return d.month
    return 12 if d.month == 1 else d.month - 1


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

    operator = db.query(Operator).filter(
        Operator.agent_id == agent_id
    ).first()

    if operator:
        return operator

    sheet_row = sheet_map.get(login)
    if not sheet_row:
        # logger.warning(
        #     f"Operator not found in sheet | login={login}, agent_id={agent_id}"
        # )
        return None

    operator = Operator(
        agent_id=agent_id,
        operator_id=login,
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


def run_daily_job():
    target_day = date.today() - timedelta(days=1)

    logger.info(f"Daily ETL started | target_day={target_day}")

    try:
        kpi_map = load_kpi_map()
    except Exception:
        logger.exception("KPI sheet failed, continue without KPI")
        kpi_map = {} 

    try:
        sheet_map = load_operator_sheet()
    except Exception:
        logger.exception("Operators sheet failed, ETL cannot continue")
        return
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


def run_range_job(start_date: date, end_date: date):
    logger.info(
        f"ETL range started | from={start_date} to={end_date}"
    )

    try:
        kpi_map = load_kpi_map()
    except Exception:
        logger.exception("KPI sheet failed, continue without KPI")
        kpi_map = {}

    try:
        sheet_map = load_operator_sheet()
    except Exception:
        logger.exception("Operators sheet failed, ETL cannot continue")
        return

    d = start_date
    while d <= end_date:
        cycle = resolve_cycle_for_date(d)
        logger.info(f"Processing date={d} | cycle={cycle}")

        success = False
        retry_started_at = datetime.now()

        while True:
            if try_fetch_and_save(d, kpi_map, cycle, sheet_map):
                logger.info(f"ETL success for {d}")
                success = True
                break

            if datetime.now().hour > MAX_RETRY_HOUR:
                logger.error(f"23:00 bo‘ldi, data kelmadi: {d}")
                break

            logger.info(f"Retry {d} after 1 hour...")
            time.sleep(RETRY_INTERVAL)

        if not success:
            logger.warning(f"ETL skipped date={d}")

        d += timedelta(days=1)

    logger.info("ETL range finished")


if __name__ == "__main__":
    run_daily_job()
