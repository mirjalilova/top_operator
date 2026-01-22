import requests
from datetime import date
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Operator


def fetch_agent_map(day: date):
    params = {
        "columns": "1,2,3,4,5,8,9,10,11,14",
        "year": day.year,
        "month": f"{day.month:02d}",
        "day": f"{day.day:02d}",
    }

    r = requests.get(
        "http://csv.ccenter.uz:5000/csv-to-json/day_by",
        params=params,
        timeout=30
    )
    r.raise_for_status()

    mapping = {}
    for row in r.json().get("data", []):
        login = str(row.get("login")).strip()
        agent_id = row.get("ID")

        if login and agent_id and str(agent_id).isdigit():
            mapping[login] = int(agent_id)

    return mapping


def update_agent_ids(day: date):
    db: Session = SessionLocal()

    agent_map = fetch_agent_map(day)

    updated = 0
    skipped = 0

    for login, agent_id in agent_map.items():
        operator = db.query(Operator).filter(
            Operator.operator_id == login
        ).first()

        if not operator:
            skipped += 1
            continue

        if operator.agent_id == agent_id:
            continue

        if operator.agent_id is not None:
            skipped += 1
            continue

        exists = db.query(Operator).filter(
            Operator.agent_id == agent_id,
            Operator.id != operator.id
        ).first()

        if exists:
            skipped += 1
            continue

        operator.agent_id = agent_id
        updated += 1

    db.commit()
    db.close()

    print(f"✅ agent_id updated: {updated}")
    print(f"⚠️ skipped: {skipped}")

if __name__ == "__main__":
    update_agent_ids(date(2026, 1, 18))
