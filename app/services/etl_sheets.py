import re
import pandas as pd
import gspread
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Operator

SHEET_URL = "https://docs.google.com/spreadsheets/d/1lOyz1d6iL6Ok0uzElqrn_KM8Im-MgrEslRHu2Hi8ZKE"
GOOGLE_CREDS = "genial-smoke-461106-e4-1ff74dbbfcd0.json"

ALLOWED_GROUPS = {"1009", "1000", "1242", "1170", "1093", "ДОП"}


def normalize(col: str) -> str:
    return col.replace("\n", " ").strip()


def extract_agent_id(text):
    text = str(text).strip()
    if not text:
        return None

    match = re.search(r"\b\d{2,10}\b", text)
    if match:
        return match.group()  
    return None

def run_etl():
    # ---------- SHEETS ----------
    gc = gspread.service_account(GOOGLE_CREDS)
    sh = gc.open_by_url(SHEET_URL)
    sheet = sh.worksheet("Operators")

    values = sheet.get_all_values()

    headers = values[0]
    data = values[1:]

    df = pd.DataFrame(data, columns=headers)
    df.columns = [normalize(c) for c in df.columns]

    # ---------- USTUNLAR ----------
    ID_COL = "ID"                   # login
    GROUP_COL = "Группа (дано)"
    PHOTO_COL = "Фото"

    # ⚠️ F.O.I ustuni — header yo‘q → INDEX orqali
    FOI_SERIES = df.iloc[:, 4]      # E ustun

    # ---------- agent_id (F.O.I ichidan) ----------
    df["agent_id"] = FOI_SERIES.apply(extract_agent_id)
    df = df[df["agent_id"].notna()]

    # ---------- group filter ----------
    df["group_name"] = df[GROUP_COL].astype(str).str.strip()
    df = df[df["group_name"].isin(ALLOWED_GROUPS)]

    # ---------- login (ID ustunidan) ----------
    df["login"] = df[ID_COL].astype(str).str.strip()
    df = df[df["login"].str.isdigit()]

    # ---------- DUPLICATES ----------
    dups = df[df.duplicated("agent_id", keep=False)]
    if not dups.empty:
        print("❌ REAL DUPLICATE agent_id:")
        print(dups[["agent_id", "login", GROUP_COL]])

    df = df.drop_duplicates(subset=["agent_id"], keep="last")

    # ---------- DB ----------
    db: Session = SessionLocal(autoflush=False)

    inserted = 0
    updated = 0

    for _, row in df.iterrows():
        agent_id = row["agent_id"]
        login = row["login"]

        full_name = (
            str(FOI_SERIES.loc[row.name])
            .split("\n")[0]
            .replace("👤", "")
            .strip()
        )

        avatar_url = (
            str(row[PHOTO_COL]).strip()
            if PHOTO_COL in df.columns and pd.notna(row[PHOTO_COL])
            else None
        )

        existing = db.query(Operator).filter(
            Operator.operator_id == agent_id
        ).first()

        if existing:
            existing.full_name = full_name
            existing.group_name = row["group_name"]
            existing.avatar_url = avatar_url
            updated += 1
        else:
            db.add(
                Operator(
                    operator_id=str(agent_id),
                    full_name=full_name,
                    group_name=row["group_name"],
                    avatar_url=avatar_url,
                )
            )
            inserted += 1

    db.commit()
    db.close()

    print(f"✅ Operators synced | inserted={inserted}, updated={updated}")


if __name__ == "__main__":
    run_etl()
