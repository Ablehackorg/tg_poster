import asyncio
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.custom import Button
from telethon.tl.functions.contacts import ResolveUsernameRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

# ================== USER CONFIG (o'zgartirish mumkin) ==================
# Excel fayl yo'li: loyihangiz papkasidagi data.xlsx
EXCEL_PATH = Path(__file__).parent / "data.xlsx"

# Agar varaq nomini aniq bilsangiz qo'ying. Bilmasangiz None qoldiring — 1-varaq olinadi.
SHEET_NAME: Optional[str] = None  # masalan: "Posts" yoki "Sheet1"; None = birinchi varaq

# Exceldagi vaqtlar uchun default timezone (agar ustunda bo'sh bo'lsa)
DEFAULT_TZ = "Asia/Tashkent"

# .env ichida bo'lsa qulay: API_ID, API_HASH, SESSION_NAME (ixtiyoriy)
# ======================================================================


# -------------------------- ENV / CLIENT -------------------------------
load_dotenv()
API_ID = int(os.getenv("API_ID", "0")) or None
API_HASH = os.getenv("API_HASH") or None
SESSION_NAME = os.getenv("SESSION_NAME", "poster_session")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

scheduler = AsyncIOScheduler(timezone=DEFAULT_TZ)


# ---------------------- UTIL: TIME PARSING -----------------------------
def parse_when_local(value) -> datetime:
    """
    Qabul qiladi:
    - Excel datetime (pandas tushungan datetime)
    - "YYYY-MM-DD HH:MM" (masalan: 2025-10-22 09:30)
    - "DD.MM.YYYY HH:MM" (masalan: 21.10.2025 20:10)
    """
    if isinstance(value, datetime):
        return value

    s = str(value).strip()
    fmts = ["%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M"]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    raise ValueError(f"when_local format noto'g'ri: {value}")


def ensure_localize(dt: datetime, tz_name: str) -> datetime:
    tz = pytz.timezone(tz_name or DEFAULT_TZ)
    if dt.tzinfo is None:
        return tz.localize(dt)
    return dt.astimezone(tz)


def to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(pytz.utc)


# ---------------------- UTIL: BUTTONS PARSING --------------------------
def parse_buttons_cell(raw: str) -> Optional[List[List[Button]]]:
    """
    Kutilyotgan ko'rinish (bitta katak ichida):
      Google|https://google.com ; Site|https://example.com

    Natija: Telethon Button.url bilan 1 qatorda 2ta tugma (har bir juftlik alohida qatorda bo'ladi).
    """
    if not isinstance(raw, str) or not raw.strip():
        return None

    parts = [p.strip() for p in raw.split(";") if p.strip()]
    rows: List[List[Button]] = []
    for part in parts:
        if "|" in part:
            label, url = part.split("|", 1)
            label, url = label.strip(), url.strip()
        else:
            # agar "|" bo'lmasa, to'g'ridan-to'g'ri URL deb qabul qilamiz
            label, url = part, part
        if not label or not url:
            continue
        rows.append([Button.url(label, url)])
    return rows or None


def merge_buttons_if_split(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ba'zi hollarda `buttons` yonida ajratilgan URL alohida ustunda qoladi
    (sarlavhasi bo'sh yoki 'Unnamed: X' bo'lib ko'rinishi mumkin).
    Bu funksiya bitta katakka "Label|URL" ko'rinishida birlashtirib beradi.
    Qoidalar:
      - Agar `buttons` bor va keyingi ustun URLga o'xshasa → "buttons | nextcol" ni birlashtiradi
      - Keyingi ustun bo'sh bo'lsa yoki URL bo'lmasa → o'zgartirmaydi
    """
    if "buttons" not in df.columns:
        return df

    idx = list(df.columns).index("buttons")
    if idx + 1 >= len(df.columns):
        return df

    next_col = df.columns[idx + 1]
    # faqat "Unnamed" yoki bo'sh sarlavhalarda ishlaymiz; boshqalarga tegmaymiz
    if str(next_col).lower().startswith("unnamed") or str(next_col).strip() == "":
        # qatorma-qator tekshirib birlashtiramiz
        for i in range(len(df)):
            left = df.at[i, "buttons"]
            right = df.at[i, next_col] if next_col in df.columns else None
            if pd.isna(left) and pd.isna(right):
                continue

            left_s = "" if pd.isna(left) else str(left).strip()
            right_s = "" if pd.isna(right) else str(right).strip()

            # right URLga o'xshaydimi?
            if right_s and re.match(r"https?://", right_s, flags=re.I):
                if left_s:
                    # Agar left faqat label bo'lsa (masalan "Google"), LABEL|URL ga aylantiramiz,
                    # aks holda, leftda allaqachon bir nechta tugma bo'lsa, oxiriga '; Label|URL' qo'shamiz.
                    if "|" not in left_s and ";" not in left_s:
                        merged = f"{left_s}|{right_s}"
                    else:
                        merged = f"{left_s} ; {right_s}|{right_s}"
                else:
                    merged = f"{right_s}|{right_s}"
                df.at[i, "buttons"] = merged

        # ixtiyoriy: next_col’ni qoldiramiz (agar o'chirmoqchi bo'lsangiz, quyini yoqing)
        # df.drop(columns=[next_col], inplace=True)

    return df


# ---------------------- TELEGRAM ENTITY RESOLVE ------------------------
async def resolve_chat_entity(chat_value: str):
    """
    Qabul qiladi:
     - t.me/+invite...
     - https://t.me/username yoki t.me/username
     - @username
     - -100... numeric id
    """
    s = str(chat_value).strip()
    if not s:
        raise ValueError("chat bo'sh")

    # t.me/+invite
    m = re.search(r"t\.me/\+([A-Za-z0-9_-]{16,})", s)
    if m:
        invite_hash = m.group(1)
        res = await client(ImportChatInviteRequest(invite_hash))
        # chat entity qaytaramiz (channel/group)
        if hasattr(res, "chats") and res.chats:
            return res.chats[0]
        return s  # fallback

    # https://t.me/username  yoki  t.me/username
    m = re.search(r"t\.me/(@?[\w\d_]+)$", s)
    if m:
        username = m.group(1).lstrip("@")
        r = await client(ResolveUsernameRequest(username))
        if r.chats:
            return list(r.chats.values())[0] if isinstance(r.chats, dict) else r.chats[0]
        if r.users:
            return list(r.users.values())[0] if isinstance(r.users, dict) else r.users[0]
        return s

    # @username
    if s.startswith("@"):
        username = s.lstrip("@")
        r = await client(ResolveUsernameRequest(username))
        if r.chats:
            return list(r.chats.values())[0] if isinstance(r.chats, dict) else r.chats[0]
        if r.users:
            return list(r.users.values())[0] if isinstance(r.users, dict) else r.users[0]
        return s

    # numeric id
    if re.fullmatch(r"-?\d{5,20}", s):
        return int(s)

    return s


# -------------------------- CORE SENDER -------------------------------
async def send_row_task(row_idx: int,
                        chat_value: str,
                        text: str,
                        media_path: Optional[str],
                        parse_mode: Optional[str],
                        buttons_raw: Optional[str],
                        pin: bool):
    try:
        entity = await resolve_chat_entity(chat_value)
        buttons = parse_buttons_cell(buttons_raw) if buttons_raw else None

        message_kwargs = {}
        if isinstance(parse_mode, str) and parse_mode.lower() in {"html", "markdown", "md"}:
            message_kwargs["parse_mode"] = "markdown" if parse_mode.lower() in {"markdown", "md"} else "html"

        # media bor-yo‘qligiga qarab yuboramiz
        if media_path:
            media_file = Path(media_path)
            if not media_file.is_file():
                print(f"[Row {row_idx}] WARNING: media not found → {media_file}")
                media_file = None
            else:
                media_file = str(media_file)

        else:
            media_file = None

        if media_file:
            msg = await client.send_file(entity=entity,
                                         file=media_file,
                                         caption=text or "",
                                         buttons=buttons,
                                         **message_kwargs)
        else:
            msg = await client.send_message(entity=entity,
                                            message=text or "",
                                            buttons=buttons,
                                            **message_kwargs)

        if pin:
            try:
                await client.pin_message(entity, msg, notify=False)
            except Exception as e:
                print(f"[Row {row_idx}] Pin failed: {e}")

        print(f"[Row {row_idx}] Sent → {chat_value}")
        return True

    except FloodWaitError as e:
        wait_s = int(getattr(e, "seconds", 30))
        print(f"[Row {row_idx}] FloodWait: sleeping {wait_s}s…")
        await asyncio.sleep(wait_s + 1)
        # bitta retry
        return await send_row_task(row_idx, chat_value, text, media_path, parse_mode, buttons_raw, pin)

    except RPCError as e:
        print(f"[Row {row_idx}] RPCError: {e}")
        return False

    except Exception as e:
        print(f"[Row {row_idx}] ERROR: {e}")
        return False


# -------------------------- LOAD & SCHEDULE ---------------------------
def pick_sheet_name(xls: pd.ExcelFile) -> str:
    if SHEET_NAME:
        if SHEET_NAME in xls.sheet_names:
            return SHEET_NAME
        raise ValueError(f"Worksheet named '{SHEET_NAME}' not found. Available: {xls.sheet_names}")
    # Aks holda birinchi varaq
    return xls.sheet_names[0]


def load_rows() -> List[dict]:
    if not EXCEL_PATH.is_file():
        raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")

    xls = pd.ExcelFile(EXCEL_PATH)
    sheet = pick_sheet_name(xls)
    df = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")

    # Sarlavhalarni normalize (bo'sh joylarni olib tashlash, kichik harf)
    df.columns = [str(c).strip().lower().replace("\n", " ") for c in df.columns]

    # Buttons yonidagi URL ustuni bo'lsa — birlashtirish
    df = merge_buttons_if_split(df)

    required_cols = ["enabled", "chat", "when_local", "text"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: '{col}'")

    rows = []
    for i, r in df.iterrows():
        try:
            enabled = str(r.get("enabled", "")).strip()
            enabled = enabled in {"1", "true", "yes", "y"}

            if not enabled:
                continue

            chat = str(r.get("chat", "")).strip()
            if not chat:
                print(f"[Row {i}] Skip: empty chat")
                continue

            when_raw = r.get("when_local")
            if pd.isna(when_raw):
                print(f"[Row {i}] Skip: empty when_local")
                continue

            # datetime parse
            dt_local = parse_when_local(when_raw)

            tz_name = str(r.get("timezone", "")).strip() or DEFAULT_TZ
            dt_local = ensure_localize(dt_local, tz_name)
            dt_utc = to_utc(dt_local)

            text = "" if pd.isna(r.get("text")) else str(r.get("text"))
            media_path = None if pd.isna(r.get("media_path")) else str(r.get("media_path")).strip()
            parse_mode = None if pd.isna(r.get("parse_mode")) else str(r.get("parse_mode")).strip().lower()
            buttons = None if pd.isna(r.get("buttons")) else str(r.get("buttons"))
            pin = str(r.get("pin", "no")).strip().lower() in {"yes", "1", "true", "y"}

            rows.append({
                "idx": i,
                "chat": chat,
                "dt_utc": dt_utc,
                "text": text,
                "media_path": media_path,
                "parse_mode": parse_mode,
                "buttons": buttons,
                "pin": pin
            })
        except Exception as e:
            print(f"[Row {i}] Row parse ERROR: {e}")

    return rows


def schedule_rows(rows: List[dict]):
    now = datetime.now(pytz.utc)
    count = 0
    for r in rows:
        run_dt = r["dt_utc"]
        if run_dt <= now:
            print(f"[Row {r['idx']}] Past time ({run_dt.isoformat()}) — skipping.")
            continue

        trigger = DateTrigger(run_date=run_dt)
        scheduler.add_job(
            send_row_task,
            trigger=trigger,
            args=[r["idx"], r["chat"], r["text"], r["media_path"], r["parse_mode"], r["buttons"], r["pin"]],
            id=f"row_{r['idx']}",
            misfire_grace_time=3600  # 1 soat
        )
        print(f"[Row {r['idx']}] Scheduled at {run_dt.isoformat()} → {r['chat']}")
        count += 1
    print(f"Scheduled jobs: {count}")


# ------------------------------- MAIN ---------------------------------
async def main():
    await client.start()  # Birinchi marta telefon raqami orqali login so'raydi
    print("Telegram client started.")
    rows = load_rows()
    schedule_rows(rows)
    scheduler.start()
    print("Scheduler started. Waiting for jobs…")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
