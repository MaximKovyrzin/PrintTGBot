import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, time as dt_time
from io import BytesIO
from pathlib import Path
from threading import RLock
from zoneinfo import ZoneInfo

import telebot
from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pypdf import PdfReader
from telebot import types


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PRINTER_ADMIN_ID_RAW = os.getenv("PRINTER_ADMIN_ID", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEEKLY_PAGE_LIMIT = int(os.getenv("WEEKLY_PAGE_LIMIT", "10"))
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "20"))
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Moscow").strip()
HISTORY_RETENTION_DAYS = int(os.getenv("HISTORY_RETENTION_DAYS", "0"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in .env")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in .env")

try:
    PRINTER_ADMIN_ID = int(PRINTER_ADMIN_ID_RAW)
except ValueError as exc:
    raise RuntimeError("PRINTER_ADMIN_ID must be an integer Telegram ID") from exc

if WEEKLY_PAGE_LIMIT <= 0:
    raise RuntimeError("WEEKLY_PAGE_LIMIT must be > 0")
if MAX_FILE_SIZE_MB <= 0:
    raise RuntimeError("MAX_FILE_SIZE_MB must be > 0")
if HISTORY_RETENTION_DAYS < 0:
    raise RuntimeError("HISTORY_RETENTION_DAYS must be >= 0")

try:
    TZ = ZoneInfo(APP_TIMEZONE)
except Exception as exc:
    raise RuntimeError(f"Invalid APP_TIMEZONE: {APP_TIMEZONE}") from exc

MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
DB_PATH = BASE_DIR / "print_bot.sqlite3"

bot = telebot.TeleBot(BOT_TOKEN)
db_lock = RLock()
admin_states = {}

# Shared Neon PostgreSQL is used only for access control.
# Print jobs continue to live in the local SQLite database.
access_pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=4,
    open=False,
    timeout=15,
)


@contextmanager
def db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def _create_print_jobs_table(conn, table_name="print_jobs"):
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            telegram_file_id TEXT NOT NULL,
            telegram_file_unique_id TEXT,
            filename TEXT NOT NULL,
            pages INTEGER NOT NULL CHECK (pages > 0),
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'accepted', 'ready', 'rejected', 'cancelled')),
            pickup_time TEXT,
            rejection_reason TEXT,
            cancellation_reason TEXT,
            cancelled_by TEXT,
            created_at INTEGER NOT NULL,
            ready_at INTEGER,
            cancelled_at INTEGER,
            admin_message_id INTEGER
        )
        """
    )


def _ensure_schema(conn):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'print_jobs'"
    ).fetchone()

    if row is None:
        _create_print_jobs_table(conn)
        return

    create_sql = (row["sql"] or "").lower()
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(print_jobs)")}
    required_columns = {"cancellation_reason", "cancelled_by", "cancelled_at"}

    if "cancelled" in create_sql and required_columns.issubset(columns):
        return

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DROP TABLE IF EXISTS print_jobs_new")
        _create_print_jobs_table(conn, "print_jobs_new")
        conn.execute(
            """
            INSERT INTO print_jobs_new (
                id, user_id, username, full_name,
                telegram_file_id, telegram_file_unique_id,
                filename, pages, status, pickup_time, rejection_reason,
                created_at, ready_at, admin_message_id
            )
            SELECT
                id, user_id, username, full_name,
                telegram_file_id, telegram_file_unique_id,
                filename, pages, status, pickup_time, rejection_reason,
                created_at, ready_at, admin_message_id
            FROM print_jobs
            """
        )
        conn.execute("DROP TABLE print_jobs")
        conn.execute("ALTER TABLE print_jobs_new RENAME TO print_jobs")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_db():
    with db_connection() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        _ensure_schema(conn)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_print_jobs_user_created
            ON print_jobs(user_id, created_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_print_jobs_status_created
            ON print_jobs(status, created_at)
            """
        )


def cleanup_old_history():
    if HISTORY_RETENTION_DAYS <= 0:
        return 0

    cutoff = int((now_local() - timedelta(days=HISTORY_RETENTION_DAYS)).timestamp())
    with db_lock, db_connection() as conn:
        cursor = conn.execute(
            """
            DELETE FROM print_jobs
            WHERE status IN ('ready', 'rejected', 'cancelled')
              AND created_at < ?
            """,
            (cutoff,),
        )
    return cursor.rowcount


def now_local():
    return datetime.now(TZ)


def week_bounds(reference=None):
    current = reference or now_local()
    monday = (current - timedelta(days=current.weekday())).date()
    start = datetime.combine(monday, dt_time.min, tzinfo=TZ)
    end = start + timedelta(days=7)
    return int(start.timestamp()), int(end.timestamp())


def get_weekly_used_pages(user_id):
    start_ts, end_ts = week_bounds()
    with db_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(pages), 0) AS used
            FROM print_jobs
            WHERE user_id = ?
              AND created_at >= ?
              AND created_at < ?
              AND status NOT IN ('rejected', 'cancelled')
            """,
            (user_id, start_ts, end_ts),
        ).fetchone()
    return int(row["used"])


def create_job_if_within_limit(
    *, user_id, username, full_name, telegram_file_id,
    telegram_file_unique_id, filename, pages
):
    start_ts, end_ts = week_bounds()
    created_at = int(now_local().timestamp())

    with db_lock, db_connection() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT COALESCE(SUM(pages), 0) AS used
                FROM print_jobs
                WHERE user_id = ?
                  AND created_at >= ?
                  AND created_at < ?
                  AND status NOT IN ('rejected', 'cancelled')
                """,
                (user_id, start_ts, end_ts),
            ).fetchone()
            used = int(row["used"])

            if used + pages > WEEKLY_PAGE_LIMIT:
                conn.execute("ROLLBACK")
                return None, used

            cursor = conn.execute(
                """
                INSERT INTO print_jobs (
                    user_id, username, full_name,
                    telegram_file_id, telegram_file_unique_id,
                    filename, pages, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    user_id, username, full_name,
                    telegram_file_id, telegram_file_unique_id,
                    filename, pages, created_at,
                ),
            )
            job_id = cursor.lastrowid
            conn.execute("COMMIT")
            return job_id, used + pages
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise


def get_job(job_id):
    with db_connection() as conn:
        row = conn.execute(
            "SELECT * FROM print_jobs WHERE id = ?", (job_id,)
        ).fetchone()
    return dict(row) if row else None


def set_admin_message_id(job_id, message_id):
    with db_lock, db_connection() as conn:
        conn.execute(
            "UPDATE print_jobs SET admin_message_id = ? WHERE id = ?",
            (message_id, job_id),
        )


def update_job_pickup_time(job_id, pickup_time):
    with db_lock, db_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE print_jobs
            SET status = 'accepted', pickup_time = ?, rejection_reason = NULL
            WHERE id = ? AND status IN ('pending', 'accepted')
            """,
            (pickup_time, job_id),
        )
    return cursor.rowcount > 0


def mark_job_ready(job_id):
    ready_at = int(now_local().timestamp())
    with db_lock, db_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE print_jobs
            SET status = 'ready', ready_at = ?
            WHERE id = ? AND status IN ('pending', 'accepted')
            """,
            (ready_at, job_id),
        )
    return cursor.rowcount > 0


def reject_job(job_id, reason):
    with db_lock, db_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE print_jobs
            SET status = 'rejected', rejection_reason = ?, pickup_time = NULL
            WHERE id = ? AND status IN ('pending', 'accepted')
            """,
            (reason, job_id),
        )
    return cursor.rowcount > 0


def cancel_job(job_id, cancelled_by, reason=None, expected_user_id=None):
    cancelled_at = int(now_local().timestamp())
    with db_lock, db_connection() as conn:
        if expected_user_id is None:
            cursor = conn.execute(
                """
                UPDATE print_jobs
                SET status = 'cancelled', cancellation_reason = ?,
                    cancelled_by = ?, cancelled_at = ?, pickup_time = NULL
                WHERE id = ? AND status IN ('pending', 'accepted')
                """,
                (reason, cancelled_by, cancelled_at, job_id),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE print_jobs
                SET status = 'cancelled', cancellation_reason = ?,
                    cancelled_by = ?, cancelled_at = ?, pickup_time = NULL
                WHERE id = ? AND user_id = ?
                  AND status IN ('pending', 'accepted')
                """,
                (reason, cancelled_by, cancelled_at, job_id, expected_user_id),
            )
    return cursor.rowcount > 0


def get_user_jobs(user_id, limit, offset):
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM print_jobs
            WHERE user_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (user_id, limit, offset),
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) AS count FROM print_jobs WHERE user_id = ?",
            (user_id,),
        ).fetchone()["count"]
    return [dict(row) for row in rows], int(total)


def get_active_admin_jobs(limit=30):
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM print_jobs
            WHERE status IN ('pending', 'accepted')
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]



# ============================================================
# SHARED USER ACCESS (NEON POSTGRESQL)
# ============================================================

class AccessDatabaseUnavailable(RuntimeError):
    pass


def init_access_db():
    """
    Creates the shared users table if it does not exist yet.
    The laundry bot can later use exactly the same table.
    """
    with access_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    full_name VARCHAR(128),
                    role VARCHAR(16) NOT NULL DEFAULT 'user'
                        CHECK (role IN ('user', 'admin', 'superadmin')),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        conn.commit()


def get_access_profile(user_id):
    """
    Returns a shared user from Neon or None when access is not granted.

    The printer responsible person is always allowed, even if their ID is
    absent from the shared users table.
    """
    if is_admin(user_id):
        return {
            "telegram_id": user_id,
            "full_name": "Ответственный за принтер",
            "role": "printer_admin",
        }

    try:
        with access_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT telegram_id, full_name, role
                    FROM users
                    WHERE telegram_id = %s
                    LIMIT 1
                    """,
                    (user_id,),
                )
                row = cur.fetchone()
    except Exception as exc:
        print(f"Could not check shared user access for {user_id}: {exc}")
        raise AccessDatabaseUnavailable from exc

    return dict(row) if row else None


def require_message_access(message):
    """Checks Neon access for message handlers and replies on failure."""
    try:
        profile = get_access_profile(message.from_user.id)
    except AccessDatabaseUnavailable:
        bot.send_message(
            message.chat.id,
            "⚠️ Сейчас не удалось проверить доступ к системе. "
            "Попробуйте ещё раз через несколько секунд.",
        )
        return None

    if profile is None:
        bot.send_message(
            message.chat.id,
            "⛔ У вас нет доступа к системе печати.",
        )
        return None

    return profile


def require_callback_access(call):
    """Checks Neon access for callback buttons."""
    try:
        profile = get_access_profile(call.from_user.id)
    except AccessDatabaseUnavailable:
        bot.answer_callback_query(
            call.id,
            "Не удалось проверить доступ. Попробуйте ещё раз.",
            show_alert=True,
        )
        return None

    if profile is None:
        bot.answer_callback_query(
            call.id,
            "У вас больше нет доступа к системе.",
            show_alert=True,
        )
        return None

    return profile


STATUS_LABELS = {
    "pending": "⏳ Ожидает",
    "accepted": "🖨 Принято в печать",
    "ready": "✅ Готово",
    "rejected": "❌ Отклонено",
    "cancelled": "🚫 Отменено",
}


def is_admin(user_id):
    return user_id == PRINTER_ADMIN_ID


def user_full_name(user):
    parts = [user.first_name or "", user.last_name or ""]
    full_name = " ".join(part.strip() for part in parts if part and part.strip())
    return full_name or "Без имени"


def safe_username(username):
    return f"@{username}" if username else "не указан"


def format_created_at(timestamp):
    return datetime.fromtimestamp(timestamp, TZ).strftime("%d.%m.%Y %H:%M")


def shorten_filename(filename, max_len=45):
    filename = filename or "document.pdf"
    return filename if len(filename) <= max_len else filename[: max_len - 3] + "..."


def extract_pdf_page_count(pdf_bytes):
    try:
        reader = PdfReader(BytesIO(pdf_bytes), strict=False)
        if reader.is_encrypted:
            try:
                result = reader.decrypt("")
            except Exception:
                result = 0
            if not result:
                raise ValueError("PDF is password-protected")
        pages = len(reader.pages)
        if pages <= 0:
            raise ValueError("PDF has no pages")
        return pages
    except Exception as exc:
        raise ValueError(
            "Не удалось прочитать PDF. Возможно, файл повреждён или защищён паролем."
        ) from exc


def notify_user(user_id, text):
    try:
        bot.send_message(user_id, text)
        return True
    except Exception as exc:
        print(f"Could not notify user {user_id}: {exc}")
        return False


def main_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("📄 Мои заявки", callback_data="my_jobs:0"))
    keyboard.add(types.InlineKeyboardButton("📊 Лимит страниц", callback_data="show_limit"))
    return keyboard


def back_to_main_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Главное меню", callback_data="main"))
    return keyboard


def admin_job_keyboard(job):
    keyboard = types.InlineKeyboardMarkup()
    job_id = job["id"]
    if job["status"] in ("pending", "accepted"):
        keyboard.add(types.InlineKeyboardButton("🕒 Указать время", callback_data=f"pickup:{job_id}"))
        keyboard.add(types.InlineKeyboardButton("✅ Готово — уведомить студента", callback_data=f"ready_notify:{job_id}"))
        keyboard.add(types.InlineKeyboardButton("🗑 Отменить заявку", callback_data=f"admin_cancel:{job_id}"))
        keyboard.add(types.InlineKeyboardButton("❌ Отклонить с причиной", callback_data=f"reject:{job_id}"))
    elif job["status"] == "ready":
        keyboard.add(types.InlineKeyboardButton("🔔 Повторить уведомление о готовности", callback_data=f"ready_notify:{job_id}"))
    return keyboard


def pickup_options_keyboard(job_id):
    keyboard = types.InlineKeyboardMarkup()
    for minutes, label in ((30, "Через 30 минут"), (60, "Через 1 час"), (120, "Через 2 часа")):
        keyboard.add(types.InlineKeyboardButton(label, callback_data=f"ptime:{job_id}:{minutes}"))
    keyboard.add(types.InlineKeyboardButton("✍️ Ввести вручную", callback_data=f"ptime_manual:{job_id}"))
    keyboard.add(types.InlineKeyboardButton("Отмена", callback_data=f"admin_job_cancel:{job_id}"))
    return keyboard


def admin_caption(job):
    used = get_weekly_used_pages(job["user_id"])
    lines = [
        f"🖨 Заявка на печать №{job['id']}",
        "",
        f"Статус: {STATUS_LABELS.get(job['status'], job['status'])}",
        "",
        f"Студент: {job['full_name']}",
        f"Username: {safe_username(job['username'])}",
        f"Telegram ID: {job['user_id']}",
        "",
        f"Файл: {job['filename']}",
        f"Страниц: {job['pages']}",
        f"Использовано за неделю: {used} / {WEEKLY_PAGE_LIMIT}",
        f"Создано: {format_created_at(job['created_at'])}",
    ]
    if job["pickup_time"]:
        lines += ["", f"Забрать: {job['pickup_time']}"]
    if job["rejection_reason"]:
        lines += ["", f"Причина отказа: {job['rejection_reason']}"]
    if job.get("status") == "cancelled":
        cancelled_by = "студентом" if job.get("cancelled_by") == "user" else "ответственным"
        lines += ["", f"Отменено: {cancelled_by}"]
        if job.get("cancellation_reason"):
            lines.append(f"Причина отмены: {job['cancellation_reason']}")
    return "\n".join(lines)


def refresh_admin_job_message(job_id):
    job = get_job(job_id)
    if not job or not job["admin_message_id"]:
        return
    try:
        bot.edit_message_caption(
            chat_id=PRINTER_ADMIN_ID,
            message_id=job["admin_message_id"],
            caption=admin_caption(job),
            reply_markup=admin_job_keyboard(job),
        )
    except Exception as exc:
        print(f"Could not refresh admin message for job {job_id}: {exc}")


def send_job_to_admin(job_id):
    job = get_job(job_id)
    if not job:
        raise RuntimeError("Job not found")
    message = bot.send_document(
        PRINTER_ADMIN_ID,
        job["telegram_file_id"],
        caption=admin_caption(job),
        reply_markup=admin_job_keyboard(job),
    )
    set_admin_message_id(job_id, message.message_id)


@bot.message_handler(commands=["start"])
def start(message):
    profile = require_message_access(message)
    if profile is None:
        return

    if is_admin(message.from_user.id):
        admin_states.pop(message.from_user.id, None)
    bot.send_message(
        message.chat.id,
        "🖨 Бот печати документов\n\n"
        "Отправьте мне PDF-файл, и я создам заявку на печать.\n"
        f"Лимит: {WEEKLY_PAGE_LIMIT} страниц в неделю.\n\n"
        "Когда ответственный укажет время выдачи или отметит документ готовым, "
        "я пришлю вам уведомление.",
        reply_markup=main_keyboard(),
    )


@bot.callback_query_handler(func=lambda call: call.data == "main")
def main_menu(call):
    if require_callback_access(call) is None:
        return

    bot.edit_message_text(
        "Главное меню:\n\nОтправьте PDF или выберите действие:",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=main_keyboard(),
    )
    bot.answer_callback_query(call.id)


def limit_text(user_id):
    used = get_weekly_used_pages(user_id)
    left = max(0, WEEKLY_PAGE_LIMIT - used)
    return (
        "📊 Лимит печати\n\n"
        f"Использовано на этой неделе: {used} / {WEEKLY_PAGE_LIMIT}\n"
        f"Осталось: {left} страниц\n\n"
        "Неделя считается с понедельника по воскресенье.\n"
        "Отклонённые и отменённые заявки лимит не расходуют."
    )


@bot.message_handler(commands=["limit"])
def limit_command(message):
    if require_message_access(message) is None:
        return
    bot.send_message(message.chat.id, limit_text(message.from_user.id), reply_markup=back_to_main_keyboard())


@bot.callback_query_handler(func=lambda call: call.data == "show_limit")
def show_limit(call):
    if require_callback_access(call) is None:
        return

    bot.edit_message_text(
        limit_text(call.from_user.id),
        call.message.chat.id,
        call.message.message_id,
        reply_markup=back_to_main_keyboard(),
    )
    bot.answer_callback_query(call.id)


@bot.message_handler(content_types=["document"])
def handle_document(message):
    access_profile = require_message_access(message)
    if access_profile is None:
        return

    document = message.document
    user_id = message.from_user.id
    filename = document.file_name or "document.pdf"
    mime_type = (document.mime_type or "").lower()

    if not filename.lower().endswith(".pdf") or mime_type not in ("application/pdf", "application/x-pdf", ""):
        bot.reply_to(message, "❌ Я принимаю только PDF-файлы.")
        return

    if document.file_size and document.file_size > MAX_FILE_SIZE_BYTES:
        bot.reply_to(message, f"❌ Файл слишком большой.\nМаксимальный размер: {MAX_FILE_SIZE_MB} МБ.")
        return

    status_message = bot.reply_to(message, "Проверяю PDF и недельный лимит…")

    try:
        file_info = bot.get_file(document.file_id)
        file_bytes = bot.download_file(file_info.file_path)
    except Exception:
        bot.edit_message_text(
            "❌ Не удалось скачать файл из Telegram. Попробуйте отправить его ещё раз.",
            message.chat.id,
            status_message.message_id,
        )
        return

    if len(file_bytes) > MAX_FILE_SIZE_BYTES:
        bot.edit_message_text(
            f"❌ Файл слишком большой.\nМаксимальный размер: {MAX_FILE_SIZE_MB} МБ.",
            message.chat.id,
            status_message.message_id,
        )
        return

    try:
        pages = extract_pdf_page_count(file_bytes)
    except ValueError as exc:
        bot.edit_message_text(f"❌ {exc}", message.chat.id, status_message.message_id)
        return

    if pages > WEEKLY_PAGE_LIMIT:
        used = get_weekly_used_pages(user_id)
        left = max(0, WEEKLY_PAGE_LIMIT - used)
        bot.edit_message_text(
            "❌ Этот документ нельзя принять целиком.\n\n"
            f"В документе: {pages} страниц\n"
            f"Использовано на этой неделе: {used} / {WEEKLY_PAGE_LIMIT}\n"
            f"Доступно: {left} страниц",
            message.chat.id,
            status_message.message_id,
        )
        return

    try:
        job_id, used_after = create_job_if_within_limit(
            user_id=user_id,
            username=message.from_user.username,
            full_name=(access_profile.get("full_name") or user_full_name(message.from_user)),
            telegram_file_id=document.file_id,
            telegram_file_unique_id=getattr(document, "file_unique_id", None),
            filename=filename,
            pages=pages,
        )
    except sqlite3.Error:
        bot.edit_message_text("❌ Ошибка базы данных. Попробуйте ещё раз.", message.chat.id, status_message.message_id)
        return

    if job_id is None:
        used = get_weekly_used_pages(user_id)
        left = max(0, WEEKLY_PAGE_LIMIT - used)
        bot.edit_message_text(
            "❌ Невозможно добавить документ — недельный лимит будет превышен.\n\n"
            f"В документе: {pages} страниц\n"
            f"Использовано: {used} / {WEEKLY_PAGE_LIMIT}\n"
            f"Доступно: {left} страниц",
            message.chat.id,
            status_message.message_id,
        )
        return

    try:
        send_job_to_admin(job_id)
    except Exception as exc:
        reject_job(job_id, "Системная ошибка: ответственный не получил заявку.")
        bot.edit_message_text(
            "❌ Сейчас не удалось передать документ ответственному.\n"
            "Заявка отменена и страницы не списаны из недельного лимита.\n"
            "Попробуйте позже.",
            message.chat.id,
            status_message.message_id,
        )
        print(f"Could not send job {job_id} to admin: {exc}")
        return

    bot.edit_message_text(
        f"✅ Заявка №{job_id} создана.\n\n"
        f"Файл: {filename}\n"
        f"Страниц: {pages}\n"
        f"Использовано за неделю: {used_after} / {WEEKLY_PAGE_LIMIT}\n\n"
        "Ответственный получил файл. Я уведомлю вас, когда появится время выдачи.",
        message.chat.id,
        status_message.message_id,
        reply_markup=main_keyboard(),
    )


USER_JOBS_PER_PAGE = 5


def show_user_jobs(call, page):
    _, total = get_user_jobs(call.from_user.id, USER_JOBS_PER_PAGE, 0)
    if total == 0:
        bot.edit_message_text(
            "📄 У вас пока нет заявок.",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=back_to_main_keyboard(),
        )
        return

    total_pages = (total + USER_JOBS_PER_PAGE - 1) // USER_JOBS_PER_PAGE
    page = max(0, min(page, total_pages - 1))
    jobs, _ = get_user_jobs(call.from_user.id, USER_JOBS_PER_PAGE, page * USER_JOBS_PER_PAGE)

    lines = [f"📄 Мои заявки — страница {page + 1}/{total_pages}", ""]
    for job in jobs:
        lines += [
            f"№{job['id']} · {STATUS_LABELS.get(job['status'], job['status'])}",
            f"{shorten_filename(job['filename'])} — {job['pages']} стр.",
            f"Создано: {format_created_at(job['created_at'])}",
        ]
        if job["pickup_time"]:
            lines.append(f"Забрать: {job['pickup_time']}")
        if job["rejection_reason"]:
            lines.append(f"Причина: {job['rejection_reason']}")
        if job.get("cancellation_reason"):
            lines.append(f"Причина отмены: {job['cancellation_reason']}")
        lines.append("")

    keyboard = types.InlineKeyboardMarkup()
    for job in jobs:
        if job["status"] in ("pending", "accepted"):
            keyboard.add(types.InlineKeyboardButton(
                f"🚫 Отменить №{job['id']}",
                callback_data=f"ucancel:{job['id']}:{page}",
            ))
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("←", callback_data=f"my_jobs:{page - 1}"))
    if page < total_pages - 1:
        nav.append(types.InlineKeyboardButton("→", callback_data=f"my_jobs:{page + 1}"))
    if nav:
        keyboard.row(*nav)
    keyboard.add(types.InlineKeyboardButton("Главное меню", callback_data="main"))

    bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: call.data.startswith("my_jobs:"))
def my_jobs(call):
    if require_callback_access(call) is None:
        return

    try:
        page = int(call.data.split(":", 1)[1])
    except ValueError:
        page = 0
    show_user_jobs(call, page)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("ucancel:"))
def user_cancel_request(call):
    if require_callback_access(call) is None:
        return

    try:
        _, job_id_text, page_text = call.data.split(":")
        job_id = int(job_id_text)
        page = int(page_text)
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job or job["user_id"] != call.from_user.id:
        bot.answer_callback_query(call.id, "Заявка не найдена.")
        return
    if job["status"] not in ("pending", "accepted"):
        bot.answer_callback_query(call.id, "Эту заявку уже нельзя отменить.")
        return

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(
        "Да, отменить заявку", callback_data=f"ucancel_yes:{job_id}:{page}"
    ))
    keyboard.add(types.InlineKeyboardButton(
        "Нет, вернуться", callback_data=f"my_jobs:{page}"
    ))
    bot.edit_message_text(
        f"Отменить заявку №{job_id}?\n\n"
        f"Файл: {job['filename']}\n"
        f"Страниц: {job['pages']}\n\n"
        "После отмены эти страницы снова станут доступны в недельном лимите.",
        call.message.chat.id, call.message.message_id, reply_markup=keyboard,
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("ucancel_yes:"))
def user_cancel_confirm(call):
    if require_callback_access(call) is None:
        return

    try:
        _, job_id_text, page_text = call.data.split(":")
        job_id = int(job_id_text)
        page = int(page_text)
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job or job["user_id"] != call.from_user.id:
        bot.answer_callback_query(call.id, "Заявка не найдена.")
        return
    if not cancel_job(job_id, "user", "Отменено студентом", call.from_user.id):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        show_user_jobs(call, page)
        return

    notify_user(
        PRINTER_ADMIN_ID,
        f"🚫 Студент отменил заявку №{job_id}.\n\n"
        f"Студент: {job['full_name']}\n"
        f"Файл: {job['filename']}\nСтраниц: {job['pages']}",
    )
    refresh_admin_job_message(job_id)
    bot.answer_callback_query(call.id, "Заявка отменена.")
    show_user_jobs(call, page)


@bot.callback_query_handler(func=lambda call: call.data.startswith("pickup:"))
def pickup_start(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    try:
        job_id = int(call.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job or job["status"] not in ("pending", "accepted"):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        return
    bot.send_message(PRINTER_ADMIN_ID, f"Когда можно забрать заявку №{job_id}?", reply_markup=pickup_options_keyboard(job_id))
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("ptime:"))
def pickup_quick(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    try:
        _, job_id_text, minutes_text = call.data.split(":")
        job_id = int(job_id_text)
        minutes = int(minutes_text)
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректные данные.")
        return
    job = get_job(job_id)
    if not job or job["status"] not in ("pending", "accepted"):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        return

    pickup_dt = now_local() + timedelta(minutes=minutes)
    pickup_text = f"после {pickup_dt.strftime('%H:%M')} ({pickup_dt.strftime('%d.%m')})"
    if not update_job_pickup_time(job_id, pickup_text):
        bot.answer_callback_query(call.id, "Не удалось обновить заявку.")
        return

    notify_user(
        job["user_id"],
        f"🖨 Заявка №{job_id} принята в печать.\n\n"
        f"Файл: {job['filename']}\n"
        f"Забрать можно: {pickup_text}",
    )
    refresh_admin_job_message(job_id)
    bot.edit_message_text(f"✅ Для заявки №{job_id}: {pickup_text}", call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id, "Время отправлено студенту.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("ptime_manual:"))
def pickup_manual_start(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    try:
        job_id = int(call.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job or job["status"] not in ("pending", "accepted"):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        return
    admin_states[PRINTER_ADMIN_ID] = {"step": "pickup_manual", "job_id": job_id}
    bot.send_message(
        PRINTER_ADMIN_ID,
        f"Введите текст времени выдачи для заявки №{job_id}.\n\n"
        "Например:\nСегодня после 16:30\nили\nЗавтра после 12:00\n\nДля отмены: /cancel",
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(
    func=lambda call: call.data.startswith("ready:") or call.data.startswith("ready_notify:")
)
def ready_job(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    try:
        job_id = int(call.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job:
        bot.answer_callback_query(call.id, "Заявка не найдена.")
        return
    if job["status"] in ("rejected", "cancelled"):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        return
    if job["status"] != "ready":
        if not mark_job_ready(job_id):
            bot.answer_callback_query(call.id, "Не удалось обновить заявку.")
            return
        job = get_job(job_id)

    pickup_line = f"\nЗабрать: {job['pickup_time']}" if job["pickup_time"] else ""
    sent = notify_user(
        job["user_id"],
        f"✅ Ваша заявка №{job_id} готова!\n\n"
        f"Файл: {job['filename']}\n"
        f"Документ распечатан и готов к получению.{pickup_line}",
    )
    refresh_admin_job_message(job_id)
    if sent:
        bot.answer_callback_query(call.id, "Уведомление о готовности отправлено.")
    else:
        bot.answer_callback_query(
            call.id,
            "Статус обновлён, но уведомление отправить не удалось.",
            show_alert=True,
        )


@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_cancel:"))
def admin_cancel_request(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    try:
        job_id = int(call.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job or job["status"] not in ("pending", "accepted"):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        return

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(
        "Да, отменить", callback_data=f"admin_cancel_yes:{job_id}"
    ))
    keyboard.add(types.InlineKeyboardButton(
        "Нет", callback_data=f"admin_job_cancel:{job_id}"
    ))
    bot.send_message(
        PRINTER_ADMIN_ID,
        f"Отменить заявку №{job_id}?\n\n"
        f"Студент: {job['full_name']}\nФайл: {job['filename']}",
        reply_markup=keyboard,
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_cancel_yes:"))
def admin_cancel_confirm(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    try:
        job_id = int(call.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job:
        bot.answer_callback_query(call.id, "Заявка не найдена.")
        return
    if not cancel_job(job_id, "admin", "Отменено ответственным за печать"):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        return

    notify_user(
        job["user_id"],
        f"🚫 Заявка №{job_id} отменена ответственным за печать.\n\n"
        f"Файл: {job['filename']}\n"
        "Страницы этой заявки снова доступны в недельном лимите.",
    )
    refresh_admin_job_message(job_id)
    bot.edit_message_text(
        f"Заявка №{job_id} отменена.",
        call.message.chat.id, call.message.message_id,
    )
    bot.answer_callback_query(call.id, "Студент уведомлён.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("reject:"))
def reject_start(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    try:
        job_id = int(call.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(call.id, "Некорректная заявка.")
        return
    job = get_job(job_id)
    if not job or job["status"] not in ("pending", "accepted"):
        bot.answer_callback_query(call.id, "Заявка уже закрыта.")
        return
    admin_states[PRINTER_ADMIN_ID] = {"step": "reject_reason", "job_id": job_id}
    bot.send_message(
        PRINTER_ADMIN_ID,
        f"Введите причину отклонения заявки №{job_id}.\n"
        "Студент увидит этот текст.\n\nДля отмены: /cancel",
    )
    bot.answer_callback_query(call.id)


@bot.message_handler(commands=["cancel"])
def cancel(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нечего отменять.")
        return
    if admin_states.pop(PRINTER_ADMIN_ID, None):
        bot.send_message(PRINTER_ADMIN_ID, "Действие отменено.")
    else:
        bot.send_message(PRINTER_ADMIN_ID, "Активного действия нет.")


@bot.message_handler(
    func=lambda message: is_admin(message.from_user.id)
    and message.from_user.id in admin_states
    and bool(message.text)
)
def admin_text_state(message):
    state = admin_states.get(PRINTER_ADMIN_ID)
    if not state:
        return

    text = message.text.strip()
    if not text:
        bot.send_message(PRINTER_ADMIN_ID, "Текст не должен быть пустым.")
        return

    job_id = state["job_id"]
    job = get_job(job_id)
    if not job or job["status"] not in ("pending", "accepted"):
        admin_states.pop(PRINTER_ADMIN_ID, None)
        bot.send_message(PRINTER_ADMIN_ID, "Заявка уже закрыта.")
        return

    if state["step"] == "pickup_manual":
        if len(text) > 200:
            bot.send_message(PRINTER_ADMIN_ID, "Слишком длинный текст. Максимум 200 символов.")
            return
        if not update_job_pickup_time(job_id, text):
            bot.send_message(PRINTER_ADMIN_ID, "Не удалось обновить заявку.")
            return
        admin_states.pop(PRINTER_ADMIN_ID, None)
        notify_user(
            job["user_id"],
            f"🖨 Заявка №{job_id} принята в печать.\n\n"
            f"Файл: {job['filename']}\n"
            f"Забрать можно: {text}",
        )
        refresh_admin_job_message(job_id)
        bot.send_message(PRINTER_ADMIN_ID, f"✅ Время для заявки №{job_id} отправлено студенту.")
        return

    if state["step"] == "reject_reason":
        if len(text) > 500:
            bot.send_message(PRINTER_ADMIN_ID, "Причина слишком длинная. Максимум 500 символов.")
            return
        if not reject_job(job_id, text):
            bot.send_message(PRINTER_ADMIN_ID, "Не удалось отклонить заявку.")
            return
        admin_states.pop(PRINTER_ADMIN_ID, None)
        notify_user(
            job["user_id"],
            f"❌ Заявка №{job_id} отклонена.\n\n"
            f"Файл: {job['filename']}\n"
            f"Причина: {text}\n\n"
            "Страницы этой заявки больше не учитываются в недельном лимите.",
        )
        refresh_admin_job_message(job_id)
        bot.send_message(PRINTER_ADMIN_ID, f"Заявка №{job_id} отклонена.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_job_cancel:"))
def admin_job_cancel(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Доступ запрещён.")
        return
    admin_states.pop(PRINTER_ADMIN_ID, None)
    bot.edit_message_text("Действие отменено.", call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id)


@bot.message_handler(commands=["queue"])
def admin_queue(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Доступ запрещён.")
        return
    jobs = get_active_admin_jobs()
    if not jobs:
        bot.send_message(PRINTER_ADMIN_ID, "Активных заявок нет.")
        return

    lines = ["🖨 Активные заявки:", ""]
    for job in jobs:
        lines += [
            f"№{job['id']} · {STATUS_LABELS[job['status']]}",
            f"{job['full_name']} — {job['pages']} стр.",
            shorten_filename(job["filename"]),
            f"Забрать: {job['pickup_time']}" if job["pickup_time"] else "Время выдачи не указано",
            "",
        ]
    bot.send_message(PRINTER_ADMIN_ID, "\n".join(lines))


@bot.message_handler(content_types=["photo", "video", "audio", "voice", "animation"])
def reject_non_pdf_media(message):
    if require_message_access(message) is None:
        return
    bot.reply_to(message, "❌ Для печати отправьте документ именно в формате PDF.")


@bot.message_handler(content_types=["text"])
def text_fallback(message):
    if is_admin(message.from_user.id) and message.from_user.id in admin_states:
        return

    if require_message_access(message) is None:
        return

    if is_admin(message.from_user.id) and message.from_user.id in admin_states:
        return
    bot.send_message(
        message.chat.id,
        "Отправьте PDF-файл или используйте кнопки меню.",
        reply_markup=main_keyboard(),
    )


if __name__ == "__main__":
    access_pool.open(wait=True, timeout=20)
    init_access_db()
    init_db()
    deleted_history = cleanup_old_history()
    print("Print bot started.")
    print("Shared user access: Neon PostgreSQL")
    print(f"Database: {DB_PATH}")
    print(f"Responsible Telegram ID: {PRINTER_ADMIN_ID}")
    print(f"Weekly page limit: {WEEKLY_PAGE_LIMIT}")
    print(f"Timezone: {APP_TIMEZONE}")
    print(f"History retention days: {HISTORY_RETENTION_DAYS or 'forever'}")
    if deleted_history:
        print(f"Deleted old closed jobs: {deleted_history}")
    bot.infinity_polling(skip_pending=True, allowed_updates=["message", "callback_query"])
