import socket

# Принудительный приоритет IPv4 для стабильной работы в облачных сетях (без sudo)
_orig_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_ipv4_first(host, port, family=0, type=0, proto=0, flags=0):
    try:
        res = _orig_getaddrinfo(host, port, family, type, proto, flags)
        ipv4 = [r for r in res if r[0] == socket.AF_INET]
        return ipv4 if ipv4 else res
    except Exception:
        return _orig_getaddrinfo(host, port, family, type, proto, flags)
socket.getaddrinfo = _getaddrinfo_ipv4_first

import os
import sys
import time
import asyncio
import logging
import sqlite3
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
)
from telegram.error import TelegramError, RetryAfter, Forbidden, BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    Job,
)
from telegram.request import HTTPXRequest

# =============================================================================
# ЧАСОВОЙ ПОЯС: КИЕВСКОЕ ВРЕМЯ (Europe/Kyiv)
# =============================================================================
from datetime import tzinfo

def get_kyiv_tz() -> tzinfo:
    """Определяет часовой пояс Киева с автоматическим переходом на летнее/зимнее время"""
    for name in ["Europe/Kyiv", "Europe/Kiev"]:
        try:
            from zoneinfo import ZoneInfo
            return ZoneInfo(name)
        except Exception:
            pass
    class KyivTz(tzinfo):
        def utcoffset(self, dt):
            return timedelta(hours=3 if self._is_dst(dt) else 2)
        def dst(self, dt):
            return timedelta(hours=1) if self._is_dst(dt) else timedelta(0)
        def tzname(self, dt):
            return "EEST" if self._is_dst(dt) else "EET"
        def _is_dst(self, dt):
            year = (dt or datetime.now()).year
            mar_last = max(d for d in range(25, 32) if datetime(year, 3, d).weekday() == 6)
            oct_last = max(d for d in range(25, 32) if datetime(year, 10, d).weekday() == 6)
            dst_start = datetime(year, 3, mar_last, 3, 0)
            dst_end = datetime(year, 10, oct_last, 4, 0)
            naive = dt.replace(tzinfo=None) if (dt and dt.tzinfo) else (dt or datetime.now())
            return dst_start <= naive < dst_end
    return KyivTz()

KYIV_TZ = get_kyiv_tz()

def get_now_kyiv() -> datetime:
    """Возвращает текущее время по Киеву (Europe/Kyiv)"""
    return datetime.now(KYIV_TZ)


# =============================================================================
# ЗАГРУЗКА .ENV И КОНФИГУРАЦИЯ
# =============================================================================
def load_env(env_path: str = ".env") -> None:
    """Загрузка переменных окружения из .env файла без сторонних библиотек"""
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception as e:
        print(f"Предупреждение при загрузке .env: {e}")

load_env()

def _parse_id_list(env_val: str, default: List[int]) -> List[int]:
    """Парсинг списка ID из строки"""
    if not env_val:
        return default
    try:
        return [int(x.strip()) for x in env_val.split(",") if x.strip()]
    except Exception:
        return default

CONFIG = {
    "BOT_TOKEN": os.getenv("BOT_TOKEN", "").strip(),
    "CHANNEL_ID": os.getenv("CHANNEL_ID", "").strip(),
    "ADMIN_CHAT_IDS": _parse_id_list(os.getenv("ADMIN_CHAT_IDS", ""), []),
    "MOD_LEVEL1": [],
    "MOD_LEVEL2": [],
    "MEDIA_GROUP_TIMEOUT": float(os.getenv("MEDIA_GROUP_TIMEOUT", "2.0")),
    "BOT_USERNAME": os.getenv("BOT_USERNAME", "").strip(),
    "PROXY_URL": os.getenv("PROXY_URL", "").strip(),
    "DB_NAME": os.getenv("DB_NAME", "messages.db"),
}

# =============================================================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# =============================================================================
FORBIDDEN_WORDS: Set[str] = set()
media_groups_lock = threading.Lock()
user_media_groups: Dict[str, Dict] = {}
media_group_jobs: Dict[str, Job] = {}
user_report_mode: Dict[int, str] = {}  # user_id -> "report_user" | "report_post"
user_appeal_mode: Set[int] = set()     # user_id в режиме написания апелляции


# =============================================================================
# ЛОГИРОВАНИЕ И УТИЛИТЫ
# =============================================================================
def get_logger() -> logging.Logger:
    """Получить логгер приложения"""
    return logging.getLogger("telegram_bot")


def safe_print(text: str) -> None:
    """Безопасный вывод в консоль для Windows c поддержкой UTF-8"""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore"))


def setup_global_logging() -> None:
    """Глобальная настройка логирования с ротацией/UTF-8"""
    logger = get_logger()
    logger.setLevel(logging.WARNING)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        file_handler = logging.FileHandler("bot.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)
        
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)


# =============================================================================
# БАЗА ДАННЫХ (SQLite)
# =============================================================================
def get_db_connection() -> sqlite3.Connection:
    """Получить соединение с БД с включенным WAL и таймаутом"""
    conn = sqlite3.connect(CONFIG["DB_NAME"], timeout=10.0)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Инициализация и миграция таблиц БД"""
    with get_db_connection() as db:
        cursor = db.cursor()

        # Таблица сообщений
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                username TEXT,
                first_name TEXT,
                type TEXT,
                text TEXT,
                file_id TEXT,
                caption TEXT,
                status TEXT DEFAULT 'pending',
                media_group TEXT,
                admin_message_id INTEGER,
                processed_by TEXT,
                processed_at TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("PRAGMA table_info(messages)")
        columns = {col["name"] for col in cursor.fetchall()}
        if "processed_by" not in columns:
            cursor.execute("ALTER TABLE messages ADD COLUMN processed_by TEXT")
        if "processed_at" not in columns:
            cursor.execute("ALTER TABLE messages ADD COLUMN processed_at TEXT")

        # Таблица медиагрупп
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS media_groups (
                media_group_id TEXT PRIMARY KEY,
                message_ids TEXT,
                status TEXT DEFAULT 'pending',
                admin_message_id INTEGER,
                processed_by TEXT,
                processed_at TEXT
            )
        """)

        # Таблица точного сопоставления сообщений у каждого админа/модератора
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_chat_messages (
                message_id INTEGER,
                chat_id INTEGER,
                admin_message_id INTEGER,
                is_album INTEGER DEFAULT 0,
                PRIMARY KEY (message_id, chat_id)
            )
        """)

        # Чёрный список
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blacklist (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                banned_by INTEGER,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Запрещённые слова
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forbidden_words (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                word TEXT UNIQUE,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Журнал действий администрации
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                admin_name TEXT,
                action TEXT,
                target_id INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Модераторы
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moderators (
                user_id INTEGER PRIMARY KEY,
                level INTEGER,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Пользователи (хранит только ID для рассылки, без профилей)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY
            )
        """)

        # Жалобы и запросы
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                report_type TEXT,
                content TEXT,
                file_id TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_states (
                user_id INTEGER PRIMARY KEY,
                state TEXT,
                data TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        db.commit()

    get_logger().info("База данных успешно инициализирована")


def register_user(user_id: int, username: Optional[str] = None, first_name: str = "") -> None:
    """Сохранить Chat ID исключительно для работы рассылки (/say) — никаких профилей и имён не ведётся"""
    try:
        with get_db_connection() as db:
            db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            db.commit()
    except Exception:
        pass


def save_report(user_id: int, report_type: str, content: str, file_id: str = "") -> int:
    """Сохранить жалобу/запрос в БД и вернуть ID"""
    with get_db_connection() as db:
        cursor = db.execute(
            "INSERT INTO reports (user_id, report_type, content, file_id, status) VALUES (?, ?, ?, ?, 'pending')",
            (user_id, report_type, content, file_id)
        )
        db.commit()
        return cursor.lastrowid



# =============================================================================
# ДВУСТОРОННЯЯ СИНХРОНИЗАЦИЯ: БОТ ⇄ ФАЙЛЫ (.env, blacklist.txt, words.txt)
# =============================================================================
def sync_moderators_to_env(env_path: str = ".env") -> None:
    """Сохраняет актуальных модераторов L1 и L2 в .env"""
    l1_str = ",".join(str(x) for x in CONFIG.get("MOD_LEVEL1", []))
    l2_str = ",".join(str(x) for x in CONFIG.get("MOD_LEVEL2", []))
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        new_lines = []
        l1_found, l2_found = False, False
        for line in lines:
            if line.startswith("MOD_LEVEL1="):
                new_lines.append(f"MOD_LEVEL1={l1_str}\n")
                l1_found = True
            elif line.startswith("MOD_LEVEL2="):
                new_lines.append(f"MOD_LEVEL2={l2_str}\n")
                l2_found = True
            else:
                new_lines.append(line)
        if not l1_found:
            new_lines.append(f"MOD_LEVEL1={l1_str}\n")
        if not l2_found:
            new_lines.append(f"MOD_LEVEL2={l2_str}\n")
        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
    except Exception as e:
        get_logger().error(f"Ошибка сохранения модераторов в .env: {e}")


def sync_blacklist_to_file(file_path: str = "blacklist.txt") -> None:
    """Выгружает чёрный список в текстовый файл blacklist.txt"""
    try:
        with get_db_connection() as db:
            rows = db.execute("SELECT user_id, reason, banned_at FROM blacklist ORDER BY banned_at DESC").fetchall()
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("# Чёрный список (ID | Причина | Дата)\n")
            for r in rows:
                reason = r["reason"] or "Нарушение правил"
                banned_at = r["banned_at"] or ""
                f.write(f"{r['user_id']} | {reason} | {banned_at}\n")
    except Exception as e:
        get_logger().error(f"Ошибка сохранения blacklist.txt: {e}")


def load_blacklist_from_file(file_path: str = "blacklist.txt") -> int:
    """Загружает чёрный список из файла blacklist.txt в базу данных"""
    if not os.path.exists(file_path):
        return 0
    added = 0
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        with get_db_connection() as db:
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split("|")]
                try:
                    uid = int(parts[0])
                except ValueError:
                    continue
                reason = parts[1] if len(parts) > 1 and parts[1] else "Нарушение правил"
                existing = db.execute("SELECT 1 FROM blacklist WHERE user_id = ?", (uid,)).fetchone()
                if not existing:
                    db.execute(
                        "INSERT OR IGNORE INTO blacklist (user_id, reason, banned_by) VALUES (?, ?, ?)",
                        (uid, reason, 0)
                    )
                    added += 1
            db.commit()
    except Exception as e:
        get_logger().error(f"Ошибка загрузки blacklist.txt: {e}")
    return added


def sync_words_to_file(file_path: str = "words.txt") -> None:
    """Выгружает запрещённые слова в файл words.txt"""
    try:
        words = sorted(list(FORBIDDEN_WORDS))
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("# Запрещённые слова (по одному на строку)\n")
            for w in words:
                f.write(f"{w}\n")
    except Exception as e:
        get_logger().error(f"Ошибка сохранения words.txt: {e}")


def load_words_from_file(file_path: str = "words.txt") -> int:
    """Загружает запрещённые слова из файла words.txt в базу данных"""
    if not os.path.exists(file_path):
        return 0
    added = 0
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        with get_db_connection() as db:
            for line in lines:
                w = line.strip().lower()
                if not w or w.startswith("#"):
                    continue
                if w not in FORBIDDEN_WORDS:
                    try:
                        db.execute("INSERT OR IGNORE INTO forbidden_words (word, added_by) VALUES (?, ?)", (w, 0))
                        FORBIDDEN_WORDS.add(w)
                        added += 1
                    except Exception:
                        pass
            db.commit()
    except Exception as e:
        get_logger().error(f"Ошибка загрузки words.txt: {e}")
    return added


def load_moderators_from_env() -> None:
    """Синхронизирует модераторов из .env в базу данных"""
    env_l1 = _parse_id_list(os.getenv("MOD_LEVEL1", ""), [])
    env_l2 = _parse_id_list(os.getenv("MOD_LEVEL2", ""), [])
    with get_db_connection() as db:
        for uid in env_l1:
            db.execute("INSERT OR IGNORE INTO moderators (user_id, level, added_by) VALUES (?, 1, 0)", (uid,))
        for uid in env_l2:
            db.execute("INSERT OR IGNORE INTO moderators (user_id, level, added_by) VALUES (?, 2, 0)", (uid,))
        db.commit()
    load_moderators()


def sync_all_to_files() -> None:
    """Выгрузка всех актуальных данных из БД в файлы"""
    sync_moderators_to_env()
    sync_blacklist_to_file()
    sync_words_to_file()


def sync_all_from_files() -> Dict[str, int]:
    """Синхронизация из файлов в БД и обратно"""
    load_env()
    load_moderators_from_env()
    b_added = load_blacklist_from_file()
    w_added = load_words_from_file()
    sync_all_to_files()
    return {"blacklist_added": b_added, "words_added": w_added}


def add_moderator(user_id: int, level: int, added_by: int) -> bool:
    """Добавить модератора"""
    if user_id in CONFIG["ADMIN_CHAT_IDS"]:
        return False
    with get_db_connection() as db:
        db.execute(
            "INSERT OR REPLACE INTO moderators (user_id, level, added_by) VALUES (?, ?, ?)",
            (user_id, level, added_by)
        )
        db.commit()
    load_moderators()
    sync_moderators_to_env()
    return True


def remove_moderator(user_id: int) -> bool:
    """Удалить модератора"""
    with get_db_connection() as db:
        cursor = db.execute("DELETE FROM moderators WHERE user_id = ?", (user_id,))
        db.commit()
        deleted = cursor.rowcount > 0
    load_moderators()
    sync_moderators_to_env()
    return deleted


def get_moderators() -> List[sqlite3.Row]:
    """Получить список модераторов из БД"""
    with get_db_connection() as db:
        return db.execute("SELECT user_id, level, added_by FROM moderators").fetchall()


def get_next_id() -> int:
    """Получить следующий ID для сообщения"""
    with get_db_connection() as db:
        cursor = db.execute("SELECT MAX(id) FROM messages")
        max_id = cursor.fetchone()[0]
        return (max_id or 0) + 1


def save_message(msg_id: int, msg_data: Dict, user_id: int = 0, username: Optional[str] = None, first_name: str = "") -> None:
    """Сохранить сообщение в базу (100% анонимность: user_id, username и имя НЕ сохраняются!)"""
    with get_db_connection() as db:
        db.execute(
            """INSERT INTO messages 
               (id, user_id, username, first_name, type, text, file_id, caption, status, media_group, admin_message_id) 
               VALUES (?, NULL, NULL, NULL, ?, ?, ?, ?, 'pending', ?, NULL)""",
            (
                msg_id,
                msg_data["type"],
                msg_data.get("text"),
                msg_data.get("file_id"),
                msg_data.get("caption"),
                msg_data.get("media_group"),
            ),
        )
        db.commit()


def save_media_group(media_group_id: str, message_ids: List[int]) -> None:
    """Сохранить медиагруппу в базу"""
    with get_db_connection() as db:
        db.execute(
            "INSERT OR REPLACE INTO media_groups (media_group_id, message_ids, status, admin_message_id) VALUES (?, ?, ?, ?)",
            (media_group_id, ",".join(map(str, message_ids)), "pending", None),
        )
        db.commit()


def get_media_group_messages(media_group_id: str) -> Tuple[List[Dict], int]:
    """Получить сообщения медиагруппы"""
    with get_db_connection() as db:
        row = db.execute("SELECT message_ids, admin_message_id FROM media_groups WHERE media_group_id = ?", (media_group_id,)).fetchone()
        if not row:
            return [], 0
        message_ids = [int(x) for x in row["message_ids"].split(",") if x]
        messages = []
        for mid in message_ids:
            mr = db.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone()
            if mr:
                messages.append({
                    "id": mr["id"],
                    "type": mr["type"],
                    "file_id": mr["file_id"],
                    "caption": mr["caption"],
                    "status": mr["status"],
                })
        return messages, row["admin_message_id"] or 0


def update_media_group_status(media_group_id: str, status: str) -> None:
    """Обновить статус медиагруппы"""
    with get_db_connection() as db:
        db.execute("UPDATE media_groups SET status = ? WHERE media_group_id = ?", (status, media_group_id))
        db.commit()


def load_message(msg_id: int) -> Optional[Dict]:
    """Загрузить сообщение из базы"""
    with get_db_connection() as db:
        row = db.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        if row:
            return {
                "id": row["id"],
                "user_id": row["user_id"],
                "username": row["username"],
                "first_name": row["first_name"],
                "type": row["type"],
                "text": row["text"],
                "file_id": row["file_id"],
                "caption": row["caption"],
                "status": row["status"],
                "media_group": row["media_group"],
                "admin_message_id": row["admin_message_id"],
            }
        return None


def record_admin_chat_message(msg_id: int, chat_id: int, admin_message_id: int, is_album: int = 0) -> None:
    """Сохранить ID сообщения у конкретного админа/модератора в чате"""
    with get_db_connection() as db:
        db.execute(
            "INSERT OR REPLACE INTO admin_chat_messages (message_id, chat_id, admin_message_id, is_album) VALUES (?, ?, ?, ?)",
            (msg_id, chat_id, admin_message_id, is_album)
        )
        db.commit()


def get_admin_chat_messages(msg_id: int) -> List[Tuple[int, int]]:
    """Получить пары (chat_id, admin_message_id) для сообщения"""
    with get_db_connection() as db:
        cursor = db.execute("SELECT chat_id, admin_message_id FROM admin_chat_messages WHERE message_id = ?", (msg_id,))
        return [(r["chat_id"], r["admin_message_id"]) for r in cursor.fetchall()]


def update_message_status(msg_id: int, status: str, admin_name: str = "") -> None:
    """Обновить статус сообщения с фиксацией времени и автора решения"""
    now = get_now_kyiv().strftime("%H:%M")
    with get_db_connection() as db:
        db.execute(
            "UPDATE messages SET status = ?, processed_by = ?, processed_at = ? WHERE id = ?",
            (status, admin_name, now, msg_id)
        )
        db.commit()
    get_logger().info(f"Статус сообщения {msg_id}: {status} ({admin_name} в {now})")


def get_message_processed_info(msg_id: int) -> Optional[Tuple[str, str]]:
    """Получить информацию: кто и когда обработал сообщение"""
    with get_db_connection() as db:
        row = db.execute("SELECT processed_by, processed_at FROM messages WHERE id = ?", (msg_id,)).fetchone()
        if row and row["processed_by"]:
            return (row["processed_by"], row["processed_at"])
        return None


def log_admin_action(admin_id: int, admin_name: str, action: str, target_id: Optional[int] = None) -> None:
    """Записать действие администратора"""
    with get_db_connection() as db:
        db.execute(
            "INSERT INTO admin_actions (admin_id, admin_name, action, target_id) VALUES (?, ?, ?, ?)",
            (admin_id, admin_name, action, target_id)
        )
        db.commit()


# =============================================================================
# ЧЁРНЫЙ СПИСОК И АПЕЛЛЯЦИИ
# =============================================================================
def get_ban_keyboard() -> InlineKeyboardMarkup:
    """Кнопка для заблокированного пользователя: написать администратору"""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📩 Написать администратору", callback_data="appeal_ban")
    ]])


def get_ban_message_for_user(ban_reason: str) -> str:
    """Сообщение для забаненного пользователя без открытых контактов"""
    return (
        f"🚫 <b>Вы были заблокированы в боте</b>\n\n"
        f"📝 <b>Причина:</b> {ban_reason}\n\n"
        f"<i>Если вы считаете, что произошла ошибка, вы можете подать апелляцию администрации по кнопке ниже:</i>"
    )


def add_to_blacklist(user_id: int, reason: str, banned_by: int, context: Optional[ContextTypes.DEFAULT_TYPE] = None, notify_user: bool = True) -> bool:
    """Добавить пользователя в чёрный список (с возможностью тихого бана)"""
    if user_id in CONFIG["ADMIN_CHAT_IDS"]:
        return False
    now_k = get_now_kyiv().strftime("%Y-%m-%d %H:%M:%S")
    with get_db_connection() as db:
        db.execute(
            "INSERT OR REPLACE INTO blacklist (user_id, reason, banned_by, banned_at) VALUES (?, ?, ?, ?)",
            (user_id, reason, banned_by, now_k)
        )
        db.commit()
    get_logger().info("Зафиксирована блокировка пользователя")

    # Уведомляем забаненного только если не включен тихий режим
    if notify_user and context:
        ban_msg = get_ban_message_for_user(reason)
        markup = get_ban_keyboard()
        asyncio.create_task(_safe_send_message(context.bot, user_id, ban_msg, parse_mode="HTML", reply_markup=markup))

    sync_blacklist_to_file()
    return True


def remove_from_blacklist(user_id: int, context: Optional[ContextTypes.DEFAULT_TYPE] = None) -> None:
    """Удалить пользователя из чёрного списка"""
    with get_db_connection() as db:
        db.execute("DELETE FROM blacklist WHERE user_id = ?", (user_id,))
        db.commit()

    if context:
        unban_msg = (
            f"✅ <b>Вы были разблокированы в боте</b>\n\n"
            f"Теперь вы снова можете отправлять сообщения.\n\n"
            f"📜 Пожалуйста, соблюдайте правила бота."
        )
        asyncio.create_task(_safe_send_message(context.bot, user_id, unban_msg, parse_mode="HTML"))


def is_user_banned(user_id: int) -> Optional[str]:
    """Проверить, забанен ли пользователь"""
    if user_id in CONFIG["ADMIN_CHAT_IDS"]:
        return None
    with get_db_connection() as db:
        row = db.execute("SELECT reason FROM blacklist WHERE user_id = ?", (user_id,)).fetchone()
        return row["reason"] if row else None


def get_blacklist() -> List[sqlite3.Row]:
    """Получить список забаненных (от новых к старым)"""
    with get_db_connection() as db:
        return db.execute("SELECT user_id, reason, banned_by, banned_at FROM blacklist ORDER BY banned_at DESC, user_id DESC").fetchall()


# =============================================================================
# ФИЛЬТР ЗАПРЕЩЁННЫХ СЛОВ
# =============================================================================
def add_forbidden_word(word: str, added_by: int) -> bool:
    """Добавить запрещённое слово"""
    word = word.lower().strip()
    if not word:
        return False
    with get_db_connection() as db:
        try:
            db.execute("INSERT INTO forbidden_words (word, added_by) VALUES (?, ?)", (word, added_by))
            db.commit()
        except sqlite3.IntegrityError:
            return False
    FORBIDDEN_WORDS.add(word)
    get_logger().info(f"Добавлено запрещённое слово: '{word}'. Всего: {len(FORBIDDEN_WORDS)}")
    sync_words_to_file()
    return True


def remove_forbidden_word(word: str) -> bool:
    """Удалить запрещённое слово"""
    word = word.lower().strip()
    with get_db_connection() as db:
        db.execute("DELETE FROM forbidden_words WHERE word = ?", (word,))
        db.commit()
    FORBIDDEN_WORDS.discard(word)
    sync_words_to_file()
    return True


def get_forbidden_words() -> List[sqlite3.Row]:
    """Получить список запрещённых слов"""
    with get_db_connection() as db:
        return db.execute("SELECT word, added_by FROM forbidden_words").fetchall()


def contains_forbidden_word(text: str) -> Optional[str]:
    """Проверить текст на запрещённые слова"""
    if not text or not FORBIDDEN_WORDS:
        return None
    text_lower = text.lower()
    for word in FORBIDDEN_WORDS:
        if word in text_lower:
            return word
    return None


def load_forbidden_words() -> None:
    """Загрузить запрещённые слова из базы в память"""
    global FORBIDDEN_WORDS
    rows = get_forbidden_words()
    FORBIDDEN_WORDS = {r["word"] for r in rows}


# =============================================================================
# ФОРМАТИРОВАНИЕ СООБЩЕНИЙ
# =============================================================================
def format_user_info(user=None, hide: bool = True) -> str:
    """100% анонимность: автор всегда отображается как Аноним для всех уровней модерации"""
    return "👤 От: Аноним"


def format_text_admin(user, text: str, hide: bool = False) -> str:
    """Форматировать текстовое сообщение для админа"""
    return f"🔒 <b>Текстовое сообщение:</b>\n\n{format_user_info(user, hide)}\n\n💬 Текст:\n<blockquote>{text}</blockquote>"


def format_media_admin(user, mtype: str, caption: str, hide: bool = False) -> str:
    """Форматировать медиа-сообщение для админа"""
    msg = f"🔒 <b>Медиа: {mtype}</b>\n\n{format_user_info(user, hide)}"
    if caption:
        msg += f"\n\n📝 Описание:\n<blockquote>{caption}</blockquote>"
    return msg


def format_channel_msg(ctype: str, content: str) -> str:
    """Форматировать сообщение для канала"""
    titles = {
        "text": "📨 <b>Анонимное сообщение</b>",
        "photo": "📷 <b>Анонимное фото</b>",
        "video": "🎥 <b>Анонимное видео</b>",
        "animation": "🎬 <b>Анонимное GIF</b>",
        "audio": "🎵 <b>Анонимная музыка</b>",
        "voice": "🎤 <b>Голосовое сообщение</b>",
        "document": "📄 <b>Анонимный файл</b>",
        "sticker": "😀 <b>Анонимный стикер</b>",
    }
    msg = titles.get(ctype, "📨 <b>Анонимное сообщение</b>")
    if ctype != "sticker" and content:
        msg += f"\n\n<blockquote>{content}</blockquote>"
    msg += f"\n\n<i>via @{CONFIG['BOT_USERNAME']}</i>"
    return msg


def format_forbidden_words_message(words: List[sqlite3.Row]) -> str:
    """Форматировать список запрещённых слов"""
    if not words:
        return "📝 <b>Список пуст.</b>"
    msg = "🚫 <b>Запрещённые слова:</b>\n\n"
    for i, w in enumerate(words, 1):
        msg += f"{i}. <code>{w['word']}</code>\n"
    return msg


async def get_user_info_str(bot, user_id: int) -> str:
    """Получить имя, юзернейм и ID пользователя"""
    try:
        chat = await bot.get_chat(user_id)
        name = chat.full_name or chat.first_name or "—"
        username = f"@{chat.username}" if chat.username else "—"
        return f"{name} / {username} / <code>{user_id}</code>"
    except Exception:
        return f"— / — / <code>{user_id}</code>"


# =============================================================================
# КЛАВИАТУРЫ
# =============================================================================
def get_admin_keyboard(msg_id: int, ctype: str) -> InlineKeyboardMarkup:
    """Клавиатура для админа (одобрить/отклонить одиночное сообщение)"""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Принять", callback_data=f"approve_{msg_id}_{ctype}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{msg_id}"),
    ]])


def get_group_keyboard(mgid: str) -> InlineKeyboardMarkup:
    """Клавиатура для медиагруппы (альбома)"""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Принять все", callback_data=f"approve_group_{mgid}"),
        InlineKeyboardButton("❌ Отклонить все", callback_data=f"reject_group_{mgid}"),
    ]])


# =============================================================================
# БЕЗОПАСНАЯ ОТПРАВКА И СИНХРОНИЗАЦИЯ МОДЕРАТОРОВ
# =============================================================================
async def _safe_send_message(bot, chat_id: int, text: str, **kwargs) -> Optional[int]:
    """Безопасная отправка сообщения с перехватом ошибок"""
    try:
        msg = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
        return msg.message_id
    except Forbidden:
        get_logger().info(f"Бот заблокирован пользователем {chat_id}")
    except Exception as e:
        get_logger().warning(f"Ошибка отправки сообщения пользователю {chat_id}: {e}")
    return None


async def update_all_admin_messages(context: ContextTypes.DEFAULT_TYPE, msg_id: int, status_text: str) -> None:
    """
    Надёжное обновление статуса сообщения у ВСЕХ модераторов.
    Берёт сохранённые admin_message_id из таблицы admin_chat_messages.
    """
    admin_msgs = get_admin_chat_messages(msg_id)
    if not admin_msgs:
        get_logger().warning(f"Не найдены сообщения модераторов для msg_id={msg_id}")
        return

    msg_data = load_message(msg_id)
    if not msg_data:
        return

    is_text = msg_data["type"] == "text"

    for chat_id, admin_msg_id in admin_msgs:
        hide = chat_id in CONFIG["MOD_LEVEL1"]
        
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=admin_msg_id,
                reply_markup=None
            )
        except Exception:
            pass

        class DummyUser:
            def __init__(self, uid, username, first_name):
                self.id = uid
                self.username = username
                self.first_name = first_name

        user_dummy = DummyUser(msg_data["user_id"], msg_data["username"], msg_data["first_name"])

        try:
            if is_text:
                base_text = format_text_admin(user_dummy, msg_data.get("text") or "", hide)
                new_text = base_text + status_text
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=admin_msg_id,
                    text=new_text,
                    parse_mode="HTML",
                    reply_markup=None
                )
            else:
                user_info = format_user_info(user_dummy, hide)
                type_display = msg_data["type"].upper()
                full_info = f"{user_info}\n\n📦 <b>Тип:</b> {type_display}"
                caption_text = msg_data.get("caption") or ""
                new_caption = f"{caption_text}\n\n{full_info}{status_text}" if caption_text else f"{full_info}{status_text}"
                await context.bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=admin_msg_id,
                    caption=new_caption[:1024],
                    parse_mode="HTML",
                    reply_markup=None
                )
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                get_logger().debug(f"Не удалось обновить сообщение {admin_msg_id} в {chat_id}: {e}")
        except Exception as e:
            get_logger().debug(f"Ошибка обновления чата {chat_id}: {e}")


async def send_media_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, ctype: str, fid: str, caption: str, msg_id: int, user_info: str = "", reply_markup: Optional[InlineKeyboardMarkup] = None) -> Optional[int]:
    """Отправить медиафайл модератору"""
    try:
        methods = {
            "photo": context.bot.send_photo,
            "video": context.bot.send_video,
            "animation": context.bot.send_animation,
            "audio": context.bot.send_audio,
            "voice": context.bot.send_voice,
            "document": context.bot.send_document,
            "sticker": context.bot.send_sticker,
        }
        method = methods.get(ctype)
        if not method:
            return None

        full_caption = ""
        if caption:
            full_caption += f"{caption}\n\n"
        if user_info:
            full_caption += user_info

        markup = reply_markup if reply_markup is not None else get_admin_keyboard(msg_id, ctype)

        kwargs = {
            "chat_id": chat_id,
            ctype: fid,
            "caption": full_caption[:1024],
            "parse_mode": "HTML",
            "reply_markup": markup
        }
        if ctype == "sticker":
            kwargs.pop("caption", None)
            kwargs.pop("parse_mode", None)

        msg = await method(**kwargs)
        return msg.message_id
    except Exception as e:
        get_logger().error(f"Ошибка отправки медиа {ctype} модератору {chat_id}: {e}")
        return None


async def send_media_channel(context: ContextTypes.DEFAULT_TYPE, chat_id: str, ctype: str, fid: str, caption: str) -> Optional[int]:
    """Отправить медиа в целевой канал"""
    try:
        methods = {
            "photo": context.bot.send_photo,
            "video": context.bot.send_video,
            "animation": context.bot.send_animation,
            "audio": context.bot.send_audio,
            "voice": context.bot.send_voice,
            "document": context.bot.send_document,
            "sticker": context.bot.send_sticker,
        }
        method = methods.get(ctype)
        if not method:
            return None

        kwargs = {"chat_id": chat_id, ctype: fid}
        if ctype != "sticker":
            kwargs["caption"] = caption[:1024]
            kwargs["parse_mode"] = "HTML" if caption else None

        msg = await method(**kwargs)
        get_logger().info(f"Медиа {ctype} отправлено в канал: {msg.message_id}")
        return msg.message_id
    except Exception as e:
        get_logger().error(f"Ошибка публикации медиа в канал ({ctype}): {e}")
        return None


# =============================================================================
# ОБРАБОТКА МЕДИАГРУПП (АЛЬБОМОВ)
# =============================================================================
async def process_media_group(context: ContextTypes.DEFAULT_TYPE, mgid: str, user) -> None:
    """Сбор и отправка готового альбома всем модераторам"""
    logger = get_logger()

    with media_groups_lock:
        if mgid not in user_media_groups:
            return
        gd = user_media_groups[mgid]
        if not gd["messages"]:
            del user_media_groups[mgid]
            return

    try:
        save_media_group(mgid, gd["messages"])
        mtype_display = "📷 Фото" if gd["type"] == "photo" else "🎥 Видео"
        all_mods = get_all_moderators()

        msgs = [load_message(mid) for mid in gd["messages"] if load_message(mid)]
        if not msgs:
            return

        for chat_id in all_mods:
            hide = chat_id in CONFIG["MOD_LEVEL1"]
            try:
                for i, m in enumerate(msgs):
                    cap = m.get("caption") or ""
                    user_info = format_user_info(user, hide)
                    type_info = f"📦 <b>Тип:</b> {mtype_display} ({i+1}/{len(msgs)})"
                    full_info = f"🔒 <b>Альбом:</b>\n\n{user_info}\n\n{type_info}"

                    markup = get_group_keyboard(mgid) if i == len(msgs) - 1 else None
                    admin_msg_id = await send_media_admin(
                        context, chat_id, m["type"], m["file_id"], cap, m["id"], full_info, reply_markup=markup
                    )
                    if admin_msg_id:
                        record_admin_chat_message(m["id"], chat_id, admin_msg_id, is_album=1)

            except Exception as e:
                logger.error(f"Ошибка рассылки альбома {mgid} в чат {chat_id}: {e}")

        with media_groups_lock:
            user_media_groups.pop(mgid, None)
            media_group_jobs.pop(mgid, None)

    except Exception as e:
        logger.error(f"Критическая ошибка обработки группы {mgid}: {e}")


async def process_single_media(update: Update, context: ContextTypes.DEFAULT_TYPE, user, ctype: str, fid: str, caption: str, mtype_display: str) -> None:
    """Обработка одиночного медиафайла"""
    logger = get_logger()
    msg_id = get_next_id()

    save_message(msg_id, {"type": ctype, "file_id": fid, "caption": caption}, user.id, user.username, user.first_name)
    all_mods = get_all_moderators()

    for chat_id in all_mods:
        hide = chat_id in CONFIG["MOD_LEVEL1"]
        try:
            user_info = format_user_info(user, hide)
            type_info = f"📦 <b>Тип:</b> {mtype_display}"
            full_info = f"{user_info}\n\n{type_info}"

            admin_msg_id = await send_media_admin(context, chat_id, ctype, fid, caption or "", msg_id, full_info)
            if admin_msg_id:
                record_admin_chat_message(msg_id, chat_id, admin_msg_id, is_album=0)
        except Exception as e:
            logger.error(f"Ошибка отправки медиа модератору {chat_id}: {e}")

    await update.message.reply_text(f"✅ {mtype_display} отправлено на модерацию!", parse_mode="HTML")


async def process_media_message(update: Update, context: ContextTypes.DEFAULT_TYPE, user, ctype: str, fid: str, caption: str, mgid: Optional[str], mtype_display: str) -> None:
    """Диспетчер медиа-сообщений (одиночное или часть альбома)"""
    logger = get_logger()
    try:
        if mgid and ctype in ["photo", "video"]:
            with media_groups_lock:
                if mgid not in user_media_groups:
                    user_media_groups[mgid] = {
                        "user": user,
                        "messages": [],
                        "type": ctype,
                        "caption": caption,
                        "last_received": time.time()
                    }
                    job = context.job_queue.run_once(
                        lambda ctx: asyncio.create_task(process_media_group(ctx, mgid, user)),
                        CONFIG["MEDIA_GROUP_TIMEOUT"]
                    )
                    media_group_jobs[mgid] = job
                    await update.message.reply_text("✅ Альбом получен!", parse_mode="HTML")
                else:
                    user_media_groups[mgid]["last_received"] = time.time()
                    if mgid in media_group_jobs:
                        media_group_jobs[mgid].schedule_removal()
                        job = context.job_queue.run_once(
                            lambda ctx: asyncio.create_task(process_media_group(ctx, mgid, user)),
                            CONFIG["MEDIA_GROUP_TIMEOUT"]
                        )
                        media_group_jobs[mgid] = job

            mid = get_next_id()
            save_message(mid, {"type": ctype, "file_id": fid, "caption": caption, "media_group": mgid}, user.id, user.username, user.first_name)

            with media_groups_lock:
                if mgid in user_media_groups:
                    user_media_groups[mgid]["messages"].append(mid)
        else:
            await process_single_media(update, context, user, ctype, fid, caption, mtype_display)
    except Exception as e:
        logger.error(f"Ошибка медиа-диспетчера: {e}")


# =============================================================================
# ОБРАБОТКА ВХОДЯЩИХ СООБЩЕНИЙ
# =============================================================================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Главный обработчик всех входящих сообщений от пользователей"""
    if not update.message:
        return
    user = update.effective_user
    if not user:
        return

    register_user(user.id, user.username, user.first_name)
    logger = get_logger()

    # === РЕЖИМ АПЕЛЛЯЦИИ БЛОКИРОВКИ ===
    if user.id in user_appeal_mode:
        text = update.message.text or update.message.caption or "Без текста"
        photo_id = update.message.photo[-1].file_id if update.message.photo else None
        ban_reason = is_user_banned(user.id) or "Не указана"

        all_mods = get_all_moderators()
        reporter_info = f"@{user.username}" if user.username else user.first_name

        appeal_msg = (
            f"⚖️ <b>Новая апелляция блокировки!</b>\n\n"
            f"👤 От: {reporter_info} (ID: <code>{user.id}</code>)\n"
            f"📝 <b>Причина бана:</b> {ban_reason}\n\n"
            f"💬 <b>Обращение пользователя:</b>\n"
            f"<blockquote>{text}</blockquote>"
        )

        appeal_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔓 Разблокировать", callback_data=f"unban_{user.id}"),
            InlineKeyboardButton("❌ Отклонить апелляцию", callback_data=f"reject_appeal_{user.id}")
        ]])

        for mod_id in all_mods:
            try:
                if photo_id:
                    await context.bot.send_photo(mod_id, photo_id, caption=appeal_msg, parse_mode="HTML", reply_markup=appeal_markup)
                else:
                    await context.bot.send_message(mod_id, appeal_msg, parse_mode="HTML", reply_markup=appeal_markup)
            except Exception:
                pass

        user_appeal_mode.discard(user.id)
        await update.message.reply_text("✅ Ваше обращение успешно передано администрации. Ожидайте решения.", parse_mode="HTML")
        return

    # Проверка на бан (для обычных сообщений)
    if ban_reason := is_user_banned(user.id):
        await update.message.reply_text(
            get_ban_message_for_user(ban_reason),
            parse_mode="HTML",
            reply_markup=get_ban_keyboard()
        )
        return

    # === ОБРАБОТКА РЕЖИМА ЖАЛОБ / УДАЛЕНИЯ ПОСТОВ ===
    if user.id in user_report_mode:
        mode = user_report_mode[user.id]

        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            caption = update.message.caption or "Без описания"
            report_id = save_report(user.id, mode, caption, file_id)
            all_mods = get_all_moderators()
            reporter_info = f"@{user.username}" if user.username else user.first_name

            header = "🚨 <b>Новая жалоба #" if mode == "report_user" else "🗑️ <b>Запрос на удаление #"
            body_label = "📝 Жалоба:" if mode == "report_user" else "📝 Описание:"
            report_msg = (
                f"{header}{report_id}</b>\n\n"
                f"👤 От: {reporter_info} (ID: <code>{user.id}</code>)\n"
                f"{body_label} {caption}\n"
                f"📷 Приложено фото"
            )
            for mod_id in all_mods:
                try:
                    await context.bot.send_photo(mod_id, file_id, caption=report_msg, parse_mode="HTML")
                except Exception:
                    pass

            del user_report_mode[user.id]
            resp = "✅ Жалоба отправлена модераторам!" if mode == "report_user" else "✅ Запрос отправлен модераторам!"
            await update.message.reply_text(resp, parse_mode="HTML")
            return

        if update.message.text:
            text = update.message.text
            report_id = save_report(user.id, mode, text)
            all_mods = get_all_moderators()
            reporter_info = f"@{user.username}" if user.username else user.first_name

            header = "🚨 <b>Новая жалоба #" if mode == "report_user" else "🗑️ <b>Запрос на удаление #"
            body_label = "📝 Жалоба:" if mode == "report_user" else "🔗 Ссылка:"
            report_msg = (
                f"{header}{report_id}</b>\n\n"
                f"👤 От: {reporter_info} (ID: <code>{user.id}</code>)\n"
                f"{body_label} {text}"
            )
            for mod_id in all_mods:
                try:
                    await context.bot.send_message(mod_id, report_msg, parse_mode="HTML")
                except Exception:
                    pass

            del user_report_mode[user.id]
            resp = "✅ Жалоба отправлена модераторам!" if mode == "report_user" else "✅ Запрос отправлен модераторам!"
            await update.message.reply_text(resp, parse_mode="HTML")
            return

    # === ОБЫЧНЫЕ СООБЩЕНИЯ В ПРЕДЛОЖКУ ===
    try:
        mgid = update.message.media_group_id

        # Текстовое сообщение
        if update.message.text and not mgid:
            text = update.message.text

            # Проверка на запрещённые слова
            fw = contains_forbidden_word(text)
            if fw:
                logger.warning(f"Пользователь {user.id} забанен за слово: '{fw}'")
                add_to_blacklist(user.id, f"Запрещённое слово: {fw}", 0, context, notify_user=True)
                await update.message.reply_text(f"🚫 Бан за запрещённое слово: {fw}", parse_mode="HTML")
                return

            mid = get_next_id()
            save_message(mid, {"type": "text", "text": text}, user.id, user.username, user.first_name)
            all_mods = get_all_moderators()

            for chat_id in all_mods:
                hide = chat_id in CONFIG["MOD_LEVEL1"]
                try:
                    admin_msg = format_text_admin(user, text, hide)
                    sent_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=admin_msg,
                        reply_markup=get_admin_keyboard(mid, "text"),
                        parse_mode="HTML"
                    )
                    record_admin_chat_message(mid, chat_id, sent_msg.message_id, is_album=0)
                except Exception as e:
                    logger.error(f"Ошибка отправки текста модератору {chat_id}: {e}")

            await update.message.reply_text("✅ Отправлено на модерацию!", parse_mode="HTML")

        elif update.message.photo:
            await process_media_message(update, context, user, "photo", update.message.photo[-1].file_id, update.message.caption or "", mgid, "📷 Фото")
        elif update.message.video:
            await process_media_message(update, context, user, "video", update.message.video.file_id, update.message.caption or "", mgid, "🎥 Видео")
        elif update.message.animation:
            await process_media_message(update, context, user, "animation", update.message.animation.file_id, update.message.caption or "", None, "🎬 GIF")
        elif update.message.audio:
            await process_media_message(update, context, user, "audio", update.message.audio.file_id, update.message.caption or "", None, "🎵 Музыка")
        elif update.message.voice:
            await process_media_message(update, context, user, "voice", update.message.voice.file_id, "", None, "🎤 Голосовое")
        elif update.message.document:
            mt = update.message.document.mime_type or ""
            mtype = "🎵 Аудио" if "audio" in mt else "🖼️ Фото" if "image" in mt else "🎥 Видео" if "video" in mt else "📄 Файл"
            await process_media_message(update, context, user, "document", update.message.document.file_id, update.message.caption or "", mgid, mtype)
        elif update.message.sticker:
            await process_media_message(update, context, user, "sticker", update.message.sticker.file_id, "", None, "😀 Стикер")
        else:
            await update.message.reply_text("❌ Данный тип сообщения не поддерживается", parse_mode="HTML")

    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")


# =============================================================================
# ОБРАБОТКА ДЕЙСТВИЙ МОДЕРАЦИИ (КНОПКИ)
# =============================================================================
async def approve_single_message(context: ContextTypes.DEFAULT_TYPE, query, msg_id: int, ctype: str, admin_name: str) -> None:
    """Одобрить и отправить одиночное сообщение в канал"""
    logger = get_logger()
    msg_data = load_message(msg_id)

    if not msg_data:
        await query.answer("❌ Сообщение не найдено!", show_alert=True)
        return

    if msg_data["status"] != "pending":
        info = get_message_processed_info(msg_id)
        if info:
            await query.answer(f"✅ {info[0]} уже обработал в {info[1]}", show_alert=True)
        else:
            await query.answer("✅ Уже обработано другим модератором!", show_alert=True)
        return

    try:
        msg_text = format_channel_msg(ctype, msg_data.get("text") or msg_data.get("caption") or "")

        if ctype == "text":
            await context.bot.send_message(chat_id=CONFIG["CHANNEL_ID"], text=msg_text, parse_mode="HTML")
        else:
            await send_media_channel(context, CONFIG["CHANNEL_ID"], ctype, msg_data["file_id"], msg_text)

        update_message_status(msg_id, "approved", admin_name)
        log_admin_action(query.from_user.id, admin_name, "approve", msg_id)

        now = get_now_kyiv().strftime("%H:%M")
        status_text = f"\n\n✅ <b>Принято {admin_name} в {now}</b>"

        await update_all_admin_messages(context, msg_id, status_text)

        await query.answer("✅ Опубликовано в канале!", show_alert=False)
        logger.info(f"Одобрено сообщение {msg_id} → канал")

    except Exception as e:
        logger.error(f"Ошибка одобрения сообщения {msg_id}: {e}", exc_info=True)
        await query.answer(f"❌ Ошибка публикации: {e}", show_alert=True)


async def approve_media_group(context: ContextTypes.DEFAULT_TYPE, query, mgid: str, admin_name: str) -> None:
    """Одобрить и опубликовать медиагруппу в канал"""
    logger = get_logger()
    msgs, _ = get_media_group_messages(mgid)

    if not msgs:
        await query.answer("❌ Альбом не найден!", show_alert=True)
        return

    first_msg = load_message(msgs[0]["id"])
    if first_msg and first_msg["status"] != "pending":
        info = get_message_processed_info(msgs[0]["id"])
        if info:
            act = "принял" if first_msg["status"] == "approved" else "отклонил"
            await query.answer(f"ℹ️ {info[0]} уже {act} альбом в {info[1]}", show_alert=True)
        else:
            await query.answer("ℹ️ Альбом уже обработан!", show_alert=True)
        return

    try:
        msgs.sort(key=lambda x: x["id"])
        media_group = []
        cap_added = False

        for i, m in enumerate(msgs):
            if not cap_added and m.get("caption"):
                cap = f"📷 <b>Анонимный альбом</b>\n\n<blockquote>{m['caption']}</blockquote>\n\n<i>via @{CONFIG['BOT_USERNAME']}</i>"
                cap_added = True
            elif i == 0 and not cap_added:
                cap = f"📷 <b>Анонимный альбом</b>\n\n<i>via @{CONFIG['BOT_USERNAME']}</i>"
                cap_added = True
            else:
                cap = ""

            if m["type"] == "photo":
                media_group.append(InputMediaPhoto(media=m["file_id"], caption=cap, parse_mode="HTML" if cap else None))
            elif m["type"] == "video":
                media_group.append(InputMediaVideo(media=m["file_id"], caption=cap, parse_mode="HTML" if cap else None))

        if media_group:
            await context.bot.send_media_group(chat_id=CONFIG["CHANNEL_ID"], media=media_group)

        for m in msgs:
            if m["type"] not in ["photo", "video"]:
                await send_media_channel(context, CONFIG["CHANNEL_ID"], m["type"], m["file_id"], format_channel_msg(m["type"], m.get("caption", "")))
            update_message_status(m["id"], "approved", admin_name)

        update_media_group_status(mgid, "approved")
        log_admin_action(query.from_user.id, admin_name, "approve_group", 0)

        now = get_now_kyiv().strftime("%H:%M")
        status_text = f"\n\n✅ <b>Альбом опубликован {admin_name} в {now}</b>"

        for m in msgs:
            await update_all_admin_messages(context, m["id"], status_text)

        await query.answer("✅ Альбом опубликован в канале!", show_alert=False)
        logger.info(f"Альбом {mgid} опубликован модератором {admin_name}")

    except Exception as e:
        logger.error(f"Ошибка публикации альбома {mgid}: {e}", exc_info=True)
        await query.answer(f"❌ Ошибка публикации: {e}", show_alert=True)


async def reject_single_message(context: ContextTypes.DEFAULT_TYPE, query, msg_id: int, admin_name: str) -> None:
    """Отклонить одиночное сообщение"""
    msg_data = load_message(msg_id)
    if not msg_data:
        await query.answer("❌ Сообщение не найдено!", show_alert=True)
        return

    if msg_data["status"] != "pending":
        info = get_message_processed_info(msg_id)
        if info:
            act = "принял" if msg_data["status"] == "approved" else "отклонил"
            await query.answer(f"ℹ️ {info[0]} уже {act} в {info[1]}", show_alert=True)
        else:
            await query.answer("ℹ️ Уже обработано!", show_alert=True)
        return

    update_message_status(msg_id, "rejected", admin_name)
    log_admin_action(query.from_user.id, admin_name, "reject", msg_id)

    now = get_now_kyiv().strftime("%H:%M")
    status_text = f"\n\n❌ <b>Отклонено {admin_name} в {now}</b>"

    await update_all_admin_messages(context, msg_id, status_text)
    await query.answer("❌ Отклонено!", show_alert=False)


async def reject_media_group(context: ContextTypes.DEFAULT_TYPE, query, mgid: str, admin_name: str) -> None:
    """Отклонить альбом"""
    msgs, _ = get_media_group_messages(mgid)
    if not msgs:
        await query.answer("❌ Альбом не найден!", show_alert=True)
        return

    first_msg = load_message(msgs[0]["id"])
    if first_msg and first_msg["status"] != "pending":
        info = get_message_processed_info(msgs[0]["id"])
        if info:
            act = "принял" if first_msg["status"] == "approved" else "отклонил"
            await query.answer(f"ℹ️ {info[0]} уже {act} альбом в {info[1]}", show_alert=True)
        else:
            await query.answer("ℹ️ Альбом уже обработан!", show_alert=True)
        return

    for m in msgs:
        update_message_status(m["id"], "rejected", admin_name)
    update_media_group_status(mgid, "rejected")
    log_admin_action(query.from_user.id, admin_name, "reject_group", 0)

    now = get_now_kyiv().strftime("%H:%M")
    status_text = f"\n\n❌ <b>Альбом отклонён {admin_name} в {now}</b>"

    for m in msgs:
        await update_all_admin_messages(context, m["id"], status_text)

    await query.answer("❌ Альбом отклонён!", show_alert=False)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Диспетчер нажатий на inline-кнопки"""
    query = update.callback_query
    if not query:
        return

    user_id = query.from_user.id
    admin_name = query.from_user.first_name or "Модератор"
    data = query.data or ""
    parts = data.split("_")
    logger = get_logger()

    try:
        if data.startswith("approve_") and not data.startswith("approve_group_"):
            msg_id, ctype = int(parts[1]), parts[2]
            await approve_single_message(context, query, msg_id, ctype, admin_name)

        elif data.startswith("approve_group_"):
            await approve_media_group(context, query, parts[2], admin_name)

        elif data.startswith("reject_") and not data.startswith("reject_group_"):
            msg_id = int(parts[1])
            await reject_single_message(context, query, msg_id, admin_name)

        elif data.startswith("reject_group_"):
            await reject_media_group(context, query, parts[2], admin_name)

        elif data == "report_user":
            user_report_mode[user_id] = "report_user"
            await query.answer("Отправьте ID пользователя или @username и причину жалобы")
            await context.bot.send_message(
                user_id,
                "🚨 <b>Жалоба на пользователя</b>\n\n"
                "Отправьте сообщение в формате:\n"
                "<code>ID_пользователя</code> или <code>@username</code>\n"
                "<b>Причина жалобы:</b> опишите проблему\n\n"
                "Вы также можете приложить <b>скриншоты</b> в качестве доказательства.\n\n"
                "✏️ Напишите жалобу или нажмите /cancel для отмены.",
                parse_mode="HTML"
            )

        elif data == "report_post":
            user_report_mode[user_id] = "report_post"
            await query.answer("Отправьте ссылку на пост из канала")
            await context.bot.send_message(
                user_id,
                "🗑️ <b>Запрос на удаление поста</b>\n\n"
                "Отправьте <b>ссылку на пост</b> из канала, который нужно удалить.\n\n"
                "Формат ссылки: <code>https://t.me/channel_name/123</code>\n\n"
                "✏️ Отправьте ссылку или нажмите /cancel для отмены.",
                parse_mode="HTML"
            )

        # Обработка кнопки апелляции бана
        elif data == "appeal_ban":
            user_appeal_mode.add(user_id)
            await query.answer("Режим апелляции активирован")
            await context.bot.send_message(
                user_id,
                "✍️ <b>Апелляция блокировки</b>\n\n"
                "Напишите ваше сообщение для администрации (почему вас стоит разблокировать).\n"
                "Вы также можете приложить скриншот.\n\n"
                "✏️ Напишите ответ или нажмите /cancel для отмены.",
                parse_mode="HTML"
            )

        # Отклонение апелляции админом
        elif data.startswith("reject_appeal_"):
            target_id = int(parts[2])
            await query.answer("Апелляция отклонена")
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            cur_text = query.message.text or query.message.caption or ""
            status = f"\n\n❌ <b>Апелляция отклонена {admin_name}</b>"
            try:
                if query.message.caption:
                    await query.edit_message_caption(caption=cur_text + status, parse_mode="HTML")
                else:
                    await query.edit_message_text(text=cur_text + status, parse_mode="HTML")
            except Exception:
                pass

            # Уведомляем пользователя
            asyncio.create_task(_safe_send_message(
                context.bot,
                target_id,
                "❌ <b>Ваша апелляция была отклонена администрацией бота.</b>",
                parse_mode="HTML"
            ))

        elif data.startswith("unban_"):
            target_id = int(parts[1])
            remove_from_blacklist(target_id, context)
            await query.answer(f"Пользователь {target_id} разблокирован")
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            cur_text = query.message.text or query.message.caption or ""
            status = f"\n\n🔓 <b>Разблокирован {admin_name}</b>"
            try:
                if query.message.caption:
                    await query.edit_message_caption(caption=cur_text + status, parse_mode="HTML")
                else:
                    await query.edit_message_text(text=cur_text + status, parse_mode="HTML")
            except Exception:
                pass

        elif data == "cancel_blacklist":
            await query.answer("Отменено")
            try:
                await query.delete_message()
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Ошибка обработки кнопки ({data}): {e}", exc_info=True)
        await query.answer(f"❌ Ошибка: {e}", show_alert=True)


# =============================================================================
# КОМАНДЫ ПОЛЬЗОВАТЕЛЕЙ И МОДЕРАЦИИ
# =============================================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start"""
    user = update.effective_user
    register_user(user.id)

    if ban_reason := is_user_banned(user.id):
        await update.message.reply_text(
            get_ban_message_for_user(ban_reason),
            parse_mode="HTML",
            reply_markup=get_ban_keyboard()
        )
        return

    keyboard = [
        [InlineKeyboardButton("🚨 Пожаловаться на пользователя", callback_data="report_user")],
        [InlineKeyboardButton("🗑️ Удалить пост", callback_data="report_post")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "👋 <b>Добро пожаловать в @confessionsagent!</b>\n\n"
        "Здесь вы можете отправить своё сообщение, фото или видео для публикации в канале.\n\n"
        "📌 <b>Как это работает:</b>\n"
        "1. Отправьте текст, фото или видео\n"
        "2. Ваше сообщение будет передано на модерацию\n"
        "3. После одобрения модератором пост появится в канале\n\n"
        "⚠️ <b>Важно:</b>\n"
        "• Публикация <b>полностью анонимна</b> — ваше имя и юзернейм не будут известны никому\n"
        "• Соблюдайте адекватность и вежливость (не обязательно)\n"
        "• Запрещены запрещённый контент (порнография, детское порно и жестокие материалы)\n\n"
        "Для начала просто отправьте ваше сообщение ниже 👇",
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=reply_markup
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /id"""
    user = update.effective_user
    await update.message.reply_text(
        f"👤 <b>Ваша информация:</b>\n\n"
        f"🆔 <b>ID:</b> <code>{user.id}</code>\n"
        f"📛 <b>Имя:</b> {user.first_name}",
        parse_mode="HTML"
    )


async def checkban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /checkban"""
    user = update.effective_user
    if ban_reason := is_user_banned(user.id):
        await update.message.reply_text(
            get_ban_message_for_user(ban_reason),
            parse_mode="HTML",
            reply_markup=get_ban_keyboard()
        )
    else:
        await update.message.reply_text("✅ Вы не в чёрном списке!", parse_mode="HTML")


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /cancel — отмена режима репорта или апелляции"""
    user = update.effective_user
    cleared = False
    if user.id in user_report_mode:
        del user_report_mode[user.id]
        cleared = True
    if user.id in user_appeal_mode:
        user_appeal_mode.discard(user.id)
        cleared = True

    if cleared:
        await update.message.reply_text("✅ Операция отменена. Вы можете отправлять сообщения как обычно.", parse_mode="HTML")
    else:
        await update.message.reply_text("ℹ️ У вас нет активных операций.", parse_mode="HTML")


async def admin_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /admin_list"""
    bot = context.bot
    msg = "👥 <b>Список администраторов:</b>\n\n"

    msg += "👑 <b>Админы:</b>\n"
    for a in CONFIG["ADMIN_CHAT_IDS"]:
        info = await get_user_info_str(bot, a)
        msg += f"  • {info}\n"

    msg += "\n🔶 <b>Модераторы L2 (полный доступ):</b>\n"
    for m in CONFIG["MOD_LEVEL2"]:
        info = await get_user_info_str(bot, m)
        msg += f"  • {info}\n"

    msg += "\n🔵 <b>Модераторы L1 (одобрение/отклонение):</b>\n"
    for m in CONFIG["MOD_LEVEL1"]:
        info = await get_user_info_str(bot, m)
        msg += f"  • {info}\n"

    await update.message.reply_text(msg, parse_mode="HTML")


async def blacklist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /blacklist или /bl — просмотр или /bl clear"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if not context.args:
        bl = get_blacklist()
        if not bl:
            await update.message.reply_text("📋 Чёрный список пуст", parse_mode="HTML")
            return

        admin_names = {}
        for admin_id in get_all_moderators():
            try:
                chat = await context.bot.get_chat(admin_id)
                admin_names[admin_id] = chat.first_name or str(admin_id)
            except Exception:
                admin_names[admin_id] = str(admin_id)

        entries = []
        for i, row in enumerate(bl, 1):
            admin_name = admin_names.get(row["banned_by"], str(row["banned_by"]))
            entries.append(f"{i}. ID: <code>{row['user_id']}</code> — {row['reason']} (забанил {admin_name})")

        PAGE_SIZE = 15
        total_pages = (len(entries) + PAGE_SIZE - 1) // PAGE_SIZE

        for page in range(0, len(entries), PAGE_SIZE):
            chunk = entries[page:page + PAGE_SIZE]
            page_num = (page // PAGE_SIZE) + 1
            msg = f"📋 <b>Чёрный список (стр. {page_num}/{total_pages}):</b>\n\n" + "\n".join(chunk)
            await update.message.reply_text(msg, parse_mode="HTML")
        return

    action = context.args[0].lower()
    if action in ["clear", "clean", "reset", "wipe"]:
        with get_db_connection() as db:
            count = db.execute("SELECT COUNT(*) FROM blacklist").fetchone()[0]
            db.execute("DELETE FROM blacklist")
            db.commit()
        sync_blacklist_to_file()
        await update.message.reply_text(
            f"🗑️ <b>Чёрный список полностью очищен!</b>\n\n"
            f"Удалено записей: <code>{count}</code>\n"
            f"Файл <code>blacklist.txt</code> синхронизирован.",
            parse_mode="HTML"
        )
        return

    if action == "add" and len(context.args) >= 3:
        try:
            tid, reason = int(context.args[1]), " ".join(context.args[2:])
            if add_to_blacklist(tid, reason, user.id, context, notify_user=True):
                await update.message.reply_text(f"✅ Забанен ID: <code>{tid}</code>\n📝 Причина: {reason}", parse_mode="HTML")
        except Exception:
            await update.message.reply_text("❌ Ошибка выполнения команды", parse_mode="HTML")

    elif action in ["remove", "del"] and len(context.args) >= 2:
        try:
            tid = int(context.args[1])
            remove_from_blacklist(tid, context)
            await update.message.reply_text(f"✅ Разбанен ID: <code>{tid}</code>", parse_mode="HTML")
        except ValueError:
            await update.message.reply_text("❌ Неверный user_id", parse_mode="HTML")


async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /ban <user_id> [причина] — бан с уведомлением"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if not context.args:
        await update.message.reply_text(
            "ℹ️ <b>Формат:</b> <code>/ban &lt;ID&gt; [причина]</code>\n"
            "<i>Пример:</i> <code>/ban 123456789 Спам</code> (причина необязательна)",
            parse_mode="HTML"
        )
        return

    try:
        tid = int(context.args[0])
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "Нарушение правил"
        if add_to_blacklist(tid, reason, user.id, context, notify_user=True):
            all_admins = get_all_moderators()
            sender = user.first_name
            target_info = f"ID: <code>{tid}</code>"
            try:
                chat = await context.bot.get_chat(tid)
                if chat.username:
                    target_info = f"@{chat.username} / <code>{tid}</code>"
            except Exception:
                pass

            ban_msg = f"🚫 <b>{sender}</b> забанил пользователя\n\n👤 {target_info}\n📝 Причина: {reason}"
            for aid in all_admins:
                asyncio.create_task(_safe_send_message(context.bot, aid, ban_msg, parse_mode="HTML"))

            await update.message.reply_text(f"✅ Забанен ID: <code>{tid}</code>\n📝 Причина: {reason}", parse_mode="HTML")
    except ValueError:
        await update.message.reply_text("❌ Неверный ID", parse_mode="HTML")


async def banmute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /banmute <user_id> [причина] — тихий бан БЕЗ отправки сообщения пользователю"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if not context.args:
        await update.message.reply_text(
            "ℹ️ <b>Формат:</b> <code>/banmute &lt;ID&gt; [причина]</code>\n"
            "<i>Пример:</i> <code>/banmute 123456789</code> (пользователь ничего не узнает)",
            parse_mode="HTML"
        )
        return

    try:
        tid = int(context.args[0])
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "Нарушение правил (тихий бан)"
        if add_to_blacklist(tid, reason, user.id, context=context, notify_user=False):
            all_admins = get_all_moderators()
            sender = user.first_name
            target_info = f"ID: <code>{tid}</code>"
            try:
                chat = await context.bot.get_chat(tid)
                if chat.username:
                    target_info = f"@{chat.username} / <code>{tid}</code>"
            except Exception:
                pass

            ban_msg = f"🤫 <b>{sender}</b> выдал тихий бан (без оповещения)\n\n👤 {target_info}\n📝 Причина: {reason}"
            for aid in all_admins:
                asyncio.create_task(_safe_send_message(context.bot, aid, ban_msg, parse_mode="HTML"))

            await update.message.reply_text(
                f"🤫 <b>Тихий бан оформлен!</b>\n"
                f"🆔 ID: <code>{tid}</code>\n"
                f"📝 Причина: {reason}\n"
                f"🔕 Пользователь НЕ получал уведомление о блокировке.",
                parse_mode="HTML"
            )
    except ValueError:
        await update.message.reply_text("❌ Неверный ID", parse_mode="HTML")

async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /unban <user_id>"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if context.args:
        try:
            tid = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ Неверный user_id", parse_mode="HTML")
            return

        remove_from_blacklist(tid, context)
        all_admins = get_all_moderators()
        sender = user.first_name
        target_info = f"ID: <code>{tid}</code>"
        try:
            chat = await context.bot.get_chat(tid)
            if chat.username:
                target_info = f"@{chat.username} / <code>{tid}</code>"
        except Exception:
            pass

        unban_msg = f"✅ <b>{sender}</b> разблокировал пользователя\n\n👤 {target_info}"
        for aid in all_admins:
            asyncio.create_task(_safe_send_message(context.bot, aid, unban_msg, parse_mode="HTML"))

        await update.message.reply_text(f"✅ Разбанен ID: <code>{tid}</code>", parse_mode="HTML")


async def addword_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /addword <слово>"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if context.args:
        word = " ".join(context.args)
        if add_forbidden_word(word, user.id):
            all_admins = get_all_moderators()
            sender = user.first_name
            notify_msg = f"🚫 <b>{sender}</b> добавил запрещённое слово\n\n📝 Слово: <code>{word}</code>"
            for aid in all_admins:
                asyncio.create_task(_safe_send_message(context.bot, aid, notify_msg, parse_mode="HTML"))

            await update.message.reply_text(f"✅ Добавлено: <code>{word}</code>\n📢 Админы уведомлены", parse_mode="HTML")
        else:
            await update.message.reply_text("❌ Слово уже в списке или ошибка", parse_mode="HTML")


async def delword_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /delword <слово>"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if context.args:
        word = " ".join(context.args).lower()
        if remove_forbidden_word(word):
            await update.message.reply_text(f"✅ Удалено: <code>{word}</code>", parse_mode="HTML")


async def words_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /words — просмотр или /words add/del <слово>"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if not context.args:
        words = get_forbidden_words()
        if not words:
            await update.message.reply_text("📝 Список запрещённых слов пуст", parse_mode="HTML")
        else:
            await update.message.reply_text(format_forbidden_words_message(words), parse_mode="HTML")
        return

    action = context.args[0].lower()
    if action in ["add", "+"] and len(context.args) >= 2:
        context.args = context.args[1:]
        await addword_cmd(update, context)
    elif action in ["del", "rem", "remove", "-"] and len(context.args) >= 2:
        context.args = context.args[1:]
        await delword_cmd(update, context)
    else:
        words = get_forbidden_words()
        if not words:
            await update.message.reply_text("📝 Список запрещённых слов пуст", parse_mode="HTML")
        else:
            await update.message.reply_text(format_forbidden_words_message(words), parse_mode="HTML")

async def _execute_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, include_banned: bool = False) -> None:
    """Универсальная рассылка: include_banned=False для /say, True для /allsay"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    cmd_name = "/allsay" if include_banned else "/say"
    if not context.args:
        desc = "всем пользователям (включая забаненных)" if include_banned else "только активным (не забаненным)"
        await update.message.reply_text(
            f"📢 <b>Использование:</b> <code>{cmd_name} &lt;текст&gt;</code>\n"
            f"<i>Отправляет сообщение {desc}.</i>",
            parse_mode="HTML"
        )
        return

    with get_db_connection() as db:
        all_users = db.execute("SELECT DISTINCT user_id FROM (SELECT user_id FROM users UNION SELECT user_id FROM messages)").fetchall()
        banned_rows = db.execute("SELECT user_id FROM blacklist").fetchall()

    banned_ids = {r["user_id"] for r in banned_rows}
    admin_ids = set(get_all_moderators())

    target_users = []
    skipped_banned = 0
    skipped_admins = 0

    for row in all_users:
        uid = row["user_id"]
        if uid in admin_ids:
            skipped_admins += 1
            continue
        if not include_banned and uid in banned_ids:
            skipped_banned += 1
            continue
        target_users.append(uid)

    if not target_users:
        extra = f" (пропущено забаненных в ЧС: {skipped_banned})" if skipped_banned else ""
        await update.message.reply_text(f"❌ Нет подходящих пользователей для рассылки{extra}", parse_mode="HTML")
        return

    txt = " ".join(context.args)
    get_logger().info(f"Рассылка {cmd_name} запущена админом {user.id} ({user.first_name})")
    header = "📢 <b>Сообщение от Администрации</b>"

    target_desc = "всех пользователей (включая ЧС)" if include_banned else "активных пользователей (без ЧС)"
    status_msg = await update.message.reply_text(f"⏳ Начинаю рассылку для {len(target_users)} {target_desc}...")

    ok, fail = 0, 0
    for uid in target_users:
        try:
            await context.bot.send_message(uid, f"{header}\n\n{txt}", parse_mode="HTML")
            ok += 1
            await asyncio.sleep(0.05)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                await context.bot.send_message(uid, f"{header}\n\n{txt}", parse_mode="HTML")
                ok += 1
            except Exception:
                fail += 1
        except Exception:
            fail += 1

    time_str = get_now_kyiv().strftime("%H:%M:%S")
    result_msg = (
        f"✅ <b>Рассылка {cmd_name} завершена в {time_str} (Киев)!</b>\n\n"
        f"📊 Успешно доставлено: <code>{ok}</code>\n"
        f"❌ Ошибок (бот заблокирован юзером): <code>{fail}</code>\n"
    )
    if not include_banned:
        result_msg += f"🚫 Пропущено забаненных: <code>{skipped_banned}</code>\n"
    result_msg += f"🔕 Пропущено модераторов: <code>{skipped_admins}</code>"

    await status_msg.edit_text(result_msg, parse_mode="HTML")


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /say — рассылка ТОЛЬКО активным пользователям (без забаненных)"""
    await _execute_broadcast(update, context, include_banned=False)


async def allsay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /allsay — рассылка ВСЕМ пользователям (включая забаненных)"""
    await _execute_broadcast(update, context, include_banned=True)


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /admin: add, del, list"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if not context.args or context.args[0].lower() in ["list", "show"]:
        await admin_list_cmd(update, context)
        return

    action = context.args[0].lower()

    if action in ["add", "+"] and len(context.args) >= 3:
        level_str = context.args[1].lower()
        try:
            target_id = int(context.args[2])
        except ValueError:
            await update.message.reply_text("❌ Неверный user_id", parse_mode="HTML")
            return

        if level_str in ["l1", "1"]:
            level, level_name = 1, "1-й уровень"
        elif level_str in ["l2", "2"]:
            level, level_name = 2, "2-й уровень"
        else:
            await update.message.reply_text("❌ Укажите уровень: l1 или l2", parse_mode="HTML")
            return

        target_info = f"ID: <code>{target_id}</code>"
        try:
            target_chat = await context.bot.get_chat(target_id)
            if target_chat.username:
                target_info = f"@{target_chat.username} / <code>{target_id}</code>"
            elif target_chat.first_name:
                target_info = f"{target_chat.first_name} / <code>{target_id}</code>"
        except Exception:
            pass

        if add_moderator(target_id, level, user.id):
            all_admins = get_all_moderators()
            sender = user.first_name
            msg = f"🔶 <b>{sender}</b> назначил модератора\n\n👤 {target_info}\n📊 Уровень: {level_name}"

            for aid in all_admins:
                asyncio.create_task(_safe_send_message(context.bot, aid, msg, parse_mode="HTML"))

            await update.message.reply_text(f"✅ Добавлен модератор L{level}: <code>{target_id}</code>", parse_mode="HTML")
        else:
            await update.message.reply_text("❌ Нельзя добавить главного админа как модератора", parse_mode="HTML")

    elif action in ["remove", "del", "rem", "-"] and len(context.args) >= 2:
        try:
            target_id = int(context.args[1])
        except ValueError:
            await update.message.reply_text("❌ Неверный user_id", parse_mode="HTML")
            return

        target_info = f"ID: <code>{target_id}</code>"
        try:
            target_chat = await context.bot.get_chat(target_id)
            if target_chat.username:
                target_info = f"@{target_chat.username} / <code>{target_id}</code>"
            elif target_chat.first_name:
                target_info = f"{target_chat.first_name} / <code>{target_id}</code>"
        except Exception:
            pass

        if remove_moderator(target_id):
            sender = user.first_name
            msg = f"🔴 <b>{sender}</b> снял модератора\n\n👤 {target_info}"
            all_admins_after = get_all_moderators()
            for aid in all_admins_after:
                asyncio.create_task(_safe_send_message(context.bot, aid, msg, parse_mode="HTML"))

            await update.message.reply_text(f"✅ Удалён модератор: <code>{target_id}</code>", parse_mode="HTML")
        else:
            await update.message.reply_text("❌ Модератор не найден", parse_mode="HTML")
    else:
        await update.message.reply_text(
            "⚙️ <b>Управление модераторами:</b>\n\n"
            "• <code>/admin add l1 &lt;ID&gt;</code> — добавить L1\n"
            "• <code>/admin add l2 &lt;ID&gt;</code> — добавить L2\n"
            "• <code>/admin del &lt;ID&gt;</code> — снять с должности\n"
            "• <code>/admin list</code> или <code>/admins</code> — список",
            parse_mode="HTML"
        )

async def mod_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /mod_list"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    mods = get_moderators()
    if not mods:
        await update.message.reply_text("📋 Нет назначенных модераторов в базе", parse_mode="HTML")
    else:
        msg = "👥 <b>Модераторы:</b>\n\n"
        for row in mods:
            level_name = "L2 (полный доступ)" if row["level"] == 2 else "L1 (одобрение/отклонение)"
            msg += f"• ID: <code>{row['user_id']}</code> — {level_name}\n"
        await update.message.reply_text(msg, parse_mode="HTML")


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /stats — общая анонимная аналитика предложки"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    if context.args:
        await update.message.reply_text(
            "🔒 <b>Сбор досье и данных пользователей отключён!</b>\n\n"
            "В боте активирован режим полной анонимности: профили, юзернеймы и активность авторов не сохраняются в базе данных.",
            parse_mode="HTML"
        )
        return

    with get_db_connection() as db:
        def safe_count(table: str, where: str = "") -> int:
            try:
                q = f"SELECT COUNT(*) FROM {table}"
                if where:
                    q += f" WHERE {where}"
                return db.execute(q).fetchone()[0]
            except Exception:
                return 0

        total_msgs = safe_count("messages")
        approved_msgs = safe_count("messages", "status = 'approved'")
        rejected_msgs = safe_count("messages", "status = 'rejected'")
        pending_msgs = safe_count("messages", "status = 'pending'")
        today_k = get_now_kyiv().strftime("%Y-%m-%d")
        today_msgs = safe_count("messages", f"date(created_at) = '{today_k}'")

        banned_users = safe_count("blacklist")
        total_subscribers = safe_count("users")
        total_reports = safe_count("reports")

    approval_pct = f"{(approved_msgs / total_msgs * 100):.1f}%" if total_msgs > 0 else "0%"

    msg = (
        f"📈 <b>Анонимная статистика предложки:</b>\n\n"
        f"📨 <b>Посты:</b>\n"
        f"  • Всего получено: <b>{total_msgs}</b>\n"
        f"  • Одобрено в канал: <b>{approved_msgs}</b> ({approval_pct})\n"
        f"  • Отклонено: <b>{rejected_msgs}</b>\n"
        f"  • Ожидают решения: <b>{pending_msgs}</b>\n"
        f"  • Поступило сегодня: <b>{today_msgs}</b>\n\n"
        f"🛡 <b>Модерация и безопасность:</b>\n"
        f"  • В чёрном списке: <b>{banned_users}</b>\n"
        f"  • Запрещённых стоп-слов: <b>{len(FORBIDDEN_WORDS)}</b>\n"
        f"  • Жалоб / запросов на удаление: <b>{total_reports}</b>\n"
        f"  • Адресатов рассылки (/say): <b>{total_subscribers}</b>\n\n"
        f"🔒 <i>Режим приватности: сбор профилей авторов полностью отключён.</i>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def sync_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /sync — синхронизация файлов (.env, blacklist.txt, words.txt) и базы данных"""
    user = update.effective_user
    if user.id not in CONFIG["ADMIN_CHAT_IDS"] and user.id not in CONFIG["MOD_LEVEL2"]:
        await update.message.reply_text("❌ Доступ запрещён", parse_mode="HTML")
        return

    sync_all_from_files()
    with get_db_connection() as db:
        bl_cnt = db.execute("SELECT COUNT(*) FROM blacklist").fetchone()[0]
        words_cnt = db.execute("SELECT COUNT(*) FROM forbidden_words").fetchone()[0]

    msg = (
        "🔄 <b>Синхронизация успешно выполнена!</b>\n\n"
        f"📁 <b>Файлы и база синхронизированы:</b>\n"
        f"• 👑 Админы: <code>{len(CONFIG['ADMIN_CHAT_IDS'])}</code>\n"
        f"• 🔶 Модераторы L2: <code>{len(CONFIG['MOD_LEVEL2'])}</code>\n"
        f"• 🔵 Модераторы L1: <code>{len(CONFIG['MOD_LEVEL1'])}</code>\n"
        f"• 🚫 Чёрный список (blacklist.txt): <code>{bl_cnt}</code> чел.\n"
        f"• 📝 Запрещённые слова (words.txt): <code>{words_cnt}</code> слов\n\n"
        "💡 <i>Любые изменения в боте сразу пишутся в файлы, а изменения из файлов подтягиваются этой командой или при старте бота.</i>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


def main() -> None:
    """Запуск бота с полной инициализацией"""
    setup_global_logging()
    logger = get_logger()

    try:
        if not CONFIG["BOT_TOKEN"]:
            raise ValueError("❌ Ошибка: переменная BOT_TOKEN не задана в файле .env!")
        init_db()
        load_forbidden_words()
        load_moderators()
        sync_all_from_files()

        # Настройка таймаутов сети
        request_kwargs = {
            "connect_timeout": 30.0,
            "read_timeout": 30.0,
            "write_timeout": 30.0,
            "pool_timeout": 30.0,
        }
        if CONFIG["PROXY_URL"]:
            logger.info(f"Используется прокси: {CONFIG['PROXY_URL']}")
            request_kwargs["proxy"] = CONFIG["PROXY_URL"]

        req = HTTPXRequest(**request_kwargs)
        builder = (
            Application.builder()
            .token(CONFIG["BOT_TOKEN"])
            .request(req)
            .get_updates_request(req)
        )

        app = builder.build()

        # Команды бота (с короткими алиасами и группировкой)
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler(["id", "me"], id_cmd))
        app.add_handler(CommandHandler(["checkban", "check"], checkban))
        app.add_handler(CommandHandler(["cancel", "c"], cancel_cmd))

        # Админы и модераторы
        app.add_handler(CommandHandler(["admin_list", "admins"], admin_list_cmd))
        app.add_handler(CommandHandler("admin", admin_cmd))
        app.add_handler(CommandHandler(["mod_list", "mods"], mod_list_cmd))

        # Блокировки (бан с уведомлением и тихий бан)
        app.add_handler(CommandHandler(["ban", "b"], ban_cmd))
        app.add_handler(CommandHandler(["banmute", "bm"], banmute_cmd))
        app.add_handler(CommandHandler(["unban", "u"], unban_cmd))
        app.add_handler(CommandHandler(["blacklist", "bl"], blacklist_cmd))

        # Стоп-слова
        app.add_handler(CommandHandler(["words", "word"], words_cmd))
        app.add_handler(CommandHandler(["addword", "wordadd"], addword_cmd))
        app.add_handler(CommandHandler(["delword", "worddel"], delword_cmd))

        # Статистика и рассылка
        app.add_handler(CommandHandler(["stats", "stat", "user", "users"], stats_cmd))
        app.add_handler(CommandHandler(["sync", "reload"], sync_cmd))
        app.add_handler(CommandHandler("say", broadcast_cmd))
        app.add_handler(CommandHandler("allsay", allsay_cmd))

        # Обработчики сообщений и кнопок
        app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
        app.add_handler(CallbackQueryHandler(button_handler))

        safe_print("=" * 55)
        safe_print("🤖 Анонимный бот предложки успешно инициализирован!")
        safe_print(f"📬 Канал: {CONFIG['CHANNEL_ID']}")
        safe_print(f"👑 Главных админов: {len(CONFIG['ADMIN_CHAT_IDS'])}")
        safe_print(f"🔶 Модераторов L2: {len(CONFIG['MOD_LEVEL2'])}")
        safe_print(f"🔵 Модераторов L1: {len(CONFIG['MOD_LEVEL1'])}")
        safe_print(f"🚫 Запрещённых слов: {len(FORBIDDEN_WORDS)}")

        with get_db_connection() as db:
            uc = db.execute("SELECT COUNT(DISTINCT user_id) FROM users").fetchone()[0] or 0
        safe_print(f"📢 Зарегистрированных пользователей: {uc}")
        safe_print("=" * 55)
        safe_print("Бот запускает polling Telegram API...")

        app.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.critical(f"Критическая ошибка запуска бота: {e}", exc_info=True)
        safe_print(f"❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    main()
