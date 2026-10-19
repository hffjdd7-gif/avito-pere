import asyncio
import os
import re
import time
import argparse
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse

import aiohttp
import aiosqlite
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

from aiogram.client.session.aiohttp import AiohttpSession

try:
    from aiohttp_socks import ProxyConnector  # для socks5://...
except Exception:
    ProxyConnector = None


# =========================
# RUNTIME CONFIG (из env/args)
# =========================
BOT_TOKEN: str = ""
TELEGRAM_PROXY_URL: Optional[str] = None
AVITO_PROXY_URL: Optional[str] = None

DB_PATH = "avito_bot.sqlite"
DEFAULT_INTERVAL_SEC = 60
MAX_ITEMS_PER_CHECK = 60  # сколько карточек на странице проверять

AVITO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}


# =========================
# DB
# =========================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                interval_sec INTEGER NOT NULL DEFAULT 60,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_ts INTEGER NOT NULL,
                last_checked_ts INTEGER
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seen (
                search_id INTEGER NOT NULL,
                item_id TEXT NOT NULL,
                first_seen_ts INTEGER NOT NULL,
                PRIMARY KEY (search_id, item_id)
            );
        """)
        await db.commit()


@dataclass
class Search:
    id: int
    chat_id: int
    url: str
    interval_sec: int
    enabled: int
    created_ts: int
    last_checked_ts: Optional[int]


async def db_fetch_searches_enabled() -> list[Search]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, chat_id, url, interval_sec, enabled, created_ts, last_checked_ts "
            "FROM searches WHERE enabled=1"
        )
        rows = await cur.fetchall()
    return [Search(*r) for r in rows]


async def db_add_search(chat_id: int, url: str, interval_sec: int) -> int:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO searches(chat_id, url, interval_sec, enabled, created_ts, last_checked_ts) "
            "VALUES(?,?,?,?,?,NULL)",
            (chat_id, url, interval_sec, 1, now),
        )
        await db.commit()
        return cur.lastrowid


async def db_list_searches(chat_id: int) -> list[Search]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, chat_id, url, interval_sec, enabled, created_ts, last_checked_ts "
            "FROM searches WHERE chat_id=? ORDER BY id DESC",
            (chat_id,),
        )
        rows = await cur.fetchall()
    return [Search(*r) for r in rows]


async def db_get_search(chat_id: int, search_id: int) -> Optional[Search]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, chat_id, url, interval_sec, enabled, created_ts, last_checked_ts "
            "FROM searches WHERE id=? AND chat_id=?",
            (search_id, chat_id),
        )
        row = await cur.fetchone()
    return Search(*row) if row else None


async def db_remove_search(chat_id: int, search_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "UPDATE searches SET enabled=0 WHERE id=? AND chat_id=?",
            (search_id, chat_id),
        )
        await db.commit()
        return cur.rowcount


async def db_set_interval(chat_id: int, search_id: int, interval_sec: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "UPDATE searches SET interval_sec=? WHERE id=? AND chat_id=?",
            (interval_sec, search_id, chat_id),
        )
        await db.commit()
        return cur.rowcount


async def db_update_last_checked(search_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE searches SET last_checked_ts=? WHERE id=?",
            (int(time.time()), search_id),
        )
        await db.commit()


async def db_mark_seen(search_id: int, item_id: str) -> bool:
    """
    True если это новое объявление (вставили), False если уже было.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT OR IGNORE INTO seen(search_id, item_id, first_seen_ts) VALUES(?,?,?)",
            (search_id, item_id, int(time.time())),
        )
        await db.commit()
        return cur.rowcount == 1


# =========================
# Avito parsing
# =========================
def normalize_item_url(href: str) -> str:
    full = href if href.startswith("http") else urljoin("https://www.avito.ru", href)
    u = urlparse(full)
    u = u._replace(query="", fragment="")
    return urlunparse(u)


def extract_item_id(item_url: str) -> Optional[str]:
    path = urlparse(item_url).path
    m = re.search(r"_(\d+)$", path)
    if m:
        return m.group(1)
    return None


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def parse_avito_search(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select('[data-marker="item"]')
    results = []

    for card in cards[:MAX_ITEMS_PER_CHECK]:
        a = (
            card.select_one('a[data-marker="item-title"]')
            or card.select_one('a[itemprop="url"]')
            or card.find("a", href=True)
        )
        if not a or not a.get("href"):
            continue

        url = normalize_item_url(a["href"])
        title = clean_text(a.get_text(" ", strip=True)) or "Без названия"

        price_el = (
            card.select_one('[data-marker="item-price"]')
            or card.select_one('[itemprop="price"]')
            or card.select_one('[data-marker="price"]')
        )
        price = clean_text(price_el.get_text(" ", strip=True)) if price_el else "Цена не указана"

        item_id = extract_item_id(url) or url  # fallback на url
        results.append({"id": item_id, "title": title, "price": price, "url": url})

    return results


def build_connector(proxy_url: Optional[str]):
    if not proxy_url:
        return None
    if proxy_url.startswith("socks"):
        if ProxyConnector is None:
            raise RuntimeError("Нужен пакет aiohttp-socks для SOCKS5 прокси: pip install aiohttp-socks")
        return ProxyConnector.from_url(proxy_url)
    return None  # для http(s) прокси connector не нужен


async def fetch_html(url: str) -> str:
    timeout = aiohttp.ClientTimeout(total=25)
    connector = build_connector(AVITO_PROXY_URL)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=AVITO_HEADERS) as session:
        req_kwargs = {}
        if AVITO_PROXY_URL and AVITO_PROXY_URL.startswith(("http://", "https://")):
            req_kwargs["proxy"] = AVITO_PROXY_URL

        async with session.get(url, **req_kwargs) as resp:
            text = await resp.text(errors="ignore")
            if resp.status >= 400:
                raise RuntimeError(f"Avito HTTP {resp.status}. Возможно блок/капча.")
            return text


# =========================
# Bot logic
# =========================
router = Router()
IN_PROGRESS: set[int] = set()


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Команды:\n"
        "/add <url> [interval_sec] — добавить ссылку Avito в ЭТОТ чат\n"
        "/list — список ваших ссылок (в этом чате)\n"
        "/remove <id> — отключить ссылку (в этом чате)\n"
        "/interval <id> <sec> — установить интервал\n"
        "/check <id> — разово проверить\n"
        "/whoami — узнать id/chat\n"
    )


@router.message(Command("whoami"))
async def cmd_whoami(message: Message):
    if not message.from_user:
        return
    await message.answer(f"Ваш user id: {message.from_user.id}\nchat id: {message.chat.id}")


@router.message(Command("add"))
async def cmd_add(message: Message):
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: /add <url> [interval_sec]")
        return

    url = parts[1].strip()
    interval = DEFAULT_INTERVAL_SEC
    if len(parts) >= 3:
        try:
            interval = int(parts[2].strip())
        except ValueError:
            await message.answer("interval_sec должен быть числом (например 60).")
            return

    if interval < 10:
        await message.answer("Слишком часто. Минимум 10 секунд.")
        return

    chat_id = message.chat.id
    search_id = await db_add_search(chat_id, url, interval)

    # Первичная инициализация: сохраняем текущие объявления как seen, но НЕ уведомляем
    try:
        html = await fetch_html(url)
        items = parse_avito_search(html)
        for it in items:
            await db_mark_seen(search_id, it["id"])
        await db_update_last_checked(search_id)
        await message.answer(
            f"Добавлено (id={search_id}) в этот чат.\n"
            f"Инициализировано {len(items)} объявлений (без уведомлений).\n"
            f"Интервал: {interval} сек."
        )
    except Exception as e:
        await message.answer(f"Добавлено (id={search_id}), но первичная загрузка не удалась: {e}")


@router.message(Command("list"))
async def cmd_list(message: Message):
    searches = await db_list_searches(message.chat.id)
    if not searches:
        await message.answer("Список пуст.")
        return

    lines = []
    for s in searches:
        st = "ON" if s.enabled else "OFF"
        lines.append(f"#{s.id} [{st}] interval={s.interval_sec}s\n{s.url}")
    await message.answer("\n\n".join(lines))


@router.message(Command("remove"))
async def cmd_remove(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /remove <id>")
        return
    try:
        sid = int(parts[1])
    except ValueError:
        await message.answer("id должен быть числом.")
        return

    changed = await db_remove_search(message.chat.id, sid)
    if changed:
        await message.answer(f"Отключено: #{sid} (в этом чате)")
    else:
        await message.answer("Не найдено в этом чате (или уже отключено).")


@router.message(Command("interval"))
async def cmd_interval(message: Message):
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /interval <id> <sec>")
        return
    try:
        sid = int(parts[1])
        sec = int(parts[2])
    except ValueError:
        await message.answer("id и sec должны быть числами.")
        return
    if sec < 10:
        await message.answer("Слишком часто. Минимум 10 секунд.")
        return

    changed = await db_set_interval(message.chat.id, sid, sec)
    if changed:
        await message.answer(f"Интервал для #{sid} установлен: {sec} сек.")
    else:
        await message.answer("Не найдено в этом чате.")


async def notify(bot: Bot, chat_id: int, text: str):
    await bot.send_message(chat_id, text, disable_web_page_preview=False)


async def check_search(bot: Bot, s: Search, force_errors_to_chat: bool = False):
    if s.id in IN_PROGRESS:
        return
    IN_PROGRESS.add(s.id)
    try:
        html = await fetch_html(s.url)
        items = parse_avito_search(html)

        new_items = []
        for it in items:
            is_new = await db_mark_seen(s.id, it["id"])
            if is_new:
                new_items.append(it)

        await db_update_last_checked(s.id)

        if new_items:
            for it in reversed(new_items):
                msg = f"{it['title']}\n{it['price']}\n{it['url']}"
                await notify(bot, s.chat_id, msg)

    except Exception as e:
        if force_errors_to_chat:
            await notify(bot, s.chat_id, f"Ошибка проверки #{s.id}: {e}")
    finally:
        IN_PROGRESS.discard(s.id)


@router.message(Command("check"))
async def cmd_check(message: Message, bot: Bot):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /check <id>")
        return
    try:
        sid = int(parts[1])
    except ValueError:
        await message.answer("id должен быть числом.")
        return

    s = await db_get_search(message.chat.id, sid)
    if not s or not s.enabled:
        await message.answer("Не найдено или выключено (в этом чате).")
        return

    await message.answer(f"Проверяю #{sid} ...")
    await check_search(bot, s, force_errors_to_chat=True)
    await message.answer("Готово.")


async def scheduler(bot: Bot):
    while True:
        try:
            searches = await db_fetch_searches_enabled()
            now = int(time.time())
            for s in searches:
                if not s.enabled:
                    continue
                if s.last_checked_ts is None:
                    due = True
                else:
                    due = (now - int(s.last_checked_ts)) >= int(s.interval_sec)

                if due:
                    asyncio.create_task(check_search(bot, s))
        except Exception:
            pass

        await asyncio.sleep(5)


def build_telegram_session() -> AiohttpSession:
    connector = build_connector(TELEGRAM_PROXY_URL)
    if TELEGRAM_PROXY_URL and TELEGRAM_PROXY_URL.startswith(("http://", "https://")):
        return AiohttpSession(proxy=TELEGRAM_PROXY_URL)
    if connector is not None:
        return AiohttpSession(connector=connector)
    return AiohttpSession()


def _none_if_empty(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    return s if s else None


def load_runtime_config():
    """
    Источники (приоритет):
      1) аргументы командной строки
      2) переменные окружения
    """
    global BOT_TOKEN, TELEGRAM_PROXY_URL, AVITO_PROXY_URL, DB_PATH

    parser = argparse.ArgumentParser(description="Avito Telegram watcher bot")
    parser.add_argument("--token", default=os.getenv("BOT_TOKEN"), help="Telegram bot token (or env BOT_TOKEN)")
    parser.add_argument("--tg-proxy", default=os.getenv("TELEGRAM_PROXY_URL"),
                        help="Proxy for Telegram API. e.g. http://host:port or socks5://host:port")
    parser.add_argument("--avito-proxy", default=os.getenv("AVITO_PROXY_URL"),
                        help="Proxy for Avito requests. e.g. http://host:port or socks5://host:port")
    parser.add_argument("--db", default=os.getenv("DB_PATH", DB_PATH), help="Path to sqlite db (or env DB_PATH)")

    args = parser.parse_args()

    BOT_TOKEN = (args.token or "").strip()
    TELEGRAM_PROXY_URL = _none_if_empty(args.tg_proxy)
    AVITO_PROXY_URL = _none_if_empty(args.avito_proxy)
    DB_PATH = (args.db or DB_PATH).strip()

    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN. Укажи: export BOT_TOKEN='...' или запусти: python bot.py --token '...'")


async def main():
    load_runtime_config()
    await init_db()

    tg_session = build_telegram_session()
    bot = Bot(token=BOT_TOKEN, session=tg_session)
    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(scheduler(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())