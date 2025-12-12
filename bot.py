import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command
import psycopg2
from groq import Groq

TOKEN = "7844297622:AAE_nDBE8cIriWz8g9tiN4jgCBYHK1OJ6Vw"

AI_TOKEN = "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"

client = Groq(api_key=AI_TOKEN)

conn = psycopg2.connect(
    dbname="rlt_test",
    user="postgres",
    password="jalol",
    host="localhost",
    port=5432
)
cur = conn.cursor()

bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer("Задайте вопрос!")

@dp.message()
async def query_handler(message: Message):
    prompt = f"""
Дан граф схемы БД. Таблицы:

1) videos:
  - id (text)
  - creator_id (text)
  - video_created_at (timestamp)
  - views_count (bigint)
  - likes_count (bigint)
  - comments_count (bigint)
  - reports_count (bigint)

2) video_snapshots:
  - id (text)
  - video_id (text) -- foreign key -> videos.id
  - views_count, likes_count, comments_count, reports_count (bigint)
  - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (bigint)
  - created_at (timestamp) -- часовые снапшоты

Правила:
- Возвращай только один SQL запрос (без дополнительных объяснений).
- SQL должен быть только SELECT и возвращать ровно один столбец с именем "result".
- Используй только таблицы videos или video_snapshots, без JOIN'ов или подзапросов, если это возможно.
- Для подсчётов количества видео используй COUNT(DISTINCT videos.id).
- Для сумм/приращений используй SUM(...).
- Даты в тексте могут быть полными: "28 ноября 2025", "с 1 по 5 ноября 2025" и т.п. Нормализуй даты в формате 'YYYY-MM-DD'.
- Если запрос подразумевает "за всё время" — просто не накладывай фильтр по дате.
- Если нужен фильтр по creator_id — используй WHERE creator_id = '...'.
- Если выражение просит "сколько видео набрало больше X просмотров" — фильтр по videos.views_count > X.
- Если просит "на сколько просмотров в сумме выросли все видео 28 ноября 2025" — суммируй delta_views_count из video_snapshots для временного интервала с 2025-11-28 00:00:00 по 2025-11-28 23:59:59.

Примеры:
Input: "Сколько всего видео есть в системе?"
SQL: SELECT COUNT(DISTINCT id) AS result FROM videos;

Input: "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
SQL: SELECT COUNT(DISTINCT id) AS result FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63' AND video_created_at >= '2025-11-01' AND video_created_at < '2025-11-06';

Input: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
SQL: SELECT COALESCE(SUM(delta_views_count),0) AS result FROM video_snapshots WHERE created_at >= '2025-11-28' AND created_at < '2025-11-29';

Теперь сгенерируй SQL для следующего запроса: 
<<<{message}>>>    """

    resp = client.chat.completions.create(
        model="openai/gpt-oss-120b",
        messages=[{"role": "user", "content": prompt}],
    )

    sql = resp.choices[0].message.content.strip()
    print(sql)
    cur.execute(sql)
    rows = cur.fetchone()
    await message.answer(str(rows[0]))

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
