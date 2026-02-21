import os
import json
import random
import asyncio
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from telegram import Bot
from telegram.error import RetryAfter

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDS = json.loads(os.getenv("GOOGLE_CREDS"))

MAX_OPTION_LENGTH = 100


def clean_option(text):
    text = str(text)
    return text[:97] + "..." if len(text) > MAX_OPTION_LENGTH else text


def get_entity_word(sheet_title):
    title_lower = sheet_title.lower()

    if "article" in title_lower:
        return "Article"
    if "judgement" in title_lower or "judgment" in title_lower:
        return "Judgement"
    if "committee" in title_lower:
        return "Committee"

    return "Title"


def get_all_worksheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(GOOGLE_CREDS, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID)
    return sheet.worksheets()


def safe_sample(series, n):
    values = list(series.dropna().unique())
    if len(values) <= n:
        return values
    return random.sample(values, n)


def generate_questions(df, entity_word):
    questions = []

    if df.shape[1] < 2:
        return []

    col1 = df.columns[0]  # Name column
    col2 = df.columns[1]  # Description column

    df = df.sample(frac=1).reset_index(drop=True)

    if df[col1].nunique() < 4:
        return []

    # First 5 → Entity → Choose correct description
    for _, row in df.head(5).iterrows():
        correct = row[col2]
        wrong = safe_sample(df[df[col1] != row[col1]][col2], 3)
        options = random.sample([correct] + wrong, len(wrong) + 1)

        questions.append({
            "question": f"{entity_word} '{row[col1]}' relates to which summary?",
            "options": [clean_option(o) for o in options],
            "answer": options.index(clean_option(correct))
        })

    # Next 5 → Description → Choose correct entity
    for _, row in df.tail(5).iterrows():
        correct = row[col1]
        wrong = safe_sample(df[df[col1] != correct][col1], 3)
        options = random.sample([correct] + wrong, len(wrong) + 1)

        questions.append({
            "question": f"Which {entity_word} matches:\n\"{clean_option(row[col2])}\"?",
            "options": [clean_option(o) for o in options],
            "answer": options.index(correct)
        })

    return questions


async def send_poll_safe(bot, poll_data):
    while True:
        try:
            await bot.send_poll(
                chat_id=CHAT_ID,
                question=poll_data["question"],
                options=poll_data["options"],
                type="quiz",
                correct_option_id=poll_data["answer"],
                is_anonymous=False
            )
            await asyncio.sleep(3)
            break
        except RetryAfter as e:
            wait_time = int(e.retry_after)
            print(f"Rate limited. Sleeping {wait_time} seconds...")
            await asyncio.sleep(wait_time)


async def main():
    bot = Bot(token=BOT_TOKEN)
    worksheets = get_all_worksheets()

    for ws in worksheets:
        df = pd.DataFrame(ws.get_all_records())

        if df.empty:
            continue

        entity_word = get_entity_word(ws.title)

        questions = generate_questions(df, entity_word)

        if not questions:
            continue

        await bot.send_message(chat_id=CHAT_ID, text=f"📘 {ws.title}")
        await asyncio.sleep(3)

        for q in questions:
            await send_poll_safe(bot, q)


if __name__ == "__main__":
    asyncio.run(main())
