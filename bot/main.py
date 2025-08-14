import os
import asyncio
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types
from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "webhook")
WEBAPP_PUBLIC = os.getenv("WEBAPP_PUBLIC", "https://foodyweb-production.up.railway.app")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🛍 Витрина", url=f"{WEBAPP_PUBLIC}/web/buyer/"),
        InlineKeyboardButton(text="🏪 Ресторан (ЛК)", url=f"{WEBAPP_PUBLIC}/web/merchant/")
    ],[
        InlineKeyboardButton(text="📋 Регистрация ресторана", url=f"{WEBAPP_PUBLIC}/web/merchant/register/")
    ]])
    await message.answer(
        "Привет! Это Foody.\n\n"
        "• Витрина — посмотреть предложения рядом.\n"
        "• Личный кабинет — управлять офферами.\n"
        "• Регистрация — создать ресторан и получить ключи.",
        reply_markup=kb
    )

@app.post(f"/{WEBHOOK_SECRET}")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/health")
async def health():
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
