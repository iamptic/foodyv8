# BOT (aiogram 3 + FastAPI + webhook)

## Railway settings
- Root directory: `BOT`
- Start command: `python main.py`
- Env vars:
  - `BOT_TOKEN` = <твой токен>
  - `WEBHOOK_SECRET` = foodySecret123
  - `WEBAPP_PUBLIC` = https://foodyweb-production.up.railway.app
  - (опц.) `BOT_WEBHOOK_URL` = https://foodybot-production.up.railway.app  # авто-установка вебхука на старте

## Установка webhook вручную (если не используешь BOT_WEBHOOK_URL)
https://api.telegram.org/bot<BOT_TOKEN>/setWebhook?url=https://foodybot-production.up.railway.app/foodySecret123
