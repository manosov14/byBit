
import logging
from typing import Dict
from messaging.base import Messenger, CommandHandler
from core.config import settings
from telegram.ext import Application, CommandHandler as TgCH
from telegram import Update
from telegram.ext import ContextTypes

log = logging.getLogger("telegram")

class TelegramMessenger(Messenger):
    def __init__(self):
        if not settings.TELEGRAM_BOT_TOKEN:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is empty")
        self.app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
        self.commands: Dict[str, CommandHandler] = {}

    async def start(self) -> None:
        for name, handler in self.commands.items():
            async def wrap(update: Update, context: ContextTypes.DEFAULT_TYPE, _h=handler):
                text = (update.message.text or "").strip()
                parts = text.split(maxsplit=1)
                arg = parts[1] if len(parts) > 1 else ""
                await _h(arg)
            self.app.add_handler(TgCH(name.lstrip("/"), wrap))
        await self.app.initialize()
        await self.app.start()

    async def stop(self) -> None:
        await self.app.stop()

    def add_command(self, name: str, handler: CommandHandler, help_text: str = "") -> None:
        self.commands[name] = handler

    async def send_text(self, text: str) -> None:
        log.info("send_text noop: %s", text)

    async def run_forever(self) -> None:
        await self.app.updater.start_polling()
        await self.app.updater.wait()
