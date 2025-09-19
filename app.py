
import argparse, asyncio, sys, traceback
from core.config import settings
from core.logging_setup import setup_logging
from core.scheduler import Scheduler
from handlers import register_handlers
from messaging.console_adapter import ConsoleMessenger

def _load_telegram():
    from messaging.telegram_adapter import TelegramMessenger
    return TelegramMessenger

def build_messenger(name: str):
    name = (name or "console").lower()
    if name == "telegram":
        return _load_telegram()()
    if name == "console":
        return ConsoleMessenger()
    raise ValueError(f"Unknown messenger: {name}")

async def main_async(args):
    logger = setup_logging(args.log_file, settings.LOGLEVEL)
    logger.info("Starting False Breakout Bot (modular)")
    messenger = build_messenger(args.messenger)
    scheduler = Scheduler(interval_sec=settings.RUN_EVERY_SEC)
    register_handlers(messenger, scheduler)
    try:
        await messenger.start()
        await scheduler.start()
        await messenger.run_forever()
    finally:
        await scheduler.stop()
        await messenger.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--messenger", default="console", help="telegram|console")
    parser.add_argument("--pause-on-exit", action="store_true")
    parser.add_argument("--log-file", default=None)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args))
    except SystemExit:
        raise
    except Exception as e:
        print("FATAL:", e, file=sys.stderr)
        traceback.print_exc()
        if args.pause_on_exit:
            input("Нажмите Enter для выхода...")
        sys.exit(1)
