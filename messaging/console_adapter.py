
import asyncio, logging
from typing import Dict
from messaging.base import Messenger, CommandHandler

log = logging.getLogger("console")

class ConsoleMessenger(Messenger):
    def __init__(self):
        self.commands: Dict[str, CommandHandler] = {}

    async def start(self) -> None:
        log.info("Console started. Type /help")

    async def stop(self) -> None:
        log.info("Console stopped.")

    def add_command(self, name: str, handler: CommandHandler, help_text: str = "") -> None:
        self.commands[name] = handler

    async def send_text(self, text: str) -> None:
        print(text, flush=True)

    async def run_forever(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, input, "> ")
            line = line.strip()
            if not line: continue
            if line.split()[0] == "/quit":
                print("Bye!"); return
            cmd = line.split()[0]
            arg = line[len(cmd):].strip() if len(line) > len(cmd) else ""
            h = self.commands.get(cmd)
            if h: await h(arg)
            else: print("Unknown command. Try /help")
