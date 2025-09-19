# core/scheduler.py
import asyncio
import logging
from typing import Awaitable, Callable, Optional

log = logging.getLogger("scheduler")

Callback = Callable[[], Awaitable[None]]


class Scheduler:
    def __init__(self, interval_sec: int = 60):
        self.interval = int(interval_sec)
        self._callback: Optional[Callback] = None
        self._task: Optional[asyncio.Task] = None
        self._enabled = False

    def set_job(self, cb: Callback):
        self._callback = cb

    def is_enabled(self) -> bool:
        return self._enabled

    def enable(self):
        if self._enabled:
            return
        self._enabled = True
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._runner())
            log.info("Scheduler runner started")

    # ⬅️ добавлено для совместимости с app.py
    async def start(self):
        """Совместимый с app.py метод: включает планировщик и сразу возвращается."""
        self.enable()

    def disable(self):
        self._enabled = False

    async def stop(self):
        """Полная остановка (для выхода из приложения)."""
        self._enabled = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                # штатная отмена фоновой задачи
                pass
            finally:
                self._task = None
                log.info("Scheduler runner stopped")

    async def _runner(self):
        try:
            while self._enabled:
                try:
                    if self._callback:
                        await self._callback()
                except Exception as e:
                    log.error("Scheduled job failed: %s", e, exc_info=True)
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            # нормальное завершение по cancel() — гасим исключение
            pass
