import asyncio
from asyncio import StreamWriter, Queue, Task
from typing import TYPE_CHECKING

from app.metrics import set_active_subscriptions

if TYPE_CHECKING:
    from app.state import AppState

class SubscriberClient:
    def __init__(self, writer: StreamWriter, maxsize: int = 1000):
        self.writer = writer
        self.queue: Queue[bytes] = Queue(maxsize=maxsize)
        self.sender_task: Task | None = None
        self.subscribed_channels: set[bytes] = set()
        
    async def start_sender(self):
        self.sender_task = asyncio.create_task(self._sender_loop())
        
    async def _sender_loop(self):
        try:
            while True:
                payload = await self.queue.get()
                if payload is None:  # sentiel value
                    break
                
                batch = bytearray(payload)
                try:
                    while True:
                        next_payload = self.queue.get_nowait()
                        if next_payload is None:
                            self.writer.write(bytes(batch))
                            await self.writer.drain()
                            return
                        batch += next_payload
                except asyncio.QueueEmpty:
                    pass

                self.writer.write(bytes(batch))
                await self.writer.drain()
        except (ConnectionError, OSError, BrokenPipeError):
            pass
        
    async def stop(self):
        if self.sender_task is not None and not self.sender_task.done():
            await self.queue.put(None)
            try:
                await asyncio.wait_for(asyncio.shield(self.sender_task), timeout=5.0)
            except asyncio.TimeoutError:
                self.sender_task.cancel()
        if not self.writer.is_closing():
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError, OSError):
                pass


def refresh_active_subscription_metric(app_state: "AppState") -> None:
    set_active_subscriptions(sum(len(s) for s in app_state.pubsub.values()))


async def unsubscribe_client_everywhere(
    sub: SubscriberClient,
    app_state: "AppState",
    *,
    close_writer: bool = True,
) -> None:
    # 1. Remove from all channels
    for ch in list(sub.subscribed_channels):
        app_state.pubsub[ch].discard(sub)
        if not app_state.pubsub[ch]:
            app_state.pubsub.pop(ch, None)
    sub.subscribed_channels.clear()

    # 2. Metrics
    refresh_active_subscription_metric(app_state)

    if close_writer:
        await sub.stop()
    else:
        # Only task, writer stays open (e.g, on UNSUBSCRIBE)
        if sub.sender_task and not sub.sender_task.done():
            await sub.queue.put(None)
