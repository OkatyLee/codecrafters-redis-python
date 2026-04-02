

import asyncio
import time

from app.metrics import observe_bgsave_duration, set_background_tasks
from app.parser import RESPError
from app.state import AppState


def load_from_disk(app_state: AppState) -> bool:
    dir_path, filename = app_state.config.dir, app_state.config.dbfilename
    if not dir_path or not filename:
        raise ValueError("ERR Invalid directory or filename of database file")
    return app_state.storage.load(dir_path, filename)


def load_rdb_snapshot(app_state: AppState, rdb_bytes: bytes) -> bool:
    if rdb_bytes == b"":
        return True

    if rdb_bytes.startswith(b"REDIS"):
        return app_state.storage.load(
            app_state.config.dir,
            app_state.config.dbfilename,
            rdb_bytes
        )
    else:
        raise ValueError("ERR Invalid RDB snapshot")


def save_to_disk(app_state: AppState) -> bool:
    dir_path, filename = app_state.config.dir, app_state.config.dbfilename
    if not dir_path or not filename:
        raise ValueError("ERR Invalid directory or filename of database file")
    return app_state.storage.save(dir_path, filename)


def _run_bgsave(app_state: AppState) -> bool:
    started_at = time.perf_counter()
    try:
        return save_to_disk(app_state)
    finally:
        observe_bgsave_duration(time.perf_counter() - started_at)


def schedule_bgsave(app_state: AppState):
    if app_state.bgsave_task is not None and not app_state.bgsave_task.done():
        raise RESPError("ERR Background save already in progress")
    loop = asyncio.get_event_loop()
    set_background_tasks(1)
    app_state.bgsave_task = loop.run_in_executor(None, _run_bgsave, app_state)
    app_state.bgsave_task.add_done_callback(lambda _: set_background_tasks(0))
