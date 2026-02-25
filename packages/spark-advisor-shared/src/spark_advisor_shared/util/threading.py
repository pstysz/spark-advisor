from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

logger = logging.getLogger(__name__)


@contextmanager
def background_worker(
    target: Callable[[threading.Event], None],
    *,
    name: str = "worker",
    join_timeout: float = 10.0,
) -> Generator[None]:
    stop_event = threading.Event()
    thread = threading.Thread(target=target, args=(stop_event,), daemon=True, name=name)
    thread.start()
    try:
        yield
    finally:
        stop_event.set()
        thread.join(timeout=join_timeout)
        if thread.is_alive():
            logger.warning("Worker thread '%s' did not stop within %ss", name, join_timeout)
