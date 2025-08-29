import sys
import logging
import logging.handlers
import queue
from datetime import datetime
from typing import Optional, Dict, Tuple, List

from uvicorn.logging import DefaultFormatter as UvicornDefaultFormatter
from uvicorn.logging import AccessFormatter as UvicornAccessFormatter


# ---------- Microsecond-capable formatters ----------
class MicrosecondDefaultFormatter(UvicornDefaultFormatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="microseconds")


class MicrosecondAccessFormatter(UvicornAccessFormatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="microseconds")


# ---------- Queue infrastructure ----------
_q: "queue.SimpleQueue[logging.LogRecord]" = queue.SimpleQueue()
_listener: Optional[logging.handlers.QueueListener] = None
_snapshot: Dict[str, Tuple[List[logging.Handler], int, bool]] = {}


# Keep args for AccessFormatter; don't pre-format.
class PassthroughQueueHandler(logging.handlers.QueueHandler):
    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record  # no mutation; args stay intact for listener-side formatter


class OnlyLogger(logging.Filter):
    def __init__(self, name: str, allow_children: bool = False):
        super().__init__()
        self.name = name
        self.allow_children = allow_children

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name == self.name:
            return True
        return self.allow_children and record.name.startswith(self.name + ".")


def install_queue_logging(microseconds: bool = True) -> None:
    """Start a QueueListener and route root/uvicorn/aiosqlite through it."""
    global _listener
    if _listener is not None:
        return

    access_fmt: logging.Formatter
    default_fmt: logging.Formatter

    # Listener sinks (do formatting + I/O off the hot path)
    if microseconds:
        access_fmt = MicrosecondAccessFormatter(
            fmt='%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            datefmt="%Y-%m-%d %H:%M:%S.%f",
        )
        default_fmt = MicrosecondDefaultFormatter(
            fmt="%(asctime)s %(levelprefix)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S.%f",
        )
    else:
        access_fmt = UvicornAccessFormatter(
            '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'
        )
        default_fmt = UvicornDefaultFormatter("%(levelprefix)s %(message)s")

    h_stdout = logging.StreamHandler(sys.stdout)
    h_stdout.setFormatter(access_fmt)
    h_stdout.addFilter(OnlyLogger("uvicorn.access"))
    h_stdout.setLevel(logging.NOTSET)  # let logger levels govern

    class NotAccess(logging.Filter):
        def filter(self, r: logging.LogRecord) -> bool:
            return r.name != "uvicorn.access"

    h_stderr = logging.StreamHandler(sys.stderr)
    h_stderr.setFormatter(default_fmt)
    h_stderr.addFilter(NotAccess())
    h_stderr.setLevel(logging.NOTSET)

    _listener = logging.handlers.QueueListener(
        _q, h_stdout, h_stderr, respect_handler_level=True
    )
    _listener.start()

    qh = PassthroughQueueHandler(_q)

    targets = ["", "uvicorn.error", "uvicorn.access", "aiosqlite"]  # "" = root
    for name in targets:
        lg = logging.getLogger(name)
        _snapshot[name] = (lg.handlers[:], lg.level, lg.propagate)
        lg.handlers[:] = [qh]
        # Keep propagate semantics for root; turn off for named loggers
        if name:
            lg.propagate = False


def shutdown_queue_logging() -> None:
    """Stop listener and restore all prior logger configurations."""
    global _listener
    if _listener is not None:
        _listener.stop()
        _listener = None

    for name, (handlers, level, propagate) in _snapshot.items():
        lg = logging.getLogger(name)
        lg.handlers[:] = handlers
        lg.setLevel(level)
        lg.propagate = propagate
    _snapshot.clear()
