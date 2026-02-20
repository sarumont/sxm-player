from sxm_player.workers.cli import CLIPlayerWorker
from sxm_player.workers.hls import HLSWorker
from sxm_player.workers.mp3proxy import MP3ProxyWorker
from sxm_player.workers.processor import ProcessorWorker
from sxm_player.workers.server import ServerWorker
from sxm_player.workers.status import StatusWorker

from .archiver import ArchiveWorker
from .base import (
    BaseWorker,
    ComboLoopedWorker,
    EventedWorker,
    HLSLoopedWorker,
    HLSStatusSubscriber,
    InterruptableWorker,
    LoopedWorker,
    SXMLoopedWorker,
    SXMStatusSubscriber,
)

# debug.py is not included in published package
try:
    from sxm_player.debug.worker import DebugWorker
except ImportError:
    DebugWorker = None  # type: ignore

__all__ = [
    "ArchiveWorker",
    "BaseWorker",
    "CLIPlayerWorker",
    "ComboLoopedWorker",
    "DebugWorker",
    "EventedWorker",
    "HLSLoopedWorker",
    "HLSStatusSubscriber",
    "HLSWorker",
    "InterruptableWorker",
    "LoopedWorker",
    "MP3ProxyWorker",
    "ProcessorWorker",
    "ServerWorker",
    "StatusWorker",
    "SXMLoopedWorker",
    "SXMStatusSubscriber",
]
