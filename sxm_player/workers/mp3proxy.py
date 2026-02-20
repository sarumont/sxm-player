import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from aiohttp import web
from sxm import QualitySize
from sxm.models import XMLiveChannel, XMSong

from ..queue import EventMessage, EventTypes, Queue
from ..signals import TerminateInterrupt
from .base import HLSStatusSubscriber, InterruptableWorker

__all__ = ["MP3ProxyWorker"]

QUALITY_BITRATE_MAP = {
    QualitySize.SMALL_64k: "64k",
    QualitySize.MEDIUM_128k: "128k",
    QualitySize.LARGE_256k: "256k",
}

ICY_INTERVAL = 16000  # bytes between metadata blocks


@dataclass
class NowPlaying:
    title: str = ""
    artist: str = ""
    channel_name: str = ""
    updated_at: float = field(default_factory=time.monotonic)


class MP3ProxyWorker(InterruptableWorker, HLSStatusSubscriber):
    """HTTP server that transcodes SXM HLS streams to continuous MP3
    streams on the fly via ffmpeg. Each connected client gets its own
    ffmpeg process that reads HLS from the existing ServerWorker proxy
    and pipes MP3 to the HTTP response."""

    NAME = "mp3proxy"

    _ip: str
    _port: int
    _sxm_ip: str
    _sxm_port: int
    _sxm_quality: QualitySize
    _active_processes: Dict[int, asyncio.subprocess.Process]
    _now_playing: Dict[str, NowPlaying]

    def __init__(
        self,
        port: int,
        ip: str,
        sxm_port: int,
        sxm_ip: str,
        sxm_quality: QualitySize = QualitySize.LARGE_256k,
        *args,
        **kwargs,
    ):
        hls_stream_queue = kwargs.pop("hls_stream_queue")
        HLSStatusSubscriber.__init__(self, hls_stream_queue)
        super().__init__(*args, **kwargs)

        self._port = port
        self._ip = ip
        self._sxm_port = sxm_port
        self._sxm_quality = sxm_quality

        # Use loopback if binding to all interfaces
        if sxm_ip == "0.0.0.0":  # nosec
            self._sxm_ip = "127.0.0.1"
        else:
            self._sxm_ip = sxm_ip

        self._active_processes = {}
        self._now_playing = {}

    def _resolve_bitrate(self, override: Optional[str] = None) -> str:
        if override:
            return override
        return QUALITY_BITRATE_MAP.get(self._sxm_quality, "128k")

    def _get_hls_url(self, channel_id: str) -> str:
        return f"http://{self._sxm_ip}:{self._sxm_port}/{channel_id}.m3u8"

    def _get_channels_url(self) -> str:
        return f"http://{self._sxm_ip}:{self._sxm_port}/channels/"

    def _build_ffmpeg_cmd(self, hls_url: str, bitrate: str) -> list:
        return [
            "ffmpeg",
            "-loglevel", "warning",
            "-re",
            "-f", "hls",
            "-i", hls_url,
            "-c:a", "libmp3lame",
            "-b:a", bitrate,
            "-f", "mp3",
            "pipe:1",
        ]

    # -- Metadata handling --

    def _handle_metadata_update(self, raw_data):
        """Process UPDATE_METADATA event.

        raw_data is a tuple (start_time, time_offset, raw_live_dict)
        from PlayerState.get_raw_live().
        """
        start_time, time_offset, raw_live = raw_data
        if raw_live is None:
            return

        try:
            live = XMLiveChannel.from_dict(raw_live)
        except Exception as e:
            self._log.warning(f"Failed to parse live channel data: {e}")
            return

        channel_id = live.id

        now = datetime.now(timezone.utc)
        if time_offset is not None:
            radio_time = now - time_offset
        else:
            radio_time = now

        latest_cut = live.get_latest_cut(radio_time)
        if latest_cut and isinstance(latest_cut.cut, XMSong):
            song = latest_cut.cut
            artist = ", ".join(a.name for a in song.artists) if song.artists else ""
            self._now_playing[channel_id] = NowPlaying(
                title=song.title,
                artist=artist,
                channel_name=channel_id,
                updated_at=time.monotonic(),
            )
        elif latest_cut:
            self._now_playing[channel_id] = NowPlaying(
                title=latest_cut.cut.title,
                artist="",
                channel_name=channel_id,
                updated_at=time.monotonic(),
            )

    def _handle_channels_update(self, channels_data):
        """Process UPDATE_CHANNELS event (currently informational)."""
        pass

    async def _metadata_poller(self):
        """Background task: poll hls_stream_queue for metadata updates."""
        loop = asyncio.get_event_loop()
        while True:
            try:
                event = await loop.run_in_executor(
                    None, lambda: self.hls_stream_queue.safe_get(timeout=0.5)
                )
                if event is None:
                    continue
                if event.msg_type == EventTypes.UPDATE_METADATA:
                    self._handle_metadata_update(event.msg)
                elif event.msg_type == EventTypes.UPDATE_CHANNELS:
                    self._handle_channels_update(event.msg)
            except Exception as e:
                self._log.error(f"Metadata poller error: {e}")
                await asyncio.sleep(1.0)

    # -- ICY metadata --

    def _build_icy_block(self, channel_id: str) -> bytes:
        """Build an ICY metadata block for the given channel."""
        np = self._now_playing.get(channel_id)
        if np and (time.monotonic() - np.updated_at) < 60:
            if np.artist:
                stream_title = f"{np.artist} - {np.title}"
            else:
                stream_title = np.title
        else:
            stream_title = f"SiriusXM - {channel_id}"

        meta_str = f"StreamTitle='{stream_title}';"
        meta_bytes = meta_str.encode("utf-8")

        # Length byte = ceil(len / 16), actual block padded to length * 16
        length = (len(meta_bytes) + 15) // 16
        if length > 255:
            length = 255
            meta_bytes = meta_bytes[: 255 * 16]

        return bytes([length]) + meta_bytes.ljust(length * 16, b"\x00")

    # -- HTTP handlers --

    async def _handle_mp3_stream(self, request: web.Request) -> web.StreamResponse:
        """Handle GET /{channel}.mp3 — spawn ffmpeg and stream MP3."""

        raw_path = request.match_info.get("channel", "")
        # Strip .mp3 extension if present in the path
        channel_id = raw_path.replace(".mp3", "")

        if not channel_id:
            return web.Response(status=400, text="Missing channel ID")

        bitrate_override = request.query.get("bitrate")
        resolved_bitrate = self._resolve_bitrate(bitrate_override)
        icy_requested = request.headers.get("Icy-MetaData") == "1"

        hls_url = self._get_hls_url(channel_id)
        ffmpeg_cmd = self._build_ffmpeg_cmd(hls_url, resolved_bitrate)

        self._log.info(
            f"Starting MP3 stream for channel '{channel_id}' "
            f"at {resolved_bitrate} (client: {request.remote}, "
            f"icy: {icy_requested})"
        )

        process: Optional[asyncio.subprocess.Process] = None
        try:
            process = await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            proc_id = process.pid
            if proc_id is not None:
                self._active_processes[proc_id] = process

            # Give ffmpeg a moment to fail on invalid channels
            # before committing to the stream response
            await asyncio.sleep(1.0)

            if process.returncode is not None:
                # ffmpeg already exited — likely bad channel
                stderr_output = b""
                if process.stderr:
                    stderr_output = await process.stderr.read()
                self._log.warning(
                    f"ffmpeg exited immediately for channel '{channel_id}': "
                    f"{stderr_output.decode('utf-8', errors='replace').strip()}"
                )
                return web.Response(
                    status=404,
                    text=f"Channel '{channel_id}' not found or unavailable",
                )

            response_headers = {
                "Content-Type": "audio/mpeg",
                "Cache-Control": "no-cache, no-store",
                "Connection": "keep-alive",
                "icy-name": f"SiriusXM - {channel_id}",
            }

            if icy_requested:
                response_headers["icy-metaint"] = str(ICY_INTERVAL)
                response_headers["icy-br"] = resolved_bitrate.replace("k", "")
                response_headers["icy-pub"] = "0"

            response = web.StreamResponse(status=200, headers=response_headers)
            await response.prepare(request)

            assert process.stdout is not None

            if not icy_requested:
                # Simple passthrough — no metadata injection
                while True:
                    chunk = await process.stdout.read(4096)
                    if not chunk:
                        break
                    await response.write(chunk)
            else:
                # ICY-aware streaming with metadata injection
                bytes_since_meta = 0

                while True:
                    chunk = await process.stdout.read(4096)
                    if not chunk:
                        break

                    pos = 0
                    while pos < len(chunk):
                        remaining = ICY_INTERVAL - bytes_since_meta
                        available = len(chunk) - pos

                        if available <= remaining:
                            await response.write(chunk[pos:])
                            bytes_since_meta += available
                            pos = len(chunk)
                        else:
                            # Write audio up to the interval boundary
                            await response.write(chunk[pos : pos + remaining])
                            pos += remaining
                            bytes_since_meta = 0

                            # Insert metadata block
                            meta_block = self._build_icy_block(channel_id)
                            await response.write(meta_block)

            return response

        except (ConnectionResetError, asyncio.CancelledError):
            self._log.info(
                f"Client disconnected from channel '{channel_id}'"
            )
            raise
        except Exception as e:
            self._log.error(
                f"Error streaming channel '{channel_id}': {e}"
            )
            return web.Response(status=500, text="Internal server error")
        finally:
            if process is not None:
                pid = process.pid
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
                if pid is not None:
                    self._active_processes.pop(pid, None)
                self._log.debug(
                    f"Cleaned up ffmpeg (pid={pid}) for channel '{channel_id}'"
                )

    async def _handle_channels(self, request: web.Request) -> web.Response:
        """Proxy the channel list from the SXM server as JSON."""

        import httpx

        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(self._get_channels_url(), timeout=10.0)
                if r.is_error:
                    return web.Response(status=502, text="SXM proxy unavailable")
                return web.Response(
                    status=200,
                    body=r.content,
                    headers={"Content-Type": "application/json; charset=utf-8"},
                )
        except Exception as e:
            self._log.error(f"Failed to fetch channels: {e}")
            return web.Response(status=502, text="SXM proxy unavailable")

    async def _handle_now_playing(self, request: web.Request) -> web.Response:
        """Debug endpoint: GET /now-playing/{channel}.json"""
        channel_id = request.match_info.get("channel", "")
        np = self._now_playing.get(channel_id)
        if np:
            return web.json_response({
                "channel": channel_id,
                "title": np.title,
                "artist": np.artist,
                "stale": (time.monotonic() - np.updated_at) > 60,
            })
        return web.json_response({
            "channel": channel_id,
            "title": None,
            "artist": None,
        })

    async def _handle_index(self, request: web.Request) -> web.Response:
        """Simple index page listing available endpoints."""
        return web.Response(
            status=200,
            text=(
                "sxm-player MP3 Proxy\n"
                "====================\n\n"
                "Endpoints:\n"
                "  GET /{channel_id}.mp3           - MP3 audio stream\n"
                "  GET /channels.json              - Channel list (JSON)\n"
                "  GET /now-playing/{channel}.json  - Now playing (JSON)\n\n"
                "Query params:\n"
                "  ?bitrate=128k  - Override output bitrate\n\n"
                "Example:\n"
                f"  http://{self._ip}:{self._port}/octane.mp3\n"
            ),
            headers={"Content-Type": "text/plain"},
        )

    async def _cleanup_processes(self):
        """Kill any remaining ffmpeg processes on shutdown."""
        for pid, process in list(self._active_processes.items()):
            self._log.info(f"Killing orphaned ffmpeg process (pid={pid})")
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
        self._active_processes.clear()

    def run(self) -> None:
        """Run the MP3 proxy server."""

        try:
            self._run_server()
        except (KeyboardInterrupt, TerminateInterrupt):
            pass
        except Exception:
            self._log.exception("MP3 proxy worker crashed")

    def _run_server(self) -> None:
        request_logger = logging.getLogger("sxm_player.mp3proxy.request")
        request_logger._info = request_logger.info  # type: ignore
        request_logger.info = request_logger.debug  # type: ignore

        app = web.Application()
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/channels.json", self._handle_channels)
        app.router.add_get("/now-playing/{channel}.json", self._handle_now_playing)
        # Match both "octane.mp3" and "octane" patterns
        app.router.add_get("/{channel}.mp3", self._handle_mp3_stream)
        app.router.add_get("/{channel}", self._handle_mp3_stream)

        async def on_startup(app):
            app["metadata_poller"] = asyncio.ensure_future(self._metadata_poller())

        async def on_shutdown(app):
            app["metadata_poller"].cancel()
            try:
                await app["metadata_poller"]
            except asyncio.CancelledError:
                pass
            await self._cleanup_processes()

        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)

        self._log.info(
            f"{self.name} has started on http://{self._ip}:{self._port} "
            f"(bitrate: {self._resolve_bitrate()})"
        )
        web.run_app(
            app,
            host=self._ip,
            port=self._port,
            access_log=request_logger,
            print=None,  # type: ignore
        )
