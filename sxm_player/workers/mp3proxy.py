import asyncio
import logging
from typing import Dict, Optional

from aiohttp import web

from ..signals import TerminateInterrupt
from .base import InterruptableWorker

__all__ = ["MP3ProxyWorker"]

DEFAULT_MP3_BITRATE = "128k"


class MP3ProxyWorker(InterruptableWorker):
    """HTTP server that transcodes SXM HLS streams to continuous MP3
    streams on the fly via ffmpeg. Each connected client gets its own
    ffmpeg process that reads HLS from the existing ServerWorker proxy
    and pipes MP3 to the HTTP response."""

    NAME = "mp3proxy"

    _ip: str
    _port: int
    _sxm_ip: str
    _sxm_port: int
    _mp3_bitrate: str
    _active_processes: Dict[int, asyncio.subprocess.Process]

    def __init__(
        self,
        port: int,
        ip: str,
        sxm_port: int,
        sxm_ip: str,
        mp3_bitrate: str = DEFAULT_MP3_BITRATE,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self._port = port
        self._ip = ip
        self._sxm_port = sxm_port
        self._mp3_bitrate = mp3_bitrate

        # Use loopback if binding to all interfaces
        if sxm_ip == "0.0.0.0":  # nosec
            self._sxm_ip = "127.0.0.1"
        else:
            self._sxm_ip = sxm_ip

        self._active_processes = {}

    def _get_hls_url(self, channel_id: str) -> str:
        return f"http://{self._sxm_ip}:{self._sxm_port}/{channel_id}.m3u8"

    def _get_channels_url(self) -> str:
        return f"http://{self._sxm_ip}:{self._sxm_port}/channels/"

    def _build_ffmpeg_cmd(self, hls_url: str) -> list:
        return [
            "ffmpeg",
            "-loglevel", "warning",
            "-re",
            "-f", "hls",
            "-i", hls_url,
            "-c:a", "libmp3lame",
            "-b:a", self._mp3_bitrate,
            "-f", "mp3",
            "pipe:1",
        ]

    async def _handle_mp3_stream(self, request: web.Request) -> web.StreamResponse:
        """Handle GET /{channel}.mp3 — spawn ffmpeg and stream MP3."""

        raw_path = request.match_info.get("channel", "")
        # Strip .mp3 extension if present in the path
        channel_id = raw_path.replace(".mp3", "")

        if not channel_id:
            return web.Response(status=400, text="Missing channel ID")

        hls_url = self._get_hls_url(channel_id)
        ffmpeg_cmd = self._build_ffmpeg_cmd(hls_url)

        self._log.info(
            f"Starting MP3 stream for channel '{channel_id}' "
            f"(client: {request.remote})"
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

            response = web.StreamResponse(
                status=200,
                headers={
                    "Content-Type": "audio/mpeg",
                    "Cache-Control": "no-cache, no-store",
                    "Connection": "keep-alive",
                    "icy-name": channel_id,
                    "Transfer-Encoding": "chunked",
                },
            )
            await response.prepare(request)

            assert process.stdout is not None
            while True:
                chunk = await process.stdout.read(4096)
                if not chunk:
                    break
                await response.write(chunk)

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

    async def _handle_index(self, request: web.Request) -> web.Response:
        """Simple index page listing available endpoints."""
        return web.Response(
            status=200,
            text=(
                "sxm-player MP3 Proxy\n"
                "====================\n\n"
                "Endpoints:\n"
                "  GET /{channel_id}.mp3  - MP3 audio stream\n"
                "  GET /channels.json     - Channel list (JSON)\n\n"
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

        request_logger = logging.getLogger("sxm_player.mp3proxy.request")
        request_logger._info = request_logger.info  # type: ignore
        request_logger.info = request_logger.debug  # type: ignore

        app = web.Application()
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/channels.json", self._handle_channels)
        # Match both "octane.mp3" and "octane" patterns
        app.router.add_get("/{channel}.mp3", self._handle_mp3_stream)
        app.router.add_get("/{channel}", self._handle_mp3_stream)

        async def on_shutdown(app):
            await self._cleanup_processes()

        app.on_shutdown.append(on_shutdown)

        try:
            self._log.info(
                f"{self.name} has started on http://{self._ip}:{self._port}"
            )
            web.run_app(
                app,
                host=self._ip,
                port=self._port,
                access_log=request_logger,
                print=None,  # type: ignore
            )
        except (KeyboardInterrupt, TerminateInterrupt):
            pass
