import asyncio
import base64
import os
import secrets
import shutil
import signal
import uuid
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

BASE = Path("/tmp/web-runner")
BASE.mkdir(parents=True, exist_ok=True)

USERNAME = os.environ.get("RUNNER_USER", "admin")
PASSWORD = os.environ.get("RUNNER_PASSWORD")

if not PASSWORD:
    raise RuntimeError("RUNNER_PASSWORD environment variable is required")

MAX_UPLOAD = 100 * 1024 * 1024  # 100 MB
MAX_RUNTIME = 600                # 10 min

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index():
    return FileResponse("static/index.html")


def check_auth(header: str | None) -> bool:
    if not header or not header.startswith("Basic "):
        return False

    try:
        raw = base64.b64decode(header[6:]).decode()
        user, password = raw.split(":", 1)
        return (
            secrets.compare_digest(user, USERNAME)
            and secrets.compare_digest(password, PASSWORD)
        )
    except Exception:
        return False


def safe_path(root: Path, relative: str) -> Path:
    relative = relative.replace("\\", "/").lstrip("/")

    target = (root / relative).resolve()
    root = root.resolve()

    if target != root and root not in target.parents:
        raise ValueError("Invalid path")

    return target


async def terminate(process):
    if process.returncode is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
        await asyncio.wait_for(process.wait(), timeout=3)
    except Exception:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except Exception:
            pass


@app.websocket("/terminal")
async def terminal(ws: WebSocket):
    # Browser WebSocket API can't set Authorization headers,
    # so credentials are sent as the first WS message.
    await ws.accept()

    workspace = None
    process = None

    try:
        auth = await asyncio.wait_for(ws.receive_json(), timeout=10)

        if (
            auth.get("type") != "auth"
            or not secrets.compare_digest(
                str(auth.get("username", "")), USERNAME
            )
            or not secrets.compare_digest(
                str(auth.get("password", "")), PASSWORD
            )
        ):
            await ws.send_json({
                "type": "error",
                "data": "Authentication failed."
            })
            await ws.close(code=1008)
            return

        session = uuid.uuid4().hex
        workspace = BASE / session
        workspace.mkdir(parents=True)

        await ws.send_json({
            "type": "ready",
            "data": "Connected.\r\n"
        })

        while True:
            message = await ws.receive_json()
            msg_type = message.get("type")

            if msg_type == "upload":
                files = message.get("files", [])
                total = 0

                for item in files:
                    relative = item.get("path", "")
                    content = base64.b64decode(item.get("data", ""))

                    total += len(content)
                    if total > MAX_UPLOAD:
                        raise ValueError("Upload exceeds 100 MB")

                    dest = safe_path(workspace, relative)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_bytes(content)

                await ws.send_json({
                    "type": "output",
                    "data": f"Uploaded {len(files)} file(s).\r\n"
                })

            elif msg_type == "run":
                if process and process.returncode is None:
                    await ws.send_json({
                        "type": "output",
                        "data": "A process is already running.\r\n"
                    })
                    continue

                command = str(message.get("command", "")).strip()

                if not command:
                    continue

                await ws.send_json({
                    "type": "output",
                    "data": f"$ {command}\r\n"
                })

                process = await asyncio.create_subprocess_shell(
                    command,
                    cwd=workspace,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    stdin=asyncio.subprocess.PIPE,
                    start_new_session=True,
                    env={
                        "PATH": os.environ.get("PATH", ""),
                        "HOME": str(workspace),
                        "PYTHONUNBUFFERED": "1",
                        "TERM": "xterm-256color",
                    },
                )

                async def stream_output(proc):
                    try:
                        while True:
                            data = await proc.stdout.read(1024)
                            if not data:
                                break

                            await ws.send_json({
                                "type": "output",
                                "data": data.decode(
                                    "utf-8", errors="replace"
                                )
                            })

                        code = await proc.wait()

                        await ws.send_json({
                            "type": "output",
                            "data": f"\r\n[process exited: {code}]\r\n"
                        })

                    except Exception:
                        pass

                async def runtime_limit(proc):
                    await asyncio.sleep(MAX_RUNTIME)

                    if proc.returncode is None:
                        await ws.send_json({
                            "type": "output",
                            "data": "\r\n[time limit reached]\r\n"
                        })
                        await terminate(proc)

                asyncio.create_task(stream_output(process))
                asyncio.create_task(runtime_limit(process))

            elif msg_type == "input":
                if (
                    process
                    and process.returncode is None
                    and process.stdin
                ):
                    process.stdin.write(
                        str(message.get("data", "")).encode()
                    )
                    await process.stdin.drain()

            elif msg_type == "stop":
                if process and process.returncode is None:
                    await terminate(process)

                    await ws.send_json({
                        "type": "output",
                        "data": "\r\n[stopped]\r\n"
                    })

            elif msg_type == "clear":
                if process and process.returncode is None:
                    await terminate(process)

                shutil.rmtree(workspace, ignore_errors=True)
                workspace.mkdir(parents=True)

                await ws.send_json({
                    "type": "output",
                    "data": "Workspace cleared.\r\n"
                })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await ws.send_json({
                "type": "error",
                "data": str(e)
            })
        except Exception:
            pass
    finally:
        if process and process.returncode is None:
            await terminate(process)

        if workspace:
            shutil.rmtree(workspace, ignore_errors=True)