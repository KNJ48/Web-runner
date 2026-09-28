import asyncio
import base64
import os
import secrets
import shutil
import signal
import uuid
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

app = FastAPI()

BASE = Path("/tmp/web-runner")
BASE.mkdir(parents=True, exist_ok=True)

USERNAME = os.environ.get("RUNNER_USER", "admin")
PASSWORD = os.environ.get("RUNNER_PASSWORD")

if not PASSWORD:
    raise RuntimeError("RUNNER_PASSWORD is required")

MAX_UPLOAD = 100 * 1024 * 1024

app.mount("/static", StaticFiles(directory="static"), name="static")

# 簡易版なので現在動かしているpreviewのポートを保持
preview_port = None


@app.get("/")
async def index():
    return FileResponse("static/index.html")


def safe_path(root: Path, relative: str):
    relative = relative.replace("\\", "/").lstrip("/")

    target = (root / relative).resolve()
    root = root.resolve()

    if target != root and root not in target.parents:
        raise ValueError("Invalid path")

    return target


async def terminate(process):
    if not process or process.returncode is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
        await asyncio.wait_for(process.wait(), 3)

    except Exception:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except Exception:
            pass


# ---------------------------------------------------------
# localhost proxy
# ---------------------------------------------------------

async def proxy(request: Request, path: str):
    global preview_port

    if preview_port is None:
        return Response(
            "Preview server is not running.",
            status_code=503
        )

    # SSRF防止：接続先は必ずRender自身のlocalhost
    target = f"http://127.0.0.1:{preview_port}/{path}"

    body = await request.body()

    headers = dict(request.headers)

    for h in [
        "host",
        "content-length",
        "connection"
    ]:
        headers.pop(h, None)

    try:
        async with httpx.AsyncClient(
            follow_redirects=False,
            timeout=30
        ) as client:

            r = await client.request(
                request.method,
                target,
                params=request.query_params,
                content=body,
                headers=headers
            )

        response_headers = {}

        for key, value in r.headers.items():
            k = key.lower()

            if k not in [
                "content-length",
                "transfer-encoding",
                "connection",
                "content-encoding"
            ]:
                response_headers[key] = value

        return Response(
            content=r.content,
            status_code=r.status_code,
            headers=response_headers
        )

    except httpx.ConnectError:
        return Response(
            "localhost server has not started yet.",
            status_code=502
        )

    except httpx.TimeoutException:
        return Response(
            "localhost server timed out.",
            status_code=504
        )


@app.api_route(
    "/preview/",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
)
async def preview_root(request: Request):
    return await proxy(request, "")


@app.api_route(
    "/preview/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
)
async def preview(request: Request, path: str):
    return await proxy(request, path)


# ---------------------------------------------------------
# Runner websocket
# ---------------------------------------------------------

@app.websocket("/terminal")
async def terminal(ws: WebSocket):
    global preview_port

    await ws.accept()

    workspace = None
    process = None

    try:
        auth = await asyncio.wait_for(
            ws.receive_json(),
            timeout=10
        )

        if (
            auth.get("type") != "auth"
            or not secrets.compare_digest(
                str(auth.get("username", "")),
                USERNAME
            )
            or not secrets.compare_digest(
                str(auth.get("password", "")),
                PASSWORD
            )
        ):
            await ws.send_json({
                "type": "error",
                "data": "Authentication failed"
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

            # ---------------------
            # Upload
            # ---------------------

            if msg_type == "upload":

                files = message.get("files", [])

                total = 0

                for item in files:

                    content = base64.b64decode(
                        item.get("data", "")
                    )

                    total += len(content)

                    if total > MAX_UPLOAD:
                        raise ValueError(
                            "Upload exceeds 100 MB"
                        )

                    destination = safe_path(
                        workspace,
                        item.get("path", "")
                    )

                    destination.parent.mkdir(
                        parents=True,
                        exist_ok=True
                    )

                    destination.write_bytes(content)

                await ws.send_json({
                    "type": "output",
                    "data":
                        f"Uploaded {len(files)} file(s).\r\n"
                })

            # ---------------------
            # RUN
            # ---------------------

            elif msg_type == "run":

                if process and process.returncode is None:

                    await ws.send_json({
                        "type": "output",
                        "data":
                            "Process already running.\r\n"
                    })

                    continue

                command = str(
                    message.get("command", "")
                ).strip()

                try:
                    port = int(
                        message.get("port", 8000)
                    )

                    if port < 1024 or port > 65535:
                        raise ValueError()

                except Exception:

                    await ws.send_json({
                        "type": "output",
                        "data": "Invalid port.\r\n"
                    })

                    continue

                preview_port = port

                await ws.send_json({
                    "type": "output",
                    "data":
                        f"$ {command}\r\n"
                        f"Preview port: {port}\r\n"
                })

                process = (
                    await asyncio.create_subprocess_shell(
                        command,
                        cwd=workspace,
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT,
                        start_new_session=True,

                        env={
                            **os.environ,

                            "HOME": str(workspace),

                            "PYTHONUNBUFFERED": "1"
                        }
                    )
                )

                async def output_reader(proc):

                    try:

                        while True:

                            data = await proc.stdout.read(1024)

                            if not data:
                                break

                            await ws.send_json({
                                "type": "output",

                                "data": data.decode(
                                    errors="replace"
                                )
                            })

                        code = await proc.wait()

                        await ws.send_json({
                            "type": "output",
                            "data":
                                f"\r\n[process exited: {code}]"
                                "\r\n"
                        })

                    except Exception:
                        pass

                asyncio.create_task(
                    output_reader(process)
                )

            # ---------------------
            # STOP
            # ---------------------

            elif msg_type == "stop":

                await terminate(process)

                preview_port = None

                await ws.send_json({
                    "type": "output",
                    "data": "\r\n[stopped]\r\n"
                })

            # ---------------------
            # CLEAR
            # ---------------------

            elif msg_type == "clear":

                await terminate(process)

                preview_port = None

                shutil.rmtree(
                    workspace,
                    ignore_errors=True
                )

                workspace.mkdir(parents=True)

                await ws.send_json({
                    "type": "output",
                    "data":
                        "Workspace cleared.\r\n"
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

        await terminate(process)

        preview_port = None

        if workspace:
            shutil.rmtree(
                workspace,
                ignore_errors=True
            )
