"""应用入口：uvicorn main:app。"""

from __future__ import annotations

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api import router as telemetry_router
from app.dependencies import get_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 下游失败数据的后台退避重试循环（未注册处理器时为空操作）
    get_service().start_background_pump()
    try:
        yield
    finally:
        await get_service().stop_background_pump()


app = FastAPI(title="Edge Telemetry Gateway", version="0.1.0", lifespan=lifespan)

app.include_router(telemetry_router)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "edge-telemetry-gateway", "status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
