"""应用入口：uvicorn main:app。"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from app.api import router as telemetry_router

app = FastAPI(title="Edge Telemetry Gateway", version="0.1.0")

app.include_router(telemetry_router)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "edge-telemetry-gateway", "status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
