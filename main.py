from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app import api


@asynccontextmanager
async def lifespan(app: FastAPI):
    api.start_worker()   # 服务启动后后台自动补偿失败数据
    yield
    api.stop_worker()


app = FastAPI(title="Edge Telemetry Gateway", lifespan=lifespan)
app.include_router(api.router)


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
