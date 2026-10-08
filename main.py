import uvicorn
from fastapi import FastAPI

from app.api import install_lifespan, router

app = FastAPI(title="Edge Telemetry Gateway")
install_lifespan(app)
app.include_router(router)


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
