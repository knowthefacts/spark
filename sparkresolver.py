from fastapi import FastAPI
from fastapi.responses import JSONResponse
from asyncio import create_task

app = FastAPI()

async def store_event(event_data: dict):
    pass  # Placeholder for async storage

# Main endpoint matching ALB path /v1/gen/user
@app.post("/v1/gen/user")
async def receive_event(event_data: dict):
    create_task(store_event(event_data))
    return JSONResponse(
        content={"message": "stored successfully"},
        status_code=200,
        headers={"Content-Type": "application/json"}
    )

# Health check endpoint matching ALB path /v1/gen/health
@app.get("/v1/gen/health")
async def health_check():
    return JSONResponse(
        content={"status": "healthy"},
        status_code=200,
        headers={"Content-Type": "application/json"}
    )
