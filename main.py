from fastapi import FastAPI

from app.routes.tiktok import router as tiktok_router
from app.routes.youtube import router as youtube_router
from app.routes.downloads import router as downloads_router
from app.routes.pdf import router as pdf_router

API_PREFIX = "/tools"

app = FastAPI(
    openapi_url=f"{API_PREFIX}/openapi.json",
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
)

app.include_router(youtube_router, prefix=API_PREFIX)
app.include_router(tiktok_router, prefix=API_PREFIX)
app.include_router(downloads_router, prefix=API_PREFIX)
app.include_router(pdf_router, prefix=API_PREFIX)
