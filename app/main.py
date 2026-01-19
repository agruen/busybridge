"""Main FastAPI application entry point."""

import logging
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.config import get_settings
from app.database import close_database, get_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# Rate limiter
limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    settings = get_settings()
    logger.info(f"Starting Calendar Sync Engine...")
    logger.info(f"Public URL: {settings.public_url}")
    logger.info(f"Database: {settings.database_path}")

    # Initialize database
    await get_database()
    logger.info("Database initialized")

    # Initialize encryption manager if key exists
    if os.path.exists(settings.encryption_key_file):
        try:
            from app.encryption import init_encryption_manager
            from app.config import get_encryption_key
            key = get_encryption_key()
            init_encryption_manager(key)
            logger.info("Encryption manager initialized")
        except Exception as e:
            logger.warning(f"Could not initialize encryption: {e}")

    # Start background scheduler
    try:
        from app.jobs.scheduler import setup_scheduler
        scheduler = setup_scheduler()
        logger.info("Background scheduler started")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")

    yield

    # Shutdown
    logger.info("Shutting down...")

    # Stop scheduler
    try:
        from app.jobs.scheduler import shutdown_scheduler
        shutdown_scheduler()
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")

    # Close database
    await close_database()
    logger.info("Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Calendar Sync Engine",
    description="A self-hosted, multi-user calendar synchronization service",
    version="1.0.0",
    lifespan=lifespan,
)

# Add rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Add CORS middleware
settings = get_settings()
allowed_origins = [settings.public_url]
# Also allow localhost variants for development
if settings.public_url.startswith("http://localhost") or settings.public_url.startswith("https://localhost"):
    allowed_origins.extend([
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
)


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    try:
        db = await get_database()
        await db.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "error": str(e)},
        )


# Include routers
from app.auth.routes import router as auth_router
from app.api import api_router
from app.ui.routes import router as ui_router
from app.ui.setup import router as setup_router

app.include_router(auth_router)
app.include_router(api_router)
app.include_router(ui_router)
app.include_router(setup_router)

# Mount static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    logger.exception(f"Unhandled exception: {exc}")

    # For API requests, return JSON
    if request.url.path.startswith("/api/"):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    # For other requests, redirect to error page or show generic error
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "An unexpected error occurred"},
    )


# Redirect /favicon.ico to prevent 404 errors
@app.get("/favicon.ico")
async def favicon():
    """Return empty response for favicon."""
    return JSONResponse(content={}, status_code=204)


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    log_level = settings.log_level.lower()

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=3000,
        log_level=log_level,
        reload=False,
    )
