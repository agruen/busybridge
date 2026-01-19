"""API endpoints module."""

from fastapi import APIRouter

from app.api.users import router as users_router
from app.api.calendars import router as calendars_router
from app.api.sync import router as sync_router
from app.api.admin import router as admin_router
from app.api.webhooks import router as webhooks_router

api_router = APIRouter(prefix="/api")

api_router.include_router(users_router)
api_router.include_router(calendars_router)
api_router.include_router(sync_router)
api_router.include_router(admin_router)
api_router.include_router(webhooks_router)

__all__ = ["api_router"]
