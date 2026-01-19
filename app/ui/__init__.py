"""UI module for web pages."""

from app.ui.routes import router as ui_router
from app.ui.setup import router as setup_router

__all__ = ["ui_router", "setup_router"]
