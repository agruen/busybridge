"""Alerting module."""

from app.alerts.email import send_email, queue_alert

__all__ = ["send_email", "queue_alert"]
