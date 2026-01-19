"""Email sending functionality."""

import logging
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

import aiosmtplib

from app.database import get_database, get_setting
from app.encryption import decrypt_value

logger = logging.getLogger(__name__)


async def get_smtp_config() -> dict:
    """Get SMTP configuration from settings."""
    config = {}

    host = await get_setting("smtp_host")
    if host:
        config["host"] = host.get("value_plain")

    port = await get_setting("smtp_port")
    if port:
        config["port"] = int(port.get("value_plain") or 587)
    else:
        config["port"] = 587

    username = await get_setting("smtp_username")
    if username:
        config["username"] = username.get("value_plain")

    password = await get_setting("smtp_password")
    if password and password.get("value_encrypted"):
        config["password"] = decrypt_value(password["value_encrypted"])

    from_addr = await get_setting("smtp_from_address")
    if from_addr:
        config["from_address"] = from_addr.get("value_plain")

    return config


async def send_email(
    to_email: str,
    subject: str,
    body: str,
    html_body: Optional[str] = None,
) -> None:
    """
    Send an email.

    Args:
        to_email: Recipient email address
        subject: Email subject
        body: Plain text body
        html_body: Optional HTML body
    """
    config = await get_smtp_config()

    if not config.get("host"):
        logger.warning("SMTP not configured, cannot send email")
        raise ValueError("SMTP not configured")

    # Create message
    if html_body:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
    else:
        msg = MIMEText(body, "plain")

    msg["Subject"] = subject
    msg["From"] = config.get("from_address", config.get("username"))
    msg["To"] = to_email

    # Send email
    try:
        await aiosmtplib.send(
            msg,
            hostname=config["host"],
            port=config["port"],
            username=config.get("username"),
            password=config.get("password"),
            start_tls=True,
        )
        logger.info(f"Email sent to {to_email}: {subject}")

    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        raise


async def queue_alert(
    alert_type: str,
    user_id: Optional[int] = None,
    calendar_id: Optional[int] = None,
    details: str = "",
) -> None:
    """
    Queue an alert for sending.

    Handles deduplication (same alert type for same calendar within 1 hour).
    """
    db = await get_database()

    # Check if alerts are enabled
    enabled = await get_setting("alerts_enabled")
    if not enabled or enabled.get("value_plain") != "true":
        logger.debug("Alerts are disabled")
        return

    # Check for duplicate (same alert type for same calendar within 1 hour)
    dedup_cutoff = (datetime.utcnow() - timedelta(hours=1)).isoformat()
    cursor = await db.execute(
        """SELECT id FROM alert_queue
           WHERE alert_type = ? AND created_at > ?
           AND (? IS NULL OR recipient_email IN (
               SELECT email FROM users WHERE id = ?
           ))""",
        (alert_type, dedup_cutoff, user_id, user_id)
    )
    existing = await cursor.fetchone()

    if existing:
        logger.debug(f"Skipping duplicate alert: {alert_type}")
        return

    # Get recipients
    recipients = []

    # Add affected user
    if user_id:
        cursor = await db.execute("SELECT email FROM users WHERE id = ?", (user_id,))
        user = await cursor.fetchone()
        if user:
            recipients.append(user["email"])

    # Add admin emails
    admin_emails = await get_setting("alert_emails")
    if admin_emails and admin_emails.get("value_plain"):
        for email in admin_emails["value_plain"].split(","):
            email = email.strip()
            if email and email not in recipients:
                recipients.append(email)

    if not recipients:
        logger.warning("No recipients for alert")
        return

    # Generate email content
    subject, body = generate_alert_content(alert_type, details, calendar_id)

    # Queue for each recipient
    for recipient in recipients:
        await db.execute(
            """INSERT INTO alert_queue (alert_type, recipient_email, subject, body)
               VALUES (?, ?, ?, ?)""",
            (alert_type, recipient, subject, body)
        )

    await db.commit()
    logger.info(f"Queued {alert_type} alert for {len(recipients)} recipients")


def generate_alert_content(
    alert_type: str,
    details: str,
    calendar_id: Optional[int] = None,
) -> tuple[str, str]:
    """Generate email subject and body for an alert."""
    from app.config import get_settings
    settings = get_settings()

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    subjects = {
        "token_revoked": "Calendar Sync - Authentication Required",
        "calendar_inaccessible": "Calendar Sync - Calendar Access Issue",
        "sync_failures": "Calendar Sync - Sync Failures Detected",
        "webhook_registration_failed": "Calendar Sync - Webhook Issue",
        "system_error": "Calendar Sync - System Error",
    }

    subject = subjects.get(alert_type, f"Calendar Sync - {alert_type}")

    body = f"""Calendar Sync Engine Alert

Alert Type: {alert_type}
Time: {timestamp}
"""

    if calendar_id:
        body += f"Calendar ID: {calendar_id}\n"

    body += f"""
Details:
{details}

---
Manage your calendar sync settings: {settings.public_url}/app/settings
"""

    return subject, body


async def send_test_email_to(recipient: str) -> bool:
    """Send a test email to verify configuration."""
    try:
        await send_email(
            to_email=recipient,
            subject="Calendar Sync - Test Email",
            body="This is a test email from Calendar Sync Engine.\n\nIf you received this, your email configuration is working correctly.",
        )
        return True
    except Exception as e:
        logger.error(f"Test email failed: {e}")
        return False
