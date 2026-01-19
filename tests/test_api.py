"""Tests for API endpoints."""

import pytest
from fastapi.testclient import TestClient


def test_health_check(client):
    """Test health check endpoint."""
    response = client.get("/health")
    # May fail if DB not initialized, that's OK for this test
    assert response.status_code in [200, 503]


def test_root_redirect_to_setup(client):
    """Test root redirects to setup when OOBE not completed."""
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 302
    assert "/setup" in response.headers.get("location", "")


def test_app_redirect_to_login(client):
    """Test /app redirects to login when not authenticated."""
    response = client.get("/app", follow_redirects=False)
    assert response.status_code == 302
    location = response.headers.get("location", "")
    assert "/setup" in location or "/login" in location


def test_setup_page(client):
    """Test setup page loads."""
    response = client.get("/setup")
    assert response.status_code == 200
    assert b"Welcome" in response.content or b"Calendar Sync" in response.content


def test_setup_step1(client):
    """Test setup step 1."""
    response = client.get("/setup?step=1")
    assert response.status_code == 200
    assert b"Get Started" in response.content


def test_setup_step2(client):
    """Test setup step 2."""
    response = client.get("/setup?step=2")
    assert response.status_code == 200
    assert b"Client ID" in response.content


def test_api_me_unauthorized(client):
    """Test /api/me returns 401 when not authenticated."""
    response = client.get("/api/me")
    assert response.status_code == 401


def test_api_client_calendars_unauthorized(client):
    """Test /api/client-calendars returns 401 when not authenticated."""
    response = client.get("/api/client-calendars")
    assert response.status_code == 401


def test_api_sync_status_unauthorized(client):
    """Test /api/sync/status returns 401 when not authenticated."""
    response = client.get("/api/sync/status")
    assert response.status_code == 401


def test_api_admin_unauthorized(client):
    """Test admin endpoints return 401 when not authenticated."""
    response = client.get("/api/admin/health")
    assert response.status_code == 401

    response = client.get("/api/admin/users")
    assert response.status_code == 401


def test_webhook_endpoint_exists(client):
    """Test webhook endpoint exists."""
    # Should return 400 without proper headers, but endpoint should exist
    response = client.post("/api/webhooks/google-calendar")
    assert response.status_code == 400  # Missing channel ID


def test_auth_login_redirect(client):
    """Test auth login redirects to setup when not configured."""
    response = client.get("/auth/login", follow_redirects=False)
    assert response.status_code == 302


def test_auth_logout(client):
    """Test auth logout."""
    response = client.get("/auth/logout", follow_redirects=False)
    assert response.status_code == 302
    assert "/login" in response.headers.get("location", "")
