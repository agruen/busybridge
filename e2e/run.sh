#!/usr/bin/env bash
#
# Run BusyBridge E2E tests.
#
# Usage:
#   ./e2e/run.sh                    # all tests
#   ./e2e/run.sh api                # sync tests only (no browser)
#   ./e2e/run.sh browser            # UI tests only
#   ./e2e/run.sh setup              # first-time setup: auth tokens + browser session
#   ./e2e/run.sh auth-tokens        # re-authorize Google Calendar API tokens
#   ./e2e/run.sh auth-browser       # re-save browser session (every ~7 days)
#   ./e2e/run.sh <any pytest args>  # pass arbitrary args to pytest
#

set -euo pipefail

E2E_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$E2E_DIR")"
VENV_DIR="$E2E_DIR/.venv"

# ── Colors ────────────────────────────────────────────────────────────────────
bold="\033[1m"
dim="\033[2m"
green="\033[32m"
yellow="\033[33m"
red="\033[31m"
reset="\033[0m"

info()  { echo -e "${bold}${green}==>${reset} ${bold}$*${reset}"; }
warn()  { echo -e "${bold}${yellow}==> WARNING:${reset} $*"; }
error() { echo -e "${bold}${red}==> ERROR:${reset} $*" >&2; }

# ── Ensure venv exists ────────────────────────────────────────────────────────
ensure_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        info "Creating virtual environment at $VENV_DIR"
        python3 -m venv "$VENV_DIR"
    fi

    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"

    # Install/upgrade deps if requirements.txt is newer than the marker
    local marker="$VENV_DIR/.deps_installed"
    if [ ! -f "$marker" ] || [ "$E2E_DIR/requirements.txt" -nt "$marker" ]; then
        info "Installing dependencies"
        pip install --quiet --upgrade pip
        pip install --quiet -r "$E2E_DIR/requirements.txt"
        touch "$marker"
    fi

    # Ensure Chromium is installed for Playwright
    if ! playwright install --dry-run chromium &>/dev/null 2>&1; then
        info "Installing Chromium for Playwright"
        playwright install chromium
    elif [ ! -f "$VENV_DIR/.chromium_installed" ]; then
        info "Installing Chromium for Playwright"
        playwright install chromium
        touch "$VENV_DIR/.chromium_installed"
    fi
}

# ── Auth checks ───────────────────────────────────────────────────────────────
check_tokens() {
    if [ ! -f "$E2E_DIR/auth/.calendar_tokens.json" ]; then
        error "Google Calendar tokens not found."
        echo "  Run: $0 auth-tokens"
        return 1
    fi
}

check_browser_state() {
    if [ ! -f "$E2E_DIR/auth/.auth_state.json" ]; then
        warn "Browser session not found — browser tests will be skipped."
        echo "  Run: $0 auth-browser"
    fi
}

check_client_secret() {
    if [ ! -f "$E2E_DIR/auth/client_secret.json" ]; then
        error "Google OAuth client_secret.json not found at e2e/auth/client_secret.json"
        echo "  Download it from Google Cloud Console (APIs & Services > Credentials)"
        echo "  and place it at: $E2E_DIR/auth/client_secret.json"
        return 1
    fi
}

# ── Commands ──────────────────────────────────────────────────────────────────
cmd_setup() {
    info "First-time setup"
    check_client_secret
    ensure_venv
    info "Step 1/2: Authorize Google Calendar API for each test account"
    python -m e2e.auth.get_calendar_tokens
    echo
    info "Step 2/2: Save browser session (log in to BusyBridge)"
    python -m e2e.auth.save_auth_state
    echo
    info "Setup complete! Run tests with: $0"
}

cmd_auth_tokens() {
    check_client_secret
    ensure_venv
    info "Authorizing Google Calendar API tokens"
    python -m e2e.auth.get_calendar_tokens
}

cmd_auth_browser() {
    ensure_venv
    info "Saving browser session"
    python -m e2e.auth.save_auth_state
}

cmd_test() {
    ensure_venv
    check_tokens

    local pytest_args=()

    case "${1:-}" in
        api)
            info "Running API-only sync tests"
            pytest_args+=(-m api_only)
            shift
            ;;
        browser)
            info "Running browser/UI tests"
            pytest_args+=(-m browser)
            shift
            ;;
        "")
            info "Running all E2E tests"
            check_browser_state
            ;;
        *)
            info "Running E2E tests"
            ;;
    esac

    # Append any remaining args
    pytest_args+=("$@")

    cd "$PROJECT_DIR"
    exec pytest "$E2E_DIR" "${pytest_args[@]}" -v
}

# ── Main ──────────────────────────────────────────────────────────────────────
case "${1:-}" in
    setup)
        shift
        cmd_setup
        ;;
    auth-tokens)
        shift
        cmd_auth_tokens
        ;;
    auth-browser)
        shift
        cmd_auth_browser
        ;;
    -h|--help|help)
        echo "Usage: $0 [command] [pytest args...]"
        echo
        echo "Commands:"
        echo "  (none)         Run all tests"
        echo "  api            Run sync tests only (no browser)"
        echo "  browser        Run browser/UI tests only"
        echo "  setup          First-time setup (tokens + browser session)"
        echo "  auth-tokens    Re-authorize Google Calendar API tokens"
        echo "  auth-browser   Re-save browser session (~every 7 days)"
        echo "  help           Show this help"
        echo
        echo "Any other arguments are passed directly to pytest."
        echo "Examples:"
        echo "  $0                              # all tests"
        echo "  $0 api                          # sync tests only"
        echo "  $0 tests/test_05_delete_cascade.py  # single file"
        echo "  $0 -k webhook                   # tests matching 'webhook'"
        ;;
    *)
        cmd_test "$@"
        ;;
esac
