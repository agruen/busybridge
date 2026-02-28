# BusyBridge Test Mode Setup Checklist

_A step-by-step printable guide for setting up BusyBridge in TEST_MODE on a Raspberry Pi.
Check off each box as you go._

---

## Phase 1: Create Gmail Test Accounts

_You need 3-5 dedicated Gmail accounts. Never use your real personal or work accounts._

Go to https://accounts.google.com/signup for each account.

- [ ] **Home/Admin account** created
  - Email: ______________________________________@gmail.com
  - Password stored securely

- [ ] **Client account 1** created
  - Email: ______________________________________@gmail.com
  - Password stored securely

- [ ] **Client account 2** created
  - Email: ______________________________________@gmail.com
  - Password stored securely

- [ ] _(Optional)_ **Client account 3** created
  - Email: ______________________________________@gmail.com
  - Password stored securely

---

## Phase 2: Google Cloud Project & OAuth Credentials

### 2a. Create a Google Cloud Project

1. [ ] Go to https://console.cloud.google.com/
2. [ ] Sign in with your **Home/Admin Gmail account** from Phase 1
3. [ ] Click the project dropdown (top-left) and click **New Project**
4. [ ] Name it something like `BusyBridge Test`
5. [ ] Click **Create** and wait for it to finish
6. [ ] Make sure the new project is selected in the dropdown

### 2b. Enable the Google Calendar API

1. [ ] In the left sidebar, go to **APIs & Services > Library**
2. [ ] Search for **Google Calendar API**
3. [ ] Click on it and click **Enable**

### 2c. Configure the OAuth Consent Screen

1. [ ] Go to **APIs & Services > OAuth consent screen**
2. [ ] Select **External** user type, click **Create**
3. [ ] Fill in the required fields:
   - App name: `BusyBridge Test`
   - User support email: _(your home/admin Gmail)_
   - Developer contact email: _(your home/admin Gmail)_
4. [ ] Click **Save and Continue**
5. [ ] On the **Scopes** page, click **Add or Remove Scopes** and add:
   - `https://www.googleapis.com/auth/calendar`
   - `https://www.googleapis.com/auth/userinfo.email`
   - `https://www.googleapis.com/auth/userinfo.profile`
   - `openid`
6. [ ] Click **Save and Continue**
7. [ ] On the **Test users** page, add ALL your Gmail test accounts:
   - ______________________________________@gmail.com
   - ______________________________________@gmail.com
   - ______________________________________@gmail.com
8. [ ] Click **Save and Continue**, then **Back to Dashboard**

> **Important:** While the app is in "Testing" status, only the test users
> you added can log in. Refresh tokens may expire after ~7 days. To avoid
> this, you can later move the app to "In production" status.

### 2d. Create OAuth 2.0 Credentials

1. [ ] Go to **APIs & Services > Credentials**
2. [ ] Click **+ Create Credentials > OAuth client ID**
3. [ ] Application type: **Web application**
4. [ ] Name: `BusyBridge`
5. [ ] Under **Authorized redirect URIs**, add these three:

```
http://localhost:3000/auth/callback
http://localhost:3000/auth/connect-client/callback
http://localhost:3000/setup/step/3/callback
```

> If you later set up a public domain or tunnel, add those URLs too,
> replacing `http://localhost:3000` with your public URL.

6. [ ] Click **Create**
7. [ ] Copy and write down the credentials:

```
Client ID:     ____________________________________________

Client Secret: ____________________________________________
```

---

## Phase 3: Tell Claude to Set Everything Up

_This is where you hand things back to Claude. Have this info ready:_

- [ ] I have my **OAuth Client ID** written down
- [ ] I have my **OAuth Client Secret** written down
- [ ] I know my **Home/Admin Gmail address**
- [ ] I know my **Client Gmail address(es)**

**Say this to Claude:**

> Set up BusyBridge in test mode with these details:
> - Client ID: (paste it)
> - Client Secret: (paste it)
> - Home email: (your admin Gmail)
> - Client emails: (your client Gmails, comma-separated)

**Claude will then automatically:**

1. Install Python dependencies
2. Create `data/` and `secrets/` directories
3. Generate the encryption key
4. Write the `.env` configuration file
5. Start the application on port 3000

---

## Phase 4: Browser Steps (You Do These)

_Claude will tell you when the app is running. Then open a browser and do these steps._

### 4a. Complete the Setup Wizard

1. [ ] Open http://localhost:3000/setup in your browser
2. [ ] **Step 1 (Welcome):** Click **Get Started**
3. [ ] **Step 2 (Credentials):** Enter your OAuth Client ID and Secret, click **Next**
4. [ ] **Step 3 (Admin Auth):** Click **Sign in with Google**
   - Sign in with your **Home/Admin Gmail account**
   - Grant the requested permissions
   - You should see "You signed in as (your email)"
   - Click **Confirm & Continue**
5. [ ] **Step 4 (Email Alerts):** Click **Skip** (or configure if you want)
6. [ ] **Step 5 (Encryption Key):**
   - The key is auto-generated
   - Copy and save it somewhere (or let Claude handle it)
   - Check "I have saved this encryption key"
   - Click **Complete Setup**

### 4b. Connect Client Calendars

1. [ ] Log in at http://localhost:3000 with your Home/Admin Gmail
2. [ ] Click **Connect Calendar** (or similar)
3. [ ] Sign in with **Client account 1** Gmail
   - Grant calendar permissions
   - Select a writable calendar
4. [ ] Repeat for **Client account 2**:
   - [ ] Sign in with Client account 2 Gmail
   - [ ] Grant permissions and select calendar
5. [ ] _(Optional)_ Repeat for **Client account 3**

---

## Phase 5: Hand Back to Claude for Testing

_Once all calendars are connected, tell Claude:_

> All calendars are connected. Run the real-world tests.

**Claude will then automatically:**

1. Trigger a manual sync and verify it works
2. Create test events on client calendars
3. Verify events sync to your main calendar with full details
4. Verify "Busy" blocks appear on other client calendars
5. Test event updates and deletes
6. Test recurring events
7. Run the full unit test suite
8. Report results

---

## Quick Reference

| Item | Your Value |
|------|-----------|
| Home/Admin Gmail | |
| Client 1 Gmail | |
| Client 2 Gmail | |
| Client 3 Gmail | |
| OAuth Client ID | |
| OAuth Client Secret | |
| Pi URL | http://localhost:3000 |
| Encryption Key | _(generated by Claude)_ |

---

## Troubleshooting

| Error | Meaning | Fix |
|-------|---------|-----|
| `test_mode_no_home_allowlist` | Home email allowlist is empty | Check `.env` has `TEST_MODE_ALLOWED_HOME_EMAILS` set |
| `email_not_allowed` | Login email not in allowlist | Add the email to `TEST_MODE_ALLOWED_HOME_EMAILS` |
| `test_mode_no_client_allowlist` | Client email allowlist is empty | Check `.env` has `TEST_MODE_ALLOWED_CLIENT_EMAILS` set |
| `client_email_not_allowed` | Client email not in allowlist | Add the email to `TEST_MODE_ALLOWED_CLIENT_EMAILS` |
| `no_refresh_token` | Google didn't return refresh token | Reconnect the client account with full consent |

---

_Total estimated time: ~30 minutes (mostly Google Cloud setup).
Hands-on browser time after Claude sets up the app: ~5 minutes._
