#!/usr/bin/env python3
"""LinkedIn 3-legged OAuth on this Mac, for when LinkedIn's web token generator won't work.

Needs LINKEDIN_CLIENT_ID and LINKEDIN_CLIENT_SECRET in .env (set_secret.py), and this redirect
URL added to the app (Auth tab -> OAuth 2.0 settings -> Authorized redirect URLs):
    http://localhost:8766/linkedin/callback

    .venv/bin/python linkedin_login.py

Opens LinkedIn's consent page in the default browser, catches the redirect on localhost,
exchanges the code, looks up the member id (openid/profile), and writes
LINKEDIN_ACCESS_TOKEN, LINKEDIN_AUTHOR_URN and LINKEDIN_TOKEN_EXPIRES_AT to .env through
set_secret.put -- the token is never printed.
"""
import os
import secrets
import sys
import tempfile
import threading
import time
import webbrowser
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import set_secret  # noqa: E402
from config.settings import load_settings  # noqa: E402

PORT = 8766
REDIRECT = "http://localhost:%d/linkedin/callback" % PORT
SCOPES = "openid profile w_member_social"
got = {}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        u = urlparse(self.path)
        if u.path != "/linkedin/callback":
            self.send_response(404)
            self.end_headers()
            return
        got.update({k: v[0] for k, v in parse_qs(u.query).items()})
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        ok = "code" in got
        self.wfile.write(("<h2>%s</h2><p>You can close this tab.</p>" % (
            "LinkedIn connected." if ok else "LinkedIn said: %s" % got.get("error_description", got.get("error", "?")))
        ).encode())


def save(key, value):
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, "w") as f:
        f.write(value + "\n")
    set_secret.put(key, path)  # reads, writes .env, deletes the temp file; never prints the value


def main():
    s = load_settings()
    if not (s.linkedin_client_id and s.linkedin_client_secret):
        sys.exit("set LINKEDIN_CLIENT_ID and LINKEDIN_CLIENT_SECRET first (set_secret.py)")
    state = secrets.token_urlsafe(16)
    srv = HTTPServer(("127.0.0.1", PORT), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = "https://www.linkedin.com/oauth/v2/authorization?" + urlencode({
        "response_type": "code", "client_id": s.linkedin_client_id, "redirect_uri": REDIRECT,
        "state": state, "scope": SCOPES})
    webbrowser.open(url)
    print("Approve in the browser tab that just opened (waiting up to 10 min)...")
    deadline = time.time() + 600
    while not got and time.time() < deadline:
        time.sleep(1)
    srv.shutdown()
    if "code" not in got:
        sys.exit("no authorisation: %s" % (got.get("error_description") or got.get("error") or "timed out"))
    if got.get("state") != state:
        sys.exit("state mismatch; nothing saved")
    r = requests.post("https://www.linkedin.com/oauth/v2/accessToken", data={
        "grant_type": "authorization_code", "code": got["code"], "redirect_uri": REDIRECT,
        "client_id": s.linkedin_client_id, "client_secret": s.linkedin_client_secret}, timeout=30)
    tok = r.json()
    if "access_token" not in tok:
        sys.exit("token exchange failed: %s" % tok.get("error_description", r.status_code))
    me = requests.get("https://api.linkedin.com/v2/userinfo",
                      headers={"Authorization": "Bearer " + tok["access_token"]}, timeout=30).json()
    if "sub" not in me:
        sys.exit("could not read the member id (is the OpenID Connect product added?)")
    expires = (date.today() + timedelta(seconds=int(tok.get("expires_in", 5184000)))).isoformat()
    save("LINKEDIN_ACCESS_TOKEN", tok["access_token"])
    save("LINKEDIN_AUTHOR_URN", "urn:li:person:" + me["sub"])
    save("LINKEDIN_TOKEN_EXPIRES_AT", expires)
    print("LinkedIn connected as %s; token valid until %s." % (me.get("name", "?"), expires))


if __name__ == "__main__":
    main()
