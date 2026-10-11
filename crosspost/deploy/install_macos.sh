#!/usr/bin/env bash
# Idempotent macOS install for crosspost. Safe to re-run.
#
#   bash deploy/install_macos.sh            # install/update venv, .env, launchd plist (not loaded)
#   bash deploy/install_macos.sh --start    # also (re)load the launchd agent
#
# Run from the crosspost/ directory of a clone dedicated to crosspost — NOT the
# going-underground-stats clone the hourly bridge commits from.
set -euo pipefail

cd "$(dirname "$0")/.."
DIR="$(pwd)"
LABEL="com.gu.crosspost"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG="$HOME/Library/Logs/gu-crosspost.log"

# Python >= 3.10 (prefer Homebrew)
PY=""
for c in /opt/homebrew/bin/python3 /usr/local/bin/python3 python3; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
    PY="$(command -v "$c")"; break
  fi
done
[ -n "$PY" ] || { echo "need python >= 3.10 (brew install python)"; exit 1; }
echo "python: $PY ($("$PY" -V))"

[ -d .venv ] || "$PY" -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements.txt

command -v ffprobe >/dev/null 2>&1 || { command -v brew >/dev/null 2>&1 && brew install ffmpeg || echo "warn: ffmpeg not installed (optional)"; }

.venv/bin/python -m unittest discover -s tests 2>&1 | tail -1

if [ ! -f .env ]; then
  cp .env.example .env
  # First install: start from "now" so no back-catalogue gets reposted.
  sed -i '' 's/^INITIAL_LOOKBACK_HOURS=.*/INITIAL_LOOKBACK_HOURS=0/' .env
  echo "created .env (fill in credentials)"
fi
chmod 600 .env
mkdir -p data temp "$(dirname "$PLIST")" "$(dirname "$LOG")"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>WorkingDirectory</key><string>$DIR</string>
  <key>ProgramArguments</key>
  <array><string>$DIR/.venv/bin/python</string><string>main.py</string></array>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>$DIR/.venv/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>60</integer>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict>
</plist>
EOF
echo "wrote $PLIST"

if [ "${1:-}" = "--start" ]; then
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$PLIST"
  echo "started $LABEL; logs: $LOG"
else
  echo "not started. When .env is filled and 'python main.py --dry-run' looks right: bash deploy/install_macos.sh --start"
fi
