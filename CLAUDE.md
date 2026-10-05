# Going Underground stats — notes for Claude sessions

## Never make the operator a human clipboard

Before asking Afshin to run ANY command on one of his Macs, check for a Remote Control session
on that Mac: `ListAgents`, then `list_sessions` (look for `environment_kind: bridge`,
`connection_status: connected`; titles like `afshins-macbook-pro-4-local-*` are the M2 Pro).

- Reachable → send the work to that session.
- Exists but not reachable from this session (a cloud session cannot message it) → commit a
  handoff file (to the private SignalFlash repo if it names hosts or devices; this repo is
  public) and give Afshin ONE line to send to that session in the Claude app. Never a terminal
  paste.
- None exists → say so, give the one-time `claude remote-control` setup, and only then fall back
  to a command.

## Where things run

- **M2 Pro (user `afshin`)**: the bridge `m2_rumble_to_upstream_v1.py` in
  `~/going-underground-stats` (hourly at :20, commits "M2 local->upstream"), and every LaMetric
  pusher (`canonical_gu_pusher_v2` reads the local `videos_health_v1.json`).
- **M3 Pro (user `afshinrattansi`)**: no clone of this repo.
- **Cloud**: the GitHub Action on `main` (`fetch_and_push.py`), which pushes the Tidbyt.
