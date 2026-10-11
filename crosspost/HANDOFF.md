# crosspost — handoff / resume notes

A later session picks up from here. Keep this file current.

## Done
- Full pipeline built and pushed on branch `ccr-400b2c35-sg77nn` (see README.md).
- 16 offline tests pass: `python -m unittest discover -s tests` from `crosspost/`.
- Not yet tested against any live API, because no credentials have been provided.
- 2026-10-11: install on the M2 Pro handed to its Remote Control session (separate clone at
  `~/gu-crosspost`; launchd agent starts only once credentials exist). Afshin will add
  credentials himself at the M2, guided by CREDENTIALS.md.
- Nothing is live yet. The existing stats jobs are unaffected; crosspost doesn't touch them.

## Waiting on Afshin
1. **Where to run it:** decided, the M2 Pro. Run exactly one instance per state DB.
2. **Credentials** go in `.env` on that machine, never in this public repo (see CREDENTIALS.md):
   - x.com cookie export (X_COOKIES_FILE), or an X API Basic+ token
   - Telegram bot token + channel id
   - Instagram Business user id + long-lived token
   - LinkedIn token + author URN
3. **Rednote:** decide on playwright or webhook. In playwright mode, run a one-time QR login.

## Next steps once answered
1. Install on the chosen host. Per CLAUDE.md, send this to the Remote Control session on that Mac
   rather than asking Afshin to paste commands.
2. Run `python main.py --dry-run` and check the captions.
3. Run `python main.py --once --platforms telegram` as the first live post.
4. Enable the other platforms one at a time.
5. Start the daemon (launchd / systemd / compose).
6. Optional: open a PR to merge `crosspost/` into `main`.
