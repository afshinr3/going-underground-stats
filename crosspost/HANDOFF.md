# crosspost — handoff / resume notes

A later session picks up from here. Keep this file current.

## Done
- Full pipeline built and pushed on branch `ccr-400b2c35-sg77nn` (see README.md).
- 15 offline tests pass: `python -m unittest discover -s tests` from `crosspost/`.
- Not yet tested against any live API, because no credentials have been provided.
- Nothing is deployed yet. The existing stats jobs are unaffected; crosspost doesn't touch them.

## Waiting on Afshin
1. **Where to run it:** the M2 Pro (launchd plist in `deploy/`) or a small server (Docker Compose).
   Run exactly one instance per state DB.
2. **Credentials** go in `.env` on that machine, never in this public repo:
   - X API Basic+ bearer token, or an RSS feed URL template
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
