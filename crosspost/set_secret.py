#!/usr/bin/env python3
"""Set a key in .env without the value ever appearing on screen or in shell history.

    .venv/bin/python set_secret.py TELEGRAM_BOT_TOKEN           # prompt (needs a real terminal)
    .venv/bin/python set_secret.py TELEGRAM_BOT_TOKEN --from-file ~/tg.txt
    .venv/bin/python set_secret.py --show                        # list keys, values hidden

--from-file reads the first non-empty line of the file, writes it to .env, then overwrites
and deletes the file. Use it when there is no interactive terminal, so the value is never
typed into a shell command or a chat transcript.

Replaces the key if it exists, appends it if not. Keeps mode 600. Never prints a value.
"""
import getpass, os, re, sys, stat

ENV = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

def load():
    return open(ENV).read().split("\n") if os.path.exists(ENV) else []

def show():
    for line in load():
        m = re.match(r'^([A-Z][A-Z0-9_]*)=(.*)$', line.strip())
        if not m:
            continue
        key, raw = m.group(1), m.group(2)
        val = raw.split("#")[0].strip() if not raw.strip().startswith("#") else ""
        secretish = any(w in key for w in ("TOKEN", "SECRET", "COOKIE", "KEY", "PASS"))
        if not val:
            state = "— not set"
        elif secretish:
            state = "SET (%d chars, hidden)" % len(val)
        else:
            state = val
        print("  %-28s %s" % (key, state))

def put(key, src=None):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*', key):
        sys.exit("first argument must be the KEY name in UPPER_SNAKE_CASE, e.g. TELEGRAM_BOT_TOKEN")
    if src:
        src = os.path.expanduser(src)
        if not os.path.exists(src):
            sys.exit("no such file: %s" % src)
        with open(src) as f:
            val = next((l.strip() for l in f if l.strip()), "")
        if not val:
            sys.exit("%s is empty; nothing written" % src)
    else:
        try:
            val = getpass.getpass("Value for %s (not echoed, Enter to abort): " % key).strip()
        except Exception:
            sys.exit("no interactive terminal here. Use --from-file, or run this in Terminal.app")
        if not val:
            sys.exit("aborted; nothing written")

    lines, done = load(), False
    for i, line in enumerate(lines):
        if re.match(r'^%s=' % re.escape(key), line.strip()):
            lines[i], done = "%s=%s" % (key, val), True
            break
    if not done:
        if lines and lines[-1].strip() == "":
            lines.insert(len(lines) - 1, "%s=%s" % (key, val))
        else:
            lines.append("%s=%s" % (key, val))
    tmp = ENV + ".tmp"
    with open(tmp, "w") as f:
        f.write("\n".join(lines))
    os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
    os.replace(tmp, ENV)
    print("%s written to .env (%d chars). Value not shown." % (key, len(val)))

    if src:
        try:
            n = os.path.getsize(src)
            with open(src, "r+b") as f:
                f.write(b"\0" * n)
                f.flush()
                os.fsync(f.fileno())
            os.remove(src)
            print("wiped and deleted %s" % src)
        except Exception as e:
            print("WARNING: could not remove %s (%s) - delete it yourself" % (src, e))


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a:
        sys.exit(__doc__)
    if a[0] == "--show":
        show()
    elif len(a) == 3 and a[1] == "--from-file":
        put(a[0], a[2])
    elif len(a) == 1:
        put(a[0])
    else:
        sys.exit(__doc__)
