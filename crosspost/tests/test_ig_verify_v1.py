"""IG_POST_VERIFY_V1 guard: a reel Instagram removed after publish must become 'removed' and alert;
a live one, or an unanswerable check, must not."""
import os, sys, tempfile, time
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.state import StateStore
import alerts.ig_verify as V


class N:
    def __init__(self): self.alerts = []
    def alert(self, k, t): self.alerts.append((k, t))


def test_removed_live_unknown():
    d = tempfile.mkdtemp()
    st = StateStore(os.path.join(d, "s.sqlite3"))
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    for tid, mid in (("t_gone", "m_gone"), ("t_live", "m_live"), ("t_unk", "m_unk")):
        with st._tx() as c:
            c.execute("INSERT INTO deliveries(tweet_id,platform,status,attempts,remote_id,updated_at) VALUES(?,?,?,?,?,?)",
                      (tid, "instagram", "success", 1, mid, old))
    V.exists = lambda s, m: {"m_gone": False, "m_live": True, "m_unk": None}[m]
    n = N()
    out = V.verify(st, None, n)
    status = dict(st._conn.execute("SELECT tweet_id,status FROM deliveries").fetchall())
    assert out["removed"] == ["t_gone"] and status["t_gone"] == "removed"
    assert status["t_live"] == "success" and status["t_unk"] == "success"
    assert n.alerts and n.alerts[0][0] == "instagram_removed"


if __name__ == "__main__":
    test_removed_live_unknown(); print("PASS")
