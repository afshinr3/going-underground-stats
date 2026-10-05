"""CODE_AFTER_PULL_V1_20261005 — the bridge must emit with the code it just pulled.

m2_rumble_to_upstream_v1 imports fetch_and_push at module load, BEFORE main() runs
`git pull`. So a fix merged to main reached the M3 Pro health feed (the file every
LaMetric pusher reads) one bridge cycle late, and in between the bridge re-committed
a feed built by the previous code over the one the cloud had just published fixed.

These checks rewrite real module files on disk and assert the bridge's handle sees
the new code — including a dependency fetch_and_push imports by name, which only
works if dependencies are reloaded BEFORE the entry module.
"""
import importlib.util
import os
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))
FAILS = []


def check(name, ok):
    print(("PASS " if ok else "FAIL ") + name)
    if not ok:
        FAILS.append(name)


def _load_bridge():
    # ig_matcher_v2 lives in ~/RumbleMonitor on the host only; the reload path never uses it.
    try:
        import ig_matcher_v2  # noqa: F401
    except ImportError:
        import types
        sys.modules["ig_matcher_v2"] = types.ModuleType("ig_matcher_v2")
    spec = importlib.util.spec_from_file_location(
        "m2_code_after_pull_under_test", os.path.join(HERE, "m2_rumble_to_upstream_v1.py"))
    m = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = m
    spec.loader.exec_module(m)
    return m


def _write(path, text):
    with open(path, "w") as f:
        f.write(text)


def run():
    m = _load_bridge()
    real_fp = sys.modules.get("fetch_and_push")
    real_vdf = sys.modules.get("verify_derived_feeds_v1")
    tmp = tempfile.mkdtemp()
    try:
        _write(os.path.join(tmp, "gu_dep_probe_v1.py"), "V = 1\n")
        _write(os.path.join(tmp, "fetch_and_push.py"),
               "from gu_dep_probe_v1 import V as DEPV\nV = 1\n")
        _write(os.path.join(tmp, "verify_derived_feeds_v1.py"), "V = 1\n")
        sys.path.insert(0, tmp)
        for n in ("gu_dep_probe_v1", "fetch_and_push", "verify_derived_feeds_v1"):
            sys.modules.pop(n, None)
        import fetch_and_push as fp_old           # noqa: E402
        import verify_derived_feeds_v1 as vdf_old  # noqa: E402
        m._FP, m._VDF, m.REPO = fp_old, vdf_old, tmp
        check("precondition: bridge holds the old code", m._FP.V == 1 and m._FP.DEPV == 1)

        # what a `git pull` does to the files on disk
        _write(os.path.join(tmp, "gu_dep_probe_v1.py"), "V = 22\n")
        _write(os.path.join(tmp, "fetch_and_push.py"),
               "from gu_dep_probe_v1 import V as DEPV\nV = 22\n")
        _write(os.path.join(tmp, "verify_derived_feeds_v1.py"), "V = 22\n")

        reloaded = m._reload_repo_code()
        check("fetch_and_push handle runs the pulled code", m._FP.V == 22)
        check("its dependency was reloaded first (name imported from it is new)",
              m._FP.DEPV == 22)
        check("verify_derived_feeds handle runs the pulled code", m._VDF.V == 22)
        check("the module object is the same one (other references stay valid)",
              m._FP is fp_old)
        check("modules outside the repo are not touched", "os" not in reloaded)

        # a broken pull must not take the bridge down: keep the old module
        _write(os.path.join(tmp, "fetch_and_push.py"), "this is not python (\n")
        m._reload_repo_code()
        check("syntax error in pulled code keeps the last good module", m._FP.V == 22)
    finally:
        sys.path.remove(tmp)
        for n in ("gu_dep_probe_v1",):
            sys.modules.pop(n, None)
        if real_fp is not None:
            sys.modules["fetch_and_push"] = real_fp
        if real_vdf is not None:
            sys.modules["verify_derived_feeds_v1"] = real_vdf

    src = open(os.path.join(HERE, "m2_rumble_to_upstream_v1.py")).read()
    body = src[src.index("def main():"):]
    check("main() reloads after the pull and before any feed is built",
          body.index('_git("pull"') < body.index("_reload_repo_code()")
          < body.index("_build_rumble_maps()"))


if __name__ == "__main__":
    run()
    print(f"\n{len(FAILS)} failure(s)")
    sys.exit(1 if FAILS else 0)
