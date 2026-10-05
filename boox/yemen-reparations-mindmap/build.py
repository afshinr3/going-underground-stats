#!/usr/bin/env python3
"""Render the Boox mind map: content.json + template.html -> PDF (overview + 8 detail pages) and PNG.

Usage: python3 build.py [--fonts DIR]
Fonts (OFL, from fontsource via jsDelivr) are downloaded into ./.fonts on first run.
Needs: pip install playwright pillow pypdf; a Chromium (PLAYWRIGHT_CHROMIUM or /opt/pw-browsers/chromium).
"""
import argparse, base64, json, os, pathlib, urllib.request

from playwright.sync_api import sync_playwright

HERE = pathlib.Path(__file__).resolve().parent
FONTS = [  # (css family, fontsource package, weight, style)
    ("Cormorant Garamond", "cormorant-garamond", w, s) for w in (400, 500, 600, 700) for s in ("normal", "italic")
] + [
    ("Playfair Display", "playfair-display", w, s) for w in (400, 700, 900) for s in ("normal", "italic")
] + [
    ("Playfair Display SC", "playfair-display-sc", 400, "normal"), ("Playfair Display SC", "playfair-display-sc", 700, "normal"),
    ("IBM Plex Mono", "ibm-plex-mono", 400, "normal"), ("IBM Plex Mono", "ibm-plex-mono", 500, "normal"),
    ("Josefin Sans", "josefin-sans", 300, "normal"), ("Josefin Sans", "josefin-sans", 600, "normal"),
    ("Cinzel", "cinzel", 400, "normal"), ("Cinzel", "cinzel", 700, "normal"),
]


def font_css(font_dir: pathlib.Path) -> str:
    font_dir.mkdir(parents=True, exist_ok=True)
    css = []
    for fam, pkg, w, s in FONTS:
        f = font_dir / f"{pkg}-{w}-{s}.woff2"
        if not f.exists():
            url = f"https://cdn.jsdelivr.net/npm/@fontsource/{pkg}/files/{pkg}-latin-{w}-{s}.woff2"
            f.write_bytes(urllib.request.urlopen(url).read())
        b64 = base64.b64encode(f.read_bytes()).decode()
        css.append(f"@font-face{{font-family:'{fam}';font-weight:{w};font-style:{s};"
                   f"src:url(data:font/woff2;base64,{b64}) format('woff2')}}")
    return "\n".join(css)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fonts", default=str(HERE / ".fonts"))
    ap.add_argument("--out", default=str(HERE))
    a = ap.parse_args()
    out = pathlib.Path(a.out)

    data = json.loads((HERE / "content.json").read_text())
    html = (HERE / "template.html").read_text()
    html = html.replace("/*FONTS*/", font_css(pathlib.Path(a.fonts))).replace("/*DATA*/null", json.dumps(data))
    page_file = out / ".render.html"
    page_file.write_text(html)

    exe = os.environ.get("PLAYWRIGHT_CHROMIUM", "/opt/pw-browsers/chromium-1194/chrome-linux/chrome")
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=exe if os.path.exists(exe) else None)
        # 1) full-resolution PNG of the canvas (for inserting into a Boox infinite note)
        pg = b.new_page(viewport={"width": 7200, "height": 5400}, device_scale_factor=1.5)
        pg.goto(page_file.as_uri() + "#canvas")
        pg.wait_for_selector("body[data-ready='1']", timeout=60000, state="attached")
        fs = pg.evaluate("Object.fromEntries([...document.querySelectorAll('.zone')].map(z=>[z.id,z.dataset.fs]))")
        print("zone font sizes:", fs)
        pg.locator("#canvas").screenshot(path=str(out / ".canvas.png"))
        pg.close()
        # 2) vector PDF, every page printed at native size so nothing reflows:
        #    page 1 = the whole map (4:3, like the Note Max screen), pages 2-9 = one branch each
        parts = []
        pg = b.new_page(viewport={"width": 7200, "height": 5400})
        pg.goto(page_file.as_uri() + "#canvas")
        pg.wait_for_selector("body[data-ready='1']", timeout=60000, state="attached")
        parts.append(out / ".p0.pdf")
        pg.pdf(path=str(parts[-1]), width="7200px", height="5400px", print_background=True)
        pg.close()
        for br in data["branches"]:
            pg = b.new_page(viewport={"width": 3000, "height": 2250})
            pg.goto(page_file.as_uri() + "#zone=" + br["id"])
            pg.wait_for_selector("body[data-ready='1']", timeout=60000, state="attached")
            w, h = pg.evaluate("[+document.body.dataset.w, +document.body.dataset.h]")
            pg.set_viewport_size({"width": w, "height": h})
            parts.append(out / f".p{br['num']}.pdf")
            pg.pdf(path=str(parts[-1]), width=f"{w}px", height=f"{h}px", print_background=True)
            pg.close()
        b.close()

    from pypdf import PdfWriter
    wr = PdfWriter()
    for f in parts:
        wr.append(str(f))
    wr.add_metadata({"/Title": data["title"], "/Author": "Mind map of Politics Theory Other with David Wearing"})
    with open(out / "what-the-uk-owes-to-yemen.pdf", "wb") as fh:
        wr.write(fh)
    for f in parts:
        f.unlink()

    from PIL import Image
    im = Image.open(out / ".canvas.png").convert("L")  # monochrome e-ink: 8-bit greyscale
    im.save(out / "what-the-uk-owes-to-yemen.png", optimize=True)
    (out / ".canvas.png").unlink()
    page_file.unlink()
    print("wrote", out / "what-the-uk-owes-to-yemen.pdf", out / "what-the-uk-owes-to-yemen.png")


if __name__ == "__main__":
    main()
