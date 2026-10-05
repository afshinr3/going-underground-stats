#!/usr/bin/env python3
"""Build the Yemen mind map as a native Boox Notes file (.note) for the Note Max.

Every box, header, marker, arrow, bracket, arc, wave and frame is a native Boox geometric shape
(pen_type 40: editable with the lasso/shape tools); connectors are real pen strokes (ballpoint,
fountain, marker, charcoal, calligraphy, highlighter); text is typeset in IBM Plex Sans
(regular, italic, condensed) and IBM Plex Mono and laid down as scanline-fill ink.

Usage: python3 build_note.py [--out DIR]   (needs pillow + numpy; fonts are fetched once)
"""
import argparse, json, math, pathlib, urllib.request
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from notefile import Note, BLACK, WHITE, grey

HERE = pathlib.Path(__file__).resolve().parent
FONT_DIR = HERE.parent / '.fonts'
W, H = 7440.0, 5580.0          # canvas in Boox page points (4 x the 1860-wide page, 4:3 like the Note Max)
K = 2.0                        # raster resolution for typesetting: px per point (0.5 pt scanlines)
G1, G2, G3, G4 = grey(0x33), grey(0x66), grey(0xBB), grey(0xE6)


# ------------------------------------------------------------------------------------------------
# typesetting
_fcache = {}


def font(name, size):
    pkg, wt, st = {
        'title': ('ibm-plex-sans', 700, 'normal'), 'titlei': ('ibm-plex-sans', 300, 'italic'),
        'num': ('ibm-plex-sans', 700, 'normal'), 'sc': ('ibm-plex-sans', 600, 'normal'),
        'head': ('ibm-plex-sans-condensed', 700, 'normal'), 'body': ('ibm-plex-sans', 400, 'normal'),
        'bodyb': ('ibm-plex-sans', 600, 'normal'), 'it': ('ibm-plex-sans', 400, 'italic'),
        'sub': ('ibm-plex-sans', 400, 'italic'), 'mono': ('ibm-plex-mono', 400, 'normal'),
        'caps': ('ibm-plex-sans', 600, 'normal'), 'capsl': ('ibm-plex-sans', 400, 'normal'),
    }[name]
    f = FONT_DIR / f'{pkg}-{wt}-{st}.woff2'
    if not f.exists():
        FONT_DIR.mkdir(exist_ok=True)
        url = f'https://cdn.jsdelivr.net/npm/@fontsource/{pkg}/files/{pkg}-latin-{wt}-{st}.woff2'
        f.write_bytes(urllib.request.urlopen(url).read())
    key = (name, round(size * K))
    if key not in _fcache:
        _fcache[key] = ImageFont.truetype(str(f), round(size * K))
    return _fcache[key]


def wrap(text, fnt, width_px):
    words, lines, cur = text.split(' '), [], ''
    for w in words:
        t = (cur + ' ' + w).strip()
        if fnt.getlength(t) <= width_px or not cur:
            cur = t
        else:
            lines.append(cur); cur = w
    if cur:
        lines.append(cur)
    return lines


class Block:
    """A column of typeset paragraphs, rendered to a 1-bit raster and converted to fill rects."""

    def __init__(self, width):
        self.w = width
        self.items = []     # (kind, text, fontname, size, indent, gap_before, align, leading)

    def add(self, text, fname, size, indent=0.0, before=0.0, align='l', leading=1.22, upper=False):
        self.items.append((text.upper() if upper else text, fname, size, indent, before, align, leading))
        return self

    def layout(self):
        """Returns (height_pt, lines) where lines = [(x_pt, y_pt, text, fnt, item_index)]."""
        y, out = 0.0, []
        for idx, (text, fn, size, ind, before, align, lead) in enumerate(self.items):
            y += before
            f = font(fn, size)
            for ln in wrap(text, f, (self.w - ind) * K):
                lw = f.getlength(ln) / K
                x = ind if align == 'l' else (self.w - lw) / 2 if align == 'c' else self.w - lw
                out.append((x, y, ln, f, idx))
                y += size * lead
        return y, out

    def rects(self, ox, oy):
        h, lines = self.layout()
        img = Image.new('L', (int(math.ceil(self.w * K)) + 4, int(math.ceil(h * K)) + 40), 0)
        d = ImageDraw.Draw(img)
        for x, y, ln, f, _ in lines:
            d.text((x * K, y * K), ln, font=f, fill=255)
        return raster_rects(np.array(img) > 110, ox, oy), h, lines


def raster_rects(mask, ox, oy):
    """Run-length rows, merging identical runs in consecutive rows into taller rectangles."""
    rects, active = [], {}
    rows = mask.shape[0]
    for r in range(rows + 1):
        runs = set()
        if r < rows:
            row = mask[r].astype(np.int8)
            dif = np.diff(np.concatenate(([0], row, [0])))
            starts, ends = np.where(dif == 1)[0], np.where(dif == -1)[0]
            runs = set(zip(starts.tolist(), ends.tolist()))
        for key in list(active):
            if key not in runs:
                r0 = active.pop(key)
                rects.append((ox + key[0] / K, oy + r0 / K, ox + key[1] / K, oy + r / K))
        for key in runs:
            active.setdefault(key, r)
    return rects


def text(note, block, x, y, color=BLACK):
    rects, h, lines = block.rects(x, y)
    note.scanfill(rects, color=color)
    return h, lines


def one_line(note, s, fname, size, x, y, align='l', color=BLACK, width=None):
    f = font(fname, size)
    w = f.getlength(s) / K
    bx = x if align == 'l' else x - w / 2 if align == 'c' else x - w
    b = Block(w + 2).add(s, fname, size, leading=1.0)
    text(note, b, bx, y, color)
    return w


# ------------------------------------------------------------------------------------------------
# pen helpers

def bez(p0, p1, p2, p3, n=48):
    out = []
    for i in range(n + 1):
        t = i / n
        out.append(((1 - t) ** 3 * p0[0] + 3 * (1 - t) ** 2 * t * p1[0] + 3 * (1 - t) * t * t * p2[0] + t ** 3 * p3[0],
                    (1 - t) ** 3 * p0[1] + 3 * (1 - t) ** 2 * t * p1[1] + 3 * (1 - t) * t * t * p2[1] + t ** 3 * p3[1]))
    return out


def taper(pts, p0=3900, p1=700):
    n = len(pts) - 1
    return [(x, y, p0 + (p1 - p0) * (i / n) ** 0.8) for i, (x, y) in enumerate(pts)]


def swell(pts, lo=900, hi=3900):
    n = len(pts) - 1
    return [(x, y, lo + (hi - lo) * math.sin(math.pi * i / n)) for i, (x, y) in enumerate(pts)]


def marker(note, kind, x, y, s=16):
    if kind == 'dot':
        note.oval(x - s * .6, y - s * .6, x + s * .6, y + s * .6, width=1.5, fill=BLACK)
    elif kind == 'ring':
        note.oval(x - s * .7, y - s * .7, x + s * .7, y + s * .7, width=3, fill=WHITE)
        note.oval(x - s * .25, y - s * .25, x + s * .25, y + s * .25, width=1, fill=BLACK)
    elif kind == 'diamond':
        note.polygon([(x, y - s * .8), (x + s * .8, y), (x, y + s * .8), (x - s * .8, y)], width=3, fill=WHITE)
    elif kind == 'fdiamond':
        note.polygon([(x, y - s * .9), (x + s * .9, y), (x, y + s * .9), (x - s * .9, y)], width=1.5, fill=BLACK)
    elif kind == 'square':
        note.rect(x - s * .5, y - s * .5, x + s * .5, y + s * .5, width=1.5, fill=BLACK)
    elif kind == 'osquare':
        note.rect(x - s * .55, y - s * .55, x + s * .55, y + s * .55, width=3, fill=WHITE)
    elif kind == 'triangle':
        note.polygon([(x - s * .6, y - s * .75), (x + s * .8, y), (x - s * .6, y + s * .75)], width=1.5, fill=BLACK)
    elif kind == 'star':
        pts = []
        for i in range(10):
            r = s * (.95 if i % 2 == 0 else .4)
            a = -math.pi / 2 + i * math.pi / 5
            pts.append((x + r * math.cos(a), y + r * math.sin(a)))
        note.polygon(pts, width=1.5, fill=BLACK)


def ngon(cx, cy, rx, ry, n, rot=0.0):
    return [(cx + rx * math.cos(rot + 2 * math.pi * i / n), cy + ry * math.sin(rot + 2 * math.pi * i / n)) for i in range(n)]


# ------------------------------------------------------------------------------------------------
# branch styling: every branch gets its own header shape, spine line, box, marker and bullet

STYLE = {
    'b1': dict(head='hexagon', spine='ballpoint', box='solid', mark='dot', bullet='dot'),
    'b2': dict(head='frame', spine='double', box='grey', mark='diamond', bullet='diamond'),
    'b3': dict(head='ellipse', spine='dashed', box='dashed', mark='square', bullet='square'),
    'b4': dict(head='parallelogram', spine='fountain', box='dotted', mark='triangle', bullet='triangle'),
    'b5': dict(head='octagon', spine='marker', box='shadow', mark='ring', bullet='ring'),
    'b6': dict(head='inverse', spine='calligraphy', box='double', mark='fdiamond', bullet='fdiamond'),
    'b7': dict(head='rhombus', spine='dotted', box='bracket', mark='osquare', bullet='osquare'),
    'b8': dict(head='pennant', spine='charcoal', box='chamfer', mark='star', bullet='star'),
}


def draw_spine(note, style, pts, rib=False):
    """pts: list of (x,y) along the path."""
    wmul = 0.6 if rib else 1.0
    if style == 'ballpoint':
        note.stroke(pts, pen=2, width=4.5 * wmul)
    elif style == 'fountain':
        note.stroke(taper(pts, 4000, 1200) if not rib else swell(pts, 1200, 3600), pen=5, width=12 * wmul)
    elif style == 'marker':
        note.stroke([(x, y, 3000) for x, y in pts], pen=21, width=5.5 * wmul)
    elif style == 'calligraphy':
        note.stroke(swell(pts, 1500, 4000), pen=61, width=11 * wmul, tilt=(32, 30))
    elif style == 'charcoal':
        note.stroke([(x, y, 3300) for x, y in pts], pen=22, width=9 * wmul, tilt=(10, 24))
    elif style in ('double', 'dashed', 'dotted'):
        segs = list(zip(pts, pts[1:]))
        straight = len(pts) == 2
        if style == 'double':
            # two parallel native lines
            (x0, y0), (x1, y1) = pts[0], pts[-1]
            L = math.hypot(x1 - x0, y1 - y0) or 1
            nx, ny = -(y1 - y0) / L * 5, (x1 - x0) / L * 5
            if straight:
                note.line(x0 + nx, y0 + ny, x1 + nx, y1 + ny, width=3 * wmul)
                note.line(x0 - nx, y0 - ny, x1 - nx, y1 - ny, width=3 * wmul)
            else:
                c = pts[len(pts) // 2]
                note.curve(pts[0], (2 * c[0] - (pts[0][0] + pts[-1][0]) / 2, 2 * c[1] - (pts[0][1] + pts[-1][1]) / 2), pts[-1], width=3 * wmul)
        else:
            dash = (16, 9) if style == 'dashed' else (2, 9)
            if straight:
                note.line(*pts[0], *pts[-1], width=4 * wmul, dash=dash)
            else:
                c = pts[len(pts) // 2]
                note.curve(pts[0], (2 * c[0] - (pts[0][0] + pts[-1][0]) / 2, 2 * c[1] - (pts[0][1] + pts[-1][1]) / 2), pts[-1], width=4 * wmul, dash=dash)


def draw_head_shape(note, kind, x0, y0, x1, y1):
    cx, cy, w, h = (x0 + x1) / 2, (y0 + y1) / 2, x1 - x0, y1 - y0
    if kind == 'hexagon':
        k = h * 0.45
        note.polygon([(x0 - k, cy), (x0 + 10, y0), (x1 - 10, y0), (x1 + k, cy), (x1 - 10, y1), (x0 + 10, y1)], width=5, fill=WHITE)
        i = 16
        note.polygon([(x0 - k + i * 1.6, cy), (x0 + 10 + i * .6, y0 + i), (x1 - 10 - i * .6, y0 + i), (x1 + k - i * 1.6, cy),
                      (x1 - 10 - i * .6, y1 - i), (x0 + 10 + i * .6, y1 - i)], width=1.5)
    elif kind == 'frame':
        note.rect(x0 - 12, y0 - 12, x1 + 12, y1 + 12, width=8, fill=WHITE)
        note.rect(x0 + 6, y0 + 6, x1 - 6, y1 - 6, width=1.5)
        for px, py in ((x0 - 12, y0 - 12), (x1 + 12, y0 - 12), (x0 - 12, y1 + 12), (x1 + 12, y1 + 12)):
            note.polygon([(px, py - 18), (px + 18, py), (px, py + 18), (px - 18, py)], width=1, fill=BLACK)
    elif kind == 'ellipse':
        rx, ry = w / 2 * 1.14, h / 2 * 1.25
        note.oval(cx - rx - 22, cy - ry - 22, cx + rx + 22, cy + ry + 22, width=2, dash=(4, 8))
        note.oval(cx - rx, cy - ry, cx + rx, cy + ry, width=5, fill=WHITE)
    elif kind == 'parallelogram':
        k = 70
        note.polygon([(x0 + k + 18, y0 + 14), (x1 + k + 18, y0 + 14), (x1 - k + 18, y1 + 22), (x0 - k + 18, y1 + 22)], width=1.5, fill=G3)
        note.polygon([(x0 + k, y0 - 4), (x1 + k, y0 - 4), (x1 - k, y1 + 4), (x0 - k, y1 + 4)], width=5, fill=WHITE)
    elif kind == 'octagon':
        c = min(h * 0.3, 70)
        X0, Y0, X1, Y1 = x0 - 16, y0 - 12, x1 + 16, y1 + 12
        pts = [(X0 + c, Y0), (X1 - c, Y0), (X1, Y0 + c), (X1, Y1 - c), (X1 - c, Y1), (X0 + c, Y1), (X0, Y1 - c), (X0, Y0 + c)]
        note.polygon(pts, width=10, fill=WHITE)
        i = 14
        note.polygon([(X0 + c, Y0 + i), (X1 - c, Y0 + i), (X1 - i, Y0 + c), (X1 - i, Y1 - c), (X1 - c, Y1 - i), (X0 + c, Y1 - i), (X0 + i, Y1 - c), (X0 + i, Y0 + c)], width=1.5)
    elif kind == 'inverse':
        note.rect(x0 - 24, y0 - 24, x1 + 24, y1 + 24, width=2)
        note.rect(x0 - 8, y0 - 8, x1 + 8, y1 + 8, width=2, fill=BLACK)
        note.rect(x0 + 10, y0 + 10, x1 - 10, y1 - 10, width=1.5, color=WHITE, dash=(6, 6))
    elif kind == 'rhombus':
        k = h * 0.7
        note.polygon([(cx, y0 - k * .55), (x1 + k, cy), (cx, y1 + k * .55), (x0 - k, cy)], width=5, fill=WHITE)
        note.polygon([(cx, y0 - k * .55 + 18), (x1 + k - 34, cy), (cx, y1 + k * .55 - 18), (x0 - k + 34, cy)], width=1.5, dash=(3, 7))
    elif kind == 'pennant':
        k = 80
        note.polygon([(x0 - k, y0 - 6), (x1, y0 - 6), (x1 + k, cy), (x1, y1 + 6), (x0 - k, y1 + 6), (x0 - k + 56, cy)], width=5, fill=WHITE)
        note.line(x1 - 8, y0 + 14, x1 + k - 30, cy, width=2)
        note.line(x1 + k - 30, cy, x1 - 8, y1 - 14, width=2)


def draw_box(note, kind, x0, y0, x1, y1):
    if kind == 'solid':
        note.rect(x0, y0, x1, y1, width=2.5, fill=WHITE)
    elif kind == 'grey':
        note.rect(x0, y0, x1, y1, width=1, color=G4, fill=G4)
    elif kind == 'dashed':
        note.rect(x0, y0, x1, y1, width=3, dash=(14, 8), fill=WHITE)
    elif kind == 'dotted':
        note.rect(x0, y0, x1, y1, width=3.5, dash=(2, 8), fill=WHITE)
    elif kind == 'shadow':
        note.rect(x0 + 10, y0 + 10, x1 + 10, y1 + 10, width=2, fill=BLACK)
        note.rect(x0, y0, x1, y1, width=3, fill=WHITE)
    elif kind == 'double':
        note.rect(x0 - 7, y0 - 7, x1 + 7, y1 + 7, width=2.5)
        note.rect(x0, y0, x1, y1, width=2.5, fill=WHITE)
    elif kind == 'bracket':
        h = y1 - y0
        note.bracket((x0 - 22, (y0 + y1) / 2), (x0 + 6, y0), (x0 + 6, y1), width=3.5)
        note.line(x1 - 30, y1, x1, y1, width=3)
        note.line(x1, y1 - 30, x1, y1, width=3)
    elif kind == 'chamfer':
        c = 26
        note.polygon([(x0 + c, y0), (x1 - c, y0), (x1, y0 + c), (x1, y1 - c), (x1 - c, y1), (x0 + c, y1), (x0, y1 - c), (x0, y0 + c)], width=2.5, fill=WHITE)


# ------------------------------------------------------------------------------------------------

def node_block(nd, width, fs):
    b = Block(width)
    if 'q' in nd:
        b.add(nd['q'], 'it', fs * 1.22, indent=70, leading=1.16)
        if nd.get('c'):
            b.add(nd['c'], 'capsl', fs * 0.72, indent=70, before=fs * 0.35, leading=1.35, upper=True)
    else:
        b.add(nd['h'], 'head', fs * 0.88, before=0, leading=1.3, upper=True)
        for i, s in enumerate(nd['b']):
            b.add(s, 'body', fs, indent=34, before=fs * (0.55 if i == 0 else 0.3), leading=1.2)
    return b


FIT = {}


def build(content, out_note, uniform=None):
    note = Note(str(HERE / 'template' / 'empty.note'), 'What the UK owes to Yemen', W, H)
    M, GX, GY = 150.0, 130.0, 120.0
    colw = [2230.0, 2490.0, 2230.0]
    rowh = [1760.0, 1640.0, 1760.0]
    colx = [M, M + colw[0] + GX, M + colw[0] + colw[1] + 2 * GX]
    rowy = [M, M + rowh[0] + GY, M + rowh[0] + rowh[1] + 2 * GY]
    area = {'b1': (0, 0), 'b2': (1, 0), 'b3': (2, 0), 'b8': (0, 1), 'b4': (2, 1), 'b7': (0, 2), 'b6': (1, 2), 'b5': (2, 2)}

    # ---- frame: double rule, corner stars, wave borders top and bottom
    note.rect(52, 52, W - 52, H - 52, width=7)
    note.rect(70, 70, W - 70, H - 70, width=1.5)
    for cx, cy in ((52, 52), (W - 52, 52), (52, H - 52), (W - 52, H - 52)):
        marker(note, 'star', cx, cy, 34)
    note.wave(W / 2 - 900, 98, W / 2 + 900, 98, length=40, peak=7, width=2)

    heads = {}
    for br in content['branches']:
        st = STYLE[br['id']]
        c, r = area[br['id']]
        zx, zy, zw, zh = colx[c], rowy[r], colw[c], rowh[r]
        top = r == 0

        # header block
        hb = Block(zw * 0.62)
        hb.add(br['num'], 'sc', 46, align='c', leading=1.05)
        hb.add(br['title'], 'title', 92, align='c', leading=1.02, before=4)
        hb.add(br['sub'], 'sub', 50, align='c', leading=1.15, before=8)
        hb.add(br['ts'], 'mono', 30, align='c', leading=1.2, before=10)
        hh, _ = hb.layout()
        hw = max(font('title', 92).getlength(br['title']), font('sub', 50).getlength(br['sub'])) / K + 40
        hb.w = hw
        hx = zx + (zw - hw) / 2
        hy = zy + zh - hh - 60 if top else zy + 50
        pad = 40
        hbox = (hx - pad - 30, hy - pad, hx + hw + pad + 30, hy + hh + pad - 18)
        heads[br['id']] = hbox

        # nodes: fit body size so the two columns fill the zone
        gap = 230.0
        cw = (zw - gap) / 2
        avail = zh - (hbox[3] - hbox[1]) - 260
        fs = uniform if uniform else 46.0
        while True:
            blocks = [node_block(nd, cw - 60, fs) for nd in br['nodes']]
            hs = [b.layout()[0] + (56 if 'h' in nd else 40) for b, nd in zip(blocks, br['nodes'])]
            total = sum(hs) + 44 * len(hs)
            # sequential split into two columns, as balanced as possible
            best = None
            for k in range(1, len(hs)):
                L, R = sum(hs[:k]) + 44 * k, sum(hs[k:]) + 44 * (len(hs) - k)
                if best is None or max(L, R) < best[0]:
                    best = (max(L, R), k)
            if uniform or best[0] <= avail or fs <= 20:
                break
            fs -= 0.5
        split = best[1]
        FIT[br['id']] = fs

        # place nodes
        y_area0 = zy + 30 if top else hbox[3] + 150
        y_area1 = hbox[1] - 150 if top else zy + zh - 20
        spine_x = zx + zw / 2
        cols = [list(range(split)), list(range(split, len(hs)))]
        centres = []
        for ci, idxs in enumerate(cols):
            colh = sum(hs[i] for i in idxs) + 44 * (len(idxs) - 1)
            y = (y_area1 - colh) if top else y_area0
            x0 = zx if ci == 0 else zx + cw + gap
            for i in idxs:
                nd, b, h = br['nodes'][i], blocks[i], hs[i]
                bx0, by0, bx1, by1 = x0, y, x0 + cw, y + h
                if 'q' in nd:
                    # quotation: hairline above, wave below, oversized quote mark
                    note.line(bx0 + 70, by0, bx1, by0, width=1.5)
                    note.wave(bx0 + 70, by1 - 6, bx1, by1 - 6, length=30, peak=5, width=1.8)
                    one_line(note, '“', 'title', 150, bx0 + 2, by0 - 22)
                    text(note, b, bx0 + 24, by0 + 22)
                else:
                    draw_box(note, st['box'], bx0, by0, bx1, by1)
                    th, lines = text(note, b, bx0 + 30, by0 + 26)
                    # native bullet markers at the first line of each bullet
                    seen = set()
                    for (lx, ly, ln, f, item) in lines:
                        if item > 0 and item not in seen:
                            seen.add(item)
                            marker(note, st['bullet'], bx0 + 30 + 14, by0 + 26 + ly + fs * 0.62, fs * 0.42)
                    # rule under the node heading
                    hl = [l for l in lines if l[4] == 0]
                    yl = by0 + 26 + hl[-1][1] + fs * 0.88 * 1.18
                    note.line(bx0 + 30, yl, bx1 - 30, yl, width=1.2, color=G2)
                centres.append((bx0, by0, bx1, by1, ci))
                y += h + 44

        # spine + ribs
        hy_edge = hbox[1] if top else hbox[3]
        ys = [(b[1] + 50) for b in centres]
        far = min(ys) if top else max(ys)
        draw_spine(note, st['spine'], [(spine_x, hy_edge), (spine_x, far)] if st['spine'] in ('double', 'dashed', 'dotted')
                   else [(spine_x, hy_edge + (far - hy_edge) * i / 30) for i in range(31)])
        for (bx0, by0, bx1, by1, ci) in centres:
            yy = by0 + 50
            ex = bx1 + 26 if ci == 0 else bx0 - 26
            sy = yy + (40 if top else -40)
            if st['spine'] in ('double', 'dashed', 'dotted'):
                draw_spine(note, st['spine'], [(spine_x, sy), ((spine_x + ex) / 2, yy - (6 if top else -6)), (ex, yy)], rib=True)
            else:
                draw_spine(note, st['spine'], bez((spine_x, sy), (spine_x, yy), ((spine_x + ex) / 2, yy), (ex, yy), 24), rib=True)
            marker(note, st['mark'], ex, yy, 22)
            note.oval(spine_x - 7, sy - 7, spine_x + 7, sy + 7, width=1, fill=BLACK)

        # header shape + text (drawn last so it sits on top)
        draw_head_shape(note, st['head'], *hbox)
        text(note, hb, hx, hy, color=WHITE if st['head'] == 'inverse' else BLACK)

    # ---- centre medallion
    cx0, cy0, cw_, ch_ = colx[1], rowy[1], colw[1], rowh[1]
    ccx, ccy = cx0 + cw_ / 2, cy0 + ch_ / 2
    rx, ry = cw_ / 2 - 120, ch_ / 2 - 40
    note.oval(ccx - rx - 70, ccy - ry - 70, ccx + rx + 70, ccy + ry + 70, width=2, dash=(3, 10))
    for i in range(72):
        a = i * math.pi / 36
        r1, r2 = (34, 58) if i % 6 == 0 else (34, 46)
        note.line(ccx + (rx + r1) * math.cos(a), ccy + (ry + r1) * math.sin(a),
                  ccx + (rx + r2) * math.cos(a), ccy + (ry + r2) * math.sin(a), width=4 if i % 6 == 0 else 1.5)
    note.oval(ccx - rx - 14, ccy - ry - 14, ccx + rx + 14, ccy + ry + 14, width=10, fill=WHITE)
    note.oval(ccx - rx + 10, ccy - ry + 10, ccx + rx - 10, ccy + ry - 10, width=1.5)
    # arcs flanking the title
    note.arc(ccx - rx + 60, ccy - ry + 60, ccx + rx - 60, ccy + ry - 60, 150, 60, width=3)
    note.arc(ccx - rx + 60, ccy - ry + 60, ccx + rx - 60, ccy + ry - 60, -30, 60, width=3)

    yy = ccy - ry + 150
    one_line(note, content['kicker'], 'caps', 30, ccx, yy, align='c')
    yy += 70
    one_line(note, content['title'][0], 'title', 190, ccx, yy, align='c')
    yy += 200
    # second title line: italic "owes", heavy "to Yemen"
    wa = font('titlei', 190).getlength('owes ') / K
    wb = font('title', 190).getlength('to Yemen') / K
    one_line(note, 'owes', 'titlei', 190, ccx - (wa + wb) / 2, yy)
    one_line(note, 'to Yemen', 'title', 190, ccx - (wa + wb) / 2 + wa, yy)
    yy += 245
    # calligraphy flourish under the title
    fl = bez((ccx - 520, yy), (ccx - 180, yy - 70), (ccx + 180, yy + 70), (ccx + 520, yy), 60)
    note.stroke(swell(fl, 400, 4000), pen=60, width=14, tilt=(30, 30))
    yy += 50
    one_line(note, content['guest'], 'sc', 64, ccx, yy, align='c')
    yy += 90
    gb = Block(1500).add(content['guest_line'], 'sub', 36, align='c', leading=1.2)
    h, _ = text(note, gb, ccx - 750, yy)
    yy += h + 30
    tb = Block(1560).add('“' + content['thesis'] + '”', 'it', 50, align='c', leading=1.16)
    h, _ = text(note, tb, ccx - 780, yy)
    yy += h + 40
    # four headline figures, each over a grey highlighter swash, separated by thin rules
    fx = [ccx - 780 + 195 + i * 390 for i in range(4)]
    for i, (num, lab) in enumerate(content['figures']):
        note.stroke([(fx[i] - 150, yy + 58, 3000), (fx[i] + 150, yy + 54, 3000)], pen=15, width=60, color=G3)
        one_line(note, num, 'num', 78, fx[i], yy, align='c')
        lb = Block(350).add(lab, 'capsl', 25, align='c', leading=1.25, upper=True)
        text(note, lb, fx[i] - 175, yy + 104)
        if i:
            note.line(fx[i] - 195, yy + 10, fx[i] - 195, yy + 160, width=1.2)
    yy += 200
    rb = Block(1150).add(content['report'], 'mono', 23, align='c', leading=1.4)
    text(note, rb, ccx - 575, yy, color=G1)

    # ---- connectors centre -> heads: tapered fountain-pen strokes routed through the gutters
    gxL, gxR = colx[1] - GX / 2, colx[2] - GX / 2
    gyT, gyB = rowy[1] - GY / 2, rowy[2] - GY / 2

    def on_ell(tx, ty):
        a = math.atan2((ty - ccy) / (ry + 14), (tx - ccx) / (rx + 14))
        return ccx + (rx + 14) * math.cos(a), ccy + (ry + 14) * math.sin(a)

    for bid, (x0, y0, x1, y1) in heads.items():
        hx_, hy_ = (x0 + x1) / 2, (y0 + y1) / 2
        if bid in ('b2', 'b6'):
            E = (hx_, y1 + 30 if bid == 'b2' else y0 - 30)
            S = (ccx, ccy - ry - 70 if bid == 'b2' else ccy + ry + 70)
            P = bez(S, (S[0], (S[1] + E[1]) / 2), (E[0], (S[1] + E[1]) / 2), E, 30)
        elif bid in ('b8', 'b4'):
            Lft = bid == 'b8'
            gx = gxL if Lft else gxR
            E = (x1 + 90 if Lft else x0 - 90, hy_)
            S = on_ell(gx, hy_ + (ccy - hy_) * 0.55)
            P = bez(S, (gx + (60 if Lft else -60), S[1]), (gx, hy_), E, 40)
        else:
            topz, Lft = bid in ('b1', 'b3'), bid in ('b1', 'b7')
            gy = gyT if topz else gyB
            E = (hx_, y1 + 40 if topz else y0 - 40)
            S = on_ell(ccx + (-1 if Lft else 1) * rx * 0.62, gy)
            P = bez(S, (S[0] + (-320 if Lft else 320), gy), (hx_ + (320 if Lft else -320), gy), E, 50)
        note.stroke(taper(P, 4095, 900), pen=5, width=20)

    # ---- cross-links between heads: dashed native curves with arrowheads + pill labels
    for a, b, lab in content['links']:
        A, B = heads[a], heads[b]
        if abs(A[1] - B[1]) < 50:            # same row
            right = B[0] > A[0]
            yv = (A[1] if a in ('b1', 'b2', 'b3') else A[1]) - 50
            p0 = (A[2] + 110 if right else A[0] - 110, yv)
            p1 = (B[0] - 110 if right else B[2] + 110, yv)
            ctrl = ((p0[0] + p1[0]) / 2, yv - 120)
            note.curve(p0, ctrl, p1, width=3, dash=(14, 8))
            # arrowheads as short native direction lines along the curve ends
            note.line(p0[0] + (p1[0] - p0[0]) * 0.06, yv - 14, p0[0], p0[1], kind='DirectionLine', width=3)
            note.line(p1[0] - (p1[0] - p0[0]) * 0.06, yv - 14, p1[0], p1[1], kind='DirectionLine', width=3)
            mx, my = ctrl[0], (p0[1] + 2 * ctrl[1] + p1[1]) / 4
            lw = font('it', 34).getlength(lab) / K
            note.rect(mx - lw / 2 - 26, my - 28, mx + lw / 2 + 26, my + 28, width=1.8, fill=WHITE)
            one_line(note, lab, 'it', 34, mx, my - 22, align='c')

    # ---- legend + footer
    ly = rowy[2] - GY / 2 - 6
    items = [('rect', 'Observation'), ('quote', 'Quotation'), ('fig', 'Figure'), ('arrow', 'Cross-link'), ('ts', 'Episode time')]
    x = ccx - 980
    for kind, lab in items:
        if kind == 'rect':
            note.rect(x, ly - 14, x + 40, ly + 14, width=2)
        elif kind == 'quote':
            one_line(note, '“', 'title', 60, x + 4, ly - 26)
        elif kind == 'fig':
            note.stroke([(x - 4, ly + 2, 3000), (x + 44, ly + 2, 3000)], pen=15, width=24, color=G3)
            one_line(note, '#', 'num', 30, x + 20, ly - 18, align='c')
        elif kind == 'arrow':
            note.line(x, ly, x + 44, ly, kind='BidirectionalLine', width=2.5)
        elif kind == 'ts':
            one_line(note, '00:00', 'mono', 22, x, ly - 12)
            x += 40
        w = one_line(note, lab, 'capsl', 26, x + 60, ly - 12)
        x += 60 + w + 70
    foot = ('Native Boox note: every box, header, marker, arrow, arc and frame is an editable Boox shape. '
            'Text typeset from a machine transcript of the episode; figures checked against the CAAT/TSP report.')
    fb = Block(3600).add(foot, 'sub', 30, align='c')
    text(note, fb, W / 2 - 1800, H - 112, color=G1)

    size = note.save(out_note)
    return note, size


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default=str(HERE))
    a = ap.parse_args()
    content = json.loads((HERE / 'content.json').read_text())
    out = pathlib.Path(a.out) / 'what-the-uk-owes-to-yemen.note'
    build(content, str(out))                       # pass 1: largest body size each zone can take
    body = min(FIT.values()); print('zone maxima', dict(FIT))  # pass 2: one body size everywhere
    note, size = build(content, str(out), uniform=body)
    print(f"body text {body}pt")
    pens = {}
    for _, m, _ in note.shapes:
        pens[m['pen']] = pens.get(m['pen'], 0) + 1
    print(f'wrote {out} ({size / 1e6:.2f} MB), {len(note.shapes)} shapes, by pen_type {pens}')
