"""Decode a Boox .note (ZIP) and render a preview PNG, as a round-trip check of the writer.

Usage: python3 preview.py in.note out.png [scale]
Decodes note_info (page size), the #points blob (via its trailing index) and the shape
protobuf, then draws pen strokes, scanline fills and pen_type-40 geometry with Pillow.
"""
import io, json, math, struct, sys, zipfile
from PIL import Image, ImageDraw
from notefile import pb_fields


def argb(c):
    return ((c >> 16) & 255, (c >> 8) & 255, c & 255)


def load(path):
    z = zipfile.ZipFile(path)
    names = z.namelist()
    info = pb_fields(pb_fields(z.read([n for n in names if n.endswith('note/pb/note_info')][0]))[0][2])
    d = {f: v for f, w, v in info}
    W, H = d.get(22, 1860.0), d.get(23, 2480.0)
    pts_name = [n for n in names if n.endswith('#points') and '/stash/' not in n][0]
    blob = z.read(pts_name)
    ist = struct.unpack('>I', blob[-4:])[0]
    strokes = {}
    for k in range((len(blob) - 4 - ist) // 44):
        e = blob[ist + 44 * k: ist + 44 * k + 44]
        sid = e[:36].rstrip(b'\0').decode()
        off, size = struct.unpack('>II', e[36:])
        n = (size - 4) // 16
        strokes[sid] = [struct.unpack('>ffBBHI', blob[off + 4 + 16 * i: off + 20 + 16 * i]) for i in range(n)]
    shp = [n for n in names if '/shape/' in n and n.endswith('.zip') and '/stash/' not in n][0]
    iz = zipfile.ZipFile(io.BytesIO(z.read(shp)))
    metas = []
    for f, w, v in pb_fields(iz.read(iz.namelist()[0])):
        m = {}
        for ff, ww, vv in pb_fields(v):
            m[ff] = vv.decode() if ww == 2 and ff != 25 else vv
        metas.append(m)
    return W, H, strokes, metas


def render(path, out, scale=0.25):
    W, H, strokes, metas = load(path)
    S = scale
    im = Image.new('RGB', (int(W * S), int(H * S)), 'white')
    dr = ImageDraw.Draw(im)
    metas.sort(key=lambda m: m.get(2, 0))
    counts = {}
    for m in metas:
        pen = m.get(12); counts[pen] = counts.get(pen, 0) + 1
        col = argb(m.get(4, 0xFF000000)); th = m.get(5, 2.0)
        mat = json.loads(m[8])['values'] if 8 in m else [1, 0, 0, 0, 1, 0]
        X = lambda x, y: ((mat[0] * x + mat[1] * y + mat[2]) * S, (mat[3] * x + mat[4] * y + mat[5]) * S)
        if pen == 37:
            p = strokes.get(m[1], [])
            for i in range(0, len(p) - 1, 2):
                (x0, y0), (x1, y1) = X(p[i][0], p[i][1]), X(p[i + 1][0], p[i + 1][1])
                dr.rectangle([x0, y0, max(x0, x1 - 0.01), max(y0, y1 - 0.01)], fill=col)
        elif pen == 40:
            fc = json.loads(json.loads(m[20])['featureCollection'])
            for ft in fc['features']:
                g, pr = ft['geometry'], ft['properties']
                sub = pr.get('subType', '')
                w = max(1, int(pr['strokeAttr']['width'] * max(abs(mat[0]), abs(mat[4])) * S + 0.5)) if mat[0] != 1 else max(1, int(pr['strokeAttr']['width'] * S + .5))
                fill = argb(pr['fillAttr']['color'] & 0xFFFFFFFF) if pr['fillAttr']['enableColor'] else None
                sc = argb(pr['strokeAttr']['color'] & 0xFFFFFFFF)
                c = g['coordinates']
                if g['type'] == 'Polygon':
                    poly = [X(*e[0]) for e in c]
                    dr.polygon(poly, fill=fill, outline=sc, width=w)
                elif sub == 'Oval':
                    (x0, y0), (x1, y1) = X(*c[0]), X(*c[1])
                    dr.ellipse([min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)], fill=fill, outline=sc, width=w)
                elif sub == 'Arc':
                    (x0, y0), (x1, y1) = X(*c[0]), X(*c[1])
                    st, sw = c[2]
                    dr.arc([x0, y0, x1, y1], st, st + sw, fill=sc, width=w)
                elif sub == 'Curve':
                    (ax, ay), (bx, by), (cx, cy) = X(*c[0]), X(*c[1]), X(*c[2])
                    pts = [((1 - t) ** 2 * ax + 2 * (1 - t) * t * bx + t * t * cx,
                            (1 - t) ** 2 * ay + 2 * (1 - t) * t * by + t * t * cy) for t in [i / 40 for i in range(41)]]
                    dash_line(dr, pts, sc, w, pr.get('lineStyle'), S)
                elif sub == 'Bracket':
                    t, e1, e2 = X(*c[0]), X(*c[1]), X(*c[2])
                    for e in (e1, e2):
                        dr.line([t, (e[0], t[1] + (e[1] - t[1]) * 0.1), e], fill=sc, width=w)
                elif sub == 'WaveLine':
                    (x0, y0), (x1, y1) = X(*c[0]), X(*c[1])
                    L = math.hypot(x1 - x0, y1 - y0); wa = pr.get('waveAttr', {})
                    wl = (wa.get('wavyLength', 24) + 2) * S; pk = (wa.get('wavyPeak', 6)) * S
                    ux, uy = (x1 - x0) / L, (y1 - y0) / L
                    pts = [(x0 + ux * s - uy * pk * math.sin(2 * math.pi * s / wl), y0 + uy * s + ux * pk * math.sin(2 * math.pi * s / wl))
                           for s in [L * i / 200 for i in range(201)]]
                    dr.line(pts, fill=sc, width=w)
                else:
                    pts = [X(*q) for q in c]
                    dash_line(dr, pts, sc, w, pr.get('lineStyle'), S)
                    if g['type'] in ('DirectionLine', 'BidirectionalLine'):
                        arrow(dr, pts[-2], pts[-1], sc, w)
                        if g['type'] == 'BidirectionalLine':
                            arrow(dr, pts[1], pts[0], sc, w)
        else:
            p = strokes.get(m[1], [])
            for i in range(len(p) - 1):
                pr = (p[i][4] + p[i + 1][4]) / 2 / 4095
                if pen in (5, 22, 60, 61):
                    w = th * 1.37 * pr ** 0.59
                elif pen == 21:
                    w = th * 2.35 * pr ** 0.43
                else:
                    w = th
                c2 = (190, 190, 190) if pen == 15 else col
                dr.line([X(p[i][0], p[i][1]), X(p[i + 1][0], p[i + 1][1])], fill=c2, width=max(1, int(w * S + 0.5)))
    im.save(out)
    return counts, (W, H)


def dash_line(dr, pts, col, w, ls, S):
    if not ls or not ls.get('dashLineIntervals'):
        dr.line(pts, fill=col, width=w); return
    on, off = [v * S * 2 for v in ls['dashLineIntervals'][:2]]
    acc, draw = 0.0, True
    for a, b in zip(pts, pts[1:]):
        L = math.hypot(b[0] - a[0], b[1] - a[1]); s = 0.0
        while s < L:
            step = min((on if draw else off) - acc, L - s)
            if draw:
                p0 = (a[0] + (b[0] - a[0]) * s / L, a[1] + (b[1] - a[1]) * s / L)
                p1 = (a[0] + (b[0] - a[0]) * (s + step) / L, a[1] + (b[1] - a[1]) * (s + step) / L)
                dr.line([p0, p1], fill=col, width=w)
            s += step; acc += step
            if acc >= (on if draw else off) - 1e-9:
                acc, draw = 0.0, not draw


def arrow(dr, a, b, col, w):
    ang = math.atan2(b[1] - a[1], b[0] - a[0]); L = 6 + 3 * w
    for d in (2.6, -2.6):
        dr.line([b, (b[0] + L * math.cos(ang + d), b[1] + L * math.sin(ang + d))], fill=col, width=w)


if __name__ == '__main__':
    c, size = render(sys.argv[1], sys.argv[2], float(sys.argv[3]) if len(sys.argv) > 3 else 0.25)
    print('page', size, 'shapes by pen_type', c)
