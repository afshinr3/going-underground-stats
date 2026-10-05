"""Minimal writer for Boox Notes `.note` files (ZIP + protobuf + big-endian point blobs).

Format per the reverse-engineering notes in github.com/nrontsis/boox-note-optimizer (MIT) and
github.com/hhornbacher/boox-note-parser. We start from a real device-exported empty note
(template/empty.note, from boox-note-optimizer) and replace its IDs, page size and shape data.

Primitives written:
  * pen strokes        pen_type 2 ballpoint, 5 fountain, 15 highlighter, 21 marker, 22 charcoal,
                       60/61 calligraphy (binary #points + shape protobuf)
  * geometric shapes   pen_type 40, GeoJSON featureCollection in field 20 (native, editable)
  * scanline fills     pen_type 37, rectangles as corner pairs (used here for typeset text)
"""
import io, json, struct, time, uuid, zipfile

BLACK = 0xFF000000
WHITE = 0xFFFFFFFF


def grey(v):
    return 0xFF000000 | (v << 16) | (v << 8) | v


def _varint(n):
    out = bytearray()
    n &= (1 << 64) - 1
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def f_varint(field, n):
    return _varint(field << 3) + _varint(n)


def f_bytes(field, b):
    if isinstance(b, str):
        b = b.encode()
    return _varint(field << 3 | 2) + _varint(len(b)) + b


def f_float(field, x):
    return _varint(field << 3 | 5) + struct.pack('<f', x)


def _signed(c):
    return c - (1 << 32) if c & 0x80000000 else c


def _rv(b, o):
    r = s = 0
    while True:
        c = b[o]; o += 1; r |= (c & 0x7F) << s; s += 7
        if c < 0x80:
            return r, o


def pb_fields(b):
    o, out = 0, []
    while o < len(b):
        t, o = _rv(b, o); f, w = t >> 3, t & 7
        if f == 0:
            break
        if w == 0:
            v, o = _rv(b, o)
        elif w == 1:
            v = b[o:o + 8]; o += 8
        elif w == 5:
            v = struct.unpack('<f', b[o:o + 4])[0]; o += 4
        elif w == 2:
            l, o = _rv(b, o); v = b[o:o + l]; o += l
        else:
            raise ValueError(w)
        out.append((f, w, v))
    return out


def pb_rewrite(b, fn):
    """Re-encode a flat message; fn(field, wiretype, value) returns new value or None to keep."""
    out = bytearray()
    for f, w, v in pb_fields(b):
        nv = fn(f, w, v)
        v = v if nv is None else nv
        if w == 0:
            out += f_varint(f, v)
        elif w == 1:
            out += _varint(f << 3 | 1) + v
        elif w == 5:
            out += f_float(f, v)
        else:
            out += f_bytes(f, v)
    return bytes(out)


PEN_CONFIG = {"alphaFactor": 1.0, "displayScale": 0.9435484, "dpi": 320.0, "maxPressure": 4095.0,
              "newBrushRatio": 0.0, "pressure": 0.0, "pressureSensitivity": 0.3, "smoothLevel": 0.2,
              "source": 0, "tiltX": 0, "tiltY": 0}


def _bbox_json(x0, y0, x1, y1):
    return json.dumps({"bottom": y1, "empty": False, "left": x0, "right": x1, "stability": 0, "top": y0},
                      separators=(',', ':'))


def _fill_path(gen):
    return {"convex": True, "empty": False, "fillType": "WINDING", "generationId": gen,
            "inverseFillType": False, "mNativePath": 0, "pathIterator": {}}


class Note:
    def __init__(self, template_path, name, width, height):
        self.template = template_path
        self.name = name
        self.w, self.h = float(width), float(height)
        self.shapes = []        # (uuid, meta dict, points list or None)
        self.t0 = int(time.time() * 1000)
        self._n = 0

    # ---- low level -------------------------------------------------------------------------
    def _ts(self):
        self._n += 1
        return self.t0 + self._n

    def _add(self, meta, points=None):
        sid = str(uuid.uuid4())
        meta.setdefault('created', self._ts())
        self.shapes.append((sid, meta, points))
        return sid

    # ---- pen strokes -----------------------------------------------------------------------
    def stroke(self, pts, pen=2, width=3.0, color=BLACK, tilt=(20, 26)):
        """pts: list of (x, y) or (x, y, pressure 0..4095)."""
        P = []
        for i, p in enumerate(pts):
            pr = p[2] if len(p) > 2 else 2600
            P.append((float(p[0]), float(p[1]), tilt[0], tilt[1], int(max(1, min(4095, pr))), 0 if i == 0 else 4))
        xs = [p[0] for p in P]; ys = [p[1] for p in P]
        pad = width * 2
        meta = dict(pen=pen, width=float(width), color=color,
                    bbox=(min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad))
        return self._add(meta, P)

    # ---- scanline fill (typeset text) ------------------------------------------------------
    def scanfill(self, rects, color=BLACK):
        """rects: list of (x0, y0, x1, y1) in page coordinates."""
        if not rects:
            return None
        P = []
        for (x0, y0, x1, y1) in rects:
            P.append((x0, y0, 0, 0, 4095, 0))
            P.append((x1, y1, 0, 0, 4095, 0))
        xs = [r[0] for r in rects] + [r[2] for r in rects]
        ys = [r[1] for r in rects] + [r[3] for r in rects]
        meta = dict(pen=37, width=1.0, color=color, matrix=[1, 0, 0, 0, 1, 0],
                    bbox=(min(xs), min(ys), max(xs), max(ys)))
        return self._add(meta, P)

    # ---- geometric shapes (pen_type 40) ----------------------------------------------------
    def _geo(self, geom_type, coords, sub='', matrix=(1, 0, 0, 0, 1, 0), width=2.0, color=BLACK,
             fill=None, dash=None, universal='', props=None, bbox=None):
        a, b, tx, c, d, ty = matrix
        sw_local = width / max(abs(a), abs(d), 1e-6) if (a, d) != (1, 1) else width
        fill_c = _signed(fill) if fill is not None else 0
        p = {"fillAttr": {"color": fill_c, "enableColor": fill is not None}, "radius": 0.0,
             "selectionPointType": "SCALE",
             "strokeAttr": {"color": _signed(color), "colorTransformMode": 0, "enableColor": True,
                            "roundCorner": True, "width": sw_local},
             "subType": sub, "underContent": False, "useFixedRatio": False}
        if dash:
            p["lineStyle"] = {"dashLineIntervals": list(map(float, dash)), "phase": 0.0, "type": 1}
        if props:
            p.update(props)
        feat = {"displayFillColor": fill_c, "fillPath": _fill_path(1), "fromShapeType": -1,
                "geometry": {"coordinates": coords, "fillPath": _fill_path(2), "fromShapeType": -1,
                             "type": geom_type},
                "properties": p, "type": "Feature", "universalShapeType": "", "version": 1}
        fc = {"type": "FeatureCollection", "features": [feat], "fillPath": _fill_path(3), "fromShapeType": -1,
              "properties": {"radius": 0.0, "selectionPointType": "SCALE", "subType": sub,
                             "underContent": False, "useFixedRatio": False},
              "universalShapeType": universal, "version": 2}
        extra = json.dumps({"featureCollection": json.dumps(fc, separators=(',', ':'))}, separators=(',', ':'))
        meta = dict(pen=40, width=float(width), color=color, matrix=list(matrix), extra=extra,
                    fill=fill, bbox=bbox, dash=dash)
        return self._add(meta)

    @staticmethod
    def _box_matrix(x0, y0, x1, y1):
        w, h = max(x1 - x0, 1.0), max(y1 - y0, 1.0)
        return (w / 200.0, 0, x0, 0, h / 200.0, y0)

    def rect(self, x0, y0, x1, y1, **kw):
        m = self._box_matrix(x0, y0, x1, y1)
        coords = [[[0.0, 0.0], [200.0, 0.0]], [[200.0, 0.0], [200.0, 200.0]],
                  [[200.0, 200.0], [0.0, 200.0]], [[0.0, 200.0], [0.0, 0.0]]]
        return self._geo("Polygon", coords, matrix=m, universal="rectangle", bbox=(x0, y0, x1, y1), **kw)

    def oval(self, x0, y0, x1, y1, **kw):
        m = self._box_matrix(x0, y0, x1, y1)
        return self._geo("MultiPoint", [[0.0, 0.0], [200.0, 200.0]], sub="Oval", matrix=m,
                         bbox=(x0, y0, x1, y1), **kw)

    def polygon(self, pts, **kw):
        xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
        x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
        m = self._box_matrix(x0, y0, x1, y1)
        L = [[(x - x0) / m[0], (y - y0) / m[4]] for x, y in pts]
        coords = [[L[i], L[(i + 1) % len(L)]] for i in range(len(L))]
        return self._geo("Polygon", coords, matrix=m, bbox=(x0, y0, x1, y1), **kw)

    def line(self, x0, y0, x1, y1, kind="LineString", sub="", props=None, **kw):
        # kind: LineString | DirectionLine | BidirectionalLine ; sub "WaveLine" for wavy lines
        bb = (min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))
        return self._geo(kind, [[x0, y0], [x1, y1]], sub=sub, props=props, bbox=bb, **kw)

    def wave(self, x0, y0, x1, y1, length=24.0, peak=6.0, **kw):
        return self.line(x0, y0, x1, y1, sub="WaveLine",
                         props={"waveAttr": {"wavyLength": length, "wavyPeak": peak, "wavyOffset": 0.0}}, **kw)

    def curve(self, p0, ctrl, p1, **kw):
        xs = [p0[0], ctrl[0], p1[0]]; ys = [p0[1], ctrl[1], p1[1]]
        return self._geo("MultiPoint", [list(p0), list(ctrl), list(p1)], sub="Curve",
                         bbox=(min(xs), min(ys), max(xs), max(ys)), **kw)

    def arc(self, x0, y0, x1, y1, start_deg, sweep_deg, **kw):
        return self._geo("MultiPoint", [[x0, y0], [x1, y1], [float(start_deg), float(sweep_deg)]], sub="Arc",
                         bbox=(x0, y0, x1, y1), **kw)

    def bracket(self, tip, end1, end2, **kw):
        xs = [tip[0], end1[0], end2[0]]; ys = [tip[1], end1[1], end2[1]]
        return self._geo("MultiPoint", [list(tip), list(end1), list(end2)], sub="Bracket",
                         bbox=(min(xs), min(ys), max(xs), max(ys)), **kw)

    # ---- serialisation ---------------------------------------------------------------------
    def _shape_pb(self, page_id, points_doc, shape_doc):
        out = bytearray()
        for sid, m, _ in self.shapes:
            x0, y0, x1, y1 = m.get('bbox') or (0, 0, 0, 0)
            ls = {"lineStyle": {"phase": 0.0, "type": 1 if m.get('dash') else 0}}
            if m.get('dash'):
                ls["lineStyle"]["dashLineIntervals"] = list(map(float, m['dash']))
            inner = (f_bytes(1, sid) + f_varint(2, m['created']) + f_varint(3, m['created'])
                     + f_varint(4, m['color']) + f_float(5, m['width'])
                     + f_bytes(7, _bbox_json(x0, y0, x1, y1)))
            if 'matrix' in m:
                inner += f_bytes(8, json.dumps({"values": [float(v) for v in m['matrix']] + [0.0, 0.0, 1.0]},
                                               separators=(',', ':')))
            if m['pen'] != 40:
                inner += f_bytes(11, json.dumps(PEN_CONFIG, separators=(',', ':')))
            inner += f_varint(12, m['pen']) + f_bytes(16, points_doc)
            inner += f_bytes(17, json.dumps(ls, separators=(',', ':'))) + f_bytes(18, shape_doc)
            if m['pen'] == 40:
                inner += f_bytes(20, m['extra'])
            inner += f_bytes(21, "[]")
            if m.get('fill') is not None:
                inner += f_varint(23, m['fill'])
            inner += f_bytes(26, '{"repo":{}}')
            out += f_bytes(1, inner)
        return bytes(out)

    def _points_blob(self, page_id, points_doc):
        head = struct.pack('>I', 1) + page_id.ljust(36).encode() + points_doc.encode()
        body, index = bytearray(), bytearray()
        off = len(head)
        for sid, m, pts in self.shapes:
            if pts is None:
                continue
            blk = bytearray(b'\0\0\0\0')
            for (x, y, tx, ty, pr, dt) in pts:
                blk += struct.pack('>ffBBHI', x, y, tx, ty, pr, dt)
            index += sid.encode().ljust(36, b'\0') + struct.pack('>II', off, len(blk))
            body += blk
            off += len(blk)
        return head + bytes(body) + bytes(index) + struct.pack('>I', off)

    def save(self, path):
        src = zipfile.ZipFile(self.template)
        names = src.namelist()
        old_note = names[0].split('/')[0]
        old_page = [n for n in names if '/point/' in n and n.endswith('#points')][0].split('/')[-1].split('#')[0]
        old_pdoc = [n for n in names if n.endswith('#points')][0].split('#')[1]
        old_shape = [n for n in names if '/shape/' in n and n.endswith('.zip') and '/stash/' not in n][0]
        old_sdoc = old_shape.split('#')[1]
        new_note, new_page = uuid.uuid4().hex, uuid.uuid4().hex
        new_pdoc, new_sdoc = str(uuid.uuid4()), str(uuid.uuid4())
        ts = str(self.t0)
        repl = [(old_note, new_note), (old_page, new_page), (old_pdoc, new_pdoc), (old_sdoc, new_sdoc)]
        # the template's cloud/account id (note_info field 39, extra field 3) is not ours
        acct = [v.decode() for f, w, v in pb_fields(pb_fields(src.read(f"{old_note}/note/pb/note_info"))[0][2])
                if f == 39 and w == 2]
        if acct:
            repl.append((acct[0], uuid.uuid4().hex[:len(acct[0])]))

        def swap(s):
            for a, b in repl:
                s = s.replace(a, b)
            return s

        W, H = self.w, self.h
        rect = {"bottom": H, "empty": False, "left": 0.0, "right": W, "stability": 0, "top": 0.0}

        def fix_json(text):
            try:
                v = json.loads(text)
            except Exception:
                return None
            def walk(o):
                if isinstance(o, dict):
                    # keep each number's original JSON type (the device writes ints in some places)
                    same = lambda old, new: int(new) if isinstance(old, int) else float(new)
                    if {'left', 'right', 'top', 'bottom'} <= set(o):
                        o['right'], o['bottom'] = same(o['right'], W), same(o['bottom'], H)
                    if isinstance(o.get('width'), (int, float)) and o['width'] in (1860, 1860.0):
                        o['width'] = same(o['width'], W)
                    if isinstance(o.get('height'), (int, float)) and o['height'] in (2480, 2480.0):
                        o['height'] = same(o['height'], H)
                    if 'contentId' in o:
                        o['contentId'] = "0"; o['contentRelativePath'] = ""
                    for x in o.values():
                        walk(x)
                elif isinstance(o, list):
                    for x in o:
                        walk(x)
            walk(v)
            return json.dumps(v, separators=(',', ':'), ensure_ascii=False)

        def fix_msg(b, top_name=None):
            def fn(f, w, v):
                if w == 2:
                    try:
                        s = v.decode()
                    except UnicodeDecodeError:
                        return None
                    if top_name and f == 6:
                        return top_name.encode()
                    if top_name and f == 14:
                        return None                      # device info: leave as exported
                    j = fix_json(s) if s[:1] in '{[' else None
                    if j is not None:
                        return swap(j).encode()
                    return swap(s).encode()
                if w == 5 and top_name and f in (22, 23):
                    return W if f == 22 else H
                if w == 0 and f in (2, 3, 5, 6) and v > 10**12:
                    return self.t0
                return None
            return pb_rewrite(b, fn)

        def wrapped(b, top_name=None):
            # messages wrapped as repeated field-1 submessages
            out = bytearray()
            for f, w, v in pb_fields(b):
                out += f_bytes(f, fix_msg(v, top_name)) if w == 2 else b''
            return bytes(out)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as out:
            for n in names:
                if '/stash/' in n and not n.endswith('stash/'):
                    continue
                nn = swap(n)
                if n.endswith('/'):
                    out.writestr(zipfile.ZipInfo(nn), b'')
                    continue
                data = src.read(n)
                if n.endswith('note/pb/note_info'):
                    data = wrapped(data, top_name=self.name)
                elif '/virtual/page/pb/' in n or '/pageModel/pb/' in n:
                    data = wrapped(data)
                elif '/virtual/doc/pb/' in n or n.endswith('extra/pb/extra'):
                    data = fix_msg(data)
                elif n.endswith('#points'):
                    data = self._points_blob(new_page, new_pdoc)
                elif n == old_shape:
                    nn = f"{new_note}/shape/{new_page}#{new_sdoc}#{ts}.zip"
                    inner_name = nn.split('/')[-1][:-4]
                    ib = io.BytesIO()
                    with zipfile.ZipFile(ib, 'w', zipfile.ZIP_DEFLATED) as iz:
                        iz.writestr(inner_name, self._shape_pb(new_page, new_pdoc, new_sdoc))
                    data = ib.getvalue()
                out.writestr(nn, data)
        with open(path, 'wb') as fh:
            fh.write(buf.getvalue())
        return len(buf.getvalue())
