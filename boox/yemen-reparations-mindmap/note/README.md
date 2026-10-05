# What the UK Owes to Yemen: native Boox note

`what-the-uk-owes-to-yemen.note` is a **Boox Notes file**, not a picture.

On the Note Max, open it from the file manager, or from Downloads after fetching the raw link, and it opens in Notes.
It holds a single 7440 × 5580 pt page (4× a standard page, 4:3 like the Note Max screen) that you pan and zoom like an infinite canvas.

What's inside (589 native objects):

| Boox object | pen_type | Used for |
|---|---|---|
| Geometric shapes (editable with the shape/lasso tools) | 40 | Header shapes (hexagon, framed rectangle, ellipse, parallelogram, octagon, inverted rectangle, rhombus, pennant), node boxes (solid, grey-filled, dashed, dotted, shadowed, double, bracketed, chamfered), bullet and end markers (dot, ring, diamond, square, triangle, star), the centre medallion's ovals, ticks and arcs, wave lines, curves, direction and bidirectional arrows, the frame |
| Ballpoint / fountain / marker / charcoal / calligraphy strokes | 2 / 5 / 21 / 22 / 61 | A different pen for each branch's spine and ribs; tapered fountain-pen strokes from the centre |
| Calligraphy brush | 60 | The flourish under the title |
| Highlighter | 15 | Grey swashes behind the headline figures |
| Scanline fill | 37 | Text typeset in IBM Plex Sans (regular, italic, condensed) and IBM Plex Mono, for legibility on e-ink |

`preview.png` is rendered by `preview.py`, which decodes the `.note` back from its ZIP, protobuf and point data.

## How it's built

- `content.json`: the mind map content (from a machine transcript of the episode, figures checked against the CAAT/TSP report).
- `build_note.py`: layout and typesetting. Rebuild with `python3 build_note.py` (needs pillow and numpy).
- `notefile.py`: a small `.note` writer. It starts from a real device-exported empty note (`template/empty.note`, from [boox-note-optimizer](https://github.com/nrontsis/boox-note-optimizer), MIT, licence alongside) and replaces its IDs, page size, point blob and shape protobuf.
- The format follows the reverse-engineering notes in [nrontsis/boox-note-optimizer](https://github.com/nrontsis/boox-note-optimizer) and [hhornbacher/boox-note-parser](https://github.com/hhornbacher/boox-note-parser).

## Known limits

- **Infinite note type.** No infinite note has been published to copy from, so this is a large standard note (`canvasExpandType: DEFAULT`). One blank Infinite Note exported from the Note Max as `.note` is enough to switch it to the true infinite type.
- **Text.** Text is ink, not Boox text boxes: the native text-box encoding hasn't been verified on a device. You can lasso it, move it and erase it, but not retype it.
