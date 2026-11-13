// The anatomy of a sas7bdat file.
//
// The reverse-engineered descriptions of this format are field tables: offset,
// width, meaning, one row per field. They are correct and they are not
// illustrative, because they cannot show the thing that actually makes the format
// awkward — that TWO SINGLE BYTES near the front rewrite the table itself.
//
//   byte 32 (`a2`) == 0x33  ->  every pointer in the file is 8 bytes, not 4.
//                               Page headers go 24 -> 40, subheader pointers
//                               12 -> 24.
//   byte 35 (`a1`) == 0x33  ->  four bytes of padding are inserted, so every
//                               offset after it shifts.
//
// A table has to be printed twice to say that, or printed once with a footnote.
// Drawn, it is one panel.
//
// The third panel is the page, which is the other thing a field table flattens: a
// page is not a sequence, it is two allocators pointed at each other, and where
// they meet is the page's capacity.
//
// SOURCE: crates/sas7bdat/src/probe.rs (header, the two alignment bytes, the
// magic), crates/sas7bdat/src/internal.rs (the page-type masks),
// crates/sas7bdat/src/pages.rs (subheader pointers and `contiguous_data_start`).

#import "@preview/cetz:0.4.2"
#import "style.typ": *

// ── A · the file ───────────────────────────────────────────────────────────

#let panel-file = cetz.canvas({
  import cetz.draw: *
  let w = 14.6
  let y = 0.0
  let h = 0.56

  let seg(x0, x1, fill, stroke-col, label, sub) = {
    rect((x0, y), (x1, y + h), fill: fill, stroke: W-EDGE + stroke-col)
    content(((x0 + x1) / 2, y + h / 2 + 0.10), fine(label))
    if sub != none { content(((x0 + x1) / 2, y + h / 2 - 0.13), fine(text(fill: muted, sub))) }
  }

  seg(0, 2.6, tint-accent, accent, [32-byte magic], [sas7bdat or sas7bcat])
  seg(2.6, 3.5, tint-second, second, [a2 · a1], none)
  seg(3.5, 6.4, tint-flat, hairline, [the rest of the header], [name, timestamps, sizes])
  seg(6.4, 9.1, tint-flat, hairline, [page 0], none)
  seg(9.1, 11.8, tint-flat, hairline, [page 1], none)
  seg(11.8, w, tint-flat, hairline, [page 2 …], none)

  // the header is a page-sized unit of its own
  line((0, y - 0.16), (6.4, y - 0.16), stroke: W-HAIR + muted)
  content((3.2, y - 0.40), fine[header — at least 1024 bytes, its own length in it])
  line((6.4, y - 0.16), (w, y - 0.16), stroke: W-HAIR + muted)
  content((10.5, y - 0.40), fine[page_count pages of page_size each])

  // the two bytes, called out upward
  line((3.05, y + h + 0.06), (3.05, y + h + 0.44), stroke: W-EDGE + second)
  content((3.05, y + h + 0.56), anchor: "south", fine(text(fill: second, weight: "semibold")[bytes 32 and 35 decide the shape of everything after them]))
})

// ── B · what those two bytes do ────────────────────────────────────────────

#let variant(title, ptr, page-hdr, sub-ptr, tint, edge) = block(
  width: 4.7cm,
  fill: tint,
  stroke: W-EDGE + edge,
  inset: (x: 8pt, y: 7pt),
  radius: 2pt,
)[
  #fine(text(fill: edge, weight: "semibold", title))
  #v(3pt)
  #grid(
    columns: (auto, 1fr),
    row-gutter: 2.5pt,
    column-gutter: 6pt,
    fine[pointer width], fine(text(fill: ink, ptr)),
    fine[page header], fine(text(fill: ink, page-hdr)),
    fine[subheader pointer], fine(text(fill: ink, sub-ptr)),
  )
]

#let panel-bytes = block(width: FIG-W, {
  grid(
    columns: (auto, 1fr),
    column-gutter: 10pt,
    align: (top, top),
    block(width: 3.4cm)[
      #fine(text(fill: second, weight: "semibold")[a2 · byte 32])
      #v(2pt)
      #fine[`0x33` → 64-bit \ anything else → 32-bit]
      #v(7pt)
      #fine(text(fill: second, weight: "semibold")[a1 · byte 35])
      #v(2pt)
      #fine[`0x33` → insert 4 bytes \ of padding, so every \ offset after it shifts]
    ],
    grid(
      columns: (1fr, 1fr),
      column-gutter: 8pt,
      variant("32-bit layout", "4 bytes", "24 bytes", "12 bytes", tint-flat, ground),
      variant("64-bit layout", "8 bytes", "40 bytes", "24 bytes", tint-accent, accent),
    ),
  )
})

// ── C · a page ─────────────────────────────────────────────────────────────

#let panel-page = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ
  let w = 8.4
  let h = 3.5

  rect((0, 0), (w, h), stroke: W-EDGE + hairline)

  // header
  rect((0, h - 0.5), (w, h), fill: tint-flat)
  content((w / 2, h - 0.25), fine[page header — type, block count, subheader count])

  // pointers descend
  for i in range(3) {
    let yy = h - 0.5 - (i + 1) * 0.42
    rect((0, yy), (w, yy + 0.38), fill: tint-accent, stroke: W-HAIR + accent)
    content((0.9, yy + 0.19), fine(text(fill: accent)[pointer #i]))
    content((w - 2.4, yy + 0.19), fine(text(fill: muted)[offset · length · type]))
  }
  line((w + 0.22, h - 0.56), (w + 0.22, h - 1.82), stroke: W-EDGE + accent, mark: (end: ">", scale: MARK-SCALE))
  content((w + 0.36, h - 1.2), anchor: "west", fine(text(fill: accent)[grow down]))

  // rows ascend
  for i in range(2) {
    let yy = 0.10 + i * 0.42
    rect((0, yy), (w, yy + 0.38), fill: tint-flat)
    content((0.7, yy + 0.19), fine[row])
  }
  line((w + 0.22, 0.94), (w + 0.22, 0.14), stroke: W-EDGE + ground, mark: (end: ">", scale: MARK-SCALE))
  content((w + 0.36, 0.54), anchor: "west", fine[grow up])

  // where they meet
  line((0, 1.06), (w, 1.06), stroke: (paint: second, thickness: W-EDGE, dash: "dashed"))
  content((w / 2, 1.34), fine(text(fill: second)[where they meet is the page's capacity]))
})

#let page-kinds = block(width: 6.0cm, {
  fine(text(weight: "semibold")[page type, masked with 0x0F00])
  v(4pt)
  grid(
    columns: (auto, 1fr),
    row-gutter: 2.5pt,
    column-gutter: 7pt,
    fine[`0x0000`], fine[meta — subheaders only],
    fine[`0x0100`], fine[data — rows only],
    fine[`0x0200`], fine[mix — both, on one page],
    fine[`0x0400`], fine[amd — amended],
  )
  v(5pt)
  fine(text(fill: muted)[
    Three more are tested *before* the mask, so they win: `0x9000` comp,
    `0x8000` comp-table, `0x4000` meta2.
  ])
})

#let panels = fig-style(block(width: FIG-W, {
  lead[A · the file]
  v(GAP-TITLE)
  panel-file
  v(GAP-PANEL)
  lead[B · what those two bytes do to every offset after them]
  v(GAP-TITLE)
  panel-bytes
  v(GAP-PANEL)
  lead[C · one page, from both ends]
  v(GAP-TITLE)
  grid(
    columns: (auto, 1fr),
    column-gutter: 8pt,
    align: (top, top),
    panel-page,
    page-kinds,
  )
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      Two single bytes near the front rewrite the offset table for everything
      behind them: byte 32 widens every pointer from four to eight, and byte 35
      inserts four bytes of padding. That is why a field table of offsets and
      widths has to be printed twice to describe this format — and why a page is
      not a sequence but two allocators growing toward each other, with the
      point where they meet as the page's capacity.
    ])
  ]
}))

#let body = panels
