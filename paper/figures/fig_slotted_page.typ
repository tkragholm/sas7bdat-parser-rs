// The three ways a row is found, and what each costs.
//
// This is the figure the format's mental model turns on. A slot directory exists
// to locate VARIABLE-LENGTH records. Uncompressed rows in a sas7bdat are all
// exactly `row_length` bytes, so they need no slots — they are packed into the
// data region and located by multiplying. Compressed rows are not the same size
// as each other, so they go back into the directory, one slot each.
//
// That single difference is behind almost everything anyone notices about
// performance: whether a page can be decoded from its header alone, and whether
// the reader has to carry per-row bookkeeping. A compressed file carries one span
// per row, and a few hundred million rows cost several GB of span table before
// any data is decoded.
//
// The middle case is the one that makes the format feel arbitrary until the rule
// is stated: a slot may hold a BLOCK of uncompressed rows, which the reader walks
// in row_length strides. So "slots hold variable-length records" is right, and a
// record is sometimes itself an array of fixed-length ones.
//
// SOURCE: crates/sas7bdat/src/internal.rs (`PageExecClass`),
// crates/sas7bdat/src/pages.rs (`contiguous_data_start`, `parse_pointer`, and the
// stride walk at the `is_compressed_data` branch).

#import "@preview/cetz:0.4.2"
#import "style.typ": *

#let W = 4.5
#let H = 3.4

#let page-frame(name, tint) = {
  import cetz.draw: *
  rect((0, 0), (W, H), stroke: W-EDGE + hairline)
  rect((0, H - 0.42), (W, H), fill: tint-flat, stroke: W-HAIR + hairline)
  content((W / 2, H - 0.21), fine[page header])
}

#let case-a = cetz.canvas({
  import cetz.draw: *
  page-frame("contiguous", none)
  // no slots at all
  content((W / 2, H - 0.78), fine(text(fill: muted)[no slot directory]))
  for i in range(5) {
    let y = 0.18 + i * 0.46
    rect((0.3, y), (W - 0.3, y + 0.40), fill: tint-accent, stroke: W-HAIR + accent)
    content((W / 2, y + 0.20), fine(text(fill: accent)[row #(4 - i)]))
  }
  line((0.12, 0.18), (0.12, 2.48), stroke: W-EDGE + accent)
  content((-0.06, 1.33), anchor: "east", angle: 90deg, fine(text(fill: accent)[stride = row_length]))
})

#let case-b = cetz.canvas({
  import cetz.draw: *
  page-frame("blocked", none)
  rect((0, H - 0.86), (W, H - 0.44), fill: tint-second, stroke: W-HAIR + second)
  content((W / 2, H - 0.65), fine(text(fill: second)[1 slot → offset, length]))
  rect((0.3, 0.46), (W - 0.3, 2.26), fill: tint-accent, stroke: W-EDGE + accent)
  for i in range(1, 4) {
    line((0.3, 0.46 + i * 0.45), (W - 0.3, 0.46 + i * 0.45), stroke: W-HAIR + accent)
  }
  content((W / 2, 2.46), fine(text(fill: accent)[one record, four rows in it]))
  content((W / 2, 0.24), fine(text(fill: muted)[walked in strides]))
})

#let case-c = cetz.canvas({
  import cetz.draw: *
  page-frame("compressed", none)
  for i in range(3) {
    let y = H - 0.86 - i * 0.30
    rect((0, y), (W, y + 0.28), fill: tint-second, stroke: W-HAIR + second)
    content((W / 2, y + 0.14), fine(text(fill: second)[slot #i]))
  }
  for (i, ww) in (2.6, 3.4, 1.9).enumerate() {
    let y = 0.30 + i * 0.46
    rect((0.3, y), (0.3 + ww, y + 0.40), fill: tint-accent, stroke: W-HAIR + accent)
  }

  content((W / 2, 0.10), fine(text(fill: accent)[each a different length]))
})

#let col(title, sub, canvas, cost) = block(width: 100%, {
  lead(title)
  linebreak()
  fine(text(fill: muted, sub))
  v(6pt)
  canvas
  v(9pt)
  note(width: 100%)[
    #fine(text(fill: ink, cost))
  ]
})

#let panels = fig-style(block(width: FIG-W, {
  grid(
    columns: (1fr, 1fr, 1fr),
    column-gutter: 8mm,
    align: (top, top, top),
    col(
      "Contiguous",
      "uncompressed rows, packed",
      case-a,
      [Found by *multiplying*. No per-row bookkeeping at all, and the page can be
       decoded from its header.],
    ),
    col(
      "Blocked",
      "uncompressed rows, in one slot",
      case-b,
      [One slot, many rows. Still a stride — the slot only says where the block
       starts and how long it is.],
    ),
    col(
      "Indexed",
      "compressed rows, one slot each",
      case-c,
      [*One span per row.* A few hundred million rows cost several GB of span
       table before any data is decoded.],
    ),
  )
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      A slot directory exists to locate *variable-length* records. Uncompressed
      rows are all exactly `row_length` bytes, so they do not need one — which is
      why turning compression on changes not just the bytes but the way the whole
      file is read.
    ])
  ]
}))

#let body = panels
