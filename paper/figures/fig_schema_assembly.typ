// A column is a join across six subheader families, not a record.
//
// This is the other thing a field table cannot show. Reading the schema out of a
// sas7bdat is not a scan: no subheader describes a column. One family carries the
// names, another the widths and types, a third the formats, and the parser joins
// them by ordinal position. That is why layout.rs has a `ColumnBuilder` and a
// `MetadataState` rather than a parse function, and why a truncated metadata page
// loses a FACET of every column rather than the last few columns.
//
// The names themselves are not stored inline either. A name is a `TextRef` —
// (blob index, offset, length) — into a set of concatenated text blobs delivered
// by the COLUMN_TEXT subheaders. So the parser borrows; it never materialises a
// schema's worth of strings.
//
// SOURCE: crates/sas7bdat/src/pages.rs (the signatures),
// crates/sas7bdat/src/layout.rs (`TextRef`, `TextStore`, `ColumnBuilder`).

#import "@preview/cetz:0.4.2"
#import "style.typ": *

#let SIGS = (
  ("F7F7F7F7", "row size", "row length, row count", false),
  ("F6F6F6F6", "column size", "how many columns", false),
  ("FFFFFFFD", "column text", "the blobs the rest point into", true),
  ("FFFFFFFF", "column name", "a TextRef, per column", true),
  ("FFFFFFFC", "column attrs", "width, type, offset in the row", true),
  ("FFFFFBFE", "column format", "format and label TextRefs", true),
)

#let panel-join = cetz.canvas({
  import cetz.draw: *
  let x0 = 0.0
  let bw = 7.0
  let rh = 0.52
  let gap = 0.10

  for (i, (sig, name, what, feeds)) in SIGS.enumerate() {
    let y = (SIGS.len() - 1 - i) * (rh + gap)
    let col = if feeds { accent } else { ground }
    let tint = if feeds { tint-accent } else { tint-flat }
    rect((x0, y), (x0 + bw, y + rh), fill: tint, stroke: W-HAIR + col)
    content((x0 + 0.62, y + rh / 2), fine(text(fill: col, weight: "semibold", sig)))
    content((x0 + 1.9, y + rh / 2), anchor: "west", fine(text(fill: ink, name)))
    content((x0 + 3.6, y + rh / 2), anchor: "west", fine(text(fill: muted, what)))
    if feeds {
      line((x0 + bw + 0.06, y + rh / 2), (x0 + bw + 1.1, 1.62), stroke: W-HAIR + accent, mark: (end: ">", scale: MARK-SCALE))
    }
  }

  // the assembled column
  rect((bw + 1.16, 0.72), (bw + 5.6, 2.52), fill: white, stroke: W-MARK + accent)
  content((bw + 1.44, 2.26), anchor: "west", fine(text(fill: accent, weight: "semibold")[one column, assembled by index]))
  content((bw + 1.44, 1.92), anchor: "west", fine[name → TextRef])
  content((bw + 1.44, 1.62), anchor: "west", fine[width · type · offset in the row])
  content((bw + 1.44, 1.32), anchor: "west", fine[format · label → TextRefs])
  content((bw + 1.44, 0.98), anchor: "west", fine(text(fill: second)[nothing joins them but ordinal position]))
})

#let panel-textref = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ

  // the ref
  rect((0, 0.9), (2.5, 1.86), fill: tint-accent, stroke: W-EDGE + accent)
  content((1.25, 1.66), fine(text(fill: accent, weight: "semibold")[TextRef]))
  content((1.25, 1.36), fine[index · offset · length])
  content((1.25, 1.08), fine(text(fill: muted)[three u16s]))

  line((2.58, 1.38), (3.7, 1.38), stroke: W-EDGE + accent, mark: (end: ">", scale: MARK-SCALE))

  // the blobs
  for (i, lab) in ("blob 0", "blob 1", "blob 2").enumerate() {
    let x = 3.8 + i * 1.85
    rect((x, 1.72), (x + 1.7, 2.16), fill: tint-flat)
    content((x + 0.85, 1.94), fine(lab))
  }

  // one blob's bytes, with the slice picked out
  rect((3.8, 0.62), (9.35, 1.30), fill: tint-flat)
  rect((5.55, 0.62), (7.05, 1.30), fill: tint-accent, stroke: W-EDGE + accent)
  content((4.65, 0.96), fine(text(fill: muted)[…AGE]))
  content((6.30, 0.96), fine(text(fill: ink, weight: "semibold")[BIRTHWT]))
  content((8.20, 0.96), fine(text(fill: muted)[PARITY…]))
  line((5.65, 1.66), (5.65, 1.36), stroke: W-HAIR + accent)

  content((3.8, 0.22), anchor: "west", fine(text(fill: muted)[
    names, labels and formats are all slices of this
  ]))
})

#let panels = fig-style(block(width: FIG-W, {
  lead[A · six families, and a column is the join across them]
  v(GAP-TITLE)
  panel-join
  v(GAP-PANEL)
  lead[B · and a name is a reference, not a string]
  v(GAP-TITLE)
  panel-textref
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      Two consequences that fall straight out of the picture: a truncated metadata
      page loses a *facet of every column* rather than the last few columns, and
      repeated names cost nothing because they are the same slice twice.
    ])
  ]
}))

#let body = panels
