// The one stage that cannot be parallel, and why.
//
// The two-pass scan walks the whole file once to build the descriptor table and
// again to decode it. On a source whose cost is transfer rather than CPU — a
// network share — that doubles wall-clock. It also holds the table in memory for
// the whole scan, and a compressed file carries one row span per row, so a few
// hundred million rows cost several GB.
//
// Fusing the two needs a sequential stage. `row_base` is a running total: the
// first row index of page N depends on every page before it, and the classifier
// uses it to cap the last page at the dataset's declared row count. Readers
// deliver extents out of order, so that stage keeps a small reorder buffer,
// parses each extent's page headers in file order, and hands the extent plus its
// descriptors to the decode pool.
//
// SOURCE: crates/sas7bdat/src/scan/fused.rs.

#import "@preview/cetz:0.4.2"
#import "style.typ": *

#let diagram = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ

  let arrow(a, b) = line(a, b, stroke: W-EDGE + ground, mark: (end: ">", scale: MARK-SCALE))

  // readers — wide
  content((0.9, 4.28), fine[readers · 4, concurrent])
  for (i, lab) in ("extent 3", "extent 1", "extent 4", "extent 2").enumerate() {
    let y = 3.5 - i * 0.62
    rect((0, y), (1.9, y + 0.48), fill: tint-flat)
    content((0.95, y + 0.24), fine(lab))
  }
  for i in range(4) { arrow((1.98, 3.74 - i * 0.62), (2.5, 2.5)) }

  // reorder buffer
  rect((2.56, 1.9), (3.72, 3.1), fill: tint-second, stroke: W-EDGE + second)
  content((3.14, 2.68), fine(text(fill: second, weight: "semibold")[reorder]))
  content((3.14, 2.36), fine(text(fill: second, weight: "semibold")[buffer]))
  arrow((3.8, 2.5), (4.36, 2.5))

  // the sequential stage — the point of the figure
  rect((4.36, 1.9), (7.12, 3.1), fill: tint-accent, stroke: W-MARK + accent)
  content((5.71, 2.74), fine(text(fill: accent, weight: "semibold")[parse — ONE thread]))
  content((5.74, 2.40), fine(text(fill: accent)[page headers, in file order]))
  content((5.71, 2.10), fine(text(fill: accent)[assigns row_base]))
  // decoders — wide again
  content((8.9, 4.28), fine[decoders · all cores])
  // Labelled, and in order: these are the same four extents the readers
  // produced out of order. Restoring that order is the reorder buffer's whole
  // job, and leaving these boxes blank hid the one thing the figure argues.
  for (i, lab) in ("extent 1", "extent 2", "extent 3", "extent 4").enumerate() {
    let y = 3.5 - i * 0.62
    rect((7.7, y), (10.1, y + 0.48), fill: tint-flat)
    content((8.9, y + 0.24), fine(lab))
  }
  // Fan from the parse box itself. This used to run (7.64,2.5)→(7.68,y): four
  // hundredths of a unit wide, so it drew as a vertical bar of arrowheads.
  for i in range(4) { arrow((7.20, 2.5), (7.66, 3.74 - i * 0.62)) }
  for i in range(4) { arrow((10.18, 3.74 - i * 0.62), (10.7, 2.5)) }

  // collector
  rect((10.76, 2.1), (12.4, 2.9), fill: tint-flat)
  content((11.58, 2.5), fine[collector])

  // the width of the pipeline, annotated under it
  line((0, 0.94), (12.4, 0.94))
  content((0.95, 0.66), fine[wide])
  content((5.71, 0.66), fine(text(fill: accent, weight: "semibold")[narrow]))
  content((8.9, 0.66), fine[wide])
  content((11.58, 0.66), fine[one])
})

#let panels = fig-style(block(width: FIG-W, {
  diagram
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      `row_base` is a running total — page *N*'s first row index depends on every
      page before it. That is why one stage is sequential, and why the reorder
      buffer exists at all: readers return extents in whatever order the storage
      gives them back.
    ])
  ]
  v(5pt)
  fine[
    What fusing buys, besides the second pass: the two-pass scan holds the whole
    descriptor table for the duration, and a compressed file carries one row span
    per row — a few hundred million rows cost several GB.
  ]
}))

#let body = panels
