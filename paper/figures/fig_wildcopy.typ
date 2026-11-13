// Why a three-byte run writes thirty-two bytes.
//
// The short RLE command families encode runs of at most 32 bytes — copies via
// commands 8 and 9 (1..=32), fills via 12 through 15 (2..=18). At those sizes a
// call into memcpy or memset costs about as much as the bytes it moves.
//
// So the short forms store a fixed 32 bytes regardless of the true run length and
// then truncate back, which is what LZ4 and zstd call wildcopy. The long-form
// commands carry their own length bytes and are meant for long runs, where the
// libc call is the right tool; they keep it.
//
// The reason this is safe code rather than `unsafe`: the row buffer is reserved
// with WILD_OVERRUN of slack, so the overshoot always lands inside capacity the
// vector already owns. `extend_from_slice` with a fixed-size array cannot
// reallocate, and `truncate` on u8 only moves a length.
//
// MEASUREMENT, from the same doc comment: profiling a production-shaped file
// (14 columns, RLE) put `_platform_memset` and `_platform_memmove` at 678 samples
// against `decompress_row`'s own 721.

#import "@preview/cetz:0.4.2"
#import "style.typ": *

#let W = 12.8 // label column + this must stay inside FIG-W; see figures/README.md
#let CELL = W / 40 // one byte

#let diagram = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ

  let row(y, label) = content((-0.14, y + 0.22), anchor: "east", fine(label))

  // ── 1. the run the command asks for
  row(3.5, [the run: 3 bytes])
  rect((0, 3.5), (3 * CELL, 3.94), fill: tint-accent, stroke: W-EDGE + accent)
  for i in range(1, 3) {
    line((i * CELL, 3.5), (i * CELL, 3.94), stroke: W-HAIR + accent)
  }

  // ── 2. what is actually stored
  row(2.3, [one fixed store: 32 bytes])
  rect((0, 2.3), (3 * CELL, 2.74), fill: tint-accent, stroke: W-EDGE + accent)
  rect((3 * CELL, 2.3), (32 * CELL, 2.74), fill: tint-wrong, stroke: W-EDGE + wrong)
  content((17.5 * CELL, 2.52), fine(text(fill: wrong)[29 bytes nobody asked for]))

  // ── 3. the buffer, and where the slack is
  row(1.1, [the row buffer])
  rect((0, 1.1), (24 * CELL, 1.54), fill: tint-flat)
  rect((24 * CELL, 1.1), (40 * CELL, 1.54), fill: none, stroke: (paint: ground, thickness: W-HAIR, dash: "dashed"))
  content((12 * CELL, 1.32), fine[expected row length])
  content((32 * CELL, 1.32), fine(text(fill: ground)[reserved slack]))

  // the overshoot lands in the slack, not past the allocation
  line((32 * CELL, 2.26), (32 * CELL, 1.60), stroke: (paint: wrong, thickness: W-EDGE, dash: "dashed"), mark: (end: ">", scale: MARK-SCALE))

  // ── 4. after truncate
  row(0.0, [after truncate])
  rect((0, 0.0), (3 * CELL, 0.44), fill: tint-accent, stroke: W-EDGE + accent)
  content((3.4 * CELL, 0.22), anchor: "west", fine[len moves; not one byte is rewritten])
})

#let panels = fig-style(block(width: FIG-W, {
  diagram
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      The short RLE families cap at 32 bytes, which is where a `memcpy` costs about
      what the bytes cost. The buffer is reserved with `WILD_OVERRUN` of slack, so
      the overshoot lands inside capacity the vector already owns — which is what
      keeps this safe code rather than `unsafe`.
    ])
  ]
  v(6pt)
  // A grid, not a run of text with linebreaks: "*678 samples*" was one bold
  // span, and the line wrapped inside it, leaving "samples" alone and bold on
  // its own line. Two measured numbers being compared want a column.
  note(width: 6.2cm)[
    #fine(text(fill: ink)[
      Profiled on a 14-column RLE file, in samples:
      #v(3pt)
      #grid(
        columns: (1fr, auto),
        column-gutter: 8pt,
        row-gutter: 2pt,
        align: (left, right),
        [`_platform_memset` + `_platform_memmove`], strong[678],
        [`decompress_row` itself], strong[721],
      )
    ])
  ]
}))

#let body = panels
