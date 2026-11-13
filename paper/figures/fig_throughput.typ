// Read throughput across every reader, on a log scale, for three files.
//
// A log scale is not a stylistic choice here. The span is 32 MB/s to 3555 — a
// factor of 111 — and on a linear axis the bottom four readers are invisible.
//
// The ordering is the obvious part of this figure. The part worth drawing is that
// `rust-core`, `sas7bdat-polars` and `fastsas` are ONE ENGINE seen through three
// different amounts of language boundary: 3436, 2308 and 739 on the same file. So
// the gaps between those three measure the cost of getting the data into a host
// language, not seven different parsers.
//
// Three files, because a single file would leave the ordering looking more stable
// than it is: fastsas moves from 739 to 1443 between AP and NYYTS, and pandas
// from 258 to 216 the other way.
//
// DATA: benchmarks/README.md, Table A. In-memory read, file to table, MB/s.
// Not re-measured here.

#import "@preview/cetz:0.4.2"
#import "style.typ": *

// (reader, threads, ours, AP 99 MB, NYYTS 193 MB, AHS 2.15 GB)
#let ROWS = (
  ("rust-core", "12t", true, 3436, 3555, 3232),
  ("sas7bdat-polars", "12t", true, 2308, 2377, 1816),
  ("fastsas (R)", "12t", true, 739, 1443, 482),
  ("rust-core, serial", "1t", true, 572, 1047, 1201),
  ("pandas", "1t", false, 258, 216, 429),
  ("pyreadstat", "1t", false, 90, 128, 103),
  ("haven (R)", "1t", false, 47, 60, 32),
)

#let W = 9.7
#let LO = 30.0
#let HI = 4000.0

// log10 position, normalised to the axis
#let lx(v) = (calc.log(v) - calc.log(LO)) / (calc.log(HI) - calc.log(LO)) * W

#let chart = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ
  let rh = 0.30
  let gap = 0.10
  let group = 3 * rh + gap * 2 + 0.30

  // gridlines first, so bars sit on them
  for t in (100, 1000) {
    line((lx(t), -0.2), (lx(t), ROWS.len() * group), stroke: W-HAIR + rule-soft)
  }

  for (i, (name, threads, ours, a, b, c)) in ROWS.enumerate() {
    let y0 = (ROWS.len() - 1 - i) * group
    let col = if ours { accent } else { ground }
    for (j, v) in (a, b, c).enumerate() {
      let y = y0 + (2 - j) * (rh + gap)
      rect((0, y), (lx(v), y + rh), fill: col.lighten(j * 24%), stroke: none)
    }
    content((-0.16, y0 + group / 2 - 0.18), anchor: "east", fine(text(
      fill: if ours { ink } else { muted },
      weight: if ours { "semibold" } else { "regular" },
      name,
    )))
    content((-0.16, y0 + group / 2 - 0.52), anchor: "east", fine(text(fill: muted, threads)))
    content((lx(a) + 0.12, y0 + 2 * (rh + gap) + rh / 2), anchor: "west", fine(text(
      fill: if ours { ink } else { muted }, str(a),
    )))
  }

  // axis
  line((0, -0.28), (W, -0.28))
  for t in (30, 100, 300, 1000, 3000) {
    line((lx(t), -0.28), (lx(t), -0.42))
    content((lx(t), -0.72), fine[#t])
  }
  content((W / 2, -1.06), fine[MB/s, log scale])
})

#let panels = fig-style(block(width: FIG-W, {
  // The sidebar used to carry the legend, the claim and the axis note, which
  // left the claim stranded beside the chart's top quarter and about three
  // hundred points of white below it. The claim is the figure's conclusion, so
  // it belongs in the takeaway bar the other sixteen figures all end with.
  grid(
    columns: (auto, 1fr),
    column-gutter: 10pt,
    align: (top, top),
    chart,
    block(width: 3.4cm)[
      #fine(text(weight: "semibold")[three files, per reader])
      #v(3pt)
      #stack(spacing: 3pt, ..(
        ("AP · 99 MB", 0%), ("NYYTS · 193 MB", 24%), ("AHS · 2.15 GB", 48%),
      ).map(((lab, light)) => box({
        box(width: 8pt, height: 6pt, fill: accent.lighten(light), baseline: 0pt)
        h(4pt)
        fine(lab)
      })))
      #v(GAP-NOTE)
      // Names the cue that survives a mono printer. "Grey is every reader
      // outside this family" was true only in colour -- printed greyscale, every
      // bar is grey and the sentence contradicts the picture. The label weight
      // carries the same flag, so it is the one to point at.
      #fine(text(fill: muted)[
        Bold names are this engine and its bindings; the rest are other readers.
        Bars are shaded by file, the same three throughout.
      ])
    ],
  )
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      The top three are *one engine* behind three different amounts of language
      boundary — the gaps between them measure the boundary, not the parser. A
      factor of 111 separates top from bottom, which is why the axis is
      logarithmic; the printed numbers are the AP file.
    ])
  ]
}))

#let body = panels
