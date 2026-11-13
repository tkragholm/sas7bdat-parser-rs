// Runtime dispatch is flat; compile-time lowering is not.
//
// The argument for `fearless_simd` being the default backend, drawn. `std::simd`
// has no runtime dispatch: it lowers to whatever target features the build
// enabled, which for a portable binary on x86-64 is SSE2. `fearless_simd`
// detects AVX2/AVX-512 at runtime, so it does not care what the build allowed.
//
// The repository's own `.cargo/config.toml` raises the build to x86-64-v3, but
// cargo only finds that file by walking up from the working directory — a crate
// consumed from crates.io, a wheel, or an R package tarball never sees it. That
// is why the left-hand column of panel A is the one that matters.
//
// TWO PANELS, because one of them would be dishonest alone. Panel B is the
// kernel where scalar wins at every level, which is why `scan::string` gates on
// `< 64` rather than calling a backend at all.
//
// DATA: the x86-64 sweep in crates/sas7bdat/src/simd/mod.rs, measured on an
// AVX-512 EPYC (Zen 3), GiB/s. Not re-measured here — if the sweep is repeated,
// update these three arrays and nothing else.

#import "@preview/cetz:0.4.2"
#import "style.typ": *

#let LEVELS = ("baseline\nSSE2", "x86-64-v3\nAVX2", "x86-64-v4\nAVX-512")

// A dash per backend, so the three are told apart without colour. Measured in
// greyscale, `portable` and `scalar` land 22 luma levels apart out of 255: the
// LINES stay readable because they sit at different heights, but the legend
// swatches -- two short bars side by side -- do not. Keyed by name so a panel
// and its legend cannot disagree.
#let DASH = (
  "fearless": none,
  "std::simd": (4pt, 2.5pt),
  "scalar": (1pt, 1.8pt),
)

// (label, colour, values across the three levels)
#let SERIES-A = (
  ("fearless", fearless, (59.3, 58.9, 59.0)),
  ("std::simd", portable, (12.5, 54.4, 60.3)),
  ("scalar", scalar, (10.0, 16.4, 33.7)),
)
#let SERIES-B = (
  ("scalar", scalar, (10.9, 10.9, 10.9)),
  ("std::simd", portable, (7.8, 7.8, 7.8)),
  ("fearless", fearless, (4.6, 5.5, 5.4)),
)

#let panel(title, subtitle, series, y-max, w, h, ticks) = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ

  let px(i) = i / 2 * w
  // cetz's y grows UPWARD, so this is v/max*h and not h - v/max*h. The
  // subtraction draws the whole chart upside down, with 0 at the top, and it
  // looks plausible enough at a glance to survive a first read.
  let py(v) = v / y-max * h

  // The baseline column, marked. This is the argument: a crate consumed from
  // crates.io, a wheel or a CRAN tarball is built here, and the gap between the
  // two upper lines at this x is the whole reason for the default.
  rect((-0.30, 0), (0.62, h), fill: tint-flat, stroke: none)

  // frame: two rules only, because a full box competes with three thin lines
  line((0, 0), (0, h))
  line((0, 0), (w, 0))

  // y ticks
  for t in ticks {
    line((-0.06, py(t)), (0, py(t)))
    content((-0.14, py(t)), anchor: "east", fine[#t])
  }
  content((-0.72, h / 2), anchor: "center", angle: 90deg, fine[GiB/s])

  // x levels
  for (i, lv) in LEVELS.enumerate() {
    line((px(i), 0), (px(i), -0.06))
    content((px(i), -0.20), anchor: "north", fine(align(center, lv)))
  }

  // series
  for (name, col, vals) in series {
    line(
      ..vals.enumerate().map(((i, v)) => (px(i), py(v))),
      // a plotted series, not an outline
      stroke: (paint: col, thickness: W-LEAD, dash: DASH.at(name)),
    )
    for (i, v) in vals.enumerate() {
      circle((px(i), py(v)), radius: 0.075, fill: col, stroke: none)
    }
  }

  content((0, h + 0.62), anchor: "west", lead(title))
  content((0, h + 0.30), anchor: "west", fine(subtitle))
  // An explicit box width. Without one cetz sizes this content to something
  // near the tick spacing, and the line breaks fell inside "what a" — four
  // lines of one and two words stacked under the baseline tick.
  content(
    (-0.05, -0.86),
    anchor: "north-west",
    box(width: 2.0cm, fine([what a portable\ build gets], fill: ink)),
  )
})

// End labels sit outside the canvas so they cannot collide with the plot area.
#let legend(series) = block(above: 4pt, {
  set text(size: FINE)
  // Tightened when the swatch grew from 9pt to 11pt to show a dash: three
  // entries per legend and two legends put that 12px past the page edge.
  stack(
    dir: ltr,
    spacing: 5pt,
    ..series.map(((name, col, vals)) => box(baseline: 1.5pt, {
      box(width: 11pt, height: 0pt, baseline: -3pt, stroke: (
        top: (paint: col, thickness: W-LEAD, dash: DASH.at(name)),
      ))
      h(2.5pt)
      text(fill: ink, weight: "semibold", name)
      h(3pt)
      text(fill: muted, str(vals.first()) + "→" + str(vals.last()))
    })),
  )
})

#let panels = fig-style(block(width: FIG-W, {
  grid(
    columns: (1fr, 1fr),
    column-gutter: 6mm,
    rows: (auto, auto),
    row-gutter: 3pt,
    panel(
      "A · classify_missing",
      "the wide-vector kernel",
      SERIES-A, 65, 5.4, 3.4, (0, 20, 40, 60),
    ),
    panel(
      "B · is_ascii, 16-byte strings",
      "where a vector backend loses",
      SERIES-B, 12, 5.4, 3.4, (0, 4, 8, 12),
    ),
    legend(SERIES-A),
    legend(SERIES-B),
  )
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      Runtime dispatch is flat; compile-time lowering is not. `std::simd` gets
      whatever the build enabled, which for a portable binary on x86-64 is SSE2 —
      the left-hand column, and the only one a crate consumed from crates.io, a
      wheel or an R tarball ever sees. Panel B is why there are two: `is_ascii`
      on short strings is a kernel where scalar wins at every level, which is
      what `scan::string` gates on rather than calling a backend at all.
    ])
  ]
}))

#let body = panels
