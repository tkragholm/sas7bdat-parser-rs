// Both knobs have an interior optimum, which is why they are constants.
//
// Reading a page at a time costs one seek and one read_exact per page. Pages are
// 64-256 KB and a 100 GB file holds on the order of a million of them, so on
// network storage the round-trips cost more than the transfer. An extent covers
// many pages in one read, and several reads run at once.
//
// The point of the figure is that neither knob is monotone. More readers stop
// helping after four and get worse; bigger extents stop helping after four
// megabytes and fall off a cliff at thirty-two. A reader tuning this for their
// own storage needs the SHAPE, not the two numbers — the doc comment already
// points at scripts/probe.py to repeat the sweep.
//
// DATA: the SMB-over-LAN sweep in crates/sas7bdat/src/scan/extent.rs. One host,
// one protocol; the axes say so, because the figure would otherwise claim more
// than the sweep does.

#import "@preview/cetz:0.4.2"
#import "style.typ": *

#let READERS = ((1, 176), (4, 323), (8, 280), (16, 221))
#let EXTENTS = ((1, 348), (4, 387), (8, 350), (16, 361), (32, 233))

#let curve(pts, xs, x-label, peak-label, w, h, y-max) = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ
  let n = xs.len()
  let px(v) = xs.position(x => x == v) / (n - 1) * w
  let py(v) = v / y-max * h

  line((0, 0), (0, h))
  line((0, 0), (w, 0))
  for t in (100, 200, 300, 400) {
    if t <= y-max {
      line((-0.06, py(t)), (0, py(t)))
      content((-0.16, py(t)), anchor: "east", fine[#t])
    }
  }
  for (x, _) in pts {
    content((px(x), -0.22), fine[#x])
  }
  content((w / 2, -0.58), fine(x-label))
  // Both axes carry their unit, the way fig_simd_dispatch's do. The y unit used
  // to live only in the takeaway, which left four bare numbers on the axis.
  content((-0.78, h / 2), anchor: "center", angle: 90deg, fine[MB/s])

  line(..pts.map(((x, v)) => (px(x), py(v))), stroke: W-LEAD + accent)
  let best = pts.sorted(key: p => -p.at(1)).first()
  for (x, v) in pts {
    let is-best = x == best.at(0)
    circle(
      (px(x), py(v)),
      radius: if is-best { 0.10 } else { 0.065 },
      fill: if is-best { accent } else { white },
      stroke: W-MARK + accent,
    )
  }
  content((px(best.at(0)), py(best.at(1)) + 0.42), fine(text(
    fill: accent, weight: "semibold", peak-label,
  )))
})

#let panels = fig-style(block(width: FIG-W, {
  grid(
    columns: (1fr, 1fr),
    column-gutter: 14mm,
    align: (top, top),
    block[
      #lead[A · concurrent readers]
      #v(GAP-TITLE)
      #curve(READERS, (1, 4, 8, 16), [readers], [4 → 323], 5.4, 3.0, 420)
    ],
    block[
      #lead[B · extent size]
      #v(GAP-TITLE)
      #curve(EXTENTS, (1, 4, 8, 16, 32), [MB per read], [4 MB → 387], 5.4, 3.0, 420)
    ],
  )
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      MB/s over SMB on a LAN, one host. Pages are 64–256 KB and a 100 GB file holds
      on the order of a million of them, so on network storage the round-trips cost
      more than the transfer — but both curves turn. Readers fall away steadily
      after four; extents peak at four, wander between eight and sixteen, and fall
      off a cliff at thirty-two. That is why these are constants and not
      *as many as possible*.
    ])
  ]
}))

#let body = panels
