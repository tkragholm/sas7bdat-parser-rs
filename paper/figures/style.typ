// Shared styling for the crate's publication figures.
//
// These are drawn for a PAGE, not a slide: a single-column tool paper at about
// 15.5cm, base 9pt. If they are ever wanted for a talk, raise `size` — every
// figure here takes it — rather than scaling the PDF.
//
// ## What a colour is allowed to mean
//
// One rule, because a figure that uses a hue for two things teaches the reader
// nothing:
//
//   BACKEND IDENTITY.  `fearless` is blue, `portable` (std::simd) is orange,
//   `scalar` is grey. That holds in every figure that names a backend, and no
//   other figure uses those two hues for anything else.
//
//   EMPHASIS.  Everywhere else, `accent` marks the one thing the figure exists
//   to point at, and `ground` is everything else. One accent per figure.
//
// The two backend hues are Okabe-Ito #0072B2 and #E69F00 — chosen so the figures
// survive greyscale printing and red-green colour vision deficiency, which the
// obvious blue/red pairing does not.

#let ink = rgb("#14181d")
#let muted = rgb("#5c6670")
#let hairline = rgb("#c6ccd2")
#let paper = white

// Backend identity — see above.
#let fearless = rgb("#0072B2")
#let portable = rgb("#E69F00")
#let scalar = rgb("#8a939b")

// Emphasis, for the figures that name no backend.
#let accent = rgb("#0072B2")
#let ground = rgb("#9aa3ab")
#let ground-soft = rgb("#ccd2d8")
// ── what the loaded colour means ───────────────────────────────────────────
//
// There was one token, `warn`, and an audit found it labelling four different
// things across the figures: a date, a method that is wrong, data that is
// removed, and the study's own subject. A reader who learned the colour in one
// figure had it retrained by the next.
//
// It is split by MEANING below. The surprise is that splitting the meanings did
// not free up any colour: this palette is tight, and every unused hue collides
// with a neighbour in some figure that needs the token (a second red reads as the
// first; a vermillion sits on top of `second`'s orange). So two of these share a
// hex, deliberately, and the rule that makes that safe is enforced rather than
// hoped for:
//
//   TOKENS THAT SHARE A HEX MUST NEVER APPEAR IN THE SAME FIGURE.
//
// `just tokens` fails if they do. What changed for the reader is that a date and
// an exclusion stopped being loud -- a date is structure and an exclusion is a
// removal, and neither was ever a warning.
#let wrong = rgb("#A03A24") // a byte sequence that breaks, a read that overruns
#let second = rgb("#E69F00") // a second category, in figures that name no backend


// ── the type ladder ────────────────────────────────────────────────────────
//
// Three steps and no fourth. FIG-W is the column these are authored at, so a
// figure placed at its natural size is already the right size.
// ── the tints ──────────────────────────────────────────────────────────────
//
// Named and DERIVED, because they were neither. An audit across the figures
// found fifty-seven hard-coded fills and four all-but-identical greys —
// #f2f4f6, #eef1f3, #f4f5f6 and #f7f8f9 — doing one job between them. Nobody
// could tell them apart and nothing chose between them; they were whatever was
// typed that afternoon.
//
// One neutral. The two tints track their own base colours, so changing `accent`
// changes everything tinted with it and nothing else.
#let tint-accent = accent.lighten(88%)
#let tint-wrong = wrong.lighten(88%)
#let tint-second = second.lighten(88%)
#let tint-flat = luma(245) // any fill that is just "not the background"
#let rule-soft = luma(224) // gridlines, and rules that must not compete

#let FIG-W = 15.5cm
#let LEAD = 9.5pt // the thing the figure is for: a series name, a value
#let BODY = 8pt // axis labels, row labels
#let FINE = 7pt // ticks, units, the note under the drawing

// ── the face ───────────────────────────────────────────────────────────────
//
// Libertinus Serif, which TYPST BUNDLES. That is the whole reason: anyone who
// clones this repo can build these figures with no font path and no install, and
// gets the same PDF. A stack like ("Helvetica Neue", "Arial") looks portable and
// is not — none of those ship with Linux, so the figures would silently render in
// a fallback on exactly the machines CI runs on.
//
// It is also the right face on the merits. Libertinus is Typst's default body
// font and a Linux Libertine sibling, so a figure set in it matches the body text
// of a Typst paper exactly and sits correctly in a LaTeX one.
//
// `font` is a parameter, so a build that does have a sans available can pass one:
//     #fig-style(f.panels, font: "Source Sans 3")
#let FACE = "Libertinus Serif"

#let fig-style(body, size: BODY, font: FACE) = {
  set text(font: font, size: size, fill: ink)
  set align(left)
  body
}

// Standalone crop page, for previewing one figure while drawing it.
#let fig(body, font: FACE) = {
  set page(width: auto, height: auto, margin: 8pt, fill: paper)
  fig-style(body, font: font)
}

// ── stroke weights ─────────────────────────────────────────────────────────
//
// Four, mapped to what a line is doing. The figures had fourteen values between
// 0.4pt and 2.2pt across a hundred and fifteen strokes -- 0.5, 0.55 and 0.6
// among them, which differ by less than two hundredths of a millimetre on
// paper. Nobody chose those; they were what got typed.
// Every canvas that draws axis furniture opens with
//
//     set-style(stroke: W-HAIR + hairline)
//
// so the axis line, its ticks and anything else that is structure rather than
// data is the DEFAULT stroke and is not retyped per call. Everything louder than
// furniture states its stroke at its own call site, which is what makes a loud
// stroke visible when reading the source.
//
// A helper defined OUTSIDE a canvas body cannot see that default -- it is called
// from a canvas but its own scope is not the canvas's -- so such helpers keep an
// explicit stroke. Stripping theirs silently falls back to cetz's own 1pt black,
// which is how three kappa figures regressed before this was understood.
#let W-HAIR = 0.5pt // axes, gridlines, subdivisions inside a shape
#let W-EDGE = 0.8pt // an ordinary shape's outline
#let W-MARK = 1.2pt // the shape or reference line the figure points at
#let W-LEAD = 1.6pt // a plotted series, or the one subject the panel is about

// One arrowhead. There were five sizes between 0.32 and 0.5.
#let MARK-SCALE = 0.45

// ── vertical rhythm ────────────────────────────────────────────────────────
//
// Three gaps, because there are only three things a gap separates here. Before
// these were named the seventeen figures used fourteen different spacer values
// between 2pt and 30pt, and fig_file_anatomy used 14pt and 6pt for the SAME
// role — a panel title and the drawing under it — two panels apart.
#let GAP-TITLE = 8pt // a panel's title, and the drawing it belongs to
#let GAP-PANEL = 18pt // one finished panel, and the next panel's title
#let GAP-NOTE = 10pt // the last drawing, and the takeaway under it

// ── the two callouts ───────────────────────────────────────────────────────
//
// There were sixteen of these across the figures, hand-rolled each time, with
// three different inset pairs and no rule about which fill meant what: the
// figure-wide summary came out accent in three figures and neutral in ten.
// Nobody chose that; it was just what got typed.
//
// Two shapes, and the difference between them is scope:
//
//   takeaway  the sentence the reader should leave with. Full width, under
//             everything, one per figure.
//   note      an aside that belongs to ONE panel, and sits under that panel.
//
// Both are neutral on purpose. `accent` is a data colour inside these drawings
// — a blue bar at the foot of the figure reads as one more series rather than
// as the conclusion.
#let takeaway(body) = block(
  width: FIG-W,
  fill: tint-flat,
  stroke: (left: 2pt + ink),
  inset: (x: 8pt, y: 6pt),
  body,
)

#let note(body, width: auto) = block(
  width: width,
  fill: tint-flat,
  stroke: (left: 2pt + ink),
  inset: (x: 7pt, y: 5pt),
  body,
)

#let lead(t, fill: ink) = text(size: LEAD, weight: "semibold", fill: fill, t)
#let body-t(t, fill: ink) = text(size: BODY, fill: fill, t)
#let fine(t, fill: muted) = text(size: FINE, fill: fill, t)

// A caption line, for the standalone previews. In a paper the caption belongs to
// the document, so `panels` is always exported without one.
#let note-line(t) = block(width: FIG-W, above: 6pt, fine(t))
