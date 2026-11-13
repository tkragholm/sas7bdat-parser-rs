// Why repairing mojibake by inverting the decode loses the uppercase half.
//
// When a file declares a single-byte encoding but holds UTF-8, every non-ASCII
// character arrives as two bytes read as two characters. The obvious repair is to
// invert: walk the damaged string back to bytes and re-read them as UTF-8.
//
// The inverse people reach for is latin-1. The damage is done by windows-1252.
// They agree on every byte except 0x80..=0x9F, where latin-1 has the C1 controls
// and windows-1252 has typographic characters ABOVE U+00FF — so `u8::try_from`
// fails there and the whole string is handed back unrepaired.
//
// That byte band is exactly the second byte of the UTF-8 encodings of
// U+00C0..=U+00DF, which is the uppercase half of Latin-1. So the lowercase half
// was repaired and the uppercase half was not: 27 of 32 characters. The five that
// survived did so only because their second byte lands on one of windows-1252's
// five undefined slots, which encoding_rs maps below U+0100.
//
// SOURCE: commit 2568491, "repair mojibake from the bytes, not from the decoded
// string". The fix is not a better inverse — it is to stop inverting.

#import "@preview/cetz:0.4.2"
#import "style.typ": *

// U+00C0..=U+00DF. The five that survived the old repair, from the commit.
#let UPPER = (
  "À", "Á", "Â", "Ã", "Ä", "Å", "Æ", "Ç", "È", "É", "Ê", "Ë", "Ì", "Í", "Î", "Ï",
  "Ð", "Ñ", "Ò", "Ó", "Ô", "Õ", "Ö", "×", "Ø", "Ù", "Ú", "Û", "Ü", "Ý", "Þ", "ß",
)
#let SURVIVED = ("Á", "Í", "Ï", "Ð", "Ý")

#let panel-a = cetz.canvas({
  import cetz.draw: *
  set-style(stroke: W-HAIR + hairline) // axis furniture; see style.typ
  let w = 15.0
  let bx(v) = v / 256 * w

  // the byte axis
  rect((0, 0), (bx(128), 0.52), fill: tint-flat)
  rect((bx(128), 0), (bx(160), 0.52), fill: tint-wrong, stroke: W-EDGE + wrong)
  rect((bx(160), 0), (w, 0.52), fill: tint-flat)

  content((bx(64), 0.26), fine[0x00–0x7F · ASCII, identical])
  content((bx(208), 0.26), fine[0xA0–0xFF · identical])
  content((bx(144), 0.92), fine(text(fill: wrong, weight: "semibold")[0x80–0x9F]))

  // what each codec puts there
  line((bx(144), -0.06), (bx(144), -0.52), stroke: W-EDGE + wrong)
  content((bx(144), -0.62), anchor: "north", fine(align(center)[
    #text(fill: ink)[latin-1 → C1 control codes] \
    #text(fill: ink)[windows-1252 → typographic characters, above U+00FF] \
    #text(fill: wrong, weight: "semibold")[u8::try_from fails here, so the whole cell is returned unchanged]
  ]))
})

// The five survived cells are marked THREE ways, only one of which is hue.
// Measured in greyscale, tint-accent and tint-wrong are two levels apart out of
// 255 and their strokes sixteen -- so on a mono printer, which is how a JOSS
// paper is usually read, the five vanished into the twenty-seven. The weight of
// the border and the weight of the glyph both survive the conversion.
#let cell(ch) = {
  let ok = ch in SURVIVED
  box(
    width: 0.42cm, height: 0.46cm,
    fill: if ok { tint-accent } else { tint-wrong },
    stroke: (if ok { W-MARK } else { W-HAIR }) + (if ok { accent } else { wrong }),
    // A half point over BODY on purpose: these are accented capitals in a
    // 32-cell grid, and at 8pt the diacritics are what the reader loses.
    align(center + horizon, text(
      size: BODY + 0.5pt,
      fill: ink,
      weight: if ok { "bold" } else { "regular" },
      ch,
    )),
  )
}

#let panel-b = block(width: FIG-W, {
  grid(
    columns: (0.42cm,) * 32,
    column-gutter: 1.4pt,
    ..UPPER.map(cell),
  )
  v(4pt)
  grid(
    columns: (auto, 1fr),
    column-gutter: 10pt,
    fine[U+00C0 – U+00DF],
    fine(align(right)[
      #text(fill: wrong, weight: "semibold")[27 left mangled] · #text(fill: accent, weight: "semibold")[5 survived]
      — and only because their second byte hits one of windows-1252's five undefined slots
    ]),
  )
})

#let panels = fig-style(block(width: FIG-W, {
  lead[A · the thirty-two bytes where the two codecs disagree]
  v(GAP-TITLE)
  panel-a
  v(GAP-PANEL)
  lead[B · the codepoints that band is the second byte of]
  v(GAP-TITLE)
  panel-b
  v(GAP-NOTE)
  takeaway[
    #fine(text(fill: ink)[
      One column, two spellings: *Nørrebro* repaired, *KÃ˜BENHAVN* not. The
      inconsistency hides the problem instead of showing it — which is worse than
      repairing neither.
    ])
  ]
}))

#let body = panels
