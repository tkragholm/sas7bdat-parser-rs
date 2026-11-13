# Figures

Figures for the JOSS paper. Three describe the *format*; six describe things the
crate already measured and wrote into doc comments but never showed. Drawn in
[cetz](https://github.com/cetz-package/cetz), built with Typst, tracked as source
rather than as images so they can be re-measured.

```
just -f figures/justfile all      every figure to figures/build/
just -f figures/justfile one fig_simd_dispatch
just -f figures/justfile check    do they all still build, with no font fallback?
just -f figures/justfile png      PNG previews beside the PDFs
```

**They live here, not in `tkragholm/sas7bdat-parser-rs`.** That repository is
public and this paper is not ready to be — the reasoning is in
[../../README.md](../../README.md). These are inside `paper/`, so they travel with
it when it goes: nothing here imports anything outside this directory, and
`--root` is this directory, so the same `just all` works in either home without a
path fixup.

No font path and no install. The figures are set in **Libertinus Serif**, which
Typst bundles, so anyone who clones this repo gets the same PDF. A stack like
`("Helvetica Neue", "Arial")` looks portable and is not — none of those ship with
Linux, so the figures would silently render in a fallback on exactly the machines
CI runs on. `check` greps for that and fails.

## What is here

| figure | what it explains | source of its numbers |
| --- | --- | --- |
| `fig_file_anatomy` | the file, the two bytes that rewrite every offset after them, and a page growing from both ends | `probe.rs`, `internal.rs`, `pages.rs` |
| `fig_slotted_page` | the three ways a row is found, and why compression changes how the whole file is read | `internal.rs`, `pages.rs` |
| `fig_throughput` | the ordering, and that the top three are one engine behind three language boundaries | `benchmarks/README.md`, Table A |
| `fig_extent_tuning` | both network knobs have an interior optimum | the SMB sweep in `scan/extent.rs` |
| `fig_schema_assembly` | a column is a join across six subheader families, and a name is a reference | `pages.rs`, `layout.rs` |
| `fig_simd_dispatch` | runtime dispatch is flat across build levels; compile-time lowering swings 4× | the x86-64 sweep in `simd/mod.rs` |
| `fig_mojibake_band` | why repairing mojibake by inverting the decode loses the uppercase half of Latin-1 | commit `2568491` |
| `fig_wildcopy` | why a three-byte RLE run writes thirty-two bytes and truncates back | `compression.rs`, `WILD_OVERRUN` |
| `fig_fused_pipeline` | why one stage of the fused scan cannot be parallel | `scan/fused.rs` |

Every number in them is one the repository already measured and wrote into a doc
comment or a commit message. Nothing here was re-benchmarked; if a sweep is
repeated, the arrays at the top of each figure are the only thing to change.

## On the two format figures

The published reverse-engineered descriptions of sas7bdat are field tables:
offset, width, meaning, one row per field. They are correct and they are not
illustrative, because a table cannot show the thing that actually makes this
format awkward — **two single bytes near the front rewrite the table itself**.
`a2` at byte 32 flips every pointer in the file from 4 bytes to 8, taking page
headers from 24 to 40 and subheader pointers from 12 to 24; `a1` at byte 35
inserts four bytes of padding, so every offset after it shifts. A table has to be
printed twice to say that, or printed once with a footnote.

The same goes for the page, which a field list flattens into a sequence. It is
not a sequence: subheader pointers descend from the header while row data ascends
from the end, and where they meet is the page's capacity. And for the schema,
which is not stored as records — no subheader describes a column, so reading it is
a join by ordinal position across six families.

Those three facts are what `fig_file_anatomy`, `fig_slotted_page` and
`fig_schema_assembly` exist to show, and they are the parts a newcomer to the
format gets wrong.

The prose that ties them together is [`../FORMAT.md`](../FORMAT.md) — the format
in six ideas, written so the figures have something to be figures *of*.

## Conventions

`style.typ` holds them, with the reasoning. Two that matter:

**One colour, one meaning.** `fearless` is blue, `std::simd` orange, `scalar`
grey, in every figure that names a backend — and no other figure uses those hues
for anything else. Elsewhere there is one accent per figure and everything else is
grey. The two backend hues are Okabe–Ito, so the figures survive greyscale
printing and red-green colour vision deficiency; the obvious blue/red pairing does
not.

**Three type steps and no fourth**: `LEAD` 9.5pt for the thing the figure is for,
`BODY` 8pt for labels, `FINE` 7pt for ticks and notes. Authored at `FIG-W` =
15.5cm, so a figure placed at its natural size is already the right size for a
single-column paper.

**Everything a figure fills is a named token.** An audit across the seventeen
figures found fifty-seven hard-coded fills and four all-but-identical greys —
`#f2f4f6`, `#eef1f3`, `#f4f5f6`, `#f7f8f9` — doing one job between them. There is
now one neutral, `tint-flat`, and the two tints are derived from their own base
colours, so changing `accent` changes what is tinted with it and nothing else.

**Two callout shapes, and the difference is scope.** `takeaway` is the sentence
the reader should leave with: full width, under everything, exactly one per
figure. `note` is an aside belonging to one panel and sits under that panel. Both
are neutral on purpose — `accent` is a data colour inside these drawings, so a
blue bar at the foot of a figure reads as one more series rather than as the
conclusion. Before they were helpers there were sixteen hand-rolled blocks, three
inset pairs, and no rule: the figure-wide summary came out accent in three
figures and neutral in ten.

**Three vertical gaps, because a gap separates only three things here.**
`GAP-TITLE` between a panel's title and its drawing, `GAP-PANEL` between one
finished panel and the next title, `GAP-NOTE` between the last drawing and the
takeaway. These replaced fourteen different values between 2pt and 30pt.

**Every figure states its own conclusion.** Three did not: `fig_simd_dispatch`'s
was written only in a source comment, so a reader saw two charts and no claim.

**Nothing may cross the page edge.** `FIG-W` is a promise, not a suggestion, and
a cetz canvas wider than it silently runs off and is cut. Five figures were doing
this — `fig_wildcopy` by 42pt. The check is mechanical: render to PPM and look for
ink inside the 8pt margin band. `fig_slotted_page`'s cause is the one to remember,
because it is invisible in the source — a `block(width: 4.9cm)` column helper
inside a grid whose columns are 4.63cm wide.

**Redundant encoding, checked in greyscale.** `just grey` renders every figure
the way a mono printer would, which is how a JOSS paper is usually read. The
palette is Okabe–Ito and survives colour-vision deficiency — the closest pair
under deuteranopia is dE 31 — but greyscale is a separate question and the
figures were failing it. `accent`, `warn` and `muted` land at luma 108, 92 and
101 out of 255, so anything separated by those hues alone collapses.

Nothing may be encoded by hue alone. Where it was:

- `fig_mojibake_band` marked its five surviving codepoints with a blue fill
  against twenty-seven red ones. In greyscale those fills are **two levels
  apart**. The five now also carry a heavier border and a bold glyph.
- `fig_simd_dispatch` drew three backends as three coloured lines. The lines
  were readable by position, but the legend swatches — two short bars side by
  side — were not. Each backend now has a dash, keyed by name so a panel and
  its legend cannot disagree.
- `fig_throughput`'s sidebar said *"Grey is every reader outside this family"*,
  a sentence that contradicts the picture once everything is grey. The label
  weight already carried the same flag, so the text points at that instead.

The general rule: if a figure's own caption says *the grey one* or *the blue
line*, that sentence is a bug even when the encoding underneath is fine.

## Adding one

Copy the shape of `fig_wildcopy.typ`: a header comment that says what the figure
argues and where its numbers come from, the data as named arrays near the top, and
a `panels` export with no caption — the caption belongs to the document that
places it. Then add a branch to `_preview.typ`.

Two things that will bite:

- **cetz's y axis grows upward.** `v / max * h`, not `h - v / max * h`. The
  subtraction draws the chart upside down and looks plausible enough at a glance
  to survive a first read.
- **Give any `fr` layout an explicit width.** An auto-width preview page has
  nothing to resolve fractions against.
