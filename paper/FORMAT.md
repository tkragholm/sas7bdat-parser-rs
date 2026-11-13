# sas7bdat, in six ideas

There is no specification for this format. Everything known about it was
reverse-engineered, and what has been published is mostly **field tables**:
offset, width, meaning, one row per field. Those are correct and they are the
reason people find the format hard, because a table of offsets cannot tell you
*why* any of them are where they are — and this format's oddities all have
reasons.

This is the mental model. Six ideas, in the order they make each other make
sense. If you hold the first one, most of the rest stop being surprising.

---

## 1. It is a slotted-page store

Not a stream of records. A **paged store**, and each page is laid out the way
database pages have been laid out since the 1970s — the same design as a
PostgreSQL heap page or an SQLite b-tree page.

A slotted page has three parts:

```
┌──────────────────────────────────────┐
│ page header                          │  fixed, at the top
├──────────────────────────────────────┤
│ slot 0 → (offset, length, type)      │  the slot directory,
│ slot 1 → (offset, length, type)      │  growing DOWN
│ slot 2 → …                           │
│                                      │
│            free space                │  they meet here
│                                      │
│ record data                          │  growing UP
└──────────────────────────────────────┘
```

The slot directory grows down from just under the header; record data grows up
from the end of the page; where they meet is the page's capacity. That is why a
page cannot be read as a sequence of fields — it is two allocators pointed at
each other, and the interesting quantity is where they stop.

In sas7bdat the slots are called *subheader pointers*, and a slot is
`(offset, length, compression, type)`. Everything else follows from what goes in
them.

## 2. Fixed-length records do not need slots — and that is the whole performance story

A slot directory exists to locate **variable-length** records. If every record is
the same size, you do not need one: you can find record *n* by multiplying.

Uncompressed rows in a sas7bdat are all exactly `row_length` bytes. So they are
packed contiguously into the data region and located arithmetically, with no
slots at all. Compressed rows are not the same size as each other, so they go
back into the slot directory, one slot each.

That single difference is behind almost everything you will notice about
performance:

|                      | uncompressed              | compressed                    |
| -------------------- | ------------------------- | ----------------------------- |
| how a row is found   | `data_start + n × row_len` | a slot lookup                 |
| per-row bookkeeping  | none                      | one span per row              |
| a page can be…       | decoded from its header   | decoded only after its slots  |

A reader that must keep one span per row pays for it: a few hundred million rows
cost several gigabytes of span table before any data is decoded. It is also why
this reader's fused scan exists at all — see `crates/sas7bdat/src/scan/fused.rs`.

One refinement, and it is the kind of thing that makes the format feel arbitrary
until you see the rule: **a slot may hold a block of uncompressed rows**, which
the reader walks in `row_length` strides. So "slots hold variable-length records"
is right, and a record is sometimes itself an array of fixed-length ones.

## 3. It is a memory image, so machine words leak into the file

Two single bytes near the front of the header rewrite every offset after them:

- **byte 32** (`a2`): if it is `0x33`, every pointer in the file is 8 bytes rather
  than 4. Page headers go from 24 bytes to 40; subheader pointers from 12 to 24.
- **byte 35** (`a1`): if it is `0x33`, four bytes of padding are inserted, and
  every offset after that point shifts.

No wire format does this. A wire format picks a width and states it. This one
carries the *host's* word size, because the file is closer to a dump of SAS's own
in-memory structures than to a serialisation of them. Alignment padding is in the
file for the same reason: the structures were aligned in memory and the alignment
came along.

Once you know that, the two bytes stop being a quirk and become the single most
important thing to read first — and you understand why a published field table is
always subtly wrong for half its readers. There are two tables.

## 4. The schema is a join, not a record

Nothing in the file says "here is column 7". Reading the schema means collecting
six kinds of subheader, each of which carries **one facet of every column**, and
joining them by ordinal position:

| signature  | carries                                   |
| ---------- | ----------------------------------------- |
| `F7F7F7F7` | row length, row count, rows per page      |
| `F6F6F6F6` | how many columns there are                |
| `FFFFFFFD` | the text blobs everything else points into |
| `FFFFFFFF` | each column's **name**                    |
| `FFFFFFFC` | each column's **width, type, offset**      |
| `FFFFFBFE` | each column's **format and label**         |

Two consequences fall straight out, and both surprise people:

- A truncated or corrupt metadata page does not cost you the last few columns. It
  costs you **a facet of every column** — all the names, or all the widths.
- The order the subheaders appear in is not guaranteed and does not matter. The
  join key is position within each family, not proximity in the file.

## 5. Strings live in a heap, and names are references into it

A column name is not stored with the column. It is a `TextRef` — three `u16`s:
`(blob index, offset, length)` — pointing into a set of concatenated text blobs
delivered by the `FFFFFFFD` subheaders.

So the schema's strings are a **heap with references into it**, which is again
what an in-memory structure looks like. A reader never has to materialise a
schema's worth of strings; it borrows slices. Repeated names cost nothing,
because they are the same slice twice.

## 6. There is no spec, only writers — and there is more than one writer

The last idea is the one that will actually bite you.

Because the format was never specified, it is defined in practice by what its
writers emit. SAS pads the start of a page's row data up to an 8-byte boundary.
**StatTransfer does not.** A file written by StatTransfer with an odd subheader
count puts its rows four bytes earlier than the rule predicts, and a reader that
applies the rule shifts every column.

This reader detects it from the release string in the header and undoes the pad —
see the comment in `contiguous_data_start` in `crates/sas7bdat/src/pages.rs`.
That is not a hack around a bug. It is what "no specification" means in practice:
the format is a family of dialects, and a reader is judged on how many it speaks.

---

## Putting it together

To read one row you need, in order:

1. **The two bytes** — because they decide the width of everything else.
2. **The header** — page size, page count, encoding, and the file's own header
   length.
3. **The metadata subheaders**, gathered across the metadata pages and joined by
   ordinal, which gives you `row_length` and the column table.
4. **A page's type**, which tells you whether its rows are contiguous, in slots,
   or absent.
5. **The row's bytes**, either by arithmetic or by slot — and decompressed first
   if the file is compressed.
6. **Each cell**, sliced out of the row at the column's offset and width, with
   numerics being IEEE-754 doubles truncated to between 3 and 8 bytes and
   characters being fixed-width, space-padded bytes in the file's declared
   encoding.

Step 6 has its own trap, which is worth stating because it is the one that
produces silently wrong text rather than an error: the declared encoding is often
not the encoding the bytes are in. See `MojibakePolicy` and the figure
`figures/fig_mojibake_band.typ`.

## Figures

The diagrams for all of this are in [`figures/`](figures/), built with
`just -f figures/justfile all`:

| idea | figure |
| --- | --- |
| 1, 3 | `fig_file_anatomy` — the file, the two bytes, a page from both ends |
| 2 | `fig_slotted_page` — the three ways a row is found, and what each costs |
| 4, 5 | `fig_schema_assembly` — the six families, and `TextRef` |
| — | `fig_wildcopy`, `fig_simd_dispatch`, `fig_fused_pipeline`, `fig_mojibake_band` |
