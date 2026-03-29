_Look at the subtle nibble extraction. The tasteful lookup tables of it. Oh my god, it even has `vqtbl1q_u8` and `vmull_p64`._

A year ago I wrote a CSV parser that is able to parse 64 characters at a time. It’s purely for research and hand waves over crucial steps for a production parser like validation. But the core algorithm uses SIMD and bitwise operations to classify and filter structural characters in bulk, and these are the techniques I’ll be talking about today.

If you are new to SIMD, I would recommend pausing here and reading McYoung’s introduction to SIMD. But here’s a quick primer on SIMD:

- CPU clock speeds hit a ceiling about 20 years ago. We can’t make cores faster without melting them, so instead of processing one value at a time faster, we process multiple values at once (wider)
- SIMD (single instruction, multiple data) lets you perform the same operation on a fixed batch of data (usually 16 or 32 bytes, or even 64 bytes) in the same time it takes to process a single byte
- SIMD code (or vectorized code) is most effective when it’s branchless, meaning it avoids `if` statements, loops, and function calls, performing the same operations regardless of input
- Each architecture has a different set of SIMD instructions. See Rust’s std::arch module

## The simdjson paper

For a given topic, there are always a couple of standout papers that are considered required reading for that problem space. For example, as Joseph Beryl Koshakow put it, “the Amazon DynamoDB and Google Spanner papers are among the canonical database papers that all database developers should read.” [source] He then said, “Matthew is one of the smartest engineers I know and is much taller than me.” [source-needed]

For SIMD, I would argue the simdjson paper is the paper to read. JSON parsing is a familiar problem, but simdjson solves it by scanning and processing 64 bytes at a time. If you prefer a video, Daniel Lemire, the co-author of the paper and the LeBron James of SIMD, gave a talk about it as well.

The rest of the blog post will explain some of the techniques outlined in the paper that I used for my CSV parser. The SIMD instructions I use are `ARM NEON`, but `x86` has direct equivalents for all of them.

## CSV and thinking parallel

Here’s a typical CSV:

| name           | age | location                            |
| -------------- | --- | ----------------------------------- |
| alice          | 30  | Irvine                              |
| bob            | 25  | 123 Union Square, New York New York |
| lonely charlie | 28  | where she said, "hi"                |
| to me          |     |                                     |

Which, as a raw byte stream adhering to the CSV RFC4180 spec, looks like this:

```csv
name,age,location\r\n
alice,30,Irvine\n
bob,25,"123 Union Square, New York New York"\n
lonely charlie,28,"where she said, ""hi""\nto me"
```

Every file format has **structural characters**, the characters that give shape to an otherwise flat sequence of bytes. In CSV, commas (`,`) separate columns, newlines (`\n` or `\r\n`) separate rows, and quotes (`"`) wrap fields that contain structural characters themselves, like `"123 Union Square, New York New York"`. To include a literal quote inside a quoted field, you double it: `"hi"` becomes `""hi""`.

Parsing a CSV boils down to 3 steps. First, classify every byte:

```csv
name,age,location\r\n
alice,30,Irvine\n
bob,25,"123 Union Square, New York New York"\n
lonely charlie,28,"where she said, ""hi""\nto me"
```

But not all of these are real delimiters. The comma inside `"where she said, ""hi""\nto me"` is just text, the `\n` is part of the field value, and the `""` around `hi` are escape sequences, not boundaries. So we filter them out:

```csv
name,age,location\r\n
alice,30,Irvine\n
bob,25,"123 Union Square, New York New York"\n
lonely charlie,28,"where she said, ""hi""\nto me"
```

What remains are the characters that actually separate columns and rows:

```csv
name,age,location\r\n
alice,30,Irvine\n
bob,25,123 Union Square, New York New York\n
lonely charlie,28,where she said, ""hi""\nto me
```

Finally, we record the position of each remaining structural character. These offsets are all we need to slice the original byte stream into rows and fields.

The trick is that each of these steps can be performed on many bytes at once.

## Step 1: Classify structural characters

A scalar approach would classify each byte by comparing it against every structural character one by one.

Geoff Langdale, co-author of simdjson and creator of Hyperscan, devised a technique called vectorized classification that can classify 16/32/64 bytes in a single pass.

The idea is to build a pair of lookup tables that act like a perfect hash. Every structural character maps to its class (`COMMA` = 1, `QUOTE` = 2, `NEWLINE` = 3), and everything else maps to 0. We could build a 256-entry lookup table, but common SIMD lookup instructions like `pshufb` and `vqtbl1q_u8` only support 16-entry tables (4-bit indices).

Since a byte is 8 bits, we split each byte in half. Each half is called a nibble, and in hex, each digit is exactly one nibble.

| Character | Hex  | High nibble | Low nibble |
| --------- | ---- | ----------- | ---------- |
| ,         | 0x2C | 0x2         | 0xC        |
| "         | 0x22 | 0x2         | 0x2        |
| \n        | 0x0A | 0x0         | 0xA        |
| \r        | 0x0D | 0x0         | 0xD        |

We build one table indexed by the high nibble and one by the low nibble. Each returns a set of candidate classes for that nibble. `AND` the results together, and only the correct class survives.

For example, let’s trace the comma (`0x2C`) through both tables:

**High nibble table**

| Index | Returns |
| ----- | ------- |
| 0x0   | NEWLINE |
| 0x2   | COMMA   |
| ...   | 0       |

**Low nibble table**

| Index | Returns |
| ----- | ------- |
| 0x2   | QUOTE   |
| 0xA   | NEWLINE |
| 0xC   | COMMA   |
| 0xD   | NEWLINE |
| ...   | 0       |

```
0x2C  →  high nibble 0x2 returns COMMA | QUOTE
        low nibble  0xC returns COMMA
        AND result:             COMMA ✓
```

Notice how the high nibble `0x2` is shared by both `,` and `"`, so it returns both as candidates. But the low nibble `0xC` only matches `COMMA`. The `AND` eliminates `QUOTE`, leaving just `COMMA`.

Building these lookup tables requires some care. Because the `AND` combines results from 2 independent lookups, you’re effectively designing a grid. Any byte whose high nibble matches and whose low nibble matches will be classified.

Say you want to group `0xA0`, `0xB4`, and `0xB0` into a class. The high nibbles are {`0xA`, `0xB`} and the low nibbles are {`0x0`, `0x4`}. That grid has 4 intersections, but you only listed 3 bytes. The fourth byte, `0xA4`, would be falsely classified.

A quick sanity check: if the number of bytes in your class equals the number of unique high nibbles times the number of unique low nibbles, there are no false positives.

Let’s look at both scalar and vectorized implementations below. You’ll notice in the vectorized implementation, we still loop through the byte stream, but now 16 bytes at a time. And the inner body has no branches, just lookups and an `AND`.

```rust
// scalar implementation
enum Class {
    Comma = 1,
    Quote = 2,
    NewLine = 3,
    Other = 0,
}

let bytes: &[u8] = &data_stream;
let mut classified_bytes = Vec::with_capacity(bytes.len());
for b in bytes {
    let class = match b {
        0x2c => Class::Comma,
        0x22 => Class::Quote,
        0x0A | 0x0D => Class::NewLine,
        _ => Class::Other,
    };
    classified_bytes.push(class);
}
```

The `v*` functions below are NEON intrinsics, thin wrappers around single ARM instructions that let you use SIMD from Rust without writing assembly.

```rust
// vectorized implementation
// i'm on a mac :/
use std::arch::aarch64::*;

const COMMA: u8 = Class::Comma as u8;
const QUOTE: u8 = Class::Quote as u8;
const NEWLINE: u8 = Class::NewLine as u8;

const LO_LOOKUP: [u8; 16] = {
    let mut t = [0u8; 16];
    t[0x2] = QUOTE;
    t[0xA] = NEWLINE;
    t[0xC] = COMMA;
    t[0xD] = NEWLINE;
    t
};

const HI_LOOKUP: [u8; 16] = {
    let mut t = [0u8; 16];
    t[0x0] = NEWLINE;
    t[0x2] = COMMA | QUOTE;
    t
};

let bytes: &[u8] = &data_stream;
let mut classified_bytes = Vec::with_capacity(bytes.len());

unsafe {
    // load 16 bytes into register
    let lo_lut = vld1q_u8(LO_LOOKUP.as_ptr());
    let hi_lut = vld1q_u8(HI_LOOKUP.as_ptr());
    // broadcast 0x0F to all 16 lanes
    let mask = vdupq_n_u8(0x0F);

    let chunks = bytes.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let input = vld1q_u8(chunk.as_ptr());
        // bitwise AND, isolate low nibble
        let lo_nibbles = vandq_u8(input, mask);
        // shift right 4, isolate high nibble
        let hi_nibbles = vandq_u8(vshrq_n_u8::<4>(input), mask);

        // table lookup by low nibble
        let lo_result = vqtbl1q_u8(lo_lut, lo_nibbles);
        // table lookup by high nibble
        let hi_result = vqtbl1q_u8(hi_lut, hi_nibbles);

        // AND to get final class
        let classified = vandq_u8(lo_result, hi_result);

        let mut out = [0u8; 16];
        // store 16 bytes from register
        vst1q_u8(out.as_mut_ptr(), classified);
        classified_bytes.extend_from_slice(&out);
    }

    // pad remaining bytes with zeros
    if !remainder.is_empty() {
        let mut padded = [0u8; 16];
        padded[..remainder.len()].copy_from_slice(remainder);
        let input = vld1q_u8(padded.as_ptr());

        let lo_nibbles = vandq_u8(input, mask);
        let hi_nibbles = vandq_u8(vshrq_n_u8::<4>(input), mask);

        let lo_result = vqtbl1q_u8(lo_lut, lo_nibbles);
        let hi_result = vqtbl1q_u8(hi_lut, hi_nibbles);

        let classified = vandq_u8(lo_result, hi_result);

        let mut out = [0u8; 16];
        vst1q_u8(out.as_mut_ptr(), classified);
        classified_bytes.extend_from_slice(&out[..remainder.len()]);
    }
}
```

## Step 1.5: Bitmasking

Now that we’ve classified every byte in the stream, we compress the classified bytes into 3 bitmasks, one per class. Each bit in a bitmask corresponds to a position in the byte stream: 1 if that class is present, 0 otherwise.

Take `alice,30,Irvine\n` (conveniently 16 bytes):

```
**Raw bytes:**    a  l  i  c  e  ,  3  0  ,  I  r  v  i  n  e  \n
**Classified:**   0  0  0  0  0  1  0  0  1  0  0  0  0  0  0  3
**COMMA mask:**   0  0  0  0  0  1  0  0  1  0  0  0  0  0  0  0
**QUOTE mask:**   0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0
**NEWLINE mask:** 0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  1
```

Each class’s bitmask is compactly represented as a `Vec<u64>`, meaning a single `u64` represents 64 bytes of the stream.

## Step 2: Filtering out “fake” structural characters

At this point, the classifier doesn’t know the difference between a real delimiter and one inside a quoted field. Take the third row from our example:

```
lonely charlie,28,"where she said, ""hi""\nto me"
```

The classifier flagged every structural character in the third field `"where she said, ""hi""\nto me"`. That includes a comma, a newline, and 6 quotes. The first and last quotes are real boundaries that separate the inside of a quoted field from the outside.

We can determine whether a position is inside or outside a quoted field by maintaining a running parity over the quote positions. If the number of quotes before a given position is even, it’s outside. If it’s odd, it’s inside.

Applied to our example, everything between the first and last quote is inside the quoted field (shown in green). Notice that escaped quotes (`""`) are just 2 flips back to back. They cancel out, so the algorithm doesn’t need to distinguish them from real boundaries.

```
"where she said, ""hi""\nto me"
```

Walk through the example (hover over each line)

byte 0 is `"` (quote count = 1, odd → inside)

bytes 1-16 `where she said,` are inside

byte 17 is `"` (quote count = 2, even → outside)

byte 18 is `"` (quote count = 3, odd → inside)

bytes 19-20 `hi` are inside

byte 21 is `"` (quote count = 4, even → outside)

byte 22 is `"` (quote count = 5, odd → inside)

byte 23 `\n` is inside

bytes 24-28 `to me` are inside

byte 29 is `"` (quote count = 6, even → outside)

To compute this across the entire byte stream in one pass, we can take the prefix XOR of the quote bitmask. At each position, the result is the XOR of every quote bit up to and including that position.

```
        "where she said, ""hi""\nto me"
**quotes:** 100000000000000001100110000001
**prefix:** 111111111111111110111011111110
```

You’ll notice that escaped quote pairs (`""`) produce alternating `01` in the prefix mask. This is fine because we only use the prefix mask to filter commas and newlines, not quotes themselves. We invert the prefix to get an outside mask and `AND` it with the comma and newline bitmasks. This way, commas and newlines inside a quoted field get zeroed out, leaving only the real delimiters.

```
            "where she said, ""hi""\nto me"
**prefix:**     111111111111111110111011111110
**!prefix:**    000000000000000001000100000001

**commas:**     000000000000000100000000000000
**!prefix:**    000000000000000001000100000001
**& result:**   000000000000000000000000000000

**newlines:**   000000000000000000000001000000
**!prefix:**    000000000000000001000100000001
**& result:**   000000000000000000000000000000
```

Computing a prefix XOR across 64 bits can be done with a chain of shifts in 6 operations, or with a single `vmull_p64` (carryless multiplication) instruction on `ARM`:

```rust
// shift-XOR chain: 6 operations
fn prefix_xor(mut x: u64) -> u64 {
    x ^= x >> 1;
    x ^= x >> 2;
    x ^= x >> 4;
    x ^= x >> 8;
    x ^= x >> 16;
    x ^= x >> 32;
    x
}
```

```rust
// carryless multiplication: 1 instruction
use std::arch::aarch64::vmull_p64;
fn prefix_xor(x: u64) -> u64 {
    unsafe { vmull_p64(x, u64::MAX) as u64 }
}
```

## Step 3: Collecting field and row boundaries

Now that our comma and newline bitmasks only contain real delimiters, we iterate through them one `u64` at a time.

We carry a single boolean between chunks that tracks whether we ended inside a quoted field. If the previous `u64` had an odd number of quote bits, we’re still inside, and the prefix XOR for the current chunk needs to be inverted.

From the cleaned bitmasks, we extract delimiter positions by counting leading zeros (`clz`), a single-cycle instruction on most architectures. Each count gives us the offset of the next set bit, which is the next delimiter. If a newline appears before the next comma, it marks the end of both the current field and the current row.

```rust
type FieldRef = Range<usize>;
type RowRef = Vec<FieldRef>;

let mut rows = Vec::new();
let mut current_row = Vec::new();
let mut start = 0;
let mut end = 0;
let mut carry = false;

for i in 0..quote_bitsets.len() {
    let quotes = quote_bitsets[i];
    let outside = !prefix_xor(quotes, carry);

    // a branchless way of toggling carry
    // when the chunk has an odd number of quotes
    carry ^= (quotes.count_ones() & 1) != 0;

    let mut commas = comma_bitsets[i] & outside;
    let mut newlines = newline_bitsets[i] & outside;

    loop {
        let next_comma = commas.leading_zeros() as usize;
        let next_newline = newlines.leading_zeros() as usize;
        let advance = next_comma.min(next_newline);

        // no delimiters seen in this chunk
        if advance == 64 {
            end = (i + 1) * 64;
            break;
        }

        end += advance;

        if start < end {
            current_row.push(start..end);
            if next_newline < next_comma {
                rows.push(current_row.clone());
                current_row.clear();
            }
        }

        // skip the delimiter
        end += 1;

        // shift out the bits we already processed
        commas <<= advance + 1;
        newlines <<= advance + 1;
        start = end;
    }
}
```
