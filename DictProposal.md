Yes. For a **SAS7BDAT parser**, the fastest approach is usually **not** “intern every string as you parse it.”\
The fastest approach is:

1. **Borrow first**
2. **Stage duplicates cheaply**
3. **Promote only proven repeats into a dictionary**
4. **Return compact IDs or borrowed views on the hot path**

That avoids turning every character field into a hash-table + allocation problem.

## What to optimize for

In SAS7BDAT, character values are typically stored as **fixed-width byte regions** inside rows. That gives you a huge advantage:

- you can often reference bytes directly from the page buffer
- you can avoid allocating for unique strings
- repeated values can be deduplicated only after you know they repeat

So the core idea is:

- On first sighting of a string: keep a **slice/view** into the row/page buffer
- On second sighting: promote it into a **dictionary arena**
- On later sightings: return the dictionary ID immediately

That is much faster than eager interning.

## Recommended design: staged interning

Use a **two-level representation** for string fields:

```rust
enum StrRef {
    Borrowed { ptr, len },   // points into current page / row buffer
    Interned { dict_id },    // stable deduplicated string
}
```

And a staging structure:

```rust
staging key = (column_id, len, fingerprint, bytes...)
```

Important: include `column_id` unless you explicitly want cross-column deduplication.\
For performance, **per-column dictionaries** are usually better than one global dictionary.

## Why per-column is faster

A global dictionary creates:

- larger hash tables
- more cache misses
- more collisions
- more lock contention if parallel

Per-column gives:

- smaller tables
- much better locality
- easier cardinality heuristics
- easier parallelization

Only use a global dictionary if you have evidence that cross-column dedup is worth it.

## Best-performing pipeline

## 1. Parse rows without allocating strings

For each character column, extract:

```rust
ptr = row_base + column_offset
len = declared_width
```

Then do **lazy right-trim** of spaces only when required by your semantics.
Do not immediately build `String`.
Prefer this kind of internal value:

```rust
struct RawStr {
    const uint8_t* ptr;
    uint32_t len;
}
```

If the page buffer stays alive long enough, this is nearly free.

## 2. Compute a very cheap fingerprint

Before any dictionary lookup, compute:

- length
- first 8 bytes / first 16 bytes if available
- a fast hash only when needed

For fixed-width SAS char columns, many values differ very early, so you can reject most candidates with:

- `len`
- first 8 bytes
- maybe last 8 bytes

Then only do full compare on rare collisions.

### Good pattern

```rust
fingerprint = hash64(bytes, len)
lookup key = (len, fingerprint)
verify = memcmp(bytes, candidate_bytes, len)
```

Use a **fast non-cryptographic hash**:

- `xxh3`
- `wyhash`
- `ahash`-style keyed hash
- `farmhash` / `cityhash` class

For ultra-high performance, `XXH3_64bits` is a strong default.

## 3. Promote only on second sighting

This is the big win.

### First sighting

Store only a lightweight stage entry:

```rust
StageEntry {
    hash,
    len,
    first_ptr,
    state = SeenOnce
}
```

No allocation.

### Second sighting

If same bytes appear again:

- allocate once in an arena/slab
- assign `dict_id`
- update stage entry to `Interned`

### Third+ sightings

Return `dict_id` directly.

This avoids allocating millions of one-off values.

## Data structure that works well

For each string column:

```rust
struct ColumnDict {
    StageMap stage;       // hash -> slot
    Arena arena;          // contiguous storage for interned bytes
    Vec<DictEntry> dict;  // id -> {offset, len}
}
```

Where `StageMap` is an open-addressing table:

```rust
struct Slot {
    uint64_t hash;
    uint32_t len;
    uint32_t meta;        // state, dict_id or arena offset
}
```

Use:

- **open addressing**
- **linear probing** or **robin hood**
- power-of-two capacity
- low-branch lookup loop
- no heap objects per entry

Avoid standard chained hash maps in the hot path.

## Why open addressing wins here

It keeps:

- metadata contiguous
- probes predictable
- fewer pointer indirections
- much better cache behavior

For raw speed, flat/open-addressed maps usually beat tree maps and pointer-heavy chained maps.

## The real trick: split “seen once” from “interned”

Do **not** store all first-seen strings in permanent memory.

Use a small state machine:

```rust
enum EntryState {
    Empty,
    SeenOnce { ptr },
    Interned { dict_id }
}
```

That gives you:

- zero alloc on first sighting
- one alloc on second sighting
- constant-time reuse afterward

This is usually the sweet spot for repeated categorical strings like:

- country
- state
- gender
- status
- code tables
- repeated labels

## Critical SAS-specific optimization

## Keep values as raw bytes, not Unicode strings

SAS7BDAT character fields are byte-oriented.\
Do not decode to UTF-8/UTF-16 in the parser hot path unless you absolutely must.

Instead:

- intern raw bytes
- decode later at API boundary if needed

That avoids:

- validation cost
- transcoding cost
- extra allocations

If your API needs strings, expose both:

- `raw_bytes()`
- `decoded_string()` lazily

## Adaptive strategy: only dictionary-stage low-cardinality columns

Not every char column benefits.

A free-text comment column will destroy dictionary efficiency.

So for each column, track a tiny sample:

- first `N` rows
- count repeats vs uniques
- average length

Then decide:

- **enable staging** if duplicate rate is high enough
- **disable staging** if column looks high-cardinality

## Example heuristic

After first 4096 values:

- if duplicate rate < 5%, disable interning for that column
- if average length < 3 and duplicates are high, intern aggressively
- if strings are long and mostly unique, keep borrowed/owned without dict

This matters a lot. The fastest interning system is the one you **don’t** run on the wrong columns.

## Page-scoped staging vs file-scoped dictionary

There are two strong options.

## Option A: page-local staging + global promotion

- cheap lookups within page
- good locality
- easy reset
- lower memory pressure

## Option B: file-wide per-column dictionary

- maximum dedup across entire file
- best compression of output representation
- larger tables

For speed, I recommend:

- **page-local “seen once” staging**
- **file-wide per-column intern table only for promoted repeats**

So:

1. page-local stage catches local repetition
2. second hit promotes into stable file-wide dictionary
3. page-local stage is cleared per page

That keeps the hottest structure small.

## Memory layout for ultra-high performance

## Store interned bytes in one arena

Do not allocate one string at a time.

Use:

- bump allocator
- slab allocator
- monotonic arena

Example:

```rust
arena: [bytes bytes bytes bytes...]
dict[id] = { offset, len }
```

Benefits:

- one contiguous memory region
- no allocator contention
- lower fragmentation
- much better cache locality

## Store dictionary entries in SoA or compact AoS

Instead of heavyweight string objects:

```rust
struct DictEntry {
    uint32_t offset;
    uint32_t len;
}
```

If total arena can exceed 4 GiB, use 64-bit offsets. Otherwise 32-bit is faster and denser.

## Hash table design details that matter

## Use metadata side arrays

A very fast layout is:

```
hashes[]   // 64-bit fingerprints
lens[]     // 32-bit lengths
state[]    // byte or small int
value[]    // ptr or dict_id
```

This can outperform storing full structs because scans touch tighter arrays.

## Probe loop should be branch-light

Pseudo:

```
idx = hash & mask;
for (;;) {
    if (state[idx] == EMPTY) return MISS;
    if (hashes[idx] == hash && lens[idx] == len) {
        if (memcmp(bytes, candidate(idx), len) == 0) return HIT;
    }
    idx = (idx + 1) & mask;
}
```

Keep load factor conservative, around:

- 0.5 to 0.75 for max speed
- not 0.9+

For ultra-high performance, extra memory is often worth it.

## SIMD helps, but only in narrow places

SIMD is useful for:

- trimming trailing spaces
- comparing fixed-width strings
- scanning for all-space values
- maybe first mismatch checks

It is usually **not** where the biggest win comes from.\
The biggest win comes from:

- no allocation
- no decoding
- no pointer-heavy maps
- adaptive promotion

So use SIMD after you fix architecture.

## Fast path hierarchy

Your hot path should look like this:

## Fast path 1: repeated null/blank

Special-case empty/all-space values.

Give them reserved IDs like:

This alone can remove a huge amount of hashing in some SAS datasets.

## Fast path 2: already interned common values

If a column is known low-cardinality, cache a few most recent IDs:

```
last_1 hash/len -> id
last_2 hash/len -> id
```

A tiny 2-entry or 4-entry direct-mapped cache can be surprisingly effective.

## Fast path 3: staging lookup

Hash lookup in per-column stage map.

## Slow path: full compare + promotion

Only after hash/len match.

## Parallel parsing

If you want real throughput, parallelize by **page groups** or **row chunks**.

But avoid one shared global dictionary on the hot path.

## Best pattern

Each worker thread has:

- local page-stage map
- local per-column promotion buffer
- local arena chunk

Then periodically merge into a file-wide dictionary, or keep dictionaries sharded.

### If global IDs must be stable

Use sharded dictionaries:

```
shard = hash % num_shards
```

Each shard has its own lock or single-writer ownership.

Even better: parse with thread-local IDs first, then remap in a consolidation pass if needed.

Shared concurrent hash maps are usually slower than people expect.

## Concrete architecture I would use

## Parser output layer

For every char field during parse:

```
ParsedStr =
    Missing
    Borrowed(ptr, len)
    Interned(id)
```

## Column state

Per string column:

```rust
ColumnState {
    sample_stats,
    interning_mode,   // Off / Sample / On
    stage_map,        // page-local or chunk-local
    dict_map,         // stable promoted values
    arena,
    dict_entries
}
```

## Mode transitions

- Start in `Sample`
- after sample window:
  - switch to `On` if repeats justify it
  - otherwise `Off`

When `Off`:

- just return borrowed or owned values
- skip dict work entirely

---

## Pseudocode

Here is the core algorithm.

```rust
fn handle_str(col: &mut ColumnState, bytes: &[u8]) -> StrRef {
    let bytes = rtrim_spaces(bytes);

    if bytes.is_empty() {
        return StrRef::Interned { dict_id: EMPTY_ID };
    }

    match col.mode {
        Mode::Off => {
            return StrRef::Borrowed {
                ptr: bytes.as_ptr(),
                len: bytes.len() as u32,
            };
        }

        Mode::Sample | Mode::On => {}
    }

    let h = xxh3(bytes);

    if let Some(id) = col.recent_cache.lookup(h, bytes) {
        return StrRef::Interned { dict_id: id };
    }

    match col.stage_map.lookup(h, bytes) {
        None => {
            col.stage_map.insert_seen_once(h, bytes);
            col.sample_stats.observe_unique(bytes.len());
            StrRef::Borrowed {
                ptr: bytes.as_ptr(),
                len: bytes.len() as u32,
            }
        }

        Some(StageHit::SeenOnce(prev_bytes)) => {
            if prev_bytes == bytes {
                let id = col.promote_into_dict(bytes, h);
                col.stage_map.replace_with_interned(h, bytes, id);
                col.recent_cache.insert(h, id, bytes);
                col.sample_stats.observe_repeat(bytes.len());
                StrRef::Interned { dict_id: id }
            } else {
                // hash collision path
                col.stage_map.insert_collision_chain_or_probe(h, bytes);
                StrRef::Borrowed {
                    ptr: bytes.as_ptr(),
                    len: bytes.len() as u32,
                }
            }
        }

        Some(StageHit::Interned(id)) => {
            col.recent_cache.insert(h, id, bytes);
            col.sample_stats.observe_repeat(bytes.len());
            StrRef::Interned { dict_id: id }
        }
    }
}
```

## Things that usually kill performance

## 1. Creating owned strings too early

Bad:

```
read bytes -> trim -> String::from(...) -> hashmap lookup
```

That pays allocation before you even know if dedup helps.

## 2. Using general-purpose hash maps in the hottest loop

They are often fine functionally, but not peak-throughput.

## 3. Global synchronization

One shared dictionary across all threads can erase parallel gains.

## 4. Decoding text during parse

Keep raw bytes.

## 5. Trimming/copying every string eagerly

Lazy trim or branchless trim is better.

## When a two-pass parse is worth it

If you truly want **maximum** performance and can afford two passes:

## Pass 1

- parse metadata
- sample string columns
- estimate cardinality and duplicate rate

## Pass 2

- fully parse with optimized strategy per column:
  - `Off`
  - `Borrow-only`
  - `Stage+Intern`
  - maybe direct dictionary for very low-cardinality columns

Two-pass can win on very large files because it avoids expensive wrong choices.

## A very strong high-performance compromise

If you want something practical and very fast, do this:

### For each character column

- sample first 4096 rows
- if duplicate rate >= 10%, enable staged interning
- otherwise keep borrowed slices only

### For enabled columns

- per-column open-address stage map
- promote on second sighting
- arena storage for interned bytes
- file-wide stable IDs
- 4-entry recent-value cache

### For all columns

- special-case blank/missing
- keep bytes raw, not decoded
- avoid copying unless promoted

That will usually get you most of the gain without extreme complexity.

## If you want absolute peak speed

The likely best-performing implementation stack is:

- **Rust**: `&[u8]`, custom open-address map, bump arena, `xxhash-rust` or `xxh3`, optional `std::simd`

## My strongest recommendation

For an ultra-high-performance SAS7BDAT parser, implement:

**adaptive per-column staged interning with promotion-on-second-sighting, raw-byte storage, arena-backed dictionary, and open-address hash tables.**

That combination is usually the best balance of:

- throughput
- memory efficiency
- low allocator pressure
- good cache locality
- easy parallelization
