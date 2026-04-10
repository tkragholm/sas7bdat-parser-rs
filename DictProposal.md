# SAS7BDAT High-Performance String Dictionary Design (Revised)

This document specifies an optimized, implementation-ready strategy for handling character fields in a SAS7BDAT parser. The goal is to maximize throughput while preserving correct SAS semantics and memory safety.

---

## 1. Executive Summary: The "Borrow–Stage–Promote" Model

We do **not** eagerly allocate or intern strings. Instead:

1. **Borrow first**: Treat values as `&[u8]` views into the page buffer.
2. **Stage cheaply**: Track first sightings without allocation.
3. **Promote on proven repetition**: Allocate only after a value repeats (under well-defined rules).

Promotion is **workload-dependent**, not guaranteed after a fixed number of observations.

---

## 2. Semantic Contract (CRITICAL)

### 2.1 Storage vs Comparison vs API Semantics

We explicitly separate three layers:

| Layer                    | Behavior                                                             |
| ------------------------ | -------------------------------------------------------------------- |
| **Raw storage**          | Fixed-width byte slices from SAS pages (may include trailing blanks) |
| **Comparison semantics** | Shorter values are logically right-padded with blanks                |
| **API representation**   | Configurable: `Preserve`, `RTrim`, or `Strip`                        |

### 2.2 Trimming Modes

The parser MUST expose a configurable mode:

```rust
enum TrimMode {
    Preserve,   // Keep raw bytes exactly
    RTrim,      // Remove trailing spaces only
    Strip,      // Remove leading + trailing spaces
}
```

Default: `RTrim`

### 2.3 Blank / Missing Semantics

We define a canonical representation:

- A value is **blank** if all bytes are `0x20` after trimming mode is applied.
- Blank values map to a reserved dictionary ID:

```rust
const BLANK_ID: u32 = 0;
```

NOTE: This is a **policy decision**, not a universal SAS truth. It must be documented to users.

---

## 3. Lifetime & Safety Contract

### 3.1 Borrowed Values

Borrowed values are valid **only while the page buffer is alive**.

We encode this explicitly:

```rust
enum StrRef<'a> {
    Borrowed(&'a [u8]),
    Interned { dict_id: u32 },
}
```

### 3.2 Rules

- Borrowed values MUST NOT escape the page scope.
- Any value that must outlive the page MUST be promoted or copied.
- Public APIs returning owned data must materialize values safely.

---

## 4. Architectural Principles

### Zero Allocation by Default

No allocation on first sighting.

### Raw Byte Interning

Intern bytes, not decoded strings.

### Per-Column Dictionaries

Each column owns:

- Its staging map
- Its dictionary arena
- Its heuristics

Benefits:

- Cache locality
- No cross-column contention
- Independent tuning

---

## 5. Staging Pipeline

### Step 1: Lazy Extraction

```rust
struct RawStr<'a> {
    bytes: &'a [u8]
}
```

Trimming is applied lazily according to `TrimMode`.

---

### Step 2: Fingerprinting

Compute:

- length
- prefix (first 8–16 bytes)
- 64-bit hash (e.g. `xxh3`)

---

### Step 3: Staging Map (Correct Collision Handling)

The staging map is a flat open-addressed table.

Each slot stores:

```rust
enum StageEntry<'a> {
    Empty,
    SeenOnce { bytes: &'a [u8], hash: u64 },
    Interned { dict_id: u32, hash: u64 },
}
```

**Collision rule (IMPORTANT):**

- Collisions MUST probe to the next slot (Robin Hood or linear probing).
- Distinct values with identical hashes MUST coexist.
- No value is dropped due to collision.

---

### Step 4: Promotion Rules

Promotion occurs when:

- Same value is observed again **within the staging horizon**

Important nuance:

> Promotion is based on **local observation**, not guaranteed global second sighting.

---

## 6. Dictionary Storage

### Arena Allocation

- Bump allocator
- Contiguous storage
- No per-string allocation

### ID Semantics

- IDs are stable **within a column**
- Global determinism is optional (see §9)

---

## 7. Adaptive Heuristics (Tunable)

Heuristics are **guidelines**, not constants.

Suggested signals:

- Duplicate rate
- Average string length
- Bytes saved vs overhead

Example policy:

- Disable interning if estimated savings < cost
- Prefer interning for:

  - Short values
  - High repetition

---

## 8. Fast Path Hierarchy

Order matters:

1. **Blank Check** → return `BLANK_ID`
2. **Recent Cache** (2–4 entries)
3. **Stage Map Lookup**
4. **Probe + Compare**
5. **Promote (if repeated)**

---

## 9. Parallelism & ID Strategy

### Thread Model

Each worker has:

- Local staging map
- Local dictionary OR shard

### ID Options

**Option A: Sharded Global Dictionary**

- `shard = hash % N`
- Deterministic IDs

**Option B: Thread-Local Dictionaries + Merge**

- Faster
- IDs remapped in final pass
- NOT deterministic unless explicitly stabilized

Document which mode is used.

---

## 10. Corrected Pseudocode

```rust
fn handle_str<'a>(col: &mut ColumnState<'a>, bytes: &'a [u8]) -> StrRef<'a> {
    let bytes = apply_trim(bytes, col.trim_mode);

    if is_blank(bytes) {
        return StrRef::Interned { dict_id: BLANK_ID };
    }

    let h = xxh3(bytes);

    // Recent cache
    if let Some(id) = col.recent_cache.lookup(h, bytes) {
        return StrRef::Interned { dict_id: id };
    }

    // Probe staging map
    let mut slot = col.stage_map.find_slot(h, bytes);

    match slot.entry {
        StageEntry::Empty => {
            slot.entry = StageEntry::SeenOnce { bytes, hash: h };
            return StrRef::Borrowed(bytes);
        }
        StageEntry::SeenOnce { bytes: prev, .. } => {
            if prev == bytes {
                let id = col.promote(bytes, h);
                slot.entry = StageEntry::Interned { dict_id: id, hash: h };
                col.recent_cache.insert(h, id);
                return StrRef::Interned { dict_id: id };
            }
            // collision → continue probing handled in find_slot
        }
        StageEntry::Interned { dict_id, .. } => {
            col.recent_cache.insert(h, dict_id);
            return StrRef::Interned { dict_id };
        }
    }
}
```

---

## 11. Anti-Patterns

- Eager `String` allocation
- Standard `HashMap` in hot path
- Global mutex-protected dictionary
- Dropping collided values
- Letting borrowed data escape page lifetime

---

## 12. Recommendation

Use **adaptive staged interning with explicit semantics and lifetime guarantees**.

This design is expected to outperform eager allocation strategies on workloads with moderate repetition in fixed-width character columns, while remaining correct and safe.
