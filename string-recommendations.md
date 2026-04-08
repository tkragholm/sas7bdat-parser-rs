Yes — for **legacy string-heavy throughput**, I would look at a different set of crates than the numeric stack.

My shortlist would be:

**Core hot-path crates**

- **`bstr`** for treating fields as bytes first, with string-oriented helpers that do **not require valid UTF-8**. It gives you `BStr`, `BString`, and extension traits on `&[u8]` / `Vec<u8>`, which is a strong fit for legacy data with mixed or uncertain encoding. ([Docs.rs][1])
- **`memchr`** for fast delimiter, padding, and substring scans directly on byte slices. Its routines work on `&[u8]` regardless of encoding, and the crate exposes `memmem` for substring search too. Current docs also describe SIMD acceleration on x86_64, aarch64, and wasm32. ([Docs.rs][2])
- **`simdutf8`** if UTF-8 validation itself shows up in profiles. Its `basic` API is meant as a high-speed drop-in replacement for UTF-8 checking and is fastest on valid UTF-8. ([Docs.rs][3])

**Allocation / storage helpers**

- **`compact_str`** if you must own lots of short strings and they are commonly small. It uses a small-string optimization and stores short strings inline instead of always heap allocating. ([Docs.rs][4])
- **`smartstring`** is another good small-string option; its docs emphasize source compatibility with `String` and inline storage for short strings up to 23 bytes on 64-bit architectures. ([Docs.rs][5])

**Dedup / dictionary-style workloads**

- **`lasso`** if your corpus repeats many identical labels, category values, or short strings. It is a string interner designed for fast caching with O(1) internment and resolution, and it has both single-threaded and multi-threaded variants. ([Docs.rs][6])
- **`ahash`** if you are building hash maps or intern tables on the hot path. It is a high-performance keyed hash function, and the docs note that `HashBrown` uses aHash as its default hasher. ([Docs.rs][7])

My **practical recommendation** for your parser is this stack:

```toml
[dependencies]
bstr = "1"
memchr = "2"
simdutf8 = "0.1"
```

And then add one of these only if profiling says you need them:

```toml
compact_str = "0.9" # or smartstring = "1"
lasso = "0.7"
ahash = "0.8"
```

How I would apply them:

1. **Keep row fields as `&[u8]` for as long as possible** and use `bstr` for string-ish operations without forcing UTF-8 too early. That matches legacy corpora much better than eagerly constructing `String`. ([Docs.rs][1])

2. Use **`memchr`** for the hot loops:

   - find field terminators or separators
   - trim trailing spaces
   - search for sentinels
   - scan for line breaks or embedded delimiters
     This is usually a bigger win than fancy string abstractions. ([Docs.rs][2])

3. Only validate with **`simdutf8`** at the boundary where you truly need `&str`. If many downstream consumers can work with bytes, keep them as bytes. ([Docs.rs][3])

4. If output ownership is unavoidable and values are usually short, try **`compact_str`** or **`smartstring`** for the final materialization step, not during scanning. ([Docs.rs][4])

5. If your data has lots of repeated categorical strings, use **`lasso`** and likely pair it with **`ahash`**-backed maps/interner structures. ([Docs.rs][6])

So for your stated priority, I would rank them like this:

- **Must-look-at:** `bstr`, `memchr`
- **Likely useful:** `simdutf8`
- **Conditional:** `compact_str` or `smartstring`
- **Only if repetition is high:** `lasso`, `ahash`

If you want the most opinionated answer: for a **legacy string-heavy SAS parser**, I would start with **`bstr + memchr`**, and only add `simdutf8` after measuring UTF-8 validation cost.

[1]: https://docs.rs/bstr/latest/bstr/?utm_source=chatgpt.com "bstr - Rust - Docs.rs"
[2]: https://docs.rs/memchr/latest/memchr/?utm_source=chatgpt.com "memchr - Rust - Docs.rs"
[3]: https://docs.rs/simdutf8/latest/simdutf8/?utm_source=chatgpt.com "simdutf8 - Rust - Docs.rs"
[4]: https://docs.rs/compact_str/latest/compact_str/?utm_source=chatgpt.com "compact_str - Rust - Docs.rs"
[5]: https://docs.rs/smartstring/latest/smartstring/?utm_source=chatgpt.com "smartstring - Rust - Docs.rs"
[6]: https://docs.rs/lasso?utm_source=chatgpt.com "lasso - Rust - Docs.rs"
[7]: https://docs.rs/ahash/latest/ahash/?utm_source=chatgpt.com "ahash - Rust - Docs.rs"
