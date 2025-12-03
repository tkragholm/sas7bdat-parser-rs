## Investigation notes: noisy 0x8000 pages

- Real-world files emit thousands of warnings about `type=0x8000` pages with `subheaders=3272`, `page_header_size=24`, `pointer_size=12`. These pages are not data pages; base type is META (0) with a high-bit flag.
- The bytes at `page_header_size-4` on these pages are `0x0CC8` (3272), implying a 39 KB pointer table on a 32 KB page. That makes every page trip the “pointer table exceeds bounds” warning.
- Readstat (C), cpp, and C# libraries avoid this noise by only scanning leading META/MIX/META2 pages and trailing AMD/META2 pages; they stop when they hit DATA. They do not attempt to parse mid-file META pages with extra flags.
- Update: metadata scan now uses a PageKind classifier and skips COMP/COMP_TABLE/unknown; row iteration parses all non-COMP known kinds. 0x8000 pages are now parsed for rows but not metadata; comp-table pages are recognized and skipped.
- Remaining risk: if comp-table pages actually carry row count tables we could optionally parse them to accelerate random access; currently we still skip them. Otherwise core PGTYPE behavior mirrors docs/readstat.
