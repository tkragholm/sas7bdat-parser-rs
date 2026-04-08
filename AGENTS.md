# Codebase Navigation — Use indxr MCP tools

An MCP server called `indxr` is available with 3 compound tools. Always use indxr tools before reading full files.

## Exploration workflow
1. `find(query)` — find files/symbols by concept, name, callers, or signature pattern
   - Modes: `relevant` (default), `symbol`, `callers`, `signature`
2. `summarize(path)` — understand files/symbols without reading source
   - Auto-detects: file path → summary, glob → batch, symbol name → interface details
   - `scope: "public"` for public API only
3. `read(path, symbol?)` — read source by symbol name or line range
   - Supports `symbols` array and `collapse: true`
4. Read (full file) — ONLY when editing or need exact formatting

## When to read full files instead
- You need to edit a file
- You need exact formatting/whitespace
- The file is not source code (e.g., config files, documentation)

## Do NOT
- Read full source files just to understand what's in them — use `summarize(path)`
- Dump all files into context — use MCP tools to be surgical
- Use `git diff` when `get_diff_summary` would suffice (requires `--all-tools`)

## Wiki tools (if available)
If a codebase wiki exists, use `wiki_search(query)` and `wiki_read(page)` as a first step for understanding architecture and module design before diving into structural tools.

## After making code changes
Run `regenerate_index` to keep the index current.
