# nk play: Nested Library Refactor Plan

We need nk play to behave like nk read: it should treat the player root as a full tree of books, not just a flat directory. Below is the plan for the next session.

## 1. Normalize book identifiers
- Use path-relative IDs (e.g. `ラノベ/今さらですが…`) everywhere instead of just the folder name.
- Update `_book_id_from_target`, bookmark filenames, build-status keys, and every `@app.get/patch/post("/api/books/{book_id}")` route to accept `Path` parameters (FastAPI `/{book_id:path}`) and resolve safely against `root`.
- Ensure file writes (bookmarks, status snapshots) use the same relative path key so nested folders share the same infrastructure.

## 2. Recursive discovery helpers
- Replace `_list_books` with a function that walks the tree depth-first and returns:
  * actual book directories (contain `.txt`) with metadata, author/title, timestamps.
  * "collection" directories that contain zero `.txt` themselves but have descendant books; include their path, name, child-count, and a few sample cover paths (optional for now).
- Add helper to list children given a `prefix` (relative path). This enables browsing `books/` and `books/ノンフィクション` with the same code.

## 3. API surface changes
- `/api/books` should accept `prefix` and `sort` params and return:
  ```json
  {
    "prefix": "ラノベ",
    "parent_prefix": "",
    "collections": [...],
    "books": [...]
  }
  ```
- Each collection entry has `id`, `name`, `path`, `book_count`, optional `cover_samples` (can be empty placeholder initially). `books` entries stay as today but include their `path` (relative ID).
- Update all `/api/books/{book_id}/...` routes to resolve nested IDs.

## 4. Frontend adjustments
- Replace the single grid with:
  * a breadcrumb/back control showing the current prefix and letting users jump up one level.
  * collection cards (simple panel with folder icon + stats) that call `loadBooks(prefix+'/' + collection.id)`.
  * book cards unchanged except they now use the relative ID when requesting chapters/building/etc.
- Make the upload card stay pinned, and hide it on mobile as before.
- Persist `prefix` in state; sorting continues to work (same dropdown) but is scoped to the current prefix.

## 5. Regression checks
- Build/abort/bookmark flows should work in nested directories by verifying the new path IDs reach the same files.
- nk read and CLI chapterization remain untouched.
- No cover tile collage yet; if time remains, we can reuse up to 9 cover URLs from `cover_samples` to style the collection cards later.

This plan should fit in the remaining context; next session can follow these steps sequentially.
