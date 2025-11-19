from __future__ import annotations

import json

from nk.player import PlayerConfig, create_app


def _create_book(root, name: str = "Sample Book") -> str:
    book_dir = root / name
    book_dir.mkdir()
    (book_dir / "001.txt").write_text("テキスト", encoding="utf-8")
    return name


def _find_route(app, path: str, method: str):
    method = method.upper()
    for route in app.router.routes:
        if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
            return route.endpoint
    raise RuntimeError(f"Route {method} {path} not found")


def test_delete_book_endpoint_removes_directory(tmp_path) -> None:
    library = tmp_path / "library"
    library.mkdir()
    book_name = _create_book(library, "Book One")
    app = create_app(PlayerConfig(root=library))
    delete_route = _find_route(app, "/api/books/{book_id:path}", "DELETE")
    response = delete_route(book_name)
    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload.get("deleted") is True
    assert payload.get("book") == book_name
    assert not (library / book_name).exists()
