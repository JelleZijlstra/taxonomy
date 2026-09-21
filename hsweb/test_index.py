import asyncio
import gzip
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer
from yarl import URL

from . import index
from .index import GAME_DATA_CACHE_CONTROL, IMMUTABLE_CACHE_CONTROL, make_app


def test_static_asset_response_policy(tmp_path: Path) -> None:
    async def run_test() -> None:
        build_dir = tmp_path / "build"
        static_dir = build_dir / "static" / "js"
        static_dir.mkdir(parents=True)
        (build_dir / "index.html").write_text("<html></html>")
        favicon = b"small icon"
        (build_dir / "favicon.ico").write_bytes(favicon)
        (build_dir / "logo192.png").write_bytes(b"logo")
        (build_dir / "logo512.png").write_bytes(b"logo")
        (build_dir / "manifest.json").write_text("{}")
        javascript = b"const value = 'compressible';\n" * 100
        (static_dir / "main.12345678.js").write_bytes(javascript)
        (static_dir / "main.12345678.js.map").write_text("source map")

        client = TestClient(TestServer(make_app(str(tmp_path))), auto_decompress=False)
        await client.start_server()
        try:
            response = await client.get(
                "/static/js/main.12345678.js", headers={"Accept-Encoding": "gzip"}
            )
            assert response.status == 200
            assert response.headers["Content-Encoding"] == "gzip"
            assert response.headers["Cache-Control"] == IMMUTABLE_CACHE_CONTROL
            assert gzip.decompress(await response.read()) == javascript

            uncompressed_response = await client.get(
                "/static/js/main.12345678.js", headers={"Accept-Encoding": "identity"}
            )
            assert uncompressed_response.status == 200
            assert "Content-Encoding" not in uncompressed_response.headers
            assert await uncompressed_response.read() == javascript

            source_map_response = await client.get("/static/js/main.12345678.js.map")
            assert source_map_response.status == 404

            favicon_response = await client.get("/favicon.ico")
            assert favicon_response.status == 200
            assert favicon_response.headers["Cache-Control"] == GAME_DATA_CACHE_CONTROL
            assert await favicon_response.read() == favicon

            manifest_response = await client.get("/manifest.json")
            assert manifest_response.status == 200
            assert manifest_response.content_type == "application/json"
            assert await manifest_response.json() == {}
        finally:
            await client.close()

    asyncio.run(run_test())


def test_documentation_assets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_root = tmp_path / "docs"
    note_dir = docs_root / "research-notes"
    note_dir.mkdir(parents=True)
    svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 700"/>'
    (note_dir / "localities.svg").write_text(svg)
    (note_dir / "localities.csv").write_text("name,latitude\nbreviceps,32.7751\n")
    (note_dir / "map.html").write_text("<html><body>Interactive map</body></html>")
    (note_dir / "note.md").write_text("# Research note")
    (tmp_path / "outside.svg").write_text("private file")
    (note_dir / "outside.svg").symlink_to(tmp_path / "outside.svg")
    monkeypatch.setattr(index, "DOCS_ROOT", docs_root)
    build_dir = tmp_path / "build"
    (build_dir / "static").mkdir(parents=True)
    (build_dir / "index.html").write_text("<html>React app</html>")

    async def run_test() -> None:
        async with TestClient(TestServer(make_app(str(tmp_path)))) as client:
            for filename, content_type in (
                ("localities.svg", "image/svg+xml"),
                ("localities.csv", "text/csv"),
                ("map.html", "text/html"),
            ):
                response = await client.get(f"/docs/research-notes/{filename}")
                assert response.status == 200
                assert response.content_type == content_type
                assert await response.text() == (note_dir / filename).read_text()
                assert response.headers["Cache-Control"] == (
                    "public, max-age=0, must-revalidate"
                )
                cached = await client.get(
                    f"/docs/research-notes/{filename}",
                    headers={"If-None-Match": response.headers["ETag"]},
                )
                assert cached.status == 304

            for path in (
                "research-notes/missing.svg",
                "research-notes/note.md",
                "research-notes/outside.svg",
                "%2e%2e/outside.svg",
            ):
                response = await client.get(URL(f"/docs/{path}", encoded=True))
                assert response.status == 404

            # Extensionless documentation pages still go through React.
            response = await client.get("/docs/research-notes/note")
            assert response.status == 200
            assert await response.text() == "<html>React app</html>"

    asyncio.run(run_test())
