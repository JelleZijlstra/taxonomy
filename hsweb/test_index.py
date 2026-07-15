import asyncio
import gzip
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

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
