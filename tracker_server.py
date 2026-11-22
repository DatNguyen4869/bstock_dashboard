"""Standalone bStocks Tracker & Leaderboard Web Dashboard Server (Port 8898)."""

from __future__ import annotations

import http.server
import json
import sys
import webbrowser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from leaderboard_service import compute_leaderboard_status, fetch_leaderboard_raw
from tracker_service import fetch_wallet_data

DEFAULT_PORT = 8898
DEFAULT_WALLET = ""


class TrackerHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def do_GET(self):
        if self.path.startswith("/fetch-data?"):
            self._handle_fetch()
        elif self.path == "/tracker/default":
            self._send_json({"wallet_address": DEFAULT_WALLET})
        elif self.path == "/" or self.path == "":
            self.path = "/tracker.html"
            super().do_GET()
        else:
            super().do_GET()

    def _handle_fetch(self):
        qs = parse_qs(urlparse(self.path).query)
        wallet = qs.get("wallet", [""])[0].strip()
        if not wallet or not wallet.startswith("0x"):
            self._send_json({"error": "Địa chỉ ví không hợp lệ (phải bắt đầu bằng 0x)"})
            return
        try:
            data = fetch_wallet_data(wallet)
            lb_raw = fetch_leaderboard_raw()
            data["leaderboard"] = compute_leaderboard_status(lb_raw, wallet, data.get("totalPoints"))
            self._send_json(data)
        except Exception as e:
            print(f"[ERROR] {e}", flush=True)
            self._send_json({"error": str(e)})

    def _send_json(self, data: Any, status: int = 200) -> None:
        content = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        msg = format % args
        if "/fetch" in msg or "error" in msg.lower():
            print(f"[HTTP] {msg}", flush=True)


def run_server(port: int = DEFAULT_PORT, open_browser: bool = True) -> None:
    print("=" * 60, flush=True)
    print("  bStocks Standalone Tracker Dashboard", flush=True)
    print(f"  Workspace: {BASE_DIR}", flush=True)
    print(f"  URL:       http://localhost:{port}", flush=True)
    print("=" * 60, flush=True)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), TrackerHandler)
    if open_browser:
        webbrowser.open(f"http://localhost:{port}")
    print("[SERVER] Ready!", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER] Stopped.", flush=True)


if __name__ == "__main__":
    no_browser = "--no-browser" in sys.argv
    port = DEFAULT_PORT
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])
    run_server(port=port, open_browser=not no_browser)
