"""Standalone bStocks Tracker & Leaderboard Web Dashboard Server (Port 8898)."""

from __future__ import annotations

import http.server
import json
import os
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

import requests

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from leaderboard_service import build_leaderboard_table, compute_leaderboard_status, fetch_leaderboard_raw
from tracker_service import fetch_wallet_data

DEFAULT_PORT = 8898
DEFAULT_WALLET = ""
SUMMARY_CACHE_FILE = BASE_DIR / "data" / "lb_wallet_summaries.json"
_summary_lock = threading.Lock()


def _load_summary_cache() -> Dict[str, dict]:
    if not SUMMARY_CACHE_FILE.exists():
        return {}
    try:
        with open(SUMMARY_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_summary_cache: Dict[str, dict] = _load_summary_cache()


def _save_summary_cache() -> None:
    try:
        SUMMARY_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = SUMMARY_CACHE_FILE.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_summary_cache, f)
        os.replace(tmp, SUMMARY_CACHE_FILE)
    except Exception:
        pass


def _extract_wallet_summary(data: dict) -> dict:
    holding = data.get("holding") or {}
    holding_cost = sum(float(h.get("cost") or 0.0) for h in holding.values()) if isinstance(holding, dict) else 0.0
    holding_gas = sum(float(h.get("gas") or 0.0) for h in holding.values()) if isinstance(holding, dict) else 0.0
    return {
        "wallet": str(data.get("wallet") or "").lower(),
        "totalPoints": float(data.get("totalPoints") or 0.0),
        "totalVolume": float(data.get("totalVolume") or 0.0),
        "totalFee": float(data.get("totalFee") or 0.0),
        "totalSpread": float(data.get("totalSpread") or 0.0),
        "totalGas": float(data.get("totalGas") or 0.0),
        "costPer1000": float(data.get("costPer1000") or 0.0),
        "totalRounds": int(data.get("totalRounds") or 0),
        "totalOrders": int(data.get("totalOrders") or 0),
        "holdingCost": holding_cost,
        "holdingGas": holding_gas,
        "fetchedAt": time.time(),
    }


class TrackerHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/fetch-data?"):
            self._handle_fetch()
        elif self.path.startswith("/leaderboard/wallet?"):
            self._handle_leaderboard_wallet()
        elif self.path.startswith("/leaderboard"):
            self._handle_leaderboard()
        elif self.path == "/tracker/default":
            self._send_json({"wallet_address": DEFAULT_WALLET})
        elif self.path == "/mobile" or self.path == "/mobile/":
            self.path = "/mobile_hub.html"
            super().do_GET()
        elif self.path == "/" or self.path == "":
            self.path = "/tracker.html"
            super().do_GET()
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/hub/proxy":
            self._handle_hub_proxy()
        else:
            self._send_json({"error": "Not found"}, status=404)

    def _handle_hub_proxy(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            req_body = json.loads(raw.decode("utf-8"))
        except Exception:
            self._send_json({"ok": False, "error": "Invalid JSON"}, status=400)
            return

        target_url = str(req_body.get("url") or "").strip()
        method = str(req_body.get("method") or "GET").upper()
        payload = req_body.get("payload")
        parsed = urlparse(target_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            self._send_json({"ok": False, "error": "URL máy không hợp lệ"}, status=400)
            return

        try:
            if method == "POST":
                resp = requests.post(target_url, json=payload if payload is not None else {}, timeout=8.0)
            else:
                resp = requests.get(target_url, timeout=15.0)
            self._send_json(resp.json(), status=resp.status_code)
        except Exception as e:
            self._send_json({"ok": False, "offline": True, "error": str(e)}, status=502)

    def _handle_fetch(self):
        qs = parse_qs(urlparse(self.path).query)
        wallet = qs.get("wallet", [""])[0].strip()
        if not wallet or not wallet.startswith("0x"):
            self._send_json({"error": "Địa chỉ ví không hợp lệ (phải bắt đầu bằng 0x)"})
            return
        try:
            data = fetch_wallet_data(wallet)
            summary = _extract_wallet_summary(data)
            with _summary_lock:
                _summary_cache[wallet.lower()] = summary
                _save_summary_cache()
            lb_raw = fetch_leaderboard_raw()
            data["leaderboard"] = compute_leaderboard_status(lb_raw, wallet, data.get("totalPoints"))
            self._send_json(data)
        except Exception as e:
            print(f"[ERROR] {e}", flush=True)
            self._send_json({"error": str(e)})

    def _handle_leaderboard(self):
        qs = parse_qs(urlparse(self.path).query)
        try:
            limit = int(qs.get("limit", ["30"])[0])
        except ValueError:
            limit = 30
        force = qs.get("force", ["0"])[0] in ("1", "true", "yes")
        try:
            lb_raw = fetch_leaderboard_raw(force=force)
            table = build_leaderboard_table(lb_raw, limit=limit)
            with _summary_lock:
                for row in table.get("entries", []):
                    cached = _summary_cache.get(row["address"].lower())
                    if cached:
                        row["onchain"] = cached
            self._send_json(table)
        except Exception as e:
            print(f"[ERROR] {e}", flush=True)
            self._send_json({"error": str(e)})

    def _handle_leaderboard_wallet(self):
        qs = parse_qs(urlparse(self.path).query)
        wallet = qs.get("wallet", [""])[0].strip()
        if not wallet or not wallet.startswith("0x"):
            self._send_json({"error": "Địa chỉ ví không hợp lệ"})
            return
        try:
            data = fetch_wallet_data(wallet)
            summary = _extract_wallet_summary(data)
            with _summary_lock:
                _summary_cache[wallet.lower()] = summary
                _save_summary_cache()
            self._send_json({"wallet": wallet, "onchain": summary})
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
    print(f"  Desktop:   http://localhost:{port}", flush=True)
    print(f"  Mobile:    http://localhost:{port}/mobile (0.0.0.0:{port})", flush=True)
    print("=" * 60, flush=True)

    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), TrackerHandler)
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
