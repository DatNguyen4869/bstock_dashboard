"""Shared Trust Wallet bStocks leaderboard fetcher and rank estimator."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

import requests

LEADERBOARD_URL = "https://api.trustwallet.com/v1/campaigns/bstocks/leaderboard"
LEADERBOARD_TTL_SEC = 30.0

_lb_lock = threading.Lock()
_lb_cache: Dict[str, Any] = {"ts": 0.0, "data": None}


def fetch_leaderboard_raw(force: bool = False) -> Optional[dict]:
    """Fetch Trust Wallet bStocks leaderboard with a thread-safe TTL cache."""
    now = time.time()
    with _lb_lock:
        if not force and _lb_cache["data"] is not None and (now - _lb_cache["ts"]) < LEADERBOARD_TTL_SEC:
            return _lb_cache["data"]
    try:
        resp = requests.get(
            LEADERBOARD_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=6.0,
        )
        resp.raise_for_status()
        raw = resp.json()
        payload = {"entries": raw, "last_updated_at": None} if isinstance(raw, list) else raw
        with _lb_lock:
            _lb_cache["ts"] = time.time()
            _lb_cache["data"] = payload
        return payload
    except Exception:
        with _lb_lock:
            return _lb_cache["data"]


def compute_leaderboard_status(
    lb_payload: Optional[dict],
    wallet: str,
    realtime_points: Optional[float] = None,
) -> Optional[dict]:
    """Compute official Trust leaderboard rank, estimated realtime rank, and points to next tier."""
    if not lb_payload or not isinstance(lb_payload.get("entries"), list):
        return None
    w_low = (wallet or "").strip().lower()
    entries = lb_payload["entries"]
    parsed: List[Dict[str, Any]] = []
    official: Optional[Dict[str, Any]] = None
    for idx, e in enumerate(entries, start=1):
        addr = str(e.get("address") or "").lower()
        rank = int(e.get("rank") or idx)
        pts = float(e.get("points") or 0.0)
        vol = float(e.get("volume") or 0.0)
        row = {"rank": rank, "address": addr, "points": pts, "volume": vol}
        parsed.append(row)
        if w_low and addr == w_low:
            official = row

    parsed.sort(key=lambda r: r["points"], reverse=True)
    others = [r for r in parsed if r["address"] != w_low]

    rt_pts = float(realtime_points) if realtime_points is not None else (official["points"] if official else None)
    est_rank: Optional[int] = None
    next_rank: Optional[int] = None
    gap_to_next: Optional[float] = None
    milestone_rank: Optional[int] = None
    gap_to_milestone: Optional[float] = None

    if rt_pts is not None:
        higher = [r for r in others if r["points"] > rt_pts]
        est_rank = len(higher) + 1
        if higher:
            above = higher[-1]
            next_rank = est_rank - 1
            gap_to_next = round(max(0.0, above["points"] - rt_pts), 1)
        for m in (100, 50, 20, 10, 5, 3, 1):
            if m < est_rank and len(others) >= m:
                milestone_rank = m
                target_row = others[m - 1]
                gap_to_milestone = round(max(0.0, target_row["points"] - rt_pts), 1)
                break

    return {
        "totalEntries": len(parsed),
        "lastUpdatedAt": lb_payload.get("last_updated_at"),
        "officialRank": official["rank"] if official else None,
        "officialPoints": official["points"] if official else None,
        "officialVolume": official["volume"] if official else None,
        "estimatedRank": est_rank,
        "nextRank": next_rank,
        "gapToNext": gap_to_next,
        "milestoneRank": milestone_rank,
        "gapToMilestone": gap_to_milestone,
    }
