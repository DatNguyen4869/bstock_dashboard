"""Trust Wallet bStocks leaderboard fetcher, snapshot history, and habit-based rank projector."""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from tracker_service import CAMPAIGN_START_UTC

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LB_HISTORY_FILE = DATA_DIR / "lb_history.json"

LEADERBOARD_URL = "https://api.trustwallet.com/v1/campaigns/bstocks/leaderboard"
LEADERBOARD_TTL_SEC = 30.0
NEIGHBOR_RADIUS = 5

_lb_lock = threading.Lock()
_lb_cache: Dict[str, Any] = {"ts": 0.0, "data": None}
_history_lock = threading.Lock()


def _parse_iso_ts(iso: Optional[str]) -> Optional[float]:
    if not iso:
        return None
    try:
        s = str(iso).replace("Z", "+00:00")
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: Path, data: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception:
        pass


def _record_lb_snapshot(payload: dict) -> None:
    """Persist Trust leaderboard snapshots whenever last_updated_at advances."""
    upd = payload.get("last_updated_at")
    entries = payload.get("entries")
    if not upd or not isinstance(entries, list):
        return
    ts = _parse_iso_ts(upd)
    if ts is None:
        return

    with _history_lock:
        hist = _load_json(LB_HISTORY_FILE, {"snapshots": []})
        snaps = hist.get("snapshots") if isinstance(hist, dict) else []
        if not isinstance(snaps, list):
            snaps = []
        if any(s.get("last_updated_at") == upd for s in snaps):
            return
        pts_map = {}
        vol_map = {}
        for e in entries:
            addr = str(e.get("address") or "").lower()
            if addr:
                pts_map[addr] = float(e.get("points") or 0.0)
                vol_map[addr] = float(e.get("volume") or 0.0)
        snaps.append({"last_updated_at": upd, "ts": ts, "points": pts_map, "volume": vol_map})
        snaps.sort(key=lambda s: float(s.get("ts") or 0.0))
        if len(snaps) > 48:
            snaps = snaps[-48:]
        _save_json(LB_HISTORY_FILE, {"snapshots": snaps})


def _compute_snapshot_velocities(
    entries: List[Dict[str, Any]],
    lb_ts: Optional[float],
) -> tuple[Dict[str, float], Dict[str, float], str]:
    """Compute points/hour and volume/hour for each wallet from Trust snapshots or campaign average (0 RPC calls)."""
    with _history_lock:
        hist = _load_json(LB_HISTORY_FILE, {"snapshots": []})
    snaps = hist.get("snapshots") or []

    if len(snaps) >= 2:
        prev, curr = snaps[-2], snaps[-1]
        dt_hours = (float(curr.get("ts") or 0.0) - float(prev.get("ts") or 0.0)) / 3600.0
        if dt_hours >= 0.05:
            prev_pts = prev.get("points") or {}
            curr_pts = curr.get("points") or {}
            prev_vol = prev.get("volume") or {}
            curr_vol = curr.get("volume") or {}
            pts_vel: Dict[str, float] = {}
            vol_vel: Dict[str, float] = {}
            for r in entries:
                addr = r["address"]
                if addr in prev_pts and addr in curr_pts:
                    pts_vel[addr] = max(0.0, (float(curr_pts[addr]) - float(prev_pts[addr])) / dt_hours)
                if addr in prev_vol and addr in curr_vol:
                    vol_vel[addr] = max(0.0, (float(curr_vol[addr]) - float(prev_vol[addr])) / dt_hours)
            if pts_vel:
                return pts_vel, vol_vel, "snapshot"

    ref_ts = lb_ts or time.time()
    campaign_hours = max(1.0, (ref_ts - CAMPAIGN_START_UTC.timestamp()) / 3600.0)
    pts_vel = {r["address"]: max(0.0, r["points"] / campaign_hours) for r in entries}
    vol_vel = {r["address"]: max(0.0, r["volume"] / campaign_hours) for r in entries}
    return pts_vel, vol_vel, "campaign"


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
        _record_lb_snapshot(payload)
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
    """Compute official rank, raw realtime rank, and habit-projected rank (±5 neighbors, zero extra RPC)."""
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

    projected_rank: Optional[int] = None
    projected_next_rank: Optional[int] = None
    projected_gap_to_next: Optional[float] = None
    neighbor_avg_pts_h: Optional[float] = None
    neighbor_avg_vol_h: Optional[float] = None
    neighbor_count = 0
    habit_source = "campaign"

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

        center_idx = len(higher)
        lo = max(0, center_idx - NEIGHBOR_RADIUS)
        hi = min(len(others), center_idx + NEIGHBOR_RADIUS)
        neighbor_addrs = {r["address"] for r in others[lo:hi]}
        neighbor_count = len(neighbor_addrs)

        lb_ts = _parse_iso_ts(lb_payload.get("last_updated_at"))
        hours_since_lb = max(0.0, (time.time() - lb_ts) / 3600.0) if lb_ts else 0.0

        pts_vel, vol_vel, habit_source = _compute_snapshot_velocities(others, lb_ts)

        neighbor_pts_rates: List[float] = []
        neighbor_vol_rates: List[float] = []
        projected_others: List[float] = []

        for r in others:
            addr = r["address"]
            rate = pts_vel.get(addr, 0.0)
            if addr in neighbor_addrs:
                neighbor_pts_rates.append(rate)
                if addr in vol_vel:
                    neighbor_vol_rates.append(vol_vel[addr])
            projected_others.append(r["points"] + rate * hours_since_lb)

        projected_others.sort(reverse=True)
        proj_higher = [p for p in projected_others if p > rt_pts]
        projected_rank = len(proj_higher) + 1
        if proj_higher:
            projected_next_rank = projected_rank - 1
            projected_gap_to_next = round(max(0.0, proj_higher[-1] - rt_pts), 1)

        if neighbor_pts_rates:
            neighbor_avg_pts_h = round(sum(neighbor_pts_rates) / len(neighbor_pts_rates), 0)
        if neighbor_vol_rates:
            neighbor_avg_vol_h = round(sum(neighbor_vol_rates) / len(neighbor_vol_rates), 0)

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
        "projectedRank": projected_rank,
        "projectedNextRank": projected_next_rank,
        "projectedGapToNext": projected_gap_to_next,
        "neighborAvgPtsPerHour": neighbor_avg_pts_h,
        "neighborAvgVolPerHour": neighbor_avg_vol_h,
        "neighborCount": neighbor_count,
        "habitSource": habit_source,
    }
