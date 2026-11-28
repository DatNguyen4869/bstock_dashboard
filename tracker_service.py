"""Fast parallel BSC RPC + incremental disk cache service for bStocks trade history."""

from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(BASE_DIR, "data", "cache_bsc.json")

LOG_RPC_URLS = [
    u for u in [
        os.environ.get("BSC_LOG_RPC_URL", "").strip(),
        "https://bsc-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",
    ] if u
]
FALLBACK_SMALL_LOG_RPC = "https://bsc.drpc.org"

GENERAL_RPC_URLS = [
    "https://bsc-rpc.publicnode.com",
    "https://bsc-dataseed.binance.org/",
    "https://bsc-dataseed1.binance.org/",
    "https://bsc-dataseed2.binance.org/",
    "https://bsc-dataseed3.binance.org/",
    "https://bsc-dataseed4.binance.org/",
]

CAMPAIGN_START_UTC = datetime(2026, 11, 20, 2, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_END_UTC = datetime(2026, 12, 4, 2, 0, 0, tzinfo=timezone.utc)
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WBNB_ADDRESS = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
WBNB_DEPOSIT_TOPIC = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
WBNB_WITHDRAWAL_TOPIC = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"

BOOSTED_TOKENS = {"ANTB", "NVDAB"}
STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "FDUSD", "USR", "USDX"}

_thread_local = threading.local()
_fetch_lock = threading.Lock()
_bnb_price_cache = {"price": None, "ts": 0.0}
_wallet_result_cache: dict = {}
WALLET_CACHE_TTL_SEC = 4.0


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def load_cache() -> dict:
    default = {"tokens": {}, "blocks": {}, "txs": {}, "wallets": {}, "campaign_start_block": None}
    if not os.path.exists(CACHE_FILE):
        return default
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k, v in default.items():
            data.setdefault(k, v)
        return data
    except Exception:
        return default


def save_cache(cache: dict) -> None:
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        print(f"  [CACHE] Save error: {e}", flush=True)


CACHE = load_cache()


def rpc_call(method, params, use_log_rpc=False, retries=4, timeout=15):
    """Make a single RPC call with retry/backoff."""
    urls = LOG_RPC_URLS if use_log_rpc else GENERAL_RPC_URLS
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    sess = get_session()

    for r in range(retries):
        for attempt in range(len(urls)):
            url = urls[(r + attempt) % len(urls)]
            try:
                resp = sess.post(url, json=payload, timeout=timeout)
                data = resp.json()
                if "error" in data:
                    err = data["error"]
                    print(f"  [RPC] Error from {url}: {err}", flush=True)
                    if isinstance(err, dict) and err.get("code") == -32005:
                        time.sleep(1.2 * (r + 1))
                    continue
                return data.get("result")
            except Exception as e:
                print(f"  [RPC] Failed {url}: {e}", flush=True)
        time.sleep(0.5 * (r + 1))
    return None


def _exec_batch_chunk(chunk_calls, rpc_offset):
    payload = [
        {"jsonrpc": "2.0", "method": m, "params": p, "id": idx}
        for idx, (m, p) in enumerate(chunk_calls)
    ]
    sess = get_session()
    for attempt in range(len(GENERAL_RPC_URLS)):
        url = GENERAL_RPC_URLS[(rpc_offset + attempt) % len(GENERAL_RPC_URLS)]
        try:
            resp = sess.post(url, json=payload, timeout=10)
            data = resp.json()
            if isinstance(data, list):
                out = [None] * len(chunk_calls)
                for item in data:
                    req_id = item.get("id")
                    if req_id is not None and 0 <= req_id < len(chunk_calls) and "result" in item:
                        out[req_id] = item["result"]
                return out
        except Exception as e:
            print(f"  [RPC BATCH] Failed {url}: {e}", flush=True)

    return [rpc_call(m, p) for m, p in chunk_calls]


def rpc_batch_parallel(calls, batch_size=25, max_workers=6):
    if not calls:
        return []
    chunks = [calls[i:i + batch_size] for i in range(0, len(calls), batch_size)]
    results = [None] * len(calls)

    with ThreadPoolExecutor(max_workers=min(max_workers, len(chunks))) as pool:
        future_map = {
            pool.submit(_exec_batch_chunk, chunk, idx): idx
            for idx, chunk in enumerate(chunks)
        }
        for fut in as_completed(future_map):
            chunk_idx = future_map[fut]
            chunk_res = fut.result()
            base = chunk_idx * batch_size
            for j, val in enumerate(chunk_res):
                results[base + j] = val
    return results


def get_block_number():
    result = rpc_call("eth_blockNumber", [])
    return int(result, 16) if result else None


def get_block_by_timestamp(target_ts, latest_block):
    res = rpc_batch_parallel([
        ("eth_getBlockByNumber", [hex(latest_block), False]),
        ("eth_getBlockByNumber", [hex(max(0, latest_block - 10000)), False]),
    ], batch_size=2, max_workers=1)
    latest_info, sample_info = res[0], res[1]

    if latest_info and sample_info:
        latest_ts = int(latest_info["timestamp"], 16)
        sample_ts = int(sample_info["timestamp"], 16)
        block_time = max(0.2, (latest_ts - sample_ts) / 10000.0)
        if target_ts >= latest_ts:
            return latest_block
        est = max(0, int(latest_block - (latest_ts - target_ts) / block_time))
    else:
        est = max(0, latest_block - 500_000)
        block_time = 0.45

    for _ in range(5):
        blk = rpc_call("eth_getBlockByNumber", [hex(est), False])
        if not blk:
            break
        blk_ts = int(blk["timestamp"], 16)
        diff = target_ts - blk_ts
        if abs(diff) <= 30:
            break
        est = max(0, min(latest_block, est + int(diff / block_time)))
    return est


def pad_address(addr):
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def decode_symbol(symbol_data):
    try:
        if symbol_data and len(symbol_data) > 2:
            hex_str = symbol_data[2:]
            if len(hex_str) >= 128:
                length = int(hex_str[64:128], 16)
                return bytes.fromhex(hex_str[128:128 + length * 2]).decode("utf-8", errors="ignore").strip("\x00")
            if len(hex_str) == 64:
                return bytes.fromhex(hex_str).decode("utf-8", errors="ignore").strip("\x00")
    except Exception:
        pass
    return ""


def prefetch_tokens(contract_addrs):
    missing = [c for c in set(contract_addrs) if c not in CACHE["tokens"]]
    if not missing:
        return
    calls = []
    for c in missing:
        calls.append(("eth_call", [{"to": c, "data": "0x95d89b41"}, "latest"]))
        calls.append(("eth_call", [{"to": c, "data": "0x313ce567"}, "latest"]))
    res = rpc_batch_parallel(calls, batch_size=20, max_workers=4)
    for i, c in enumerate(missing):
        sym = decode_symbol(res[i * 2])
        dec = 18
        try:
            if res[i * 2 + 1] and len(res[i * 2 + 1]) > 2:
                dec = int(res[i * 2 + 1], 16)
        except Exception:
            pass
        CACHE["tokens"][c] = {"symbol": sym, "decimals": dec, "address": c}


def get_bnb_price():
    now = time.time()
    if _bnb_price_cache["price"] and (now - _bnb_price_cache["ts"] < 60):
        return _bnb_price_cache["price"]
    try:
        r = get_session().get("https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT", timeout=5)
        price = float(r.json()["price"])
        _bnb_price_cache["price"] = price
        _bnb_price_cache["ts"] = now
        return price
    except Exception:
        pass
    try:
        r = get_session().get("https://api.coingecko.com/api/v3/simple/price?ids=binancecoin&vs_currencies=usd", timeout=8)
        price = float(r.json()["binancecoin"]["usd"])
        _bnb_price_cache["price"] = price
        _bnb_price_cache["ts"] = now
        return price
    except Exception:
        return _bnb_price_cache["price"] or 550.0


def is_bstock(symbol):
    if not symbol:
        return False
    s = symbol.upper()
    known = [
        "ANTB", "NVDAB", "TSLB", "MSFTB", "AMZNB", "METAB", "GOOGLB",
        "APPLB", "AAPLB", "COINB", "TSLAB", "MSTRB", "STRATB", "PLTRB", "ABNBB",
    ]
    if s in known:
        return True
    not_bstock = ["WBNB", "BNB", "BTCB", "ETHB", "DOTB", "USDB", "USDT", "USDC", "BUSD", "DAI", "FDUSD"]
    if s in not_bstock:
        return False
    if len(s) >= 3 and s.endswith("B") and s not in STABLECOINS:
        return True
    return False


def scan_logs_range(wallet_padded, scan_start, scan_end):
    if scan_start > scan_end:
        return []
    CHUNK = 49000
    tasks = []
    for chunk_start in range(scan_start, scan_end + 1, CHUNK):
        chunk_end = min(chunk_start + CHUNK - 1, scan_end)
        tasks.append((chunk_start, chunk_end, [TRANSFER_TOPIC, None, wallet_padded]))
        tasks.append((chunk_start, chunk_end, [TRANSFER_TOPIC, wallet_padded, None]))

    def _fetch_one(args):
        c_start, c_end, topics = args
        res = rpc_call(
            "eth_getLogs",
            [{"fromBlock": hex(c_start), "toBlock": hex(c_end), "topics": topics}],
            use_log_rpc=True,
            retries=2,
        )
        if res is None and (c_end - c_start) <= 95:
            try:
                r = get_session().post(
                    FALLBACK_SMALL_LOG_RPC,
                    json={"jsonrpc": "2.0", "method": "eth_getLogs", "params": [{"fromBlock": hex(c_start), "toBlock": hex(c_end), "topics": topics}], "id": 1},
                    timeout=8,
                ).json()
                if "result" in r:
                    return r["result"] or []
            except Exception:
                pass
        return res or []

    all_logs = []
    for idx, t in enumerate(tasks):
        all_logs.extend(_fetch_one(t))
        if idx < len(tasks) - 1:
            time.sleep(0.2)
    return all_logs


def compact_log(log):
    return {
        "transactionHash": log["transactionHash"],
        "logIndex": log.get("logIndex"),
        "blockNumber": log.get("blockNumber"),
        "address": log["address"],
        "topics": log.get("topics", []),
        "data": log.get("data", "0x"),
    }


def match_orders_fifo(orders: list[dict]) -> tuple[list[dict], dict[str, dict], float, float, float]:
    """
    Match buy and sell orders using FIFO by exact token quantity (`bstockAmount`),
    splitting or merging mismatched order sizes cleanly and leaving unmatched sells un-paired.
    Returns ``(rounds, holding, unmatched_sell_volume, unmatched_sell_points, unmatched_sell_gas)``.
    """
    rounds = []
    pending = defaultdict(list)
    unmatched_sell_volume = 0.0
    unmatched_sell_points = 0.0
    unmatched_sell_gas = 0.0

    for order in orders:
        token = order["token"].upper()
        amt = float(order.get("bstockAmount") or 0.0)
        if amt <= 1e-7:
            continue
        if order["type"] == "buy":
            pending[token].append({
                "order": order,
                "remTok": amt,
                "remUsd": float(order["usdAmount"]),
                "remGas": float(order["gasFeeUsd"]),
            })
        elif order["type"] == "sell":
            rem_sell = amt
            sell_usd_total = float(order["usdAmount"])
            sell_gas_total = float(order["gasFeeUsd"])

            while rem_sell > 1e-5 and pending[token]:
                lot = pending[token][0]
                # Consume tiny <0.0001 token rounding dust on lot so 1:1 rounds close cleanly
                take = lot["remTok"] if rem_sell + 1e-4 >= lot["remTok"] else min(rem_sell, lot["remTok"])
                take_sell = min(rem_sell, take)
                f_buy = min(1.0, take / lot["remTok"]) if lot["remTok"] > 0 else 1.0
                f_sell = min(1.0, take_sell / amt) if amt > 0 else 0.0

                buy_orig = lot["order"]
                buy_usd = lot["remUsd"] * f_buy
                buy_gas = lot["remGas"] * f_buy
                sell_usd = sell_usd_total * f_sell
                sell_gas = sell_gas_total * f_sell

                spread = buy_usd - sell_usd
                total_gas = buy_gas + sell_gas
                volume = buy_usd + sell_usd
                points = volume * buy_orig["multiplier"]
                hold_time = order["timestamp"] - buy_orig["timestamp"]

                buy_slice = dict(buy_orig)
                buy_slice["usdAmount"] = buy_usd
                buy_slice["bstockAmount"] = take
                buy_slice["gasFeeUsd"] = buy_gas

                sell_slice = dict(order)
                sell_slice["usdAmount"] = sell_usd
                sell_slice["bstockAmount"] = take_sell
                sell_slice["gasFeeUsd"] = sell_gas

                rounds.append({
                    "buy": buy_slice,
                    "sell": sell_slice,
                    "spread": spread,
                    "totalGas": total_gas,
                    "totalFee": spread + total_gas,
                    "holdTime": hold_time,
                    "volume": volume,
                    "points": points,
                    "token": buy_orig["token"],
                    "multiplier": buy_orig["multiplier"],
                })

                lot["remTok"] -= take
                lot["remUsd"] -= buy_usd
                lot["remGas"] -= buy_gas
                rem_sell = max(0.0, rem_sell - take_sell)
                if lot["remTok"] <= 1e-4:
                    pending[token].pop(0)

            if rem_sell > 1e-4:
                f_un = rem_sell / amt
                u_usd = sell_usd_total * f_un
                unmatched_sell_volume += u_usd
                unmatched_sell_points += u_usd * order["multiplier"]
                unmatched_sell_gas += sell_gas_total * f_un

    holding = {}
    for token, lots in pending.items():
        for lot in lots:
            if lot["remTok"] > 1e-4:
                if token not in holding:
                    holding[token] = {"amount": 0, "cost": 0, "gas": 0}
                holding[token]["amount"] += lot["remTok"]
                holding[token]["cost"] += max(0.0, lot["remUsd"])
                holding[token]["gas"] += max(0.0, lot["remGas"])

    return rounds, holding, unmatched_sell_volume, unmatched_sell_points, unmatched_sell_gas


def fetch_wallet_data(wallet: str) -> dict:
    """Fetch and compute bStocks rounds/stats for a wallet using parallel RPC + incremental cache."""
    wallet_key = wallet.lower()
    cached_entry = _wallet_result_cache.get(wallet_key)
    if cached_entry and (time.time() - cached_entry["ts"]) < WALLET_CACHE_TTL_SEC:
        return cached_entry["data"]

    with _fetch_lock:
        cached_entry = _wallet_result_cache.get(wallet_key)
        if cached_entry and (time.time() - cached_entry["ts"]) < WALLET_CACHE_TTL_SEC:
            return cached_entry["data"]
        t_start = time.time()
        wallet = wallet_key
        padded = pad_address(wallet)

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_block = pool.submit(get_block_number)
            f_bnb = pool.submit(get_bnb_price)
            latest_block = f_block.result()
            bnb_price = f_bnb.result()

        if not latest_block:
            return {"error": "Cannot connect to BSC RPC"}

        start_ts = int(CAMPAIGN_START_UTC.timestamp())
        end_ts = int(CAMPAIGN_END_UTC.timestamp())
        now_ts = int(datetime.now(timezone.utc).timestamp())

        if CACHE.get("campaign_start_block"):
            start_block = CACHE["campaign_start_block"]
        else:
            start_block = max(0, get_block_by_timestamp(start_ts, latest_block) - 500)
            CACHE["campaign_start_block"] = start_block

        end_block = latest_block if now_ts < end_ts else min(latest_block, get_block_by_timestamp(end_ts, latest_block) + 500)

        w_cache = CACHE["wallets"].get(wallet)
        cached_logs = []
        scan_start = start_block
        if w_cache and w_cache.get("start_block", 10**18) <= start_block and w_cache.get("last_block", 0) > start_block:
            cached_logs = w_cache.get("logs", [])
            scan_start = max(start_block, w_cache["last_block"] - 10)

        new_logs = scan_logs_range(padded, scan_start, end_block)

        combined_logs = []
        seen_logs = set()
        for log in cached_logs + [compact_log(l) for l in new_logs]:
            key = (log["transactionHash"], log.get("logIndex"))
            if key in seen_logs:
                continue
            seen_logs.add(key)
            combined_logs.append(log)

        safe_block = max(start_block, end_block - 15)
        confirmed_logs = [
            l for l in combined_logs
            if l.get("blockNumber") and int(l["blockNumber"], 16) <= safe_block
        ]
        CACHE["wallets"][wallet] = {
            "start_block": start_block,
            "last_block": safe_block,
            "logs": confirmed_logs,
        }

        logs_by_tx = defaultdict(list)
        for log in combined_logs:
            logs_by_tx[log["transactionHash"]].append(log)

        all_contracts = [log["address"].lower() for log in combined_logs]
        prefetch_tokens(all_contracts)

        def _is_candidate_tx(ls):
            if len(ls) >= 2:
                return True
            for l in ls:
                sym = CACHE["tokens"].get(l["address"].lower(), {}).get("symbol", "")
                if is_bstock(sym):
                    return True
            return False

        swap_candidate_hashes = [h for h, ls in logs_by_tx.items() if _is_candidate_tx(ls)]

        # Also backfill receipts for cached txs that predate native BNB/WBNB tracking
        def _needs_bnb_backfill(h):
            td = CACHE["txs"].get(h)
            if not td:
                return True
            if "wbnbWithdrawalBnb" in td and "wbnbDepositBnb" in td:
                return False
            # Only backfill if this tx involves a bStock without a stablecoin transfer
            ls = logs_by_tx[h]
            has_bs = any(is_bstock(CACHE["tokens"].get(l["address"].lower(), {}).get("symbol", "")) for l in ls)
            has_st = any(CACHE["tokens"].get(l["address"].lower(), {}).get("symbol", "").upper() in STABLECOINS for l in ls)
            return has_bs and not has_st

        missing_txs = [h for h in swap_candidate_hashes if _needs_bnb_backfill(h)]
        if missing_txs:
            receipts = rpc_batch_parallel([("eth_getTransactionReceipt", [h]) for h in missing_txs], batch_size=25, max_workers=6)
            fallback_hashes = []
            valid_receipts = {}
            needed_blocks = set()
            for h, rc in zip(missing_txs, receipts):
                if not rc or not rc.get("blockNumber"):
                    continue
                valid_receipts[h] = rc
                if not rc.get("effectiveGasPrice"):
                    fallback_hashes.append(h)
                bn = rc["blockNumber"]
                if bn not in CACHE["blocks"]:
                    needed_blocks.add(bn)

            extra_calls = [("eth_getBlockByNumber", [bn, False]) for bn in needed_blocks]
            extra_calls.extend(("eth_getTransactionByHash", [h]) for h in fallback_hashes)
            fallback_tx_map = {}
            if extra_calls:
                extra_res = rpc_batch_parallel(extra_calls, batch_size=25, max_workers=6)
                nb_list = list(needed_blocks)
                for idx, bn in enumerate(nb_list):
                    blk = extra_res[idx]
                    if blk and "timestamp" in blk:
                        CACHE["blocks"][bn] = int(blk["timestamp"], 16)
                for idx, h in enumerate(fallback_hashes):
                    fallback_tx_map[h] = extra_res[len(nb_list) + idx]

            for h, rc in valid_receipts.items():
                gas_used = int(rc.get("gasUsed", "0x0"), 16)
                egp = rc.get("effectiveGasPrice")
                if not egp and h in fallback_tx_map and fallback_tx_map[h]:
                    egp = fallback_tx_map[h].get("gasPrice", "0x0")
                gas_price = int(egp or "0x0", 16)
                bn = rc["blockNumber"]
                wbnb_dep = 0.0
                wbnb_wd = 0.0
                for rl in rc.get("logs") or []:
                    if (rl.get("address") or "").lower() != WBNB_ADDRESS:
                        continue
                    rtopics = rl.get("topics") or []
                    if not rtopics:
                        continue
                    t0 = str(rtopics[0]).lower()
                    raw_d = rl.get("data") or "0x0"
                    val_bnb = (int(raw_d, 16) / 1e18) if raw_d != "0x" else 0.0
                    if t0 == WBNB_DEPOSIT_TOPIC:
                        wbnb_dep += val_bnb
                    elif t0 == WBNB_WITHDRAWAL_TOPIC:
                        wbnb_wd += val_bnb
                CACHE["txs"][h] = {
                    "gasFeeBnb": (gas_price * gas_used) / 1e18,
                    "timestamp": CACHE["blocks"].get(bn, 0),
                    "blockNumber": int(bn, 16),
                    "transactionIndex": int(rc.get("transactionIndex", "0x0"), 16),
                    "status": rc.get("status", "0x1"),
                    "from": (rc.get("from") or "").lower(),
                    "to": (rc.get("to") or "").lower(),
                    "wbnbDepositBnb": wbnb_dep,
                    "wbnbWithdrawalBnb": wbnb_wd,
                }

        save_cache(CACHE)

        orders = []
        for tx_hash in swap_candidate_hashes:
            td = CACHE["txs"].get(tx_hash)
            if not td or td["status"] == "0x0":
                continue
            if td["timestamp"] and (td["timestamp"] < start_ts or td["timestamp"] > end_ts):
                continue

            transfers = []
            for log in logs_by_tx[tx_hash]:
                contract = log["address"].lower()
                token_info = CACHE["tokens"].get(contract, {"symbol": "", "decimals": 18})
                topics = log.get("topics", [])
                from_addr = "0x" + topics[1][26:] if len(topics) > 1 else ""
                to_addr = "0x" + topics[2][26:] if len(topics) > 2 else ""
                raw_value = int(log["data"], 16) if log.get("data", "0x") != "0x" else 0
                amount = raw_value / (10 ** token_info["decimals"])
                transfers.append({
                    "contract": contract,
                    "symbol": token_info["symbol"],
                    "decimals": token_info["decimals"],
                    "from": from_addr.lower(),
                    "to": to_addr.lower(),
                    "amount": amount,
                })

            outgoing = [t for t in transfers if t["from"] == wallet]
            incoming = [t for t in transfers if t["to"] == wallet]

            out_bs_list = [i for i in outgoing if is_bstock(i["symbol"])]
            in_bs_list = [i for i in incoming if is_bstock(i["symbol"])]

            if not out_bs_list and not in_bs_list:
                continue
            if out_bs_list and in_bs_list:
                continue

            is_buy = len(in_bs_list) > 0
            bs_list = in_bs_list if is_buy else out_bs_list
            bstock_symbol = bs_list[0]["symbol"]
            bstock_amount = sum(i["amount"] for i in bs_list if i["symbol"].upper() == bstock_symbol.upper())

            side_items = outgoing if is_buy else incoming
            stable_amt = sum(i["amount"] for i in side_items if i["symbol"].upper() in STABLECOINS)
            wbnb_amt = sum(
                i["amount"] for i in side_items
                if i["symbol"].upper() in ("WBNB", "BNB") or i["contract"] == WBNB_ADDRESS
            )
            if stable_amt > 0:
                usd_amount = stable_amt
            elif wbnb_amt > 0:
                usd_amount = wbnb_amt * bnb_price
            elif is_buy and float(td.get("wbnbDepositBnb") or 0.0) > 0:
                usd_amount = float(td["wbnbDepositBnb"]) * bnb_price
            elif (not is_buy) and float(td.get("wbnbWithdrawalBnb") or 0.0) > 0:
                usd_amount = float(td["wbnbWithdrawalBnb"]) * bnb_price
            else:
                # Skip plain bStock wallet-to-wallet transfers with no payment leg
                continue

            multiplier = 1.5 if bstock_symbol.upper() in BOOSTED_TOKENS else 1.0
            gas_fee_bnb = td["gasFeeBnb"]

            orders.append({
                "type": "buy" if is_buy else "sell",
                "txHash": tx_hash,
                "timestamp": td["timestamp"],
                "blockNumber": td.get("blockNumber", 0),
                "transactionIndex": td.get("transactionIndex", 0),
                "token": bstock_symbol,
                "bstockAmount": bstock_amount,
                "usdAmount": usd_amount,
                "gasFeeUsd": gas_fee_bnb * bnb_price,
                "gasFeeBnb": gas_fee_bnb,
                "multiplier": multiplier,
            })

        orders.sort(key=lambda x: (x["timestamp"], x["blockNumber"], x["transactionIndex"]))

        rounds, holding, unmatched_sell_volume, unmatched_sell_points, unmatched_sell_gas = match_orders_fifo(orders)

        total_spread = sum(r["spread"] for r in rounds)
        total_gas = sum(r["totalGas"] for r in rounds) + unmatched_sell_gas
        total_volume = sum(r["volume"] for r in rounds) + unmatched_sell_volume
        total_points = sum(r["points"] for r in rounds) + unmatched_sell_points
        total_orders = len(orders)

        holding_value = sum(h["cost"] for h in holding.values())
        for token, h in holding.items():
            mult = 1.5 if token in BOOSTED_TOKENS else 1.0
            total_volume += h["cost"]
            total_points += h["cost"] * mult

        token_breakdown = {}
        for r in rounds:
            t = r["token"].upper()
            if t not in token_breakdown:
                token_breakdown[t] = {
                    "volume": 0, "points": 0, "rounds": 0, "orders": 0,
                    "gas": 0, "spread": 0, "multiplier": r["multiplier"],
                }
            token_breakdown[t]["volume"] += r["volume"]
            token_breakdown[t]["points"] += r["points"]
            token_breakdown[t]["rounds"] += 1
            token_breakdown[t]["orders"] += 2
            token_breakdown[t]["gas"] += r["totalGas"]
            token_breakdown[t]["spread"] += r["spread"]

        total_elapsed = time.time() - t_start
        result = {
            "wallet": wallet,
            "bnbPrice": bnb_price,
            "rounds": rounds,
            "orders": orders,
            "holding": holding,
            "holdingValue": holding_value,
            "totalSpread": total_spread,
            "totalGas": total_gas,
            "totalFee": total_spread + total_gas,
            "totalVolume": total_volume,
            "totalPoints": total_points,
            "totalOrders": total_orders,
            "totalRounds": len(rounds),
            "tokenBreakdown": token_breakdown,
            "costPer1000": ((total_spread + total_gas) / total_points * 1000) if total_points > 0 else 0,
            "elapsedSeconds": round(total_elapsed, 2),
        }
        _wallet_result_cache[wallet] = {"ts": time.time(), "data": result}
        return result
