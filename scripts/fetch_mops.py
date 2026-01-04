import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import requests


MOPS_BASE = "https://mops.twse.com.tw/mops/api"
LIST_API = f"{MOPS_BASE}/t05st02"
DETAIL_API = f"{MOPS_BASE}/t05st02_detail"

TAIPEI_TZ = ZoneInfo("Asia/Taipei")


def roc_year(dt: datetime) -> int:
    # 民國年 = 西元年 - 1911
    return dt.year - 1911


def normalize_text(s: str) -> str:
    # 把 \r\n 等換行、連續空白壓成單一空白
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def load_keywords(path: str) -> List[str]:
    kws = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            k = line.strip()
            if k and not k.startswith("#"):
                kws.append(k)
    # 長字優先（避免短字先匹配造成訊息太雜）
    kws.sort(key=len, reverse=True)
    return kws


def match_keywords(subject: str, keywords: List[str]) -> List[str]:
    hits = []
    for k in keywords:
        if k in subject:
            hits.append(k)
    return hits


def http_post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://mops.twse.com.tw",
        "Referer": "https://mops.twse.com.tw/mops/web/t05st02",
        "User-Agent": "Mozilla/5.0 (GitHubActions; +https://github.com)",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def load_state(state_path: str) -> Dict[str, Any]:
    if not os.path.exists(state_path):
        return {"seen_keys": []}
    with open(state_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def save_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def telegram_notify(token: str, chat_id: str, text: str) -> None:
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=20).raise_for_status()


def line_push(channel_access_token: str, to: str, text: str) -> None:
    # LINE Messaging API push
    if not channel_access_token or not to:
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {channel_access_token}", "Content-Type": "application/json"}
    body = {"to": to, "messages": [{"type": "text", "text": text}]}
    requests.post(url, headers=headers, json=body, timeout=20).raise_for_status()


def build_item_key(params: Dict[str, Any]) -> str:
    # 用 detail parameters 做唯一鍵，避免同一筆重複通知
    return f"{params.get('enterDate')}|{params.get('marketKind')}|{params.get('companyId')}|{params.get('serialNumber')}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keywords", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--state", default="public/state.json")
    args = ap.parse_args()

    keywords = load_keywords(args.keywords)

    now_tw = datetime.now(TAIPEI_TZ)
    y = str(roc_year(now_tw))              # e.g. 115
    m = str(now_tw.month)                  # e.g. "1" (不補零，符合你範例)
    d = f"{now_tw.day:02d}"                # e.g. "04"

    # 1) list
    list_payload = {"year": y, "month": m, "day": d}
    list_json = http_post_json(LIST_API, list_payload)

    data = list_json.get("result", {}).get("data", []) or []

    state = load_state(args.state)
    seen_keys = set(state.get("seen_keys", []))

    matched_items: List[Dict[str, Any]] = []
    new_matched_items: List[Dict[str, Any]] = []

    for row in data:
        # row: ["114/01/02","17:32:17","3004","豐達科","主旨",{ apiName, parameters }]
        if not isinstance(row, list) or len(row) < 6:
            continue

        speech_date = normalize_text(str(row[0]))
        speech_time = normalize_text(str(row[1]))
        company_id = normalize_text(str(row[2]))
        company_name = normalize_text(str(row[3]))
        subject_raw = str(row[4])
        subject = normalize_text(subject_raw)

        detail_meta = row[5] if isinstance(row[5], dict) else {}
        params = (detail_meta.get("parameters") or {}) if isinstance(detail_meta, dict) else {}
        if not isinstance(params, dict):
            continue

        hits = match_keywords(subject, keywords)
        if not hits:
            continue

        key = build_item_key(params)

        # 2) detail（只對命中者抓 detail）
        detail = http_post_json(DETAIL_API, params)

        item = {
            "key": key,
            "speech_date": speech_date,
            "speech_time": speech_time,
            "company_id": company_id,
            "company_name": company_name,
            "subject": subject,
            "matched_keywords": hits,
            "detail_params": params,
            "detail": detail,
            "fetched_at_tw": now_tw.strftime("%Y-%m-%d %H:%M:%S"),
        }
        matched_items.append(item)

        if key not in seen_keys:
            new_matched_items.append(item)

    # 更新 state：把本次命中也記住（避免下次重複通知）
    for it in matched_items:
        seen_keys.add(it["key"])

    # 輸出 JSON 給網頁用
    save_json(args.out_json, {
        "meta": {
            "run_at_tw": now_tw.strftime("%Y-%m-%d %H:%M:%S"),
            "query": {"year": y, "month": m, "day": d},
            "keywords": keywords,
            "matched_count": len(matched_items),
            "new_matched_count": len(new_matched_items),
        },
        "items": matched_items,
    })

    # 輸出 CSV（扁平化一些常用欄位；detail 整包塞 json 字串）
    csv_rows = []
    for it in matched_items:
        csv_rows.append({
            "key": it["key"],
            "speech_date": it["speech_date"],
            "speech_time": it["speech_time"],
            "company_id": it["company_id"],
            "company_name": it["company_name"],
            "subject": it["subject"],
            "matched_keywords": "|".join(it["matched_keywords"]),
            "enterDate": it["detail_params"].get("enterDate"),
            "marketKind": it["detail_params"].get("marketKind"),
            "serialNumber": it["detail_params"].get("serialNumber"),
            "detail_json": json.dumps(it["detail"], ensure_ascii=False),
            "fetched_at_tw": it["fetched_at_tw"],
        })

    save_csv(
        args.out_csv,
        csv_rows,
        fieldnames=[
            "key","speech_date","speech_time","company_id","company_name","subject",
            "matched_keywords","enterDate","marketKind","serialNumber","detail_json","fetched_at_tw"
        ],
    )

    # state 存檔
    save_json(args.state, {"seen_keys": sorted(seen_keys)})

    # 3) 通知（只通知「新命中」）
    if new_matched_items:
        lines = []
        lines.append(f"📣 MOPS 新公告命中 {len(new_matched_items)} 筆（{speech_date}）")
        for it in new_matched_items[:10]:  # 避免一次太長
            lines.append(f"- {it['company_id']} {it['company_name']} {it['speech_time']} {it['subject']}（{','.join(it['matched_keywords'])}）")
        if len(new_matched_items) > 10:
            lines.append(f"... 另有 {len(new_matched_items)-10} 筆")

        msg = "\n".join(lines)

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        line_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
        line_to = os.getenv("LINE_TO", "")

        # 兩個都試；其中一個沒設就跳過
        try:
            telegram_notify(telegram_token, telegram_chat_id, msg)
        except Exception as e:
            print(f"[warn] telegram notify failed: {e}")

        try:
            line_push(line_token, line_to, msg)
        except Exception as e:
            print(f"[warn] line push failed: {e}")

    print(f"matched={len(matched_items)} new_matched={len(new_matched_items)}")


if __name__ == "__main__":
    main()
