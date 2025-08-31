import os, json, datetime as dt, re
from typing import List, Dict, Any

def _date_key_ist(now_utc=None):
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%Y-%m-%d")

def _year_from_date_key(date_key: str) -> str:
    return date_key.split("-")[0]

def _strip_html(s: str) -> str:
    import re as _re
    return _re.sub('<[^<]+?>', '', s or '')

def normalize_for_viewer(results: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """Shape collector results into a compact, HTML-free array for the viewer."""
    out = []
    for r in results:
        item = r.get('item', {})
        review = r.get('review', {})
        out.append({
            "item": {
                "title": item.get("title",""),
                "link": item.get("canonical") or item.get("link",""),
                "site_name": item.get("site_name") or "",
                "novelty_hash": item.get("novelty_hash","")
            },
            "review": {
                "headline_rewrite": review.get("headline_rewrite",""),
                "bullets": [ _strip_html(b) for b in (review.get("bullets") or []) ],
                "impact": review.get("impact","Neutral")
            }
        })
    return out

def _load_json(path: str) -> dict | list:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def write_yearly_json(date_key: str, kind: str, results: List[Dict[str,Any]], base_dir="viewer"):
    """
    Append/replace the entries for a specific date inside the YEAR file.
    - kind: "tech" or "finance"
    - file: viewer/data/{YYYY}_{kind}.json
    - shape: { "YYYY-MM-DD": [ ... normalized items ... ], ... }
    """
    os.makedirs(os.path.join(base_dir, "data"), exist_ok=True)
    year = _year_from_date_key(date_key)
    fname = f"{year}_{kind}.json"
    path = os.path.join(base_dir, "data", fname)

    payload_day = normalize_for_viewer(results)
    data = _load_json(path)
    if not isinstance(data, dict):
        data = {}
    data[date_key] = payload_day

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path

def update_index(date_key: str, tech_count: int, fin_count: int, base_dir="viewer"):
    """
    Keep index.json as a quick “what dates exist and how many stories (per kind)”.
    Shape: { "YYYY-MM-DD": {"tech": N, "finance": M}, ... }
    """
    idx_path = os.path.join(base_dir, "index.json")
    try:
        idx = json.load(open(idx_path, "r", encoding="utf-8"))
    except Exception:
        idx = {}
    idx[date_key] = {"tech": tech_count, "finance": fin_count}
    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=2)
    return idx_path
