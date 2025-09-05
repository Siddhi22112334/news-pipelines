import os, json, datetime as dt
from typing import List, Dict, Any, Optional

# ----------------------- Helpers: dates & hygiene -----------------------

def _date_key_ist(now_utc: Optional[dt.datetime] = None) -> str:
    """Return IST date key like 'YYYY-MM-DD' for grouping runs."""
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%Y-%m-%d")

def _year_from_date_key(date_key: str) -> str:
    return (date_key or "").split("-")[0]

def _strip_html(s: str) -> str:
    import re as _re
    return _re.sub(r"<[^<]+?>", "", s or "")

def _ensure_data_dir(base_dir: str) -> str:
    path = os.path.join(base_dir, "data")
    os.makedirs(path, exist_ok=True)
    return path

def _year_path(kind: str, date_key: str, base_dir: str = "viewer") -> str:
    year = _year_from_date_key(date_key)
    return os.path.join(_ensure_data_dir(base_dir), f"{year}_{kind}.json")

def _load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

# ----------------------- Viewer payload shaping -----------------------

def normalize_for_viewer(results: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """
    Shape collector results into a compact, HTML-free array for the viewer.
    Expected downstream shape (compatible with your viewer.js):
      [
        {
          "item": {"title","link","site_name","novelty_hash"},
          "review": {"headline_rewrite","bullets","impact"}
        },
        ...
      ]
    """
    out = []
    for r in results or []:
        item = r.get("item", {}) or {}
        review = r.get("review", {}) or {}
        out.append({
            "item": {
                "title": item.get("title",""),
                "link": item.get("canonical") or item.get("link",""),
                "site_name": item.get("site_name") or "",
                "novelty_hash": item.get("novelty_hash","")
            },
            "review": {
                "headline_rewrite": _strip_html(review.get("headline_rewrite","")),
                "bullets": [ _strip_html(b) for b in (review.get("bullets") or []) ],
                "impact": review.get("impact","Neutral")
            }
        })
    return out

def _dedupe_by_novelty(items: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """Drop duplicates (same novelty_hash) within a single day payload."""
    seen = set()
    out = []
    for r in items:
        nh = (r.get("item") or {}).get("novelty_hash") or ""
        if nh and nh in seen:
            continue
        seen.add(nh)
        out.append(r)
    return out

# ----------------------- Persist: yearly files -----------------------

def write_yearly_json(date_key: str, kind: str, results: List[Dict[str,Any]], base_dir: str = "viewer") -> str:
    """
    Append/replace the entries for a specific date inside the YEAR file (per kind).
      - kind: "tech" or "finance"
      - file: viewer/data/{YYYY}_{kind}.json
      - shape: { "YYYY-MM-DD": [ ... normalized items ... ], ... }
    """
    path = _year_path(kind, date_key, base_dir)
    payload_day = _dedupe_by_novelty(normalize_for_viewer(results))

    data = _load_json(path)
    if not isinstance(data, dict):
        data = {}
    data[date_key] = payload_day

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    return path

# ----------------------- Index (dates → counts per kind) -----------------------

def _read_count_for(date_key: str, kind: str, base_dir: str = "viewer") -> int:
    """Read the year file and return how many items exist for date+kind."""
    path = _year_path(kind, date_key, base_dir)
    data = _load_json(path)
    if isinstance(data, dict):
        arr = data.get(date_key) or []
        if isinstance(arr, list):
            return len(arr)
    return 0

def update_index(
    date_key: str,
    tech_count: Optional[int],
    fin_count: Optional[int],
    base_dir: str = "viewer"
) -> str:
    """
    Keep index.json as a quick “what dates exist and how many stories (per kind)”.
    Shape:
      {
        "YYYY-MM-DD": { "tech": N, "finance": M },
        ...
      }

    If tech_count or fin_count is None, it will be computed from the corresponding year file.
    """
    idx_path = os.path.join(base_dir, "index.json")
    try:
        idx = json.load(open(idx_path, "r", encoding="utf-8"))
    except Exception:
        idx = {}

    if tech_count is None:
        tech_count = _read_count_for(date_key, "tech", base_dir)
    if fin_count is None:
        fin_count = _read_count_for(date_key, "finance", base_dir)

    prev = idx.get(date_key, {})
    prev["tech"] = int(tech_count or 0)
    prev["finance"] = int(fin_count or 0)
    idx[date_key] = prev

    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=2, sort_keys=True)
    return idx_path

# ----------------------- Optional: latest snapshots -----------------------

def write_latest_snapshots(
    date_key: str,
    base_dir: str = "viewer",
    include_tech: bool = True,
    include_finance: bool = True
) -> Dict[str, str]:
    """
    (Optional) Write small 'latest' files so the viewer can load them without scanning the year.
      - viewer/data/latest_tech.json
      - viewer/data/latest_finance.json
    Shape matches normalize_for_viewer output (list).
    """
    out = {}
    data_dir = _ensure_data_dir(base_dir)

    if include_tech:
        tech_year = _load_json(_year_path("tech", date_key, base_dir))
        tech = tech_year.get(date_key, []) if isinstance(tech_year, dict) else []
        p = os.path.join(data_dir, "latest_tech.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump(tech, f, ensure_ascii=False, indent=2, sort_keys=True)
        out["tech"] = p

    if include_finance:
        fin_year = _load_json(_year_path("finance", date_key, base_dir))
        fin = fin_year.get(date_key, []) if isinstance(fin_year, dict) else []
        p = os.path.join(data_dir, "latest_finance.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump(fin, f, ensure_ascii=False, indent=2, sort_keys=True)
        out["finance"] = p

    return out
