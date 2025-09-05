import os, json, datetime as dt, re
from typing import List, Dict, Any, Optional

try:
    import requests
except Exception:
    requests = None  # graceful if requests isn't available locally

# ---------- time helpers ----------
def _date_key_ist(now_utc: Optional[dt.datetime] = None) -> str:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%Y-%m-%d")

def _time_key_ist(now_utc: Optional[dt.datetime] = None) -> str:
    """e.g., '09:30' in IST"""
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%H:%M")

def _year_from_date_key(date_key: str) -> str:
    return date_key.split("-")[0]

def _strip_html(s: str) -> str:
    import re as _re
    return _re.sub('<[^<]+?>', '', s or '')

# ---------- viewer normalization ----------
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

# ---------- gh-pages raw fallback ----------
def _guess_raw_url(rel_path: str) -> Optional[str]:
    """
    Build a raw URL to the gh-pages branch for reading previously deployed data.
    rel_path must be relative to the gh-pages root, e.g. 'data/2025_tech.json' or 'index.json'.
    """
    repo = os.getenv("GITHUB_REPOSITORY", "")  # 'owner/repo'
    if not repo or not requests:
        return None
    owner, name = repo.split("/", 1)
    return f"https://raw.githubusercontent.com/{owner}/{name}/gh-pages/{rel_path}"

def _load_json_local(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _load_json_remote(rel_path: str):
    url = _guess_raw_url(rel_path)
    if not url or not requests:
        return None
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def _load_json(path: str, rel_for_remote: Optional[str] = None):
    """Try local first; if missing, try reading from gh-pages (raw)."""
    data = _load_json_local(path)
    if data is not None:
        return data
    if rel_for_remote:
        data = _load_json_remote(rel_for_remote)
        if data is not None:
            return data
    return {}

# ---------- helpers for legacy migration ----------
def _normalize_day_obj(day_obj: Any) -> Dict[str, Any]:
    """
    Accept legacy shapes and convert to:
    { "runs": { "HH:MM": [...] }, "latest": "HH:MM" }
    """
    if isinstance(day_obj, dict):
        # If it already has runs, leave it; otherwise wrap the dict as a single run list if it looks like a list
        if "runs" in day_obj and isinstance(day_obj["runs"], dict):
            return day_obj
        # Some very old shapes could be {"items":[...]} – normalize
        if "items" in day_obj and isinstance(day_obj["items"], list):
            return {"runs": {"00:00": day_obj["items"]}, "latest": "00:00"}
        # Unknown dict shape -> create empty day and let caller add the new run
        return {"runs": {}, "latest": "00:00"}
    elif isinstance(day_obj, list):
        # Pure legacy: date_key -> [ ...list of items... ]
        return {"runs": {"00:00": day_obj}, "latest": "00:00"}
    else:
        # Nothing yet
        return {"runs": {}, "latest": "00:00"}

def _bump_hhmm(hhmm: str) -> str:
    """Return hh:mm + 1 minute, capped at 23:59."""
    try:
        h, m = hhmm.split(":")
        h, m = int(h), int(m)
        m += 1
        if m >= 60:
            h += 1
            m = 0
        if h >= 24:
            return "23:59"
        return f"{h:02d}:{m:02d}"
    except Exception:
        return "23:59"

def _ensure_unique_time_key(runs: Dict[str, Any], want: str) -> str:
    """If a run with 'want' exists, bump by a minute until free."""
    key = want
    guard = 0
    while key in runs and guard < 180:  # avoid infinite loops
        key = _bump_hhmm(key)
        guard += 1
    return key

# ---------- writers (append by run) ----------
def write_yearly_json(date_key: str, kind: str, results: List[Dict[str,Any]], run_key: Optional[str] = None, base_dir="viewer"):
    """
    Append/replace the entries for a specific date+run inside the YEAR file.

    Shape (per kind file): viewer/data/{YYYY}_{kind}.json
    {
      "YYYY-MM-DD": {
        "runs": {
          "HH:MM": [ ... normalized items ... ],
          ...
        },
        "latest": "HH:MM"
      },
      ...
    }
    """
    os.makedirs(os.path.join(base_dir, "data"), exist_ok=True)
    year = _year_from_date_key(date_key)
    fname = f"{year}_{kind}.json"
    rel = f"data/{fname}"
    path = os.path.join(base_dir, "data", fname)

    payload_run = normalize_for_viewer(results)
    data = _load_json(path, rel_for_remote=rel)
    if not isinstance(data, dict):
        data = {}

    # Load (and migrate if legacy) this day's object
    day_obj_raw = data.get(date_key, {})
    day_obj = _normalize_day_obj(day_obj_raw)

    if run_key is None:
        run_key = _time_key_ist()

    # Ensure we don't collide with a legacy default like "00:00"
    runs = day_obj.get("runs", {})
    run_key = _ensure_unique_time_key(runs, run_key)

    runs[run_key] = payload_run
    day_obj["runs"] = runs
    day_obj["latest"] = run_key
    data[date_key] = day_obj

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path

def update_index(date_key: str, tech_runs: Optional[list] = None, fin_runs: Optional[list] = None, base_dir="viewer"):
    """
    Keep index.json as: { "YYYY-MM-DD": {"tech": ["HH:MM", ...], "finance": ["HH:MM", ...]}, ... }
    Handles both legacy (list) and new (runs) shapes when deriving runs.
    """
    idx_rel = "index.json"
    idx_path = os.path.join(base_dir, "index.json")
    idx = _load_json(idx_path, rel_for_remote=idx_rel)
    if not isinstance(idx, dict):
        idx = {}

    year = _year_from_date_key(date_key)

    def _derive_runs(kind: str, provided: Optional[list]) -> list:
        if provided is not None:
            return sorted(list(set(provided)))
        rel = f"data/{year}_{kind}.json"
        path = os.path.join(base_dir, "data", f"{year}_{kind}.json")
        data = _load_json(path, rel_for_remote=rel)
        if not isinstance(data, dict):
            return []
        day = data.get(date_key)
        if day is None:
            return []
        # Legacy: day is a list -> pretend it was "00:00"
        if isinstance(day, list):
            return ["00:00"]
        # New: day is an object; prefer runs keys
        if isinstance(day, dict):
            runs = day.get("runs")
            if isinstance(runs, dict):
                return sorted(list(runs.keys()))
            # Some intermediate forms may have no runs – treat as no entries
        return []

    t_runs = _derive_runs("tech", tech_runs)
    f_runs = _derive_runs("finance", fin_runs)

    prev = idx.get(date_key, {})
    # merge with anything that might already be in index
    t_full = sorted(list(set((prev.get("tech") or []) + t_runs)))
    f_full = sorted(list(set((prev.get("finance") or []) + f_runs)))

    idx[date_key] = {"tech": t_full, "finance": f_full}

    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=2)
    return idx_path
