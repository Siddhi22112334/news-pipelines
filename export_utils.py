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
    """
    Shape collector results into a compact, HTML-free array for the viewer.
    """
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

# ---------- loader with gh-pages fallback ----------
def _guess_raw_url(rel_path: str) -> Optional[str]:
    """
    Build a raw URL to the gh-pages branch for reading previously deployed data.
    rel_path must be relative to the gh-pages root, e.g. 'data/2025_tech.json' or 'index.json'.
    """
    repo = os.getenv("GITHUB_REPOSITORY", "")  # 'owner/repo'
    if not repo or not requests:
        return None
    owner, name = repo.split("/", 1)
    # We publish viewer/* to gh-pages root; JSON live under /data
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
    """
    Try local first; if missing, try reading from gh-pages (raw) so we can append instead of overwrite.
    """
    data = _load_json_local(path)
    if data is not None:
        return data
    if rel_for_remote:
        data = _load_json_remote(rel_for_remote)
        if data is not None:
            return data
    return {}

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

    if run_key is None:
        run_key = _time_key_ist()

    day_obj = data.get(date_key) or {}
    runs = day_obj.get("runs") or {}
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
    We read existing index from gh-pages if local missing, and update only today's entry.
    """
    idx_rel = "index.json"
    idx_path = os.path.join(base_dir, "index.json")
    idx = _load_json(idx_path, rel_for_remote=idx_rel)
    if not isinstance(idx, dict):
        idx = {}

    # If runs not provided, derive from the per-year files for the date
    year = _year_from_date_key(date_key)

    def _get_runs(kind: str, provided: Optional[list]) -> list:
        if provided is not None:
            return provided
        rel = f"data/{year}_{kind}.json"
        path = os.path.join(base_dir, "data", f"{year}_{kind}.json")
        data = _load_json(path, rel_for_remote=rel)
        if isinstance(data, dict) and isinstance(data.get(date_key, {}).get("runs"), dict):
            return sorted(list(data[date_key]["runs"].keys()))
        return []

    t_runs = _get_runs("tech", tech_runs)
    f_runs = _get_runs("finance", fin_runs)

    # merge (in case index had older runs)
    prev = idx.get(date_key, {})
    t_full = sorted(list(set((prev.get("tech") or []) + t_runs)))
    f_full = sorted(list(set((prev.get("finance") or []) + f_runs)))

    idx[date_key] = {"tech": t_full, "finance": f_full}

    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=2)
    return idx_path
