# export_utils.py
import os
import json
import datetime as dt
from typing import List, Dict, Any, Tuple

# ----------------- Time helpers -----------------
def _date_key_ist(now_utc: dt.datetime | None = None) -> str:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%Y-%m-%d")

def _time_key_ist(now_utc: dt.datetime | None = None) -> str:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%H:%M")

def _year_from_date_key(date_key: str) -> str:
    return date_key.split("-")[0]

# ----------------- Normalization -----------------
def _strip_html(s: str) -> str:
    import re as _re
    return _re.sub('<[^<]+?>', '', s or '')

def normalize_for_viewer(results: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """
    Shape collector results into a compact, HTML-free array for the viewer.
    Each element: { item:{title, link, site_name, novelty_hash}, review:{headline_rewrite, bullets[], impact} }
    """
    out = []
    for r in results or []:
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
                "bullets": [_strip_html(b) for b in (review.get("bullets") or [])],
                "impact": review.get("impact","Neutral")
            }
        })
    return out

# ----------------- IO helpers -----------------
def _load_json(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def _year_file_path(base_dir: str, kind: str, year: str) -> str:
    return os.path.join(base_dir, "data", f"{year}_{kind}.json")

def _ensure_runs_day(day_obj: Any) -> Dict[str, Any]:
    """
    Guarantee a day object of shape: {"runs": { "HH:MM": [ ... ] }}
    Migrate legacy shapes:
      - []  (list of items)  -> {"runs": {"00:00": [...]}}
      - {"runs": {...}}      -> unchanged
      - {} or anything else  -> {"runs": {}}
    """
    if isinstance(day_obj, dict):
        if "runs" in day_obj and isinstance(day_obj["runs"], dict):
            return {"runs": day_obj["runs"]}
        # Unknown dict shape -> wrap as empty runs
        return {"runs": {}}
    if isinstance(day_obj, list):
        return {"runs": {"00:00": day_obj}}
    return {"runs": {}}

def _sum_run_items(runs_dict: Dict[str, List[dict]]) -> int:
    total = 0
    for arr in (runs_dict or {}).values():
        try:
            total += len(arr or [])
        except Exception:
            pass
    return total

# ----------------- Public API -----------------
def write_yearly_json(
    date_key: str,
    kind: str,                     # "tech" or "finance"
    results: List[Dict[str,Any]],
    base_dir: str = "viewer",
    run_key: str | None = None     # "HH:MM" IST; if None, we generate
) -> str:
    """
    Append/replace the entries for a specific DATE + RUN into the YEAR file.
    File: viewer/data/{YYYY}_{kind}.json
    Shape (per date): {"runs": { "HH:MM": [ ... normalized items ... ], ... }}
    Back-compat: migrates legacy day shapes (list) into the new "runs" structure.
    """
    if run_key is None:
        run_key = _time_key_ist()

    os.makedirs(os.path.join(base_dir, "data"), exist_ok=True)
    year = _year_from_date_key(date_key)
    path = _year_file_path(base_dir, kind, year)

    data = _load_json(path)
    if not isinstance(data, dict):
        data = {}

    # Migrate / ensure runs structure for this date
    day_obj = _ensure_runs_day(data.get(date_key))
    runs = day_obj["runs"]
    runs[str(run_key)] = normalize_for_viewer(results)

    data[date_key] = {"runs": runs}
    _write_json(path, data)
    return path

def _compute_count_from_year_file(
    date_key: str, kind: str, base_dir: str = "viewer"
) -> int:
    """
    Read viewer/data/{YYYY}_{kind}.json and sum the number of items across all runs for date_key.
    If file/day missing or malformed, return 0.
    """
    year = _year_from_date_key(date_key)
    path = _year_file_path(base_dir, kind, year)
    js = _load_json(path)
    if not isinstance(js, dict):
        return 0
    day_obj = js.get(date_key)
    day_runs = _ensure_runs_day(day_obj)["runs"]
    return _sum_run_items(day_runs)

def _merge_runs(prev_value: Any, new_runs: List[str]) -> List[str]:
    """
    Merge a previous runs value (could be None, int (legacy), or list) with new run keys.
    Returns a sorted unique list of "HH:MM".
    """
    prev_list: List[str] = []
    if isinstance(prev_value, list):
        prev_list = [str(x) for x in prev_value]
    # if it's an int (legacy count), we ignore it for runs merging
    merged = sorted({*(prev_list or []), *(new_runs or [])})
    return merged

def update_index(
    date_key: str,
    tech_runs: List[str] | None = None,
    fin_runs: List[str] | None = None,
    base_dir: str = "viewer"
) -> str:
    """
    Keep index.json as a quick directory of available dates.
    New canonical shape per date:
      {
        "tech": <int total items that day>,
        "finance": <int total items that day>,
        "tech_runs": ["HH:MM", ...],
        "finance_runs": ["HH:MM", ...]
      }

    Back-compat: If older index existed with numbers only (or "tech" as list), we migrate.
    """
    idx_path = os.path.join(base_dir, "index.json")
    idx = _load_json(idx_path)
    if not isinstance(idx, dict):
        idx = {}

    entry = idx.get(date_key)
    if not isinstance(entry, dict):
        # Could be a bare int or list from some very old shape — replace with dict.
        entry = {}
    idx[date_key] = entry

    # Merge runs (handle legacy types)
    tech_runs = tech_runs or []
    fin_runs  = fin_runs  or []

    # Legacy handling: sometimes "tech" (or "finance") stored the runs list directly.
    prev_tech_runs = entry.get("tech_runs")
    if prev_tech_runs is None and isinstance(entry.get("tech"), list):
        prev_tech_runs = entry.get("tech")
    prev_fin_runs = entry.get("finance_runs")
    if prev_fin_runs is None and isinstance(entry.get("finance"), list):
        prev_fin_runs = entry.get("finance")

    t_full = _merge_runs(prev_tech_runs, tech_runs)
    f_full = _merge_runs(prev_fin_runs,  fin_runs)

    # Counts: prefer computing from year files; if 0, fallback to legacy int (if present)
    tech_count = _compute_count_from_year_file(date_key, "tech", base_dir=base_dir)
    fin_count  = _compute_count_from_year_file(date_key, "finance", base_dir=base_dir)

    if tech_count == 0:
        # if legacy number exists, keep it so the old viewer still shows a count
        legacy_t = entry.get("tech")
        if isinstance(legacy_t, int):
            tech_count = legacy_t
    if fin_count == 0:
        legacy_f = entry.get("finance")
        if isinstance(legacy_f, int):
            fin_count = legacy_f

    entry["tech"] = int(tech_count)
    entry["finance"] = int(fin_count)
    entry["tech_runs"] = t_full
    entry["finance_runs"] = f_full

    idx[date_key] = entry
    _write_json(idx_path, idx)
    return idx_path
