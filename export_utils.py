import os, json, datetime as dt, re
from typing import List, Dict, Any

# ---------- time helpers ----------
def _date_key_ist(now_utc=None) -> str:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%Y-%m-%d")

def _time_key_ist(now_utc=None) -> str:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    if not now_utc:
        now_utc = dt.datetime.now(dt.timezone.utc)
    return now_utc.astimezone(IST).strftime("%H:%M")

def _year_from_date_key(date_key: str) -> str:
    return date_key.split("-")[0]

# ---------- normalization ----------
def _strip_html(s: str) -> str:
    return re.sub('<[^<]+?>', '', s or '')

def normalize_for_viewer(results: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """Shape collector results into a compact, HTML-free array for the viewer."""
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

# ---------- io ----------
def _load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _dump_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# ---------- yearly files ----------
def _ensure_day_runs_container(year_map: dict, date_key: str) -> dict:
    """
    Ensure year_map[date_key] has the shape:
      { "runs": { "HH:MM": [ ... ] } }
    If legacy list is present, convert it to runs {"legacy": list}.
    """
    day = year_map.get(date_key)
    if day is None:
        day = {"runs": {}}
        year_map[date_key] = day
        return day

    # legacy: array
    if isinstance(day, list):
        day = {"runs": {"legacy": day}}
        year_map[date_key] = day
        return day

    # dict but without 'runs'
    if isinstance(day, dict) and "runs" not in day:
        day["runs"] = {}
        return day

    return day

def write_yearly_json(date_key: str, kind: str, results: List[Dict[str,Any]], run_key: str, base_dir="viewer"):
    """
    Append/replace the entries for a specific date+run inside the YEAR file.

    File path: viewer/data/{YYYY}_{kind}.json
    File shape: { "YYYY-MM-DD": { "runs": { "HH:MM": [ normalized items ] } }, ... }
    """
    assert kind in ("tech", "finance"), "kind must be 'tech' or 'finance'"
    os.makedirs(os.path.join(base_dir, "data"), exist_ok=True)

    year = _year_from_date_key(date_key)
    path = os.path.join(base_dir, "data", f"{year}_{kind}.json")

    data = _load_json(path)
    if not isinstance(data, dict):
        data = {}

    day = _ensure_day_runs_container(data, date_key)
    runs = day.get("runs") or {}

    # normalize and write/replace this run
    runs[run_key] = normalize_for_viewer(results)
    day["runs"] = runs
    data[date_key] = day

    _dump_json(path, data)
    return path

def _collect_runs_from_year_file(date_key: str, kind: str, base_dir="viewer") -> list:
    year = _year_from_date_key(date_key)
    path = os.path.join(base_dir, "data", f"{year}_{kind}.json")
    data = _load_json(path)
    if not isinstance(data, dict):
        return []
    day = data.get(date_key)
    if not day:
        return []
    if isinstance(day, list):  # legacy
        return ["legacy"]
    runs = day.get("runs") or {}
    return sorted(list(runs.keys()))

# ---------- index ----------
def update_index(date_key: str, tech_runs=None, fin_runs=None, base_dir="viewer"):
    """
    Keep index.json as quick navigation.
    New shape: { "YYYY-MM-DD": { "tech_runs":[...], "fin_runs":[...] }, ... }
    Backward compatible (if old counts existed, we keep them untouched).
    """
    path = os.path.join(base_dir, "index.json")
    idx = _load_json(path)
    if not isinstance(idx, dict):
        idx = {}

    rec = idx.get(date_key) or {}
    # If caller didn't provide run lists, infer from freshly written year files
    if tech_runs is None:
        tech_runs = _collect_runs_from_year_file(date_key, "tech", base_dir)
    if fin_runs is None:
        fin_runs = _collect_runs_from_year_file(date_key, "finance", base_dir)

    # Merge with any existing runs (avoid int+list mistakes)
    prev_t = rec.get("tech_runs")
    prev_f = rec.get("fin_runs")
    t_merged = sorted(list(set([*(prev_t if isinstance(prev_t, list) else []), *(tech_runs or [])])))
    f_merged = sorted(list(set([*(prev_f if isinstance(prev_f, list) else []), *(fin_runs or [])])))

    # Preserve old count fields if they exist (legacy viewer support)
    if "tech" in rec and isinstance(rec["tech"], int):
        pass
    if "finance" in rec and isinstance(rec["finance"], int):
        pass

    rec.update({"tech_runs": t_merged, "fin_runs": f_merged})
    idx[date_key] = rec

    _dump_json(path, idx)
    return path
