# run_combined.py
import os
import traceback
from typing import List, Dict, Any

import tech_brief
import finance_brief
from export_utils import (
    _date_key_ist,
    _time_key_ist,
    write_yearly_json,
    update_index,
)

def _safe_run(run_fn, **kwargs) -> List[Dict[str, Any]]:
    """Run a pipeline and never fail the whole job."""
    try:
        res = run_fn(**kwargs) or []
        print(f"[info] {run_fn.__module__}.run_brief returned {len(res)} items")
        return res
    except Exception as e:
        print(f"[warn] {getattr(run_fn, '__module__', 'pipeline')}.run_brief failed: {e}")
        traceback.print_exc()
        return []

def run():
    # windows/min/max from env; no telegram here
    window_min = int(os.getenv("WINDOW_MIN", "1440"))
    max_items  = int(os.getenv("MAX_ITEMS", "8"))
    diversify_per_domain = int(os.getenv("DIVERSIFY_PER_DOMAIN", "2"))
    _ = os.getenv("SEND", "false")  # ignored; keep for CLI compat

    # 1) collect (no sending)
    tech = _safe_run(
        tech_brief.run_brief,
        window_min=window_min,
        max_items=max_items,
        diversify_domains=diversify_per_domain,
        send=False
    )
    fin = _safe_run(
        finance_brief.run_brief,
        window_min=window_min,
        max_items=max_items,
        diversify_domains=diversify_per_domain,
        send=False
    )

    # 2) write year files under a specific date + run time
    date_key = _date_key_ist()   # "YYYY-MM-DD" (IST)
    run_key  = _time_key_ist()   # "HH:MM"     (IST)

    tech_path = write_yearly_json(date_key, "tech", tech, run_key=run_key)
    fin_path  = write_yearly_json(date_key, "finance", fin, run_key=run_key)

    # 3) update index with this exact run time (no remote lookups needed)
    tech_runs = [run_key] if tech else []
    fin_runs  = [run_key] if fin else []
    update_index(date_key, tech_runs=tech_runs, fin_runs=fin_runs)

    print("Wrote:", tech_path, fin_path)
    print("Index updated for", date_key, "run", run_key)

if __name__ == "__main__":
    run()
