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
    try:
        return run_fn(**kwargs) or []
    except Exception as e:
        print(f"[warn] {getattr(run_fn, '__module__', 'pipeline')}.run_brief failed: {e}")
        traceback.print_exc()
        return []

def run():
    window_min = int(os.getenv('WINDOW_MIN','1440'))
    max_items  = int(os.getenv('MAX_ITEMS','8'))
    diversify_per_domain = int(os.getenv('DIVERSIFY_PER_DOMAIN','2'))

    # Collect (no Telegram send)
    tech = _safe_run(
        tech_brief.run_brief,
        window_min=window_min,
        max_items=max_items,
        diversify_domains=diversify_per_domain,
        send=False
    )
    fin  = _safe_run(
        finance_brief.run_brief,
        window_min=window_min,
        max_items=max_items,
        diversify_domains=diversify_per_domain,
        send=False
    )

    date_key = _date_key_ist()   # e.g., 2025-09-05
    run_key  = _time_key_ist()   # e.g., 19:05

    # Append into yearly files (date + run)
    tech_path = write_yearly_json(date_key, "tech", tech, run_key=run_key)
    fin_path  = write_yearly_json(date_key, "finance", fin, run_key=run_key)

    # Update index with this run so the viewer lists it
    # We add the run regardless of whether tech/fin arrays are empty.
    update_index(date_key, tech_runs=[run_key], fin_runs=[run_key])

    print("Wrote:", tech_path, fin_path)
    print("Index updated for", date_key, "run", run_key)

if __name__ == "__main__":
    run()
