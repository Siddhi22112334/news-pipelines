import os
import traceback
from typing import List, Dict, Any

import tech_brief
import finance_brief
from export_utils import (
    _date_key_ist,
    write_yearly_json,
    update_index,
    write_latest_snapshots,   # optional snapshots for fast viewer loads
)

def _safe_run(run_fn, **kwargs) -> List[Dict[str, Any]]:
    """Run one pipeline but never crash the whole job."""
    try:
        return run_fn(**kwargs) or []
    except Exception as e:
        print(f"[warn] {getattr(run_fn, '__module__', 'pipeline')}.run_brief failed: {e}")
        traceback.print_exc()
        return []

def _send_combined_telegram(tech_items, fin_items):
    """Send a single combined message (auto-chunking handled by tech_brief helper)."""
    parts = []
    if tech_items:
        parts.append("<b>— Tech —</b>\n" + "\n".join(
            [tech_brief.to_html_block(r, []) for r in tech_items]
        ))
    if fin_items:
        # finance_brief has its own to_html_block signature (no history arg)
        parts.append("<b>— Finance —</b>\n" + "\n".join(
            [finance_brief.to_html_block(r) for r in fin_items]
        ))
    if not parts:
        print("Nothing to send to Telegram.")
        return
    combined_html = "\n\n".join(parts)
    ok = tech_brief.send_telegram_html_long(combined_html)
    print("Telegram sent:", ok)

def run():
    window_min = int(os.getenv("WINDOW_MIN", "1440"))
    max_items = int(os.getenv("MAX_ITEMS", "8"))
    diversify_per_domain = int(os.getenv("DIVERSIFY_PER_DOMAIN", "2"))
    send = os.getenv("SEND", "true").lower() in ("1", "true", "yes")
    write_latest = os.getenv("WRITE_LATEST", "true").lower() in ("1", "true", "yes")

    # 1) Run both pipelines (no sending here; we’ll send once, combined, below)
    tech = _safe_run(
        tech_brief.run_brief,
        window_min=window_min,
        max_items=max_items,
        diversify_domains=diversify_per_domain,
        send=False,
    )
    fin = _safe_run(
        finance_brief.run_brief,
        window_min=window_min,
        max_items=max_items,
        diversify_domains=diversify_per_domain,
        send=False,
    )

    # 2) Persist to yearly files (per kind)
    date_key = _date_key_ist()
    tech_path = write_yearly_json(date_key, "tech", tech)
    fin_path = write_yearly_json(date_key, "finance", fin)

    # 3) Update index.json; pass None so counts are read from the year files (after de-dupe)
    update_index(date_key, tech_count=None, fin_count=None)

    # 4) Optional small “latest_*” snapshots for faster mobile viewer load
    if write_latest:
        try:
            write_latest_snapshots(date_key)
        except Exception as e:
            print("[warn] write_latest_snapshots failed:", e)

    # 5) Single combined Telegram message (if enabled)
    if send:
        _send_combined_telegram(tech, fin)

    print("Wrote:", tech_path, fin_path)
    print("Index updated for", date_key)

if __name__ == "__main__":
    run()
