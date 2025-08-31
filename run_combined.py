import os
import tech_brief
import finance_brief
from export_utils import _date_key_ist, write_yearly_json, update_index

def run():
    window_min = int(os.getenv('WINDOW_MIN','1440'))
    max_items  = int(os.getenv('MAX_ITEMS','8'))
    diversify_per_domain = int(os.getenv('DIVERSIFY_PER_DOMAIN','2'))
    send = os.getenv('SEND','true').lower() in ('1','true','yes')

    tech = tech_brief.run_brief(window_min=window_min, max_items=max_items,
                                diversify_domains=diversify_per_domain, send=False) or []
    fin  = finance_brief.run_brief(window_min=window_min, max_items=max_items,
                                   diversify_domains=diversify_per_domain, send=False) or []

    date_key = _date_key_ist()
    tech_path = write_yearly_json(date_key, "tech", tech)
    fin_path  = write_yearly_json(date_key, "finance", fin)
    update_index(date_key, len(tech), len(fin))

    if send and (tech or fin):
        parts = []
        def render_blocks(arr, tag):
            blks = [tech_brief.to_html_block(r, []) for r in arr]
            return f"<b>— {tag} —</b>\n" + "\n".join(blks) if blks else ""
        out = [render_blocks(tech, "Tech"), render_blocks(fin, "Finance")]
        combined_html = "\n\n".join([p for p in out if p])
        ok = tech_brief.send_telegram_html_long(combined_html)
        print("Telegram sent:", ok)

    print("Wrote:", tech_path, fin_path)
    print("Index updated for", date_key)

if __name__ == "__main__":
    run()
