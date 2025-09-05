# ================== India Market News — grounded, full-text, richer snapshots (no Telegram) ==================
import os, re, json, html, textwrap, time, math
import datetime as dt
from typing import List, Dict, Any, Tuple
from urllib.parse import urlparse, quote_plus
import requests, feedparser

try:
    import trafilatura
except Exception:
    trafilatura = None

from bs4 import BeautifulSoup

# ------------------- Config -------------------
DEFAULT_WINDOW_MIN = int(os.getenv('DEFAULT_WINDOW_MIN', '15'))   # lookback minutes
DEFAULT_MAX_ITEMS  = int(os.getenv('DEFAULT_MAX_ITEMS',  '10'))
STATE_FILE   = os.getenv('STATE_FILE', os.getenv('FIN_STATE_FILE','seen_finnews.json'))

# OpenAI
LLM_PROVIDER   = os.getenv('LLM_PROVIDER','openai').lower()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY','')
OPENAI_MODEL   = os.getenv('OPENAI_MODEL','gpt-4o-mini')

UTC = dt.timezone.utc
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# Sources
FEEDS = [
    # 'https://www.sebi.gov.in/sebiweb/rss/sebi_rss.xml',
    # 'https://www.rbi.org.in/pressreleases_rss.xml',
    # 'https://www.rbi.org.in/notifications_rss.xml',
]
MEDIA_RSS = [
    'https://feeds.reuters.com/reuters/INtopNews',
    'https://www.livemint.com/rss/markets',
    'https://www.business-standard.com/rss/markets-106.rss',
    'https://www.thehindubusinessline.com/feeder/default.rss',
    'https://www.cnbctv18.com/rss/market.xml',
    # 'https://www.moneycontrol.com/rss/latestnews.xml',
    # 'https://www.moneycontrol.com/rss/marketreports.xml',
]
SITEMAPS = [
    'https://www.financialexpress.com/news-sitemap.xml',
    'https://www.financialexpress.com/stock-market-indian-indices.xml',
]

WATCHLIST = [w.strip() for w in os.getenv('WATCHLIST','').split(',') if w.strip()]
WATCHLIST_ONLY = os.getenv('WATCHLIST_ONLY','false').lower() in ('1','true','yes')

MATERIAL = re.compile(
    r"(result|earnings|revenue|profit|loss|ebitda|guidance|merger|acquisit|scheme|stake|buyback|"
    r"block deal|pledge|debt|default|downgrade|upgrade|rating|capex|order win|contract|tender|"
    r"tariff|export|import|sanction|policy|circular|regulation|ipo|fpo|ofs|dividend|split|bonus|"
    r"resignation|appointment|ceo|md|chairman|promoter|open offer|"
    r"cpi|wpi|iip|gdp|pmi|inflation|fx|rupee|bond|yield|crude|oil|opec|fed|ecb|rbi|sebi|nse|bse)",
    re.I
)

# ------------------- Utilities -------------------
def now_utc():
    return dt.datetime.now(tz=UTC)

def is_fresh(ts: dt.datetime, window_min: int) -> bool:
    return (now_utc() - ts).total_seconds() <= window_min * 60

def to_ist(ts: dt.datetime) -> str:
    return ts.astimezone(IST).strftime('%Y-%m-%d %H:%M IST')

def parse_time(entry) -> dt.datetime:
    for k in ("published_parsed", "updated_parsed"):
        t = getattr(entry, k, None)
        if t:
            return dt.datetime(*t[:6], tzinfo=UTC)
    return now_utc()

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

# ------------------- Fetchers -------------------
def fetch_rss(url: str) -> List[Dict[str, Any]]:
    out = []
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:120]:
            out.append({
                'title': (getattr(e, 'title', '') or '').strip(),
                'summary': (getattr(e, 'summary', '') or getattr(e, 'description', '') or '').strip(),
                'link': getattr(e, 'link', '') or '',
                'time': parse_time(e),
                'feed': url,
                'source': domain_of(getattr(e, 'link',''))
            })
    except Exception as ex:
        print('[warn] RSS error:', url, ex)
    return out

def fetch_sitemap(url: str, limit: int = 60) -> List[Dict[str, Any]]:
    out = []
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'xml')
        items = soup.find_all('url')[:limit]
        for it in items:
            loc = it.find('loc').text if it.find('loc') else ''
            lastmod = it.find('lastmod').text if it.find('lastmod') else None
            t = now_utc()
            if lastmod:
                try:
                    t = dt.datetime.fromisoformat(lastmod.replace('Z','+00:00')).astimezone(UTC)
                except Exception:
                    t = now_utc()
            out.append({
                'title': '',
                'summary': '',
                'link': loc,
                'time': t,
                'feed': url,
                'source': domain_of(loc)
            })
    except Exception as ex:
        print('[warn] Sitemap error:', url, ex)
    return out

# --- CLEANERS ---
CLEAN_DROP_PATTERNS = [
    r"^Comments have to be.*", r"^Sign into Unlock benefits.*", r"^Looks like you are already logged in.*",
    r"^To continue logging in.*", r"^We have migrated to a new commenting platform.*",
    r"^Subscribe|^Sign in|^Log in|^Register", r"newsletter", r"cookie", r"advertisement",
    r"^Published on \w+ \d{1,2}, \d{4}", r"^Updated\s*-\s*", r"^Download the app", r"^Read more:"
]
CLEAN_KEEP_HINTS = [
    "results","earnings","profit","loss","revenue","order","contract","tender","merger",
    "approval","policy","circular","rating","downgrade","upgrade","management","stake",
    "pledge","ipo","fpo","ofs","dividend","split","bonus","guidance","capex","rbi","sebi",
    "nse","bse","rupee","inflation","gdp","cpi","wpi","pmi","oil","crude","yield"
]

def _clean_lines(lines: List[str]) -> List[str]:
    kept = []
    for raw in lines:
        line = raw.strip()
        if not line: 
            continue
        if any(re.search(pat, line, re.I) for pat in CLEAN_DROP_PATTERNS):
            continue
        if len(line) > 280 and not any(k in line.lower() for k in CLEAN_KEEP_HINTS):
            continue
        kept.append(line)
    deduped, seen = [], set()
    for l in kept:
        key = re.sub(r"\s+", " ", l.lower())[:160]
        if key in seen: 
            continue
        seen.add(key)
        deduped.append(l)
    return deduped[:120]

def fetch_article_text(url: str) -> Tuple[str, str]:
    html_text = ''
    try:
        r = requests.get(url, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        html_text = r.text
    except Exception:
        return ("", "")

    title = ""
    soup = BeautifulSoup(html_text, 'html.parser')
    title = (soup.title.get_text(strip=True) if soup.title else "") or \
            (soup.find('meta', property='og:title') or {}).get('content', "") or ""

    extracted = ""
    if trafilatura:
        try:
            extracted = trafilatura.extract(
                html_text, include_comments=False, include_tables=False,
                url=url, favor_precision=True
            ) or ""
        except Exception:
            extracted = ""

    if not extracted:
        paras = [p.get_text(" ", strip=True) for p in soup.find_all('p')]
        extracted = "\n".join(paras)

    lines = [ln for ln in (extracted or "").splitlines()]
    cleaned_lines = _clean_lines(lines)
    cleaned = "\n".join(cleaned_lines).strip()
    return (title, cleaned)

# ------------------- Filters / scoring -------------------
def material_enough(title: str, summary: str, link: str) -> bool:
    t = f"{title} {summary} {link}".lower()
    if any(d in t for d in ("sebi.gov.in","rbi.org.in","nseindia.com","bseindia.com",
                            "moneycontrol.com","reuters.com","financialexpress.com",
                            "livemint.com","business-standard.com","thehindubusinessline.com","cnbctv18.com")):
        return True
    return bool(MATERIAL.search(t))

def watchlist_hits(text: str, wl: List[str]) -> List[str]:
    if not wl: return []
    hits = []
    for w in wl:
        if re.search(rf"\b{re.escape(w)}\b", text, re.I):
            hits.append(w)
    return sorted(set(hits))

THEME_KEYWORDS = [
    r"\bev\b", r"\belectric vehicle", r"\bsemiconductor", r"\bfab", r"\bchip",
    r"\bdefence", r"\bdefense", r"\bmissile", r"\bdrone",
    r"\brailway", r"\bmetro", r"\binfra", r"\binfrastructure",
    r"\brenewable", r"\bsolar", r"\bwind", r"\bgreen hydrogen", r"\bbattery",
]
def theme_score(text: str) -> int:
    txt = text.lower()
    return sum(1 for pat in THEME_KEYWORDS if re.search(pat, txt))

def quality_weight(link: str) -> int:
    l = link.lower()
    if any(x in l for x in ['sebi.gov.in','rbi.org.in','nseindia.com','bseindia.com']): return 12
    if 'reuters.com' in l: return 6
    if 'moneycontrol.com' in l: return 5
    if 'financialexpress.com' in l: return 4
    if 'livemint.com' in l: return 4
    if 'business-standard.com' in l: return 4
    if 'thehindubusinessline.com' in l: return 3
    if 'cnbctv18.com' in l: return 3
    return 1

def diversify(items: List[Dict[str,Any]], max_per_domain: int = 2, limit: int = 10) -> List[Dict[str,Any]]:
    per = {}
    out = []
    for x in items:
        d = domain_of(x['link'])
        if per.get(d,0) >= max_per_domain:
            continue
        out.append(x)
        per[d] = per.get(d,0)+1
        if len(out) >= limit:
            break
    return out

# ------------------- Company quick research -------------------
def wiki_summary(query: str) -> Tuple[str,str]:
    try:
        s = requests.get("https://en.wikipedia.org/w/api.php",
                         params={"action":"query","list":"search","srsearch":query,"format":"json","srlimit":1},
                         timeout=10)
        s.raise_for_status()
        hits = s.json().get("query",{}).get("search",[])
        if not hits: return ("","")
        title = hits[0]["title"]
        r = requests.get(f"https://en.wikipedia.org/api/rest_v1/page/summary/{title}",
                         timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        js = r.json()
        return (js.get("extract",""), js.get("content_urls",{}).get("desktop",{}).get("page",""))
    except Exception:
        return ("","")

def moneycontrol_company_blurb(name: str) -> Tuple[str,str]:
    try:
        q = quote_plus(name + " Moneycontrol")
        r = requests.get(f"https://www.google.com/search?q={q}", timeout=12, headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        a = soup.find('a', href=re.compile(r"^https?://www\.moneycontrol\.com/"))
        if not a: return ("","")
        url = a['href']
        p = requests.get(url, timeout=12, headers={"User-Agent":"Mozilla/5.0"})
        p.raise_for_status()
        psoup = BeautifulSoup(p.text, 'html.parser')
        md = psoup.find('meta', attrs={'name':'description'})
        desc = md['content'].strip() if md and md.get('content') else ""
        return (desc, url)
    except Exception:
        return ("","")

def guess_company_names(title: str, txt: str, wl: List[str]) -> List[str]:
    hits = watchlist_hits(f"{title} {txt}", wl)
    if hits: return hits
    cand = re.findall(r"\b([A-Z][A-Za-z&.\- ]+(?:Ltd|Limited|Industries|Motors|Bank|Steel|Power|Pharma|Cements|Airways|Airlines|Technologies|Labs|Services))\b",
                      f"{title} {txt}")
    names = [c.strip() for c in cand]
    return sorted(set(names))[:2]

# ------------------- LLM summarizer (STRICT & guard) -------------------
ANALYST_PROMPT = """You are an India-focused markets analyst writing for beginners.

GROUNDING:
- Use ONLY the facts from the provided ARTICLE TEXT.
- The SOURCE URL is given so you know where the text came from; do not add facts not present in the text.
- If something is unclear or not in the text, say "not specified in the article" rather than guessing.
- Explain jargon briefly and simply within the summary when helpful.

OUTPUT (STRICT JSON):
{
  "headline_rewrite": "≤14 words, punchy, no emojis",
  "bullets": [
    "3–5 bullets, each ≤24 words, start with a verb, explain the news, avoid rephrasing the headline",
    "Include numbers only if present in the article text",
    "Define any jargon once in parentheses, e.g., EBITDA (operating profit proxy)"
  ],
  "impact": "Bullish" | "Bearish" | "Neutral",
  "impact_reason": "≤2 sentences on why it skews that way",
  "affected": ["sectors or NSE/BSE tickers explicitly in article; else empty"],
  "why_matters": "1 sentence on investor relevance",
  "watch_next": ["1–2 concrete follow-ups, e.g., filing due date, regulator decision"]
}

Rules: concise, factual, mobile-friendly; no invented numbers/tickers; no investment advice.
Return JSON only.
"""

def openai_summarize(headline: str, article_text: str, source_url: str, company_context: str = "") -> Dict[str,Any]:
    if not (LLM_PROVIDER=='openai' and OPENAI_API_KEY):
        return {}
    try:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type":"application/json"}
        ARTICLE = (article_text or "")[:12000]
        user = f"""SOURCE URL: {source_url}
HEADLINE: {headline}

ARTICLE TEXT:
{ARTICLE}

OPTIONAL COMPANY CONTEXT (background flavour only; do not add facts beyond ARTICLE):
{(company_context or '')[:1200]}

{ANALYST_PROMPT}"""
        payload = {
            "model": OPENAI_MODEL,
            "response_format": {"type":"json_object"},
            "temperature": 0.2,
            "max_tokens": 900,
            "messages": [
                {"role":"system","content":"Respond in STRICT JSON only."},
                {"role":"user","content":user}
            ]
        }
        r = requests.post(url, headers=headers, json=payload, timeout=75)
        r.raise_for_status()
        js = json.loads(r.json()["choices"][0]["message"]["content"])
        if "bullets" not in js:
            js["bullets"] = []
        js["bullets"] = js.get("bullets", [])[:5]
        return js
    except Exception as e:
        print("[warn] OpenAI summarize error:", e)
        return {}

def _extractive_bullets(text: str, k: int = 4) -> List[str]:
    sents = re.split(r"(?<=[.!?])\s+", text or "")
    key = re.compile(r"(result|profit|loss|revenue|order|contract|tender|merger|approval|policy|rating|stake|pledge|ipo|dividend|split|bonus|guidance|capex|rbi|sebi|nse|bse|rupee|inflation|gdp|cpi|oil|yield)", re.I)
    picked = []
    for s in sents:
        if key.search(s):
            words = s.split()
            picked.append(" ".join(words[:24]))
        if len(picked) >= k:
            break
    if not picked:
        for s in sents[:3]:
            words = s.split()
            if words:
                picked.append(" ".join(words[:24]))
    return picked[:5]

def fallback_review(title: str, fulltext: str) -> Dict[str,Any]:
    bullets = _extractive_bullets(fulltext, k=4)
    low = (title + " " + fulltext).lower()
    impact = "Neutral"
    if re.search(r"(beats|surge|order win|upgrade|approval|record|wins|bags|reduces duty|cuts tax)", low): impact = "Bullish"
    if re.search(r"(downgrade|penalty|ban|raid|probe|default|fire|accident|miss|shortfall|hike duty|raises tax)", low): impact = "Bearish"
    return {
        "headline_rewrite": title[:120],
        "bullets": bullets,
        "impact": impact,
        "impact_reason": "Heuristic extractive summary; treat as preliminary.",
        "affected": [],
        "why_matters": "Likely to sway sector sentiment short term.",
        "watch_next": []
    }

def is_bad_review(rev: Dict[str,Any], fulltext: str) -> bool:
    bullets = rev.get("bullets") or []
    if not bullets: return True
    joined = " ".join(bullets).lower()
    if len(joined) < 60: return True
    if "as an ai" in joined or "cannot access" in joined: return True
    if len(set(bullets)) <= 1: return True
    bad_phrases = ["not specified in the article", "cannot be determined", "insufficient information"]
    if sum(1 for b in bullets if any(p in b.lower() for p in bad_phrases)) >= max(1, len(bullets)//2):
        return True
    return False

# ------------------- Beginner notes -------------------
GLOSSARY = {
    "ipo": "Initial Public Offering — company sells shares to the public for the first time.",
    "fpo": "Follow-on Public Offer — a listed company issues more shares to raise funds.",
    "ofs": "Offer for Sale — promoters/large holders sell shares via exchange window.",
    "pledge": "Promoters use their shares as collateral; high pledging is risky.",
    "block deal": "Large buy/sell trade between big investors in a special window.",
    "buyback": "Company purchases its own shares; usually supportive for price.",
    "ebitda": "Earnings before interest, taxes, depreciation and amortization.",
    "guidance": "Management’s outlook on revenue, margins, etc.",
    "downgrade": "Broker/ratings agency cuts its view on a stock or debt.",
    "upgrade": "Broker/ratings agency raises its view on a stock or debt.",
    "capex": "Capital expenditure — spending on plants/equipment/expansion.",
    "tender": "Competitive bidding to win a government or private contract.",
    "dividend": "Cash paid to shareholders from profits.",
    "split": "Shares are divided into more shares; market cap unchanged.",
    "bonus": "Free additional shares to shareholders; market cap unchanged.",
}
def build_beginner_notes(text: str) -> List[Dict[str,str]]:
    found = []
    low = text.lower()
    for term, meaning in GLOSSARY.items():
        if re.search(rf"\b{re.escape(term)}\b", low):
            found.append({"term": term.upper(), "meaning": meaning})
    return found[:8]

# ------------------- Output block -------------------
def to_html_block(item: Dict[str,Any]) -> str:
    it   = item['item']
    rev  = item['review']
    notes= item.get('beginner_notes',[])
    comp = item.get('company_snapshot','')
    comp_src = item.get('company_source','')

    ist_time = to_ist(it['time'])
    impact = rev.get("impact","Neutral")
    badge = {"Bullish":"🟢","Bearish":"🔴","Neutral":"⚪"}.get(impact,"⚪")
    affected = ", ".join(rev.get("affected") or []) or "—"
    watch = "; ".join(rev.get("watch_next") or []) or "—"
    bullets = rev.get("bullets") or []
    bullets_html = "\n".join([f"• {html.escape(b)}" for b in bullets[:5]]) or "• (no concise bullets available)"
    notes_str = "; ".join([f"{n['term']}: {n['meaning']}" for n in notes]) if notes else "—"
    comp_line = (
        f"{html.escape(comp)} (Source: <a href='{html.escape(comp_src)}'>{html.escape(comp_src)}</a>)"
        if comp and comp_src else (html.escape(comp) if comp else "—")
    )
    return (
        f"{badge} <b>{html.escape(rev.get('headline_rewrite') or it.get('title') or '[No title]')}</b>\n"
        f"{bullets_html}\n"
        f"<b>AI take:</b> {html.escape(impact)} — {html.escape(rev.get('impact_reason',''))}\n"
        f"<b>Who/what:</b> {html.escape(affected)}\n"
        f"<b>Company snapshot:</b> {comp_line}\n"
        f"<b>New to finance? Terms:</b> {html.escape(notes_str)}\n"
        f"<b>Why this matters:</b> {html.escape(rev.get('why_matters',''))}\n"
        f"<b>Watch next:</b> {html.escape(watch)}\n"
        f"<b>Source:</b> <a href='{html.escape(it['link'])}'>{html.escape(it['link'])}</a> ({html.escape(domain_of(it['link']))})\n"
        f"<i>{html.escape(ist_time)}</i>\n"
        f"<i>Not investment advice.</i>"
    )

# ------------------- Core runner -------------------
def run_brief(
    window_min: int = None,
    max_items: int = None,
    diversify_domains: int = 2,
    watchlist: List[str] = None,
    watchlist_only: bool = None,
    send: bool = False  # ignored
):
    window_min = window_min or DEFAULT_WINDOW_MIN
    max_items  = max_items  or DEFAULT_MAX_ITEMS
    wl = WATCHLIST if watchlist is None else [w.strip() for w in watchlist if w.strip()]
    wl_only = WATCHLIST_ONLY if watchlist_only is None else bool(watchlist_only)

    print(f"[{dt.datetime.now()}] Start finance run_brief window={window_min} min, max={max_items}, diversify_per_domain={diversify_domains}")
    print(f"   Watchlist: {wl or '—'} (mode: {'ONLY' if wl_only else 'BOOST'})")

    try:
        st = json.load(open(STATE_FILE, 'r', encoding='utf-8')) if os.path.exists(STATE_FILE) else {'seen': []}
    except Exception:
        st = {'seen': []}
    raw_seen = st.get('seen', [])
    seen = set(tuple(x) if isinstance(x, list) else tuple(x) if isinstance(x, tuple) else x for x in raw_seen)
    print(f"   Seen size: {len(seen)}")

    candidates: List[Dict[str, Any]] = []
    for url in FEEDS + MEDIA_RSS:
        arr = fetch_rss(url)
        for it in arr:
            if not it.get('link'): continue
            if not is_fresh(it['time'], window_min): continue
            if not material_enough(it.get('title', ''), it.get('summary', ''), it['link']): continue
            key = (domain_of(it['link']), urlparse(it['link']).path)
            if key in seen: continue
            wl_hits = watchlist_hits(f"{it.get('title','')} {it.get('summary','')}", wl)
            if wl_only and wl and not wl_hits: continue
            candidates.append(it | {'_key': key, '_wl_hits': wl_hits})

    for sm in SITEMAPS:
        arr = fetch_sitemap(sm, limit=60)
        fresh = [x for x in arr if is_fresh(x['time'], window_min)]
        for it in fresh:
            key = (domain_of(it['link']), urlparse(it['link']).path)
            if key in seen: continue
            it.update({'_key': key, '_wl_hits': []})
            candidates.append(it)

    if not candidates:
        print(f"[{dt.datetime.now()}] No fresh items found.")
        return []

    print(f"[{dt.datetime.now()}] Scoring {len(candidates)} candidates")
    def prelim_score(x):
        q = quality_weight(x['link'])
        rec = 1.0 / max(1, int((now_utc() - x['time']).total_seconds() // 60))
        wl_boost = 3 * len(x.get('_wl_hits', []))
        th = theme_score(f"{x.get('title','')} {x.get('summary','')}")
        return q * 10 + wl_boost + th + rec

    ranked = sorted(candidates, key=prelim_score, reverse=True)[:max_items * 6]
    diversified = diversify(ranked, max_per_domain=diversify_domains, limit=max_items * 3)

    selected: List[Dict[str, Any]] = []
    for it in diversified:
        title, text = fetch_article_text(it['link'])
        if not title and not text: continue
        if not it.get('title'): it['title'] = title
        if not it.get('summary'): it['summary'] = (text or '')[:300]
        if not material_enough(it['title'], (it.get('summary', '') or '') + " " + (text or ''), it['link']):
            continue
        wl_hits = list(set(it.get('_wl_hits', []) + watchlist_hits(f"{title} {text}", wl)))
        it['_wl_hits'] = wl_hits
        it['_themes'] = theme_score(text or "")
        it['_fulltext'] = text or ""
        selected.append(it)

    if not selected:
        print("[info] Nothing material after full-text check.")
        return []

    def final_score(x):
        q = quality_weight(x['link'])
        rec = 1.0 / max(1, int((now_utc() - x['time']).total_seconds() // 60))
        wl_boost = 4 * len(x.get('_wl_hits', []))
        th = x.get('_themes', 0)
        return q * 12 + wl_boost + th * 2 + rec

    items_all = sorted(selected, key=final_score, reverse=True)

    results = []
    print("\n=== Diversified India Market Brief ===\n")
    for it in items_all:
        if len(results) >= max_items:
            break

        comp_names = guess_company_names(it['title'], it.get('_fulltext', ''), wl)
        comp_summary, comp_url = "", ""
        if comp_names:
            comp_summary, comp_url = wiki_summary(comp_names[0])
            if not comp_summary:
                mc_desc, mc_url = moneycontrol_company_blurb(comp_names[0])
                if mc_desc:
                    comp_summary, comp_url = mc_desc, mc_url

        rev = openai_summarize(it['title'], it.get('_fulltext', '') or it.get('summary', ''), it['link'], comp_summary or "")
        if not rev:
            rev = fallback_review(it['title'], it.get('_fulltext', '') or it.get('summary', ''))
        if is_bad_review(rev, it.get('_fulltext','')):
            print("   [skip] low-quality summary for:", it['title'][:80])
            continue

        notes = build_beginner_notes(it.get('_fulltext', '') or it.get('summary', ''))
        results.append({
            'item': it,
            'review': rev,
            'beginner_notes': notes,
            'company_snapshot': (comp_summary or "")[:600],
            'company_source': comp_url
        })

        block_html = to_html_block(results[-1])
        print(textwrap.dedent(re.sub('<[^<]+?>', '', block_html)))
        print('-' * 90)

    # persist seen
    seen_keys = set(tuple(x) if isinstance(x, list) else tuple(x) if isinstance(x, tuple) else x for x in raw_seen)
    for r in results:
        it = r['item']
        seen_keys.add((domain_of(it['link']), urlparse(it['link']).path))
    try:
        json.dump({'seen': [list(k) for k in seen_keys]}, open(STATE_FILE, 'w', encoding='utf-8'))
        print(f"[{dt.datetime.now()}] Saved state. Total seen={len(seen_keys)}")
    except Exception as e:
        print("[warn] save state failed:", e)

    return results

if __name__ == "__main__":
    print("✅ Finance pipeline loaded (no Telegram, appending runs)")
