import os, re, json, html, textwrap, time
import datetime as dt
from typing import List, Dict, Any, Tuple
from urllib.parse import urlparse, urljoin

import requests, feedparser
from bs4 import BeautifulSoup

try:
    import trafilatura
except Exception:
    trafilatura = None

# ===============================
# Config
# ===============================
DEFAULT_WINDOW_MIN = int(os.getenv('DEFAULT_WINDOW_MIN', '30'))  # lookback minutes
DEFAULT_MAX_ITEMS  = int(os.getenv('DEFAULT_MAX_ITEMS',  '12'))
STATE_FILE = os.getenv('STATE_FILE', os.getenv('TECH_STATE_FILE', 'seen_technews.json'))

# OpenAI
LLM_PROVIDER   = os.getenv('LLM_PROVIDER','openai').lower()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY','')
OPENAI_MODEL   = os.getenv('OPENAI_MODEL','gpt-4o-mini')

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID', '')
# ------------------- Sources -------------------
OFFICIAL_RSS = [
    'https://blog.google/rss/',
    'https://openai.com/blog/rss.xml',
    'https://blogs.microsoft.com/feed/',
    'https://blogs.nvidia.com/feed/',
    'https://about.fb.com/news/feed/',
    'https://aws.amazon.com/blogs/aws/feed/',
    'https://www.intel.com/content/www/us/en/newsroom/rss.xml',
    'https://www.qualcomm.com/news/releases/rss.xml',
    # 'https://www.amd.com/en/rss.xml',
    'https://cloud.google.com/blog/rss/',
]

MEDIA_RSS = [
    'https://techcrunch.com/feed/',
    'https://www.theverge.com/rss/index.xml',
    'http://feeds.arstechnica.com/arstechnica/index/',
    'https://www.wired.com/feed/rss',
    'https://www.engadget.com/rss.xml',
    'https://feeds.reuters.com/reuters/technologyNews',   # use the official Reuters tech RSS
]

# HTML listing pages (no RSS) that we should crawl
HTML_LISTINGS = [
    'https://news.google.com/topics',                        # Google News topics (overview page)
    'https://www.thehindu.com/sci-tech/technology/',         # The Hindu technology section
    'https://timesofindia.indiatimes.com/technology',        # TOI technology section
]

# Single non-RSS links to consider (e.g., a specific NYT article page)
SINGLE_LINKS = [
    'https://www.nytimes.com/2025/08/24/technology',         # specific link you asked to include
]

SITEMAPS = [
    'https://www.theverge.com/sitemaps/news.xml',
    'https://techcrunch.com/sitemap-news.xml',
]

WATCHLIST = [w.strip() for w in os.getenv('WATCHLIST','NVIDIA, Apple, Google, Microsoft, OpenAI, AMD, Intel, TSMC, Qualcomm, Meta, Amazon, Anthropic').split(',') if w.strip()]
WATCHLIST_ONLY = os.getenv('WATCHLIST_ONLY','false').lower() in ('1','true','yes')

UTC = dt.timezone.utc
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

MATERIAL = re.compile(
    r"(ai|llm|gpu|npu|tpu|accelerator|chip|semiconductor|fab|foundry|tsmc|intel|amd|nvidia|qualcomm|arm|cloud|aws|azure|gcp|datacenter|server|hpc|storage|networking|5g|security|breach|cve|vulnerability|ransomware|privacy|antitrust|acquisition|merger|funding|layoff|hiring|ipo|product|launch|update|feature|api|sdk|policy|regulation|apple|google|microsoft|meta|amazon|openai|anthropic|linux|windows|macos|android|ios)",
    re.I
)

# ===============================
# Utilities
# ===============================

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

# ===============================
# Fetchers
# ===============================

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
                'title': '', 'summary': '', 'link': loc, 'time': t, 'feed': url, 'source': domain_of(loc)
            })
    except Exception as ex:
        print('[warn] Sitemap error:', url, ex)
    return out

def fetch_html_listing(url: str, limit:int=40) -> List[Dict[str, Any]]:
    out = []
    try:
        r = requests.get(url, timeout=20, headers={'User-Agent':'Mozilla/5.0'})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('#') or href.startswith('javascript:'):
                continue
            href = urljoin(url, href)
            if domain_of(href) != domain_of(url):
                continue
            if any(x in href for x in ['/privacy', '/terms', '/subscribe', '/about']):
                continue
            title = a.get_text(' ', strip=True)
            if not title or len(title) < 6:
                continue
            links.append((title, href))
        # de-dupe by path
        seen_paths = set()
        for title, href in links:
            path = urlparse(href).path
            if path in seen_paths:
                continue
            seen_paths.add(path)
            out.append({
                'title': title,
                'summary': '',
                'link': href,
                'time': now_utc(),  # listing pages rarely expose per-item timestamps
                'feed': url,
                'source': domain_of(href),
            })
            if len(out) >= limit:
                break
    except Exception as ex:
        print('[warn] HTML listing error:', url, ex)
    return out

# ===============================
# Article extraction / cleaning (returns raw_html too)
# ===============================
CLEAN_DROP_PATTERNS = [
    r"^Comments have to be.*", r"^Sign in|^Log in|^Register", r"newsletter|cookie|advertisement",
    r"^Updated\s*-\s*", r"^Read more:", r"^Subscribe"
]
CLEAN_KEEP_HINTS = [
    'ai','llm','gpu','chip','semiconductor','cloud','aws','azure','gcp','security','breach','cve',
    'acquisition','merger','funding','product','launch','feature','api','sdk','policy','antitrust'
]

def _clean_lines(lines: List[str]) -> List[str]:
    kept = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if any(re.search(pat, line, re.I) for pat in CLEAN_DROP_PATTERNS):
            continue
        # keep long lines too if they include key hints; otherwise drop super-long fluff
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
    return deduped  # NO truncation

def fetch_article_text(url: str) -> Tuple[str, str, str]:
    """Return (title, cleaned_text, raw_html)."""
    html_text = ''
    try:
        r = requests.get(url, timeout=25, headers={'User-Agent':'Mozilla/5.0'})
        r.raise_for_status()
        html_text = r.text
    except Exception:
        return ("", "", "")

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
        paras = [p.get_text(' ', strip=True) for p in soup.find_all('p')]
        extracted = "\n".join(paras)

    lines = [ln for ln in (extracted or '').splitlines()]
    cleaned = "\n".join(_clean_lines(lines)).strip()
    return (title, cleaned, html_text)

# ===============================
# Minimal enrichment helpers (canonical/meta/event/hash)
# ===============================

def enrich_meta(html_text:str, url:str):
    soup = BeautifulSoup(html_text, "html.parser")
    # canonical
    can = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    canonical = can["href"].strip() if can and can.get("href") else url
    # og
    def og(name):
        tag = soup.find("meta", property=f"og:{name}")
        return tag["content"].strip() if tag and tag.get("content") else ""
    site_name = og("site_name")
    # schema.org dates & author (best-effort)
    published_at, byline = "", []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "{}")
            data = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else {})
            t = (data.get("@type") or "").lower()
            if "newsarticle" in t or "blogposting" in t:
                published_at = data.get("datePublished") or published_at
                auth = data.get("author")
                if isinstance(auth, list):
                    byline = [a.get("name") for a in auth if isinstance(a, dict) and a.get("name")]
                elif isinstance(auth, dict) and auth.get("name"):
                    byline = [auth.get("name")]
                break
        except Exception:
            continue
    return canonical, site_name, published_at, byline

EVENT_PATTERNS = [
    ("security_advisory", r"\b(CVE-\d{4}-\d+|vulnerab|patch|zero[- ]day|ransomware|exploit|advisory)\b"),
    ("launch", r"\b(launch(es|ed)?|unveil|introduc(e|es|ed)|GA\b|general availability)\b"),
    ("update", r"\b(update|release notes|v\d+\.\d+(\.\d+)?|patch)\b"),
    ("acquisition", r"\b(acquires?|acquisition|merger|buyout|takeover)\b"),
    ("policy", r"\b(antitrust|FTC|DoJ|CMA|DMA|DSA|EU Commission|Ofcom)\b"),
]

def classify_event(text: str) -> str:
    low = (text or "").lower()
    for label, pat in EVENT_PATTERNS:
        if re.search(pat, low):
            return label
    return "update" if re.search(r"\bv\d+\.\d+", low) else "launch" if re.search(r"\b(launch|GA)\b", low) else "news"

import hashlib

def novelty_hash(text:str)->str:
    norm = re.sub(r"\s+"," ", (text or "").lower()).strip()[:]  # no truncation
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()

# ===============================
# Filters / scoring
# ===============================

def material_enough(title: str, summary: str, link: str) -> bool:
    t = f"{title} {summary} {link}".lower()
    if any(d in t for d in (
        'blog.google', 'openai.com', 'blogs.nvidia.com', 'blogs.microsoft.com', 'about.fb.com',
        'aws.amazon.com', 'intel.com', 'qualcomm.com', 'amd.com',
        'techcrunch.com','theverge.com','arstechnica.com','wired.com','engadget.com','reuters.com'
    )):
        return True
    return bool(MATERIAL.search(t))

def watchlist_hits(text: str, wl: List[str]) -> List[str]:
    if not wl: return []
    hits = []
    for w in wl:
        if re.search(rf"\b{re.escape(w)}\b", text, re.I):
            hits.append(w)
    return sorted(set(hits))

THEME_KEYWORDS = [r"\bai\b", r"\bllm\b", r"\bgpu\b", r"\bchip\b", r"\bsemiconductor\b",
                  r"\bcloud\b", r"\bsecurity\b", r"\bprivacy\b", r"\bantitrust\b", r"\bmerger\b",
                  r"\bfunding\b", r"\bproduct\b", r"\blaunch\b"]

def theme_score(text: str) -> int:
    txt = text.lower()
    return sum(1 for pat in THEME_KEYWORDS if re.search(pat, txt))

def quality_weight(link: str) -> int:
    l = link.lower()
    if any(x in l for x in ['blog.google','openai.com','blogs.nvidia.com','blogs.microsoft.com','aws.amazon.com']): return 12
    if 'reuters.com' in l: return 7
    if 'techcrunch.com' in l: return 6
    if 'theverge.com' in l: return 5
    if 'arstechnica.com' in l: return 5
    if 'wired.com' in l: return 4
    if 'engadget.com' in l: return 3
    return 1

def diversify(items: List[Dict[str,Any]], max_per_domain: int = 2, limit: int = 12) -> List[Dict[str,Any]]:
    per, out = {}, []
    for x in items:
        d = domain_of(x['link'])
        if per.get(d,0) >= max_per_domain:
            continue
        out.append(x)
        per[d] = per.get(d,0)+1
        if len(out) >= limit:
            break
    return out

# ===============================
# LLM summarizer (STRICT & GROUNDED) – tweaked fields (no "why it matters")
# ===============================
ANALYST_PROMPT = """You are a technology news analyst writing for a broad audience.

GROUNDING:
- Use ONLY the facts from the provided ARTICLE TEXT.
- The SOURCE URL is given; do not add facts not present in the text.
- If something is unclear or not in the text, say "not specified in the article".
- Define jargon briefly when helpful.

OUTPUT (STRICT JSON):
{
  "headline_rewrite": "≤14 words, punchy, no emojis",
  "bullets": [
    "3–8 bullets, full sentences, do not truncate mid-sentence",
    "Include numbers only if present in the article text"
  ],
  "impact": "Positive" | "Negative" | "Neutral",
  "impact_reason": "≤2 sentences on why",
  "affected": ["companies, products or sectors explicitly in article; else empty"],
  "motive": "One concise sentence inferring the company's motive from the article text"
}

Rules: concise, factual; no invented numbers; no investment advice.
Return JSON only.
"""

def openai_summarize(headline: str, article_text: str, source_url: str, company_context: str = "") -> Dict[str,Any]:
    if not (LLM_PROVIDER=='openai' and OPENAI_API_KEY):
        return {}
    try:
        url = 'https://api.openai.com/v1/chat/completions'
        headers = {'Authorization': f'Bearer {OPENAI_API_KEY}', 'Content-Type':'application/json'}
        ARTICLE = (article_text or '')  # no truncation
        user = f"""SOURCE URL: {source_url}
HEADLINE: {headline}

ARTICLE TEXT:
{ARTICLE}

OPTIONAL COMPANY CONTEXT (background flavour only; do not add facts beyond ARTICLE):
{(company_context or '')}

{ANALYST_PROMPT}"""
        payload = {
            'model': OPENAI_MODEL,
            'response_format': {'type':'json_object'},
            'temperature': 0.2,
            'max_tokens': 1000,
            'messages': [
                {'role':'system','content':'Respond in STRICT JSON only.'},
                {'role':'user','content':user}
            ]
        }
        r = requests.post(url, headers=headers, json=payload, timeout=75)
        r.raise_for_status()
        js = json.loads(r.json()['choices'][0]['message']['content'])
        if 'bullets' not in js:
            js['bullets'] = []
        # keep ALL bullets, no slicing
        js['bullets'] = js.get('bullets', [])
        return js
    except Exception as e:
        print('[warn] OpenAI summarize error:', e)
        return {}

def _extractive_bullets(text: str, k: int = 6) -> List[str]:
    sents = re.split(r"(?<=[.!?])\s+", text or '')
    key = re.compile(r"(ai|llm|gpu|chip|semiconductor|cloud|security|breach|cve|acquisition|merger|funding|product|launch|api|sdk|policy|antitrust)", re.I)
    picked = []
    for s in sents:
        if key.search(s):
            picked.append(s.strip())  # no truncation
        if len(picked) >= k:
            break
    if not picked:
        for s in sents[:5]:
            if s.strip():
                picked.append(s.strip())
    return picked  # no [:N]

def fallback_review(title: str, fulltext: str) -> Dict[str,Any]:
    bullets = _extractive_bullets(fulltext, k=6)
    low = (title + ' ' + fulltext).lower()
    impact = 'Neutral'
    if re.search(r'(record|wins|launch|fixes|patch|approve|reduce price|outperform|faster|lower latency)', low): impact = 'Positive'
    if re.search(r'(breach|exploit|bug|downtime|layoff|lawsuit|ban|fine|miss|delay|outage)', low): impact = 'Negative'
    return {
        'headline_rewrite': title,  # no truncation
        'bullets': bullets,
        'impact': impact,
        'impact_reason': 'Heuristic extractive summary; treat as preliminary.',
        'affected': [],
        'motive': ''
    }

# ===============================
# State helpers (remember last updates by domain)
# ===============================

def load_state():
    try:
        st = json.load(open(STATE_FILE, 'r', encoding='utf-8')) if os.path.exists(STATE_FILE) else {'seen': [], 'history': []}
        if 'history' not in st:  # backward-compat
            st['history'] = []
        return st
    except Exception:
        return {'seen': [], 'history': []}

def save_state(seen_keys:set, history:list):
    try:
        json.dump({'seen': [list(k) for k in seen_keys], 'history': history[-500:]}, open(STATE_FILE, 'w', encoding='utf-8'))
        print(f"[{dt.datetime.now()}] Saved state. Total seen={len(seen_keys)} history={len(history[-500:])}")
    except Exception as e:
        print('[warn] save state failed:', e)

def last_update_for_domain(domain:str, history:list, exclude_canonical:str=None):
    # find the most recent history item for this domain that's not the current article
    for it in reversed(history):
        if it.get('domain') == domain and it.get('canonical') != exclude_canonical:
            return it
    return None

# ===============================
# Output block / Telegram (no published date, no 'why it matters')
# ===============================

def to_html_block(item: Dict[str,Any], history:List[Dict[str,Any]]) -> str:
    it   = item['item']
    rev  = item['review']

    impact = rev.get('impact','Neutral')
    badge = {'Positive':'🟢','Negative':'🔴','Neutral':'⚪'}.get(impact,'⚪')

    bullets = rev.get('bullets') or []
    bullets_html = "\n".join([f"• {html.escape(b)}" for b in bullets]) or '• (no concise bullets available)'

    site = it.get('site_name') or domain_of(it['link'])
    et = it.get('event_type','news')
    motive = rev.get('motive','').strip()

    header_line = f"{badge} <b>{html.escape(rev.get('headline_rewrite') or it.get('title') or '[No title]')}</b>  <i>[{html.escape(et)}]</i>\n"
    motive_line = f"\n<b>Motive (inferred):</b> {html.escape(motive)}" if motive else ""

    return (
        header_line +
        f"{bullets_html}\n" +
        f"<b>Impact:</b> {html.escape(impact)}\n" +
        f"<b>Source:</b> <a href='{html.escape(it.get('canonical') or it['link'])}'>{html.escape(site)}</a>" +
        motive_line
    )

TELEGRAM_MAX = 3800

def send_telegram_html_long(text_html: str) -> bool:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print('[warn] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set')
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        parts = []
        text = text_html
        while len(text) > TELEGRAM_MAX:
            cut = text.rfind('\n', 0, TELEGRAM_MAX)
            if cut < 0: cut = TELEGRAM_MAX
            parts.append(text[:cut])
            text = text[cut:]
        parts.append(text)

        ok_all = True
        for idx, p in enumerate(parts, 1):
            hdr = f"(part {idx}/{len(parts)})\n" if len(parts) > 1 else ''
            payload = {
                'chat_id': TELEGRAM_CHAT_ID,
                'text': hdr + p,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True,
                'disable_notification': os.getenv('TELEGRAM_SILENT','false').lower() in ('1','true','yes')
            }
            r = requests.post(url, json=payload, timeout=20)
            if r.status_code != 200:
                print('[warn] Telegram send failed:', r.status_code, r.text[:400])
                # Fallback: strip HTML and retry without parse_mode
                plain = re.sub('<[^<]+?>', '', payload['text'])
                r2 = requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': plain}, timeout=20)
                if r2.status_code != 200:
                    print('[warn] Telegram fallback failed:', r2.status_code, r2.text[:400])
                    ok_all = False
            time.sleep(0.3)
        return ok_all
    except Exception as e:
        print('[warn] Telegram exception:', e)
        return False

# ===============================
# Core runner
# ===============================

def run_brief(
    window_min: int = None,
    max_items: int = None,
    diversify_domains: int = 2,
    watchlist: List[str] = None,
    watchlist_only: bool = None,
    send: bool = False
):
    """Pull fresh tech items, extract full text, summarize into bullets, enrich minimally,
    dedupe by canonical, diversify by domain, optionally send compact HTML blocks to Telegram."""

    window_min = window_min or DEFAULT_WINDOW_MIN
    max_items  = max_items  or DEFAULT_MAX_ITEMS
    wl = WATCHLIST if watchlist is None else [w.strip() for w in watchlist if w.strip()]
    wl_only = WATCHLIST_ONLY if watchlist_only is None else bool(watchlist_only)

    print(f"[{dt.datetime.now()}] Start run_brief window={window_min} min, max={max_items}, diversify_per_domain={diversify_domains}, send={send}")
    print(f"   Watchlist: {wl or '—'} (mode: {'ONLY' if wl_only else 'BOOST'})")

    # Load state
    st = load_state()
    raw_seen = st.get('seen', [])
    history = st.get('history', [])
    seen = set(tuple(x) if isinstance(x, (list, tuple)) else x for x in raw_seen)
    print(f"   Seen size: {len(seen)}  |  History size: {len(history)}")

    # Collect candidates: official first, then media
    candidates: List[Dict[str, Any]] = []
    for url in OFFICIAL_RSS + MEDIA_RSS:
        print(f"→ Fetching RSS: {url}")
        arr = fetch_rss(url)
        print(f"   pulled {len(arr)}")
        for it in arr:
            if not it.get('link'): continue
            if not is_fresh(it['time'], window_min): continue
            if not material_enough(it.get('title',''), it.get('summary',''), it['link']): continue
            key = (domain_of(it['link']), urlparse(it['link']).path)
            if key in seen: continue
            wl_hits = watchlist_hits(f"{it.get('title','')} {it.get('summary','')}", wl)
            if wl_only and wl and not wl_hits: continue
            candidates.append(it | {'_key': key, '_wl_hits': wl_hits})
        print(f"   candidates total: {len(candidates)}")

    # Add a few sitemap items (very fresh only)
    for sm in SITEMAPS:
        print(f"→ Fetching sitemap: {sm}")
        arr = fetch_sitemap(sm, limit=60)
        fresh = [x for x in arr if is_fresh(x['time'], window_min)]
        print(f"   sitemap fresh items: {len(fresh)}")
        for it in fresh:
            key = (domain_of(it['link']), urlparse(it['link']).path)
            if key in seen: continue
            it.update({'_key': key, '_wl_hits': []})
            candidates.append(it)
        print(f"   candidates total: {len(candidates)}")

    # HTML listing pages
    for url in HTML_LISTINGS:
        print(f"→ Fetching listing: {url}")
        arr = fetch_html_listing(url, limit=30)
        print(f"   listing pulled {len(arr)}")
        for it in arr:
            key = (domain_of(it['link']), urlparse(it['link']).path)
            if key in seen: continue
            it.update({'_key': key, '_wl_hits': []})
            candidates.append(it)
        print(f"   candidates total: {len(candidates)}")

    # Single links
    for link in SINGLE_LINKS:
        key = (domain_of(link), urlparse(link).path)
        if key not in seen:
            candidates.append({'title':'','summary':'','link':link,'time':now_utc(),'_key':key,'_wl_hits':[]})

    if not candidates:
        print(f"[{dt.datetime.now()}] No fresh items found.")
        return []

    # Preliminary scoring
    print(f"[{dt.datetime.now()}] Scoring {len(candidates)} candidates")
    def prelim_score(x):
        q = quality_weight(x['link'])
        rec = 1.0 / max(1, int((now_utc() - x['time']).total_seconds() // 60))
        wl_boost = 3 * len(x.get('_wl_hits', []))
        th = theme_score(f"{x.get('title','')} {x.get('summary','')}")
        return q * 10 + wl_boost + th + rec

    ranked = sorted(candidates, key=prelim_score, reverse=True)[:max_items * 4]
    diversified = diversify(ranked, max_per_domain=diversify_domains, limit=max_items * 2)

    # Fetch full text & enrich
    selected: List[Dict[str, Any]] = []
    for it in diversified:
        t0 = time.time()
        title, text, raw_html = fetch_article_text(it['link'])
        if not title and not text: continue
        if not it.get('title'): it['title'] = title
        if not it.get('summary'): it['summary'] = (text or '')  # NO truncation
        if not material_enough(it['title'], (it.get('summary','') or '') + ' ' + (text or ''), it['link']):
            continue
        wl_hits = list(set(it.get('_wl_hits', []) + watchlist_hits(f"{title} {text}", WATCHLIST)))
        it['_wl_hits'] = wl_hits
        # minimal enrichments
        canonical, site_name, published_at, byline = enrich_meta(raw_html, it['link'])
        it['canonical'] = canonical
        it['site_name'] = site_name
        it['published_at'] = published_at
        it['byline'] = byline
        it['event_type'] = classify_event(text or '')
        it['novelty_hash'] = novelty_hash(text or '')
        it['_themes'] = theme_score(text or '')
        it['_fulltext'] = text or ''
        selected.append(it)
        print(f"   [+] {domain_of(it['link'])} :: {it['title'][:80]} (fetch {time.time()-t0:.1f}s, wl_hits={len(wl_hits)}, theme={it['_themes']})")
        if len(selected) >= max_items: break

    if not selected:
        print('[info] Nothing material after full-text check.')
        return []

    # Final scoring
    def final_score(x):
        q = quality_weight(x['link'])
        rec = 1.0 / max(1, int((now_utc() - x['time']).total_seconds() // 60))
        wl_boost = 4 * len(x.get('_wl_hits', []))
        th = x.get('_themes', 0)
        return q * 12 + wl_boost + th * 2 + rec

    items = sorted(selected, key=final_score, reverse=True)[:max_items]
    print(f"[{dt.datetime.now()}] Selected {len(items)} items after final scoring")

    # Summarize
    results = []
    print('\n=== Tech News Brief (minimal) ===\n')
    for idx, it in enumerate(items, 1):
        print(f"[{dt.datetime.now()}] Summarizing {idx}/{len(items)}: {it['title'][:88]} ...")
        rev = openai_summarize(
            it['title'],
            it.get('_fulltext','') or it.get('summary',''),
            it.get('canonical') or it['link'],
            ''
        )
        if not rev:
            rev = fallback_review(it['title'], it.get('_fulltext','') or it.get('summary',''))
        block_html = to_html_block({'item': it, 'review': rev}, history)
        plain = re.sub('<[^<]+?>', '', block_html)
        print(textwrap.dedent(plain))
        print('-' * 90)
        if send and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            ok = send_telegram_html_long(block_html)
            print(f"   → Telegram sent? {ok}")
        results.append({'item': it, 'review': rev})

        # Update history with this item
        history.append({
            'domain': domain_of(it.get('canonical') or it['link']),
            'canonical': it.get('canonical') or it['link'],
            'link': it['link'],
            'title': it.get('title',''),
            'time_iso': now_utc().isoformat()
        })

    # Update state (dedupe on canonical path)
    seen_keys = set(tuple(x) if isinstance(x, (list, tuple)) else x for x in raw_seen)
    for it in items:
        canon = it.get('canonical') or it['link']
        seen_keys.add((domain_of(canon), urlparse(canon).path))

    save_state(seen_keys, history)
    return results

if __name__ == '__main__':
    print('✅ Tech News pipeline (minimal enriched) loaded')
    results = run_brief(window_min=int(os.getenv('WINDOW_MIN','1440')),
                        max_items=int(os.getenv('MAX_ITEMS','8')),
                        diversify_domains=int(os.getenv('DIVERSIFY_PER_DOMAIN','2')),
                        send=True)
    print(f"Items returned: {len(results)}") 