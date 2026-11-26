import calendar, hashlib, json, os, re, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup

MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
OLLAMA = "http://127.0.0.1:11434"
STATE_FILE = "state.json"
SITE_NAME = os.getenv("SITE_NAME") or "Newsroom"
MAX_CLUSTER_TRIES = 4      # topics tried per category per run
MAX_SOURCES = 3            # outlets whose facts are given to the AI
MAX_FAILS = 5
TIME_BUDGET = 40 * 60
TOPIC_WINDOW = 30 * 86400  # a topic is blocked from repeating for 30 days
ITEM_AGE = 72 * 3600

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"})

BBC = "http://feeds.bbci.co.uk/news/"
NYT = "https://rss.nytimes.com/services/xml/rss/nyt/"
GUA = "https://www.theguardian.com/"
SKY = "https://feeds.skynews.com/feeds/rss/"
NPR = "https://feeds.npr.org/"
CNN = "http://rss.cnn.com/rss/"
CNBC = "https://www.cnbc.com/id/"
MW = "https://feeds.content.dowjones.io/public/rss/"

# Edit freely. Dead or paywalled feeds are skipped automatically.
CATEGORIES = {
    "World News": [BBC + "world/rss.xml", "https://www.aljazeera.com/xml/rss/all.xml", SKY + "world.xml",
                   NPR + "1004/rss.xml", CNN + "edition_world.rss", NYT + "World.xml"],
    "Political": [BBC + "politics/rss.xml", GUA + "politics/rss", SKY + "politics.xml", NPR + "1014/rss.xml", NYT + "Politics.xml"],
    "Business": [BBC + "business/rss.xml", SKY + "business.xml", NPR + "1006/rss.xml", NYT + "Business.xml", GUA + "uk/business/rss"],
    "Technology": [BBC + "technology/rss.xml", "https://www.theverge.com/rss/index.xml", "https://techcrunch.com/feed/",
                   SKY + "technology.xml", CNN + "edition_technology.rss", NPR + "1019/rss.xml"],
    "Science": [BBC + "science_and_environment/rss.xml", "https://www.sciencedaily.com/rss/all.xml", NYT + "Science.xml",
                NPR + "1007/rss.xml", CNN + "edition_space.rss"],
    "Health": [BBC + "health/rss.xml", NYT + "Health.xml", GUA + "society/health/rss", NPR + "1128/rss.xml", CNN + "cnn_health.rss"],
    "Education": [BBC + "education/rss.xml", GUA + "education/rss", NYT + "Education.xml"],
    "Movie": [GUA + "film/rss", "https://variety.com/v/film/feed/", NYT + "Movies.xml", SKY + "entertainment.xml", CNN + "edition_entertainment.rss"],
    "Sports": ["http://feeds.bbci.co.uk/sport/rss.xml", "https://www.espn.com/espn/rss/news", GUA + "sport/rss",
               CNN + "edition_sport.rss", NYT + "Sports.xml"],
    "Finance": [MW + "mw_topstories", CNBC + "10000664/device/rss/rss.html", CNBC + "100003114/device/rss/rss.html", GUA + "business/rss"],
    "Economy": [NYT + "Economy.xml", GUA + "business/economics/rss", CNBC + "20910258/device/rss/rss.html", BBC + "business/rss.xml"],
    "Banking": [GUA + "business/banking/rss", CNBC + "10000664/device/rss/rss.html", MW + "mw_topstories", BBC + "business/rss.xml"],
    "Markets": [MW + "mw_marketpulse", CNBC + "15839069/device/rss/rss.html", BBC + "business/rss.xml", GUA + "business/rss"],
    "Crypto": ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/", "https://decrypt.co/feed",
               "https://cryptoslate.com/feed/", "https://bitcoinmagazine.com/.rss/full/"],
    "Environment": [GUA + "environment/rss", GUA + "environment/climate-crisis/rss", NYT + "Climate.xml", BBC + "science_and_environment/rss.xml"],
    "Travel": [NYT + "Travel.xml", GUA + "travel/rss", "https://www.cntraveler.com/feed/rss"],
    "Lifestyle": [GUA + "lifeandstyle/rss", GUA + "fashion/rss", NYT + "Style.xml", NYT + "Well.xml"],
    "Automotive": ["https://www.autoblog.com/rss.xml", "https://www.motor1.com/rss/articles/all/",
                   "https://www.caranddriver.com/rss/all.xml/", "https://www.autocar.co.uk/rss"],
    "Fact Check": ["https://www.snopes.com/feed/", "https://www.politifact.com/rss/all/", "https://www.factcheck.org/feed/"],
}

# Categories that share general business feeds only keep matching stories.
FILTERS = {
    "Banking": ["bank", "federal reserve", "the fed", "loan", "lender", "credit", "mortgage", "interest rate", "deposit",
                "jpmorgan", "citi", "hsbc", "barclays", "wells fargo", "goldman", "lloyds"],
    "Markets": ["stock", "shares", "market", "wall street", "dow", "nasdaq", "s&p", "ftse", "yield", "oil", "gold", "trading"],
    "Economy": ["economy", "economic", "inflation", "gdp", "jobs", "unemployment", "tariff", "recession", "growth", "trade", "rates"],
    "Environment": ["climate", "environment", "wildlife", "pollution", "emission", "carbon", "nature", "energy", "flood", "wildfire",
                    "heat", "species", "ocean", "forest", "green"],
}

AD_HINTS = {
    "BANNER_468X60": "468x60 banner, fits between sections on desktop",
    "BANNER_300X250": "300x250 rectangle, best mid-article",
    "BANNER_320X50": "320x50 mobile banner, fits near the top or bottom",
    "NATIVE_BANNER": "native banner, blends in between sections",
    "AD_NATIVE": "native ad, blends in between sections",
    "AD_BANNER": "generic banner",
    "AD_MULTITAG": "multi-tag ad script",
}

STOP = set("""the and for with from that this will have has had are was were been into over after about says say said new
more than what when where who how why its his her their our out you your not but can could would should may might also
just amid during under against between via off one two get gets set per vs may year years day days""".split())


class PostError(Exception):
    pass


def log(*a):
    print(*a, flush=True)


# ---------------- state ----------------
def load_state():
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
    except Exception:
        st = {}
    st.setdefault("idx", 0)
    st.setdefault("seen", [])
    st.setdefault("fails", 0)
    st.setdefault("topics", [])
    return st


def save_state(st):
    now = time.time()
    st["topics"] = [t for t in st["topics"] if now - t["t"] < 45 * 86400][-600:]
    st["seen"] = st["seen"][-4000:]
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)


# ---------------- text helpers ----------------
def clean(t):
    return re.sub(r"\s+", " ", BeautifulSoup(t or "", "lxml").get_text(" ")).strip()


def toks(s):
    return {w for w in re.findall(r"[a-z0-9]{3,}", s.lower()) if w not in STOP}


def similar(a, b, jac=0.4, ov=0.7):
    i = len(a & b)
    if i < 2:
        return False
    return i / len(a | b) >= jac or i / min(len(a), len(b)) >= ov


# ---------------- duplicate-topic guard ----------------
def is_dup(token_sets, st):
    now = time.time()
    for t in st["topics"]:
        if now - t["t"] > TOPIC_WINDOW:
            continue
        for ref in t["refs"]:
            r = set(ref)
            if any(similar(tk, r) for tk in token_sets):
                return True
    return False


def record_topic(st, cat, member_titles, gen_title):
    refs = [sorted(toks(x)) for x in member_titles + [gen_title]]
    st["topics"].append({"t": time.time(), "cat": cat, "refs": refs})


# ---------------- feeds ----------------
def feed_image(e):
    for k in ("media_content", "media_thumbnail"):
        arr = [m for m in (e.get(k) or []) if m.get("url")]
        if arr:
            return max(arr, key=lambda m: int(m.get("width") or 0))["url"]
    for l in e.get("links", []):
        if l.get("rel") == "enclosure" and "image" in (l.get("type") or ""):
            return l.get("href")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)', e.get("summary", "") or e.get("description", "") or "")
    return m.group(1) if m else None


def upgrade_img(u):
    if not u:
        return None
    u = re.sub(r"/standard/\d+/", "/standard/976/", u)
    u = re.sub(r"/news/\d+/", "/news/976/", u)
    return u if u.startswith("http") else None


def fetch_feed(url):
    try:
        r = S.get(url, timeout=12)
        r.raise_for_status()
        fp = feedparser.parse(r.content)
    except Exception as ex:
        log("feed failed:", url, ex)
        return []
    src = clean(fp.feed.get("title") or "") or urlparse(url).netloc
    out = []
    for e in fp.entries[:30]:
        link, title = e.get("link"), clean(e.get("title"))
        if not link or not title:
            continue
        ts = calendar.timegm(e.published_parsed) if e.get("published_parsed") else 0
        out.append({
            "id": hashlib.md5(link.encode()).hexdigest(),
            "title": title, "link": link, "source": src, "ts": ts,
            "summary": clean(e.get("summary") or e.get("description") or ""),
            "image": upgrade_img(feed_image(e)),
        })
    return out


def fetch_trends():
    """Google Trends 'trending now' terms. Optional: silently ignored if unavailable."""
    try:
        r = S.get("https://trends.google.com/trending/rss?geo=US", timeout=12)
        fp = feedparser.parse(r.content)
        return [t for t in (toks(clean(e.get("title"))) for e in fp.entries[:40] if e.get("title")) if t]
    except Exception:
        return []


def collect(cat):
    with ThreadPoolExecutor(max_workers=8) as ex:
        res = list(ex.map(fetch_feed, CATEGORIES[cat]))
    now = time.time()
    kw = FILTERS.get(cat)
    pool = {}
    for r in res:
        for it in r:
            if it["ts"] and now - it["ts"] > ITEM_AGE:
                continue
            if kw and not any(k in (it["title"] + " " + it["summary"]).lower() for k in kw):
                continue
            pool.setdefault(it["id"], it)
    return list(pool.values())


def cluster(items):
    items.sort(key=lambda i: i["ts"], reverse=True)
    cl = []
    for it in items:
        it["tk"] = toks(it["title"])
        for c in cl:
            if any(similar(it["tk"], m["tk"], 0.3, 0.6) for m in c):
                c.append(it)
                break
        else:
            cl.append([it])
    return cl


def score(c, trends):
    srcs = len({m["source"] for m in c})
    newest = max(m["ts"] for m in c)
    age_h = (time.time() - newest) / 3600 if newest else 72
    allt = set().union(*[m["tk"] for m in c])
    boost = min(24, sum(12 for T in trends if T <= allt))
    s = srcs * 10 + max(0, 48 - age_h) / 4 + (2 if any(m["image"] for m in c) else 0) + boost
    return s, srcs, boost


def fetch_page(url):
    try:
        r = S.get(url, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        return "", None
    og = soup.find("meta", property="og:image")
    img = upgrade_img(og.get("content")) if og and og.get("content") else None
    root = soup.find("article") or soup
    ps = [clean(p.get_text(" ")) for p in root.find_all("p")]
    return " ".join(p for p in ps if len(p) > 50)[:5000], img


def img_ok(u):
    try:
        r = S.get(u, stream=True, timeout=8)
        ok = r.status_code == 200 and r.headers.get("content-type", "").startswith("image")
        cl = r.headers.get("content-length")
        if ok and cl and int(cl) < 5000:
            ok = False
        r.close()
        return ok
    except Exception:
        return False


def pick_images(members, cl, pimgs):
    cands, seen = [], set()
    ordered = [(m["image"], m["source"]) for m in members] + [(p, m["source"]) for m, p in zip(members, pimgs)] \
        + [(m["image"], m["source"]) for m in cl]
    for u, s in ordered:
        if u and u not in seen:
            seen.add(u)
            cands.append((u, s))
    cands = cands[:8]
    with ThreadPoolExecutor(max_workers=8) as ex:
        ok = list(ex.map(lambda x: img_ok(x[0]), cands))
    return [c for c, good in zip(cands, ok) if good][:2]


# ---------------- AI ----------------
SYSTEM = (
    "You are a senior news editor and blogger at a respected English-language news website. "
    "You write clear, accurate, human-sounding blog posts. You never invent facts, numbers, quotes or names. "
    "You only use the facts provided to you."
)


def make_prompt(cat, src_block, outlets, ads):
    if ads:
        lines = "\n".join(f"  [[AD:{n}]] = {AD_HINTS[n]}" for n in ads)
        ad_txt = (
            "Optional ad slots. Use only the ones that genuinely fit, each at most once, never before the first "
            "paragraph, never two in a row. Skipping all of them is fine. To place one, put its marker alone on its own line:\n" + lines
        )
    else:
        ad_txt = "There are no ad slots. Do not write any [[AD:...]] markers."
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    return f"""Write an original, professional blog-style news post for the "{cat}" section about this topic, which is trending right now. Today is {today}.

SOURCES (several outlets covering the same story; these are the ONLY facts you may use):
{src_block}

STRUCTURE (blend all six parts into one flowing post):
1. HOOK: open with a strong question, a surprising fact from the sources, or a small mystery. 2 to 3 sentences, no heading.
2. BACKGROUND: an <h2> section that explains the context in simple words so readers see why this matters. Use only background found in the sources.
3. CORE ANALYSIS: two or three <h2> sections that present the key details, arguments and evidence step by step (figures, reports, who said what), always attributed.
4. STORYTELLING TONE: conversational and natural, like talking to a friend. Mix short punchy sentences with longer ones, address the reader as "you" sometimes. Stay factual and neutral.
5. KEY POINTS: make it scannable. Wrap important phrases in <strong>. Include <h2>Key Takeaways</h2> with a <ul> of 3 to 5 <li> items.
6. OUTRO: first <h2>What People Are Asking</h2> with 2 or 3 short <h3> questions readers would search, each followed by a <p> answer from the facts. Then a final <h2> (for example "The Bottom Line") with a brief summary and a closing open-ended question that invites readers to comment.

RULES:
- 800 to 1000 words. Rewrite everything in fresh words; never copy sentences.
- Headings must be natural and specific to the story. Never use labels like "The Hook".
- Attribute claims naturally using only these outlet names: {outlets}. Do not add any number, date, quote or name that is not in the sources.
- Banned phrases: "in conclusion", "delve", "in today's fast-paced world", "landscape", "tapestry", "it is important to note".
- Headline: under 65 characters, specific, main keyword near the start, no clickbait.
- Use only simple HTML: <p>, <h2>, <h3>, <ul>, <li>, <strong>. No <h1>, no images, no scripts, no markdown.
- {ad_txt}

OUTPUT FORMAT (exactly):
TITLE: <headline>
DESCRIPTION: <meta description, max 150 characters>
---
<post html>"""


def llm(system, user):
    for _ in range(2):
        try:
            r = requests.post(f"{OLLAMA}/api/chat", json={
                "model": MODEL, "stream": False, "keep_alive": "30m",
                "options": {"temperature": 0.65, "top_p": 0.9, "num_ctx": 8192, "num_predict": 2600},
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            }, timeout=1700)
            r.raise_for_status()
            return r.json()["message"]["content"]
        except Exception as ex:
            log("llm error:", ex)
            time.sleep(3)
    return ""


def parse_output(raw):
    raw = re.sub(r"```(?:html)?", "", raw)
    t = re.search(r"TITLE:\s*(.+)", raw)
    d = re.search(r"DESCRIPTION:\s*(.+)", raw)
    if not t or not d:
        return None
    body = re.sub(r"^\s*-{3,}\s*", "", raw[d.end():])
    title = re.sub(r"[*#\"]", "", t.group(1)).strip()[:100]
    desc = re.sub(r"[*#]", "", d.group(1)).strip()[:155]
    return title, desc, body


def sanitize(body):
    soup = BeautifulSoup(body, "lxml")
    for t in soup(["script", "style", "iframe", "h1", "form", "link", "meta", "img"]):
        t.decompose()
    for t in soup.find_all(True):
        for a in list(t.attrs):
            if a != "href":
                del t.attrs[a]
        if t.name == "a":
            t["rel"] = "nofollow noopener"
            t["target"] = "_blank"
    return soup


NUM = re.compile(r"\d[\d,]*\.?\d*")


def nn(s):
    return s.replace(",", "").rstrip(".")


def fact_check(soup, src):
    """Drop every sentence containing a number that is not present in the source facts."""
    now = datetime.now(timezone.utc)
    known = {nn(n) for n in NUM.findall(src)}
    known |= {str(now.year), str(now.year - 1), str(now.year + 1), str(now.day)}
    removed = 0
    for el in soup.find_all(["p", "li"]):
        txt = el.get_text(" ").strip()
        if "[[AD:" in txt or not txt:
            continue
        sents = re.split(r"(?<=[.!?])\s+", txt)
        keep = []
        for s in sents:
            bad = [n for n in NUM.findall(s) if len(nn(n)) > 1 and nn(n) not in known]
            if bad:
                removed += 1
            else:
                keep.append(s)
        if len(keep) != len(sents):
            if keep:
                el.string = " ".join(keep)
            else:
                el.decompose()
    log("fact-check removed sentences:", removed)


# ---------------- ads ----------------
def ads_available():
    return {n: os.getenv(n, "").strip() for n in AD_HINTS if os.getenv(n, "").strip()}


def place_ads(html_body, ads):
    used = set()

    def rep(m):
        n = m.group(1)
        if n in ads and n not in used:
            used.add(n)
            return f'<div style="text-align:center;margin:24px 0;overflow:hidden">{ads[n]}</div>'
        return ""

    html_body = re.sub(r"(?:<p>\s*)?\[\[AD:([A-Z0-9_]+)\]\](?:\s*</p>)?", rep, html_body)
    return re.sub(r"\[\[AD:[^\]]*\]\]", "", html_body)


def adsense_loader():
    v = os.getenv("ADSENSE_ID", "").strip()
    if not v:
        return ""
    if v.startswith("pub-"):
        v = "ca-" + v
    elif v.isdigit():
        v = "ca-pub-" + v
    return f'<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client={v}" crossorigin="anonymous"></script>'


# ---------------- assemble ----------------
def esc(s):
    return s.replace('"', "&quot;")


def figure(url, alt, source):
    return (f'<figure style="margin:0 0 20px"><img src="{url}" alt="{esc(alt)}" '
            f'style="width:100%;height:auto;border-radius:8px"/>'
            f'<figcaption style="font-size:12px;color:#666">Image: {esc(source)}</figcaption></figure>')


def add_inline_image(inner, fig):
    idx = [m.start() for m in re.finditer(r"<h2", inner)]
    if len(idx) >= 2:
        p = idx[min(2, len(idx) - 1)]
        return inner[:p] + fig + inner[p:]
    return inner


def assemble(title, desc, inner, imgs, members):
    hero = figure(imgs[0][0], title, imgs[0][1])
    if len(imgs) > 1:
        inner = add_inline_image(inner, figure(imgs[1][0], title, imgs[1][1]))
    links = ", ".join(f'<a href="{m["link"]}" target="_blank" rel="nofollow noopener">{m["source"]}</a>' for m in members)
    now = datetime.now(timezone.utc).isoformat()
    ld = {
        "@context": "https://schema.org", "@type": "NewsArticle", "headline": title, "description": desc,
        "image": [i[0] for i in imgs], "datePublished": now, "dateModified": now,
        "author": {"@type": "Organization", "name": SITE_NAME},
        "publisher": {"@type": "Organization", "name": SITE_NAME},
    }
    ld_html = '<script type="application/ld+json">' + json.dumps(ld).replace("</", "<\\/") + "</script>"
    return (f'<div class="news-post">{adsense_loader()}{hero}{inner}'
            f'<p style="font-size:13px;color:#555"><em>Sources: {links}</em></p>{ld_html}</div>')


# ---------------- Blogger ----------------
def access_token():
    r = S.post("https://oauth2.googleapis.com/token", data={
        "client_id": os.environ["CLIENT_ID"], "client_secret": os.environ["CLIENT_SECRET"],
        "refresh_token": os.environ["REFRESH_TOKEN"], "grant_type": "refresh_token",
    }, timeout=30)
    if r.status_code != 200:
        raise PostError(f"token error {r.status_code}: {r.text[:200]}")
    return r.json()["access_token"]


def blogger_post(title, content, label):
    r = S.post(
        f"https://www.googleapis.com/blogger/v3/blogs/{os.environ['BLOG_ID']}/posts/",
        headers={"Authorization": f"Bearer {access_token()}"},
        json={"kind": "blogger#post", "title": title, "content": content, "labels": [label]},
        timeout=60,
    )
    if r.status_code >= 300:
        raise PostError(f"blogger error {r.status_code}: {r.text[:300]}")
    return r.json().get("url")


# ---------------- pipeline ----------------
def build_post(cat, cl, st):
    # pick up to MAX_SOURCES different outlets, newest first
    members, used = [], set()
    for m in sorted(cl, key=lambda m: m["ts"], reverse=True):
        if m["source"] in used:
            continue
        used.add(m["source"])
        members.append(m)
        if len(members) == MAX_SOURCES:
            break

    with ThreadPoolExecutor(max_workers=4) as ex:
        pages = list(ex.map(lambda m: fetch_page(m["link"]), members))

    # an image is mandatory: check this first so no AI time is wasted
    imgs = pick_images(members, cl, [p[1] for p in pages])
    if not imgs:
        log("no valid image found, skipping topic")
        return None

    blocks, total = [], 0
    for n, (m, (text, _)) in enumerate(zip(members, pages), 1):
        facts = (text if len(text) > len(m["summary"]) else m["summary"])[:1800]
        total += len(facts)
        blocks.append(f"SOURCE {n} - {m['source']}\nHeadline: {m['title']}\nSummary: {m['summary'][:400]}\nDetails: {facts}")
    if total < 350:
        log("too little source text, skipping topic")
        return None

    src = " ".join([m["title"] + " " + m["summary"] for m in cl] + [p[0] for p in pages])
    outlets = ", ".join(m["source"] for m in members)
    ads = ads_available()
    parsed = parse_output(llm(SYSTEM, make_prompt(cat, "\n\n".join(blocks), outlets, ads)))
    if not parsed:
        log("bad AI output")
        return None
    title, desc, body = parsed

    if is_dup([toks(title)], st):
        log("generated title duplicates an earlier post, skipping:", title)
        return None

    soup = sanitize(body)
    fact_check(soup, src)
    if len(soup.get_text().split()) < 300:
        log("article too short after checks")
        return None
    inner = soup.body.decode_contents() if soup.body else str(soup)
    inner = place_ads(inner, ads)
    return title, assemble(title, desc, inner, imgs, members)


def process(cat, st, trends):
    seen = set(st["seen"])
    items = [i for i in collect(cat) if i["id"] not in seen]
    if not items:
        return False
    ranked = []
    for c in cluster(items):
        s, srcs, boost = score(c, trends)
        ranked.append((s, srcs, boost, c))
    ranked.sort(key=lambda x: x[0], reverse=True)

    tried = 0
    for s, srcs, boost, cl in ranked:
        if tried >= MAX_CLUSTER_TRIES:
            break
        titles = [m["title"] for m in cl]
        if is_dup([m["tk"] for m in cl], st):
            log(f"[{cat}] already covered: {titles[0]}")
            continue
        tried += 1
        log(f"[{cat}] topic (score {s:.1f}, outlets {srcs}, trend boost {boost}): {titles[0]}")
        res = build_post(cat, cl, st)
        ids = [m["id"] for m in cl]
        if not res:
            st["seen"].extend(ids)
            continue
        url = blogger_post(res[0], res[1], cat)
        st["seen"].extend(ids)
        record_topic(st, cat, titles[:6], res[0])
        log("POSTED:", url)
        return True
    return False


def main():
    start = time.time()
    st = load_state()
    cats = list(CATEGORIES)
    trends = fetch_trends()
    log("trending terms loaded:", len(trends))
    posted = False
    for _ in range(len(cats)):
        if time.time() - start > TIME_BUDGET:
            break
        cat = cats[st["idx"] % len(cats)]
        try:
            posted = process(cat, st, trends)
        except PostError as ex:
            log("BLOGGER ERROR:", ex)
            break
        except Exception as ex:
            log("ERROR:", cat, repr(ex))
        st["idx"] = (st["idx"] + 1) % len(cats)
        if posted:
            break
    st["fails"] = 0 if posted else st["fails"] + 1
    save_state(st)
    nxt = st["fails"] < MAX_FAILS
    log("next run:", nxt)
    out = os.getenv("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"next={'true' if nxt else 'false'}\n")


if __name__ == "__main__":
    main()
