import calendar, hashlib, html as htmlmod, json, os, re, sys, time, warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
OLLAMA = "http://127.0.0.1:11434"
STATE_FILE = "state.json"
SITE_NAME = os.getenv("SITE_NAME") or "Newsroom"
MAX_CLUSTER_TRIES = 4      # topics tried per category per run
MAX_SOURCES = 5            # outlets whose facts are used
MAX_FAILS = 5
TIME_BUDGET = 45 * 60
TOPIC_WINDOW = 30 * 86400  # a topic is blocked from repeating for 30 days
ITEM_AGE = 72 * 3600
MIN_WORDS = 350
BLOGGER_SCOPE = "https://www.googleapis.com/auth/blogger"

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
WP = "https://feeds.washingtonpost.com/rss/"

# Edit freely. Dead or paywalled feeds are skipped automatically.
CATEGORIES = {
    "World News": [BBC + "world/rss.xml", "https://www.aljazeera.com/xml/rss/all.xml", SKY + "world.xml", NPR + "1004/rss.xml",
                   CNN + "edition_world.rss", NYT + "World.xml", GUA + "world/rss", "https://rss.dw.com/rdf/rss-en-world",
                   "https://www.france24.com/en/rss", WP + "world", "https://abcnews.go.com/abcnews/internationalheadlines",
                   "https://www.cbsnews.com/latest/rss/world"],
    "Political": [BBC + "politics/rss.xml", GUA + "politics/rss", SKY + "politics.xml", NPR + "1014/rss.xml", NYT + "Politics.xml",
                  WP + "politics", "https://www.cbsnews.com/latest/rss/politics", "https://abcnews.go.com/abcnews/politicsheadlines",
                  "https://thehill.com/feed/"],
    "Business": [BBC + "business/rss.xml", SKY + "business.xml", NPR + "1006/rss.xml", NYT + "Business.xml", GUA + "uk/business/rss",
                 CNBC + "100003114/device/rss/rss.html", "https://fortune.com/feed/", "https://www.forbes.com/business/feed/",
                 WP + "business"],
    "Technology": [BBC + "technology/rss.xml", "https://www.theverge.com/rss/index.xml", "https://techcrunch.com/feed/",
                   SKY + "technology.xml", CNN + "edition_technology.rss", NPR + "1019/rss.xml", "https://www.wired.com/feed/rss",
                   "https://feeds.arstechnica.com/arstechnica/index", "https://www.engadget.com/rss.xml", GUA + "technology/rss"],
    "Science": [BBC + "science_and_environment/rss.xml", "https://www.sciencedaily.com/rss/all.xml", NYT + "Science.xml",
                NPR + "1007/rss.xml", CNN + "edition_space.rss", "https://www.space.com/feeds/all", "https://phys.org/rss-feed/",
                "https://www.livescience.com/feeds/all", GUA + "science/rss"],
    "Health": [BBC + "health/rss.xml", NYT + "Health.xml", GUA + "society/health/rss", NPR + "1128/rss.xml", CNN + "cnn_health.rss",
               "https://www.statnews.com/feed/", "https://www.who.int/rss-feeds/news-english.xml",
               "https://www.sciencedaily.com/rss/health_medicine.xml"],
    "Education": [BBC + "education/rss.xml", GUA + "education/rss", NYT + "Education.xml", "https://www.insidehighered.com/rss.xml",
                  "https://www.timeshighereducation.com/rss.xml"],
    "Movie": [GUA + "film/rss", "https://variety.com/v/film/feed/", NYT + "Movies.xml", SKY + "entertainment.xml",
              CNN + "edition_entertainment.rss", "https://www.hollywoodreporter.com/c/movies/feed/", "https://deadline.com/feed/",
              "https://www.indiewire.com/feed/"],
    "Sports": ["http://feeds.bbci.co.uk/sport/rss.xml", "https://www.espn.com/espn/rss/news", GUA + "sport/rss",
               CNN + "edition_sport.rss", NYT + "Sports.xml", "https://www.skysports.com/rss/12040",
               "https://www.cbssports.com/rss/headlines/", "https://sports.yahoo.com/rss/"],
    "Finance": [MW + "mw_topstories", CNBC + "10000664/device/rss/rss.html", CNBC + "100003114/device/rss/rss.html",
                GUA + "business/rss", "https://finance.yahoo.com/news/rssindex", "https://www.investing.com/rss/news.rss"],
    "Economy": [NYT + "Economy.xml", GUA + "business/economics/rss", CNBC + "20910258/device/rss/rss.html", BBC + "business/rss.xml",
                MW + "mw_topstories", "https://finance.yahoo.com/news/rssindex"],
    "Banking": [GUA + "business/banking/rss", CNBC + "10000664/device/rss/rss.html", MW + "mw_topstories", BBC + "business/rss.xml",
                "https://finance.yahoo.com/news/rssindex"],
    "Markets": [MW + "mw_marketpulse", CNBC + "15839069/device/rss/rss.html", BBC + "business/rss.xml", GUA + "business/rss",
                "https://finance.yahoo.com/news/rssindex", "https://www.investing.com/rss/news.rss"],
    "Crypto": ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/", "https://decrypt.co/feed",
               "https://cryptoslate.com/feed/", "https://bitcoinmagazine.com/.rss/full/", "https://bitcoinist.com/feed/",
               "https://www.newsbtc.com/feed/"],
    "Environment": [GUA + "environment/rss", GUA + "environment/climate-crisis/rss", NYT + "Climate.xml",
                    BBC + "science_and_environment/rss.xml", "https://www.carbonbrief.org/feed", "https://news.mongabay.com/feed/",
                    "https://insideclimatenews.org/feed/"],
    "Travel": [NYT + "Travel.xml", GUA + "travel/rss", "https://www.cntraveler.com/feed/rss", "https://skift.com/feed/"],
    "Lifestyle": [GUA + "lifeandstyle/rss", GUA + "fashion/rss", NYT + "Style.xml", NYT + "Well.xml", "https://lifehacker.com/feed/rss"],
    "Automotive": ["https://www.autoblog.com/rss.xml", "https://www.motor1.com/rss/articles/all/",
                   "https://www.caranddriver.com/rss/all.xml/", "https://www.autocar.co.uk/rss", "https://electrek.co/feed/",
                   "https://insideevs.com/rss/articles/all/", "https://jalopnik.com/rss"],
    "Fact Check": ["https://www.snopes.com/feed/", "https://www.politifact.com/rss/all/", "https://www.factcheck.org/feed/",
                   "https://leadstories.com/atom.xml", "https://fullfact.org/feed/all/"],
}

AGE_OVERRIDE = {"Fact Check": 7 * 86400, "Travel": 7 * 86400, "Lifestyle": 5 * 86400, "Education": 5 * 86400,
                "Automotive": 5 * 86400, "Science": 5 * 86400, "Health": 5 * 86400}

# Categories that share general business feeds only keep matching stories.
FILTERS = {
    "Banking": ["bank", "federal reserve", "the fed", "loan", "lender", "credit", "mortgage", "interest rate", "deposit",
                "jpmorgan", "citi", "hsbc", "barclays", "wells fargo", "goldman", "lloyds"],
    "Markets": ["stock", "shares", "market", "wall street", "dow", "nasdaq", "s&p", "ftse", "yield", "oil", "gold", "trading"],
    "Economy": ["economy", "economic", "inflation", "gdp", "jobs", "unemployment", "tariff", "recession", "growth", "trade", "rates"],
    "Environment": ["climate", "environment", "wildlife", "pollution", "emission", "carbon", "nature", "energy", "flood", "wildfire",
                    "heat", "species", "ocean", "forest", "green"],
}

# Google News RSS (used to detect what is trending right now)
GN_TOPIC = {"World News": "WORLD", "Business": "BUSINESS", "Technology": "TECHNOLOGY", "Science": "SCIENCE",
            "Health": "HEALTH", "Sports": "SPORTS", "Movie": "ENTERTAINMENT"}
GN_QUERY = {"Political": "politics", "Education": "education", "Finance": "finance", "Economy": "economy",
            "Banking": "banking", "Markets": "stock market", "Crypto": "cryptocurrency",
            "Environment": "climate change environment", "Travel": "travel", "Lifestyle": "lifestyle",
            "Automotive": "cars automotive", "Fact Check": "fact check"}

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

HARD_BANNED = ["in conclusion", "delve", "tapestry", "it's important to note", "it is important to note",
               "in today's fast-paced", "game-changer", "game changer", "shed light", "navigate the", "rich tapestry",
               "in the ever-evolving", "ever-changing landscape", "stay tuned"]

FORMS = {
    "prose": "Write 2 or 3 short paragraphs (120 to 190 words in total), separated by a blank line.",
    "list": "Write one short lead-in sentence, then 3 to 5 bullet lines. Every bullet starts with '- ' and carries one specific fact.",
    "timeline": ("Write 3 to 6 lines, each formatted 'WHEN: what happened', in time order. Use only dates or times that appear in "
                 "the facts. If the facts contain no dates or times, write two short paragraphs instead."),
    "qa": "Write 2 or 3 questions a real reader would ask, each as 'Q: ...' on its own line followed by 'A: ...' (2 to 3 sentences) on the next line.",
}

REPORT = []


class PostError(Exception):
    pass


def log(*a):
    print(*a, flush=True)


def note(msg):
    REPORT.append(msg)
    log(msg)


def write_summary():
    p = os.getenv("GITHUB_STEP_SUMMARY")
    if p:
        try:
            with open(p, "a") as f:
                f.write("## News bot report\n" + "\n".join(f"- {r}" for r in REPORT) + "\n")
        except Exception:
            pass


def write_output(nxt):
    out = os.getenv("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"next={'true' if nxt else 'false'}\n")


def fatal(msg):
    note("FATAL: " + msg)
    write_summary()
    write_output(False)
    sys.exit(1)


def blog_id():
    return os.environ["BLOG_ID"].strip()


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
    st.setdefault("formats", [])
    st.setdefault("voices", [])
    st.setdefault("write_ok", False)
    return st


def save_state(st):
    now = time.time()
    st["topics"] = [t for t in st["topics"] if now - t["t"] < 45 * 86400][-600:]
    st["seen"] = st["seen"][-4000:]
    st["formats"] = st["formats"][-12:]
    st["voices"] = st["voices"][-12:]
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


SENT_SPLIT = re.compile(r'(?<=[.!?])\s+(?=[A-Z"\u201c])')


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


def record_topic(st, cat, member_titles, gen_title, url):
    refs = [sorted(toks(x)) for x in member_titles + [gen_title]]
    st["topics"].append({"t": time.time(), "cat": cat, "refs": refs, "title": gen_title, "url": url})


def related_block(st, cat, tk):
    """Internal links to earlier posts (SEO + reader retention)."""
    cands = [t for t in reversed(st["topics"]) if t.get("url") and t.get("title")]
    pick = [t for t in cands if t.get("cat") == cat][:3]
    if len(pick) < 3:
        for t in cands:
            if t not in pick and toks(t["title"]) & tk:
                pick.append(t)
            if len(pick) == 3:
                break
    if not pick:
        return ""
    items = "".join(f'<li><a href="{t["url"]}">{htmlmod.escape(t["title"])}</a></li>' for t in pick)
    return f"<h2>Related Reading</h2><ul>{items}</ul>"


# ---------------- Blogger ----------------
def token_info():
    r = S.post("https://oauth2.googleapis.com/token", data={
        "client_id": os.environ["CLIENT_ID"].strip(), "client_secret": os.environ["CLIENT_SECRET"].strip(),
        "refresh_token": os.environ["REFRESH_TOKEN"].strip(), "grant_type": "refresh_token",
    }, timeout=30)
    if r.status_code != 200:
        raise PostError(f"token error {r.status_code}: {r.text[:300]}")
    return r.json()


def access_token():
    return token_info()["access_token"]


def hint_for(text):
    t = text.lower()
    if "invalid_grant" in t:
        return ("Refresh token is expired or revoked. If your OAuth consent screen is in 'Testing' mode, refresh tokens "
                "expire after 7 days: set the app to 'In production' and create a new refresh token.")
    if "invalid_client" in t:
        return "CLIENT_ID or CLIENT_SECRET is wrong (or not from the same OAuth client that created the refresh token)."
    if "permission" in t or "forbidden" in t or "insufficient" in t or "scope" in t:
        return ("The token can read the blog but cannot write. Most likely the Google account that created the refresh token is "
                "not an Admin/Author of this blog. Create the refresh token while signed in to the Google account that owns the blog "
                f"(scope {BLOGGER_SCOPE}), or invite that account as Admin in Blogger -> Settings -> Permissions, then update REFRESH_TOKEN.")
    if "accessnotconfigured" in t or "has not been used" in t or "disabled" in t:
        return "Blogger API v3 is not enabled. Google Cloud Console -> APIs & Services -> Library -> enable 'Blogger API v3'."
    if "404" in t or "notfound" in t:
        return "BLOG_ID is wrong, or this Google account has no access to that blog."
    return "Check the secrets CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN and BLOG_ID."


def blogger_post(title, content, labels):
    r = S.post(
        f"https://www.googleapis.com/blogger/v3/blogs/{blog_id()}/posts/",
        headers={"Authorization": f"Bearer {access_token()}"},
        json={"kind": "blogger#post", "title": title, "content": content, "labels": labels},
        timeout=60,
    )
    if r.status_code >= 300:
        raise PostError(f"blogger error {r.status_code}: {r.text[:400]}")
    return r.json().get("url")


def write_test(tok):
    """Create a throw-away draft and delete it, to prove the token can write to this blog."""
    base = f"https://www.googleapis.com/blogger/v3/blogs/{blog_id()}/posts"
    h = {"Authorization": f"Bearer {tok}"}
    r = S.post(base + "/?isDraft=true", headers=h,
               json={"kind": "blogger#post", "title": "Connection test (auto-deleted)", "content": "test"}, timeout=30)
    if r.status_code >= 300:
        msg = f"Blogger write test failed {r.status_code}: {r.text[:300]}"
        fatal(f"{msg} | HOW TO FIX: {hint_for(msg)}")
    pid = r.json().get("id")
    if pid:
        try:
            S.delete(f"{base}/{pid}", headers=h, timeout=30)
        except Exception:
            pass
    note("Blogger WRITE permission OK")


def preflight(st):
    # 1) local AI
    try:
        r = S.get(f"{OLLAMA}/api/tags", timeout=10)
        names = [m.get("name", "") for m in r.json().get("models", [])]
        if not any(MODEL.split(":")[0] in n for n in names):
            fatal(f"AI model '{MODEL}' is not installed in Ollama. Installed: {names}")
        log("Ollama OK:", names)
    except SystemExit:
        raise
    except Exception as ex:
        fatal(f"Local AI (Ollama) is not reachable: {ex}")
    # 2) Blogger credentials, scope, blog id, account access, write permission
    try:
        info = token_info()
    except PostError as ex:
        fatal(f"{ex} | HOW TO FIX: {hint_for(str(ex))}")
    tok = info["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    scopes = info.get("scope", "").split()
    if scopes and BLOGGER_SCOPE not in scopes:
        fatal(f"Refresh token scopes are {scopes}, but writing posts needs {BLOGGER_SCOPE}. "
              "HOW TO FIX: create a new refresh token with that scope (not .readonly) and update the REFRESH_TOKEN secret.")
    r = S.get(f"https://www.googleapis.com/blogger/v3/blogs/{blog_id()}", headers=h, timeout=30)
    if r.status_code != 200:
        msg = f"Blogger blog lookup failed {r.status_code}: {r.text[:300]}"
        fatal(f"{msg} | HOW TO FIX: {hint_for(msg)}")
    b = r.json()
    note(f"Blogger connection OK: '{b.get('name')}' ({b.get('url')}), existing posts: {b.get('posts', {}).get('totalItems')}")
    try:
        ub = S.get("https://www.googleapis.com/blogger/v3/users/self/blogs",
                   params=[("role", "ADMIN"), ("role", "AUTHOR")], headers=h, timeout=30)
        if ub.status_code == 200:
            items = ub.json().get("items", [])
            ids = [str(i.get("id")) for i in items]
            listing = ", ".join(f"{i.get('name')} (id {i.get('id')})" for i in items) or "none"
            if blog_id() not in ids:
                fatal(f"The Google account that created your REFRESH_TOKEN is NOT an Admin/Author of blog id {blog_id()} "
                      f"('{b.get('name')}'). It can only write to: {listing}. "
                      "HOW TO FIX: create the refresh token while signed in to the Google account that owns this blog, "
                      "then update the REFRESH_TOKEN secret.")
    except SystemExit:
        raise
    except Exception as ex:
        log("could not list blogs:", ex)
    if not st.get("write_ok"):
        write_test(tok)
        st["write_ok"] = True


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
        raw_img = feed_image(e)
        out.append({
            "id": hashlib.md5(link.encode()).hexdigest(),
            "title": title, "link": link, "source": src, "ts": ts,
            "summary": clean(e.get("summary") or e.get("description") or ""),
            "image": upgrade_img(raw_img),
            "image_raw": raw_img if raw_img and raw_img.startswith("http") else None,
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


def gn_url(cat):
    q = "hl=en-US&gl=US&ceid=US:en"
    if cat in GN_TOPIC:
        return f"https://news.google.com/rss/headlines/section/topic/{GN_TOPIC[cat]}?{q}"
    return f"https://news.google.com/rss/search?q={quote_plus(GN_QUERY.get(cat, cat) + ' when:1d')}&{q}"


def gn_signals(cat):
    """Top Google News stories for this category: a trend signal + headlines from other outlets."""
    try:
        r = S.get(gn_url(cat), timeout=12)
        fp = feedparser.parse(r.content)
    except Exception:
        return []
    out = []
    for idx, e in enumerate(fp.entries[:25]):
        head = clean(e.get("title")).rsplit(" - ", 1)[0]
        tk = toks(head)
        if not tk:
            continue
        soup = BeautifulSoup(e.get("summary") or "", "lxml")
        rel = []
        for li in soup.find_all("li"):
            a, f = li.find("a"), li.find("font")
            if a:
                rel.append((clean(a.get_text()), clean(f.get_text()) if f else ""))
        out.append({"tk": tk, "w": min(12, len(rel)) + max(0, 10 - idx * 0.5), "rel": rel[:8], "head": head})
    log(f"[{cat}] Google News signals: {len(out)}")
    return out


def collect(cat):
    with ThreadPoolExecutor(max_workers=12) as ex:
        res = list(ex.map(fetch_feed, CATEGORIES[cat]))
    now = time.time()
    age = AGE_OVERRIDE.get(cat, ITEM_AGE)
    kw = FILTERS.get(cat)
    pool = {}
    for r in res:
        for it in r:
            if it["ts"] and now - it["ts"] > age:
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


def score(c, trends, gn):
    srcs = len({m["source"] for m in c})
    newest = max(m["ts"] for m in c)
    age_h = (time.time() - newest) / 3600 if newest else 72
    allt = set().union(*[m["tk"] for m in c])
    boost = min(24, sum(12 for T in trends if T <= allt))
    best = None
    for g in gn:
        if any(similar(m["tk"], g["tk"], 0.25, 0.5) for m in c):
            if best is None or g["w"] > best["w"]:
                best = g
    if best:
        boost += min(30, 12 + best["w"])
    s = srcs * 10 + max(0, 48 - age_h) / 4 + (2 if any(m["image"] for m in c) else 0) + boost
    return s, srcs, boost, best


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
    return " ".join(p for p in ps if len(p) > 50)[:7000], img


def img_ok(u):
    try:
        r = S.get(u, stream=True, timeout=8)
        ct = r.headers.get("content-type", "").lower()
        ok = r.status_code == 200 and (ct.startswith("image") or re.search(r"\.(jpe?g|png|webp)(\?|$)", u.lower()) is not None)
        cl = r.headers.get("content-length")
        if ok and cl and int(cl) < 5000:
            ok = False
        r.close()
        return ok
    except Exception:
        return False


def pick_image(infos, cl):
    """Return (url, outlet) of the first working image, or None. Every post needs exactly one."""
    ordered = []
    for m, _, pimg in infos:
        ordered += [(m["image"], m["source"]), (m.get("image_raw"), m["source"]), (pimg, m["source"])]
    for m in cl:
        ordered += [(m["image"], m["source"]), (m.get("image_raw"), m["source"])]
    cands, seen = [], set()
    for u, s in ordered:
        if u and u not in seen:
            seen.add(u)
            cands.append((u, s))
    cands = cands[:10]
    with ThreadPoolExecutor(max_workers=8) as ex:
        ok = list(ex.map(lambda x: img_ok(x[0]), cands))
    for c, good in zip(cands, ok):
        if good:
            return c
    return None


# ---------------- fact cards (specific details pulled from all sources) ----------------
BOILER = re.compile(r"subscribe|sign up|newsletter|cookie|click here|follow us|all rights reserved|advertisement|read more|"
                    r"copyright|getty images|photograph:|\bwatch:|listen to|download the|privacy policy|terms of use", re.I)
DATEW = re.compile(r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|January|February|March|April|May|June|July|"
                   r"August|September|October|November|December|20\d\d)\b")
NUM = re.compile(r"\d[\d,]*\.?\d*")
QUOTE = re.compile(r'["\u201c]([^"\u201d]{25,})["\u201d]')


def card_score(s):
    words = s.split()
    nums = len(NUM.findall(s))
    caps = sum(1 for w in words[1:] if re.match(r"^[A-Z][a-z]{2,}", w))
    quote = 1 if QUOTE.search(s) else 0
    datew = 1 if DATEW.search(s) else 0
    return 2 * min(nums, 3) + 1.2 * min(caps, 6) + 2 * quote + 1.5 * datew + min(len(s), 260) / 130


def extract_cards(infos, limit=26):
    raw = []
    for m, facts, _ in infos:
        texts = [facts] + ([m["summary"]] if m["summary"] and m["summary"] not in facts else [])
        for text in texts:
            for s in SENT_SPLIT.split(text):
                s = s.strip()
                if not (45 <= len(s) <= 340) or BOILER.search(s):
                    continue
                raw.append({"text": s, "outlet": m["source"], "tk": toks(s), "sc": card_score(s)})
    for c in raw:
        outs = {d["outlet"] for d in raw if d["outlet"] != c["outlet"] and similar(c["tk"], d["tk"], 0.3, 0.5)}
        c["co"] = len(outs)
        c["sc"] += 3 * len(outs)
    raw.sort(key=lambda c: c["sc"], reverse=True)
    chosen, per = [], {}
    for c in raw:
        if len(chosen) >= limit:
            break
        if per.get(c["outlet"], 0) >= 10:
            continue
        if any(similar(c["tk"], d["tk"], 0.5, 0.7) for d in chosen):
            continue
        chosen.append(c)
        per[c["outlet"]] = per.get(c["outlet"], 0) + 1
    for i, c in enumerate(chosen, 1):
        c["id"] = f"C{i}"
    return chosen


def card_line(c):
    tag = c["outlet"] + (f"; also confirmed by {c['co']} other outlet{'s' if c['co'] > 1 else ''}" if c["co"] else "; single outlet")
    return f"{c['id']} [{tag}] {c['text']}"


# ---------------- AI ----------------
PLAN_SYS = (
    "You are the managing editor of a news explainer website. You plan posts that help curious readers truly understand "
    "a story, and you make every post feel different from the last."
)

WRITER_SYS = (
    "You are a staff writer at a news explainer website. You write like a smart, warm human journalist: concrete, specific, "
    "never padded. You only use the facts you are given and you never invent numbers, names, dates or quotes."
)


def llm(system, user, num_predict=600, temp=0.7):
    for attempt in range(2):
        try:
            r = requests.post(f"{OLLAMA}/api/chat", json={
                "model": MODEL, "stream": False, "keep_alive": "30m",
                "options": {"temperature": temp, "top_p": 0.92, "repeat_penalty": 1.1, "num_ctx": 8192,
                            "num_predict": num_predict},
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            }, timeout=1500)
            r.raise_for_status()
            return r.json()["message"]["content"]
        except requests.exceptions.ReadTimeout:
            note("AI took too long (timeout). Use a smaller model or fewer tokens.")
            return ""
        except Exception as ex:
            log("llm error:", ex)
            time.sleep(3)
    return ""


def hdr(name, raw):
    m = re.search(rf"^[ \t]*\**{name}\**[ \t]*:[ \t]*\**[ \t]*(.+?)[ \t]*\**[ \t]*$", raw, re.I | re.M)
    return m.group(1).strip() if m else ""


def plan_prompt(cat, cards, outlets, gn_lines, recent_f, recent_v, ads):
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    if ads:
        lines = "\n".join(f"  {n} = {AD_HINTS[n]}" for n in ads)
        ad_txt = ("Ad slots are available:\n" + lines + "\nDecide yourself whether any should be used and where. Place an ad with a line "
                  "'AD: <NAME> | <number of the section it goes after, 0 = right after the opening>'. At most one ad per position. "
                  "Skipping ads is fine if they would hurt reading.")
    else:
        ad_txt = "There are no ad slots; write no AD lines."
    card_lines = "\n".join(card_line(c) for c in cards)
    return f"""You are planning a news EXPLAINER post for the "{cat}" section. Today is {today}. The story is trending and is covered by: {outlets}.

FACT CARDS (the only facts available; ids are C1, C2, ...):
{card_lines}

OTHER HEADLINES ON THIS STORY:
{gn_lines}

Plan a post that helps a curious reader understand what is going on, why, what it means, and what is still unknown. Decide everything yourself:
- Pick the angle that makes THIS story interesting. Do not fall back on the usual "background, analysis, outlook" pattern.
- Structures used recently on this site (choose something different): {recent_f}
- Narrator voices used recently (choose something different): {recent_v}
- 3 to 5 sections. Every heading must be concrete and specific to this story (never "Background", "Analysis", "Overview", "Conclusion" or "Key Takeaways"). For each section pick the form that serves it best: prose, list, timeline (only if the cards contain dates or times) or qa. Use prose unless another form clearly helps.
- Give every fact card to the ONE section where it fits best so nothing is repeated. Each section should get 3 to 6 cards, the opening 1 to 3. Prefer cards confirmed by several outlets and be careful with single-outlet claims.
- {ad_txt}

Reply in exactly this format and nothing else:
ANGLE: <one sentence: the specific question or tension this post resolves>
FOCUS: <focus keyword phrase, 2 to 4 words>
FORMAT: <3 to 6 word name for the structure you chose>
VOICE: <6 to 10 words describing the narrator's tone for this story>
LEAD: <the concrete detail or contrast to open with> | <fact ids, e.g. C2, C5>
SECTION: <specific heading> | <prose, list, timeline or qa> | <what this part must make the reader understand> | <fact ids>
SECTION: ...
DISAGREEMENTS: <where outlets differ or what is still unknown, or none>
ANALOGY: <one everyday comparison that makes the core idea click, with no numbers or factual claims, or none>
AD: <NAME> | <number>   (optional, only if useful)"""


def parse_plan(raw, cards, ads):
    ids = {c["id"] for c in cards}

    def pick_ids(s):
        out = []
        for n in re.findall(r"C(\d+)", s):
            cid = f"C{n}"
            if cid in ids and cid not in out:
                out.append(cid)
        return out

    def low(v):
        return "" if not v or v.lower().strip(" .") in ("none", "n/a", "no") else v

    plan = {"angle": hdr("ANGLE", raw), "focus": re.sub(r"[*\"]", "", hdr("FOCUS", raw)),
            "fmt": re.sub(r"[*#\"]", "", hdr("FORMAT", raw))[:60] or "explainer",
            "voice": re.sub(r"[*#\"]", "", hdr("VOICE", raw))[:80] or "clear and friendly",
            "gaps": low(hdr("DISAGREEMENTS", raw)), "analogy": low(hdr("ANALOGY", raw)),
            "lead": None, "sections": [], "ads": {}}
    m = re.search(r"^[ \t]*\**LEAD\**[ \t]*:[ \t]*(.+)$", raw, re.I | re.M)
    if m:
        plan["lead"] = {"idea": m.group(1).split("|")[0].strip(" *"), "ids": pick_ids(m.group(1))}
    for line in re.findall(r"^[ \t]*\**SECTION\**[ \t]*:[ \t]*(.+)$", raw, re.I | re.M):
        parts = [x.strip(" *") for x in line.split("|")]
        if len(parts) < 3:
            continue
        head = re.sub(r"^\d+[.)]\s*", "", parts[0]).strip(' "*')
        form = next((f for f in FORMS if f in parts[1].lower()), "prose")
        if len(head) < 4:
            continue
        plan["sections"].append({"heading": head[:90], "form": form, "purpose": parts[2], "ids": pick_ids(line)})
    for line in re.findall(r"^[ \t]*\**AD\**[ \t]*:[ \t]*(.+)$", raw, re.I | re.M):
        parts = [x.strip(" *") for x in line.split("|")]
        if len(parts) >= 2 and parts[0] in ads:
            n = re.search(r"\d+", parts[1])
            if n:
                plan["ads"][parts[0]] = int(n.group())
    if len(plan["sections"]) < 3:
        return None
    plan["sections"] = plan["sections"][:5]
    return plan


def fallback_plan(cards):
    return {"angle": "", "focus": "", "fmt": "straight explainer", "voice": "clear and friendly", "gaps": "", "analogy": "",
            "lead": {"idea": "the single most striking specific detail", "ids": []}, "ads": {},
            "sections": [
                {"heading": "What we know so far", "form": "prose", "purpose": "the confirmed facts of the story", "ids": []},
                {"heading": "How it got here", "form": "prose", "purpose": "the context that explains why this happened", "ids": []},
                {"heading": "What happens next", "form": "prose", "purpose": "what is still unknown and what to watch", "ids": []}]}


def assign_cards(plan, cards):
    byid = {c["id"]: c for c in cards}
    used = set()
    if not plan["lead"]:
        plan["lead"] = {"idea": "the single most striking specific detail", "ids": []}
    parts = [plan["lead"]] + plan["sections"]
    for p in parts:
        ids = [i for i in p["ids"] if i not in used]
        used.update(ids)
        p["cards"] = [byid[i] for i in ids]
    leftover = [c for c in cards if c["id"] not in used]
    plan["lead"]["cards"] = (plan["lead"]["cards"] + leftover[:2])[:3] if not plan["lead"]["cards"] else plan["lead"]["cards"]
    leftover = [c for c in leftover if c not in plan["lead"]["cards"]]
    for sec in plan["sections"]:
        while len(sec["cards"]) < 3 and leftover:
            sec["cards"].append(leftover.pop(0))
    i = 0
    while leftover and plan["sections"]:
        sec = plan["sections"][i % len(plan["sections"])]
        if len(sec["cards"]) < 6:
            sec["cards"].append(leftover.pop(0))
        i += 1
        if i > 60:
            break


def part_prompt(plan, part, is_lead, covered, prev_last, first_section):
    facts = "\n".join(card_line(c) for c in part["cards"]) or "(no specific facts assigned; keep it short and general)"
    done = "\n".join(f"- {x}" for x in covered) or "- nothing yet"
    gaps = f"\nKnown disagreements or unknowns across the sources (mention only if it belongs in this part): {plan['gaps']}" if plan["gaps"] else ""
    analogy = (f"\nIf it truly fits, use this everyday comparison once, in your own words: {plan['analogy']}"
               if plan["analogy"] and first_section else "")
    focus = f'\nWork the phrase "{plan["focus"]}" in naturally.' if plan["focus"] and is_lead else ""
    rules = f"""RULES:
- Use at least {min(3, max(1, len(part['cards'])))} of the facts above and keep their specific details exactly (numbers, names, dates, places).
- Do not repeat anything already covered. Do not summarise the whole post, do not conclude, do not write a heading.
- Vary how you attribute: name an outlet only when it adds credibility or when outlets differ; never start more than one sentence with "According to".
- Do not put words in quotation marks unless copied word for word from a fact.
- Write plain text. Wrap one or two key phrases in **double asterisks**. No other markdown, no HTML.
- Avoid filler and stock phrases. Be concrete."""
    if is_lead:
        task = (f"Write the OPENING paragraph (2 or 3 sentences, 50 to 90 words) of the post. Open with: {part['idea']}.\n"
                "Make a reader want to continue, and say plainly what the story is about.")
    else:
        task = (f'Write the part titled "{part["heading"]}". Purpose: {part["purpose"]}.\n'
                f"FORM: {FORMS[part['form']]}")
    prev = f'\nThe previous part ended with: "{prev_last}"' if prev_last else ""
    return f"""You are writing ONE part of a news explainer post.
Story angle: {plan['angle'] or 'explain the story clearly'}
Narrator voice: {plan['voice']}

{task}

FACTS YOU MAY USE (each is from a named outlet):
{facts}

ALREADY COVERED EARLIER (do not repeat):
{done}{prev}{gaps}{analogy}{focus}

{rules}

Output only the text of this part."""


def clean_part(text, heading):
    t = re.sub(r"```\w*", "", text).strip()
    out = []
    for i, l in enumerate(t.splitlines()):
        s = l.strip()
        if i < 2 and s and (re.sub(r"[^a-z0-9]", "", s.lower()) == re.sub(r"[^a-z0-9]", "", heading.lower()) or re.match(r"^#+\s", s)):
            continue
        if re.match(r"^(here is|here's|sure|certainly|okay)\b", s, re.I):
            continue
        s = re.sub(r"^\*\s+", "- ", s)
        s = re.sub(r"^#+\s*", "", s)
        out.append(s)
    return "\n".join(out).strip()


def scrub(text, seen):
    """Remove clichés and sentences that repeat something already written."""
    out = []
    for line in text.splitlines():
        m = re.match(r"^(\s*(?:[-\u2022]\s+|Q:\s*|A:\s*)?)(.*)$", line)
        prefix, rest = m.group(1), m.group(2).strip()
        if not rest:
            out.append("")
            continue
        keep = []
        for s in SENT_SPLIT.split(rest):
            low = s.lower()
            tk = toks(s)
            if any(b in low for b in HARD_BANNED):
                continue
            if len(tk) >= 6 and any(similar(tk, t, 0.6, 0.85) for t in seen):
                continue
            keep.append(s)
            if len(tk) >= 6:
                seen.append(tk)
        if keep:
            out.append(prefix + " ".join(keep))
    return "\n".join(out)


def inline(t):
    t = htmlmod.escape(t, quote=False)
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t).replace("**", "")


def render_part(form, text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    faq = []
    if form == "qa":
        pairs = re.findall(r"Q:\s*(.+?)\s*\n+\s*A:\s*(.+?)(?=\n+\s*Q:|\Z)", "\n".join(lines), re.S)
        if pairs:
            h = ""
            for q, a in pairs:
                q = q.strip()
                a = " ".join(a.split())
                if not q.endswith("?"):
                    q += "?"
                h += f"<h3>{inline(q)}</h3><p>{inline(a)}</p>"
                faq.append((q.replace("**", ""), a.replace("**", "")[:600]))
            return h, faq
        form = "prose"
    if form == "list" and any(l.startswith("- ") for l in lines):
        h, items = "", []
        for l in lines:
            if l.startswith("- "):
                items.append(l[2:])
            else:
                if items:
                    h += "<ul>" + "".join(f"<li>{inline(i)}</li>" for i in items) + "</ul>"
                    items = []
                h += f"<p>{inline(l)}</p>"
        if items:
            h += "<ul>" + "".join(f"<li>{inline(i)}</li>" for i in items) + "</ul>"
        return h, faq
    if form == "timeline":
        rows = [l for l in lines if re.match(r"^[^:]{2,40}:\s+\S", l)]
        if len(rows) >= 3:
            intro = [l for l in lines if l not in rows]
            h = "".join(f"<p>{inline(l)}</p>" for l in intro)
            h += "<ol>" + "".join(
                f"<li><strong>{inline(r.split(':', 1)[0])}:</strong> {inline(r.split(':', 1)[1].strip())}</li>" for r in rows) + "</ol>"
            return h, faq
    return "".join(f"<p>{inline(l)}</p>" for l in lines), faq


def finish_prompt(article_text, focus, lead_title):
    kw = f' Include the focus keyword "{focus}" naturally if it fits.' if focus else ""
    return f"""Below is a finished news explainer article (plain text). Write its metadata.

ARTICLE:
{article_text}

Reply in exactly this format and nothing else:
TITLE: <headline under 62 characters, specific, no clickbait, not a copy of the first line.{kw}>
DESCRIPTION: <meta description, max 150 characters>
KEYWORDS: <focus keyword>, <3 to 5 related keywords>
SUMMARY_HEADING: <short natural label for a summary box, such as "In short" or "The quick version">
SUMMARY:
<2 to 4 short sentences, or 3 to 4 short lines starting with '- ', that say what the topic is, what happened and why it matters. Use only facts present in the article and do not copy its opening paragraph.>"""


def parse_finish(raw, fb_title, fb_desc):
    raw = re.sub(r"```\w*", "", raw).strip()
    title = re.sub(r"[*#\"]", "", hdr("TITLE", raw))[:100]
    if len(title) < 8:
        title = fb_title[:100]
    desc = re.sub(r"[*#]", "", hdr("DESCRIPTION", raw))[:155]
    if len(desc) < 20:
        desc = fb_desc[:150]
    kws = [k.strip(" *\"'#") for k in hdr("KEYWORDS", raw).split(",")]
    kws = [k for k in kws if 2 < len(k) <= 40][:6]
    heading = re.sub(r"[*#\"]", "", hdr("SUMMARY_HEADING", raw))[:40] or "In Short"
    m = re.search(r"^[ \t]*\**SUMMARY\**[ \t]*:[ \t]*(.*)\Z", raw, re.I | re.M | re.S)
    summary = m.group(1).strip() if m else ""
    return {"title": title, "desc": desc, "kws": kws, "heading": heading, "summary": summary}


def summary_html(s, fallback):
    lines = [l.strip() for l in s.splitlines() if l.strip()]
    if not lines:
        return f"<p>{htmlmod.escape(fallback)}</p>"
    bullets = [re.sub(r"^[-*\u2022]\s+", "", l) for l in lines if re.match(r"^[-*\u2022]\s+", l)]
    if len(bullets) >= 2:
        return "<ul>" + "".join(f"<li>{inline(b)}</li>" for b in bullets) + "</ul>"
    return "<p>" + inline(" ".join(lines)) + "</p>"


def nn(s):
    return s.replace(",", "").rstrip(".")


def normtxt(s):
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s.lower()).split())


def fact_check(soup, src):
    """Drop sentences with numbers or direct quotes that are not present in the source material."""
    now = datetime.now(timezone.utc)
    known = {nn(n) for n in NUM.findall(src)}
    known |= {str(now.year), str(now.year - 1), str(now.year + 1), str(now.day)}
    src_norm = normtxt(src)
    removed = 0
    for el in soup.find_all(["p", "li"]):
        txt = el.get_text(" ").strip()
        if not txt:
            continue
        sents = SENT_SPLIT.split(txt)
        keep = []
        for s in sents:
            bad_num = [n for n in NUM.findall(s) if len(nn(n)) > 1 and nn(n) not in known]
            bad_quote = [q for q in QUOTE.findall(s) if normtxt(q) not in src_norm]
            if bad_num or bad_quote:
                removed += 1
            else:
                keep.append(s)
        if len(keep) != len(sents):
            if keep:
                el.string = " ".join(keep)
            else:
                el.decompose()
    log("fact-check removed sentences:", removed)


def inner_html(soup):
    return soup.body.decode_contents() if soup.body else str(soup)


# ---------------- ads ----------------
def ads_available():
    return {n: os.getenv(n, "").strip() for n in AD_HINTS if os.getenv(n, "").strip()}


def ad_div(code):
    return f'<div style="text-align:center;margin:24px 0;overflow:hidden">{code}</div>'


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
    return htmlmod.escape(s, quote=True)


def ld_script(obj):
    return '<script type="application/ld+json">' + json.dumps(obj).replace("</", "<\\/") + "</script>"


def assemble(cat, p, summary_inner, body_inner, img, members, words, related, faq):
    now = datetime.now(timezone.utc)
    iso, pretty = now.isoformat(), now.strftime("%B %d, %Y")
    minutes = max(1, round(words / 200))
    title = p["title"]
    byline = (f'<p style="font-size:13px;color:#666;margin:0 0 14px">By {esc(SITE_NAME)} Staff &middot; '
              f'<time datetime="{iso}">{pretty}</time> &middot; {esc(cat)} &middot; {minutes} min read</p>')
    fig = (f'<figure style="margin:0 0 20px"><img src="{esc(img[0])}" alt="{esc(title)}" '
           f'style="width:100%;height:auto;border-radius:8px"/>'
           f'<figcaption style="font-size:12px;color:#666">Image: {esc(img[1])}</figcaption></figure>')
    box = (f'<div style="border-left:4px solid #1a73e8;background:rgba(26,115,232,.08);padding:14px 18px;'
           f'margin:0 0 24px;border-radius:6px"><p style="margin:0 0 8px"><strong>{esc(p["heading"])}</strong></p>'
           f'{summary_inner}</div>')
    links = ", ".join(f'<a href="{esc(m["link"])}" target="_blank" rel="noopener">{esc(m["source"])}</a>' for m in members)
    article = {
        "@context": "https://schema.org", "@type": "NewsArticle", "headline": title, "description": p["desc"],
        "image": [img[0]], "datePublished": iso, "dateModified": iso, "articleSection": cat,
        "keywords": p["kws"], "wordCount": words, "inLanguage": "en", "isAccessibleForFree": True,
        "author": {"@type": "Organization", "name": SITE_NAME},
        "publisher": {"@type": "Organization", "name": SITE_NAME},
    }
    ld = ld_script(article)
    if len(faq) >= 2:
        ld += ld_script({"@context": "https://schema.org", "@type": "FAQPage", "mainEntity": [
            {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq[:5]]})
    return (f'<div class="news-post">{adsense_loader()}{byline}{fig}{box}{body_inner}{related}'
            f'<p style="font-size:13px;color:#555"><em>Sources: {links}</em></p>{ld}</div>')


# ---------------- pipeline ----------------
def build_post(cat, cl, st, best):
    lead_title = cl[0]["title"]
    # up to 6 different outlets (newest first); the 5 with the most usable text are used
    members, used = [], set()
    for m in sorted(cl, key=lambda m: m["ts"], reverse=True):
        if m["source"] in used:
            continue
        used.add(m["source"])
        members.append(m)
        if len(members) == MAX_SOURCES + 1:
            break
    with ThreadPoolExecutor(max_workers=6) as ex:
        pages = list(ex.map(lambda m: fetch_page(m["link"]), members))
    infos = []
    for m, (text, pimg) in zip(members, pages):
        facts = text if len(text) > len(m["summary"]) else m["summary"]
        infos.append((m, facts, pimg))
    infos.sort(key=lambda x: len(x[1]), reverse=True)

    # an image is mandatory: check this first so no AI time is wasted
    img = pick_image(infos, cl)
    if not img:
        note(f"[{cat}] SKIPPED (no working image found): {lead_title}")
        return None

    infos = [i for i in infos if len(i[1]) >= 120][:MAX_SOURCES]
    chosen = [i[0] for i in infos]
    cards = extract_cards(infos)
    if len(cards) < 8:
        note(f"[{cat}] SKIPPED (only {len(cards)} specific facts found): {lead_title}")
        return None
    outlets = ", ".join(m["source"] for m in chosen)
    gn_rel = best["rel"] if best else []
    gn_lines = "\n".join(f"- {h} ({o})" for h, o in gn_rel) or "- none"
    ads = ads_available()

    # step 1: the AI plans angle, voice, structure and ad placement
    t0 = time.time()
    plan = None
    for attempt in range(2):
        raw = llm(PLAN_SYS, plan_prompt(cat, cards, outlets, gn_lines, ", ".join(st["formats"][-6:]) or "none yet",
                                        ", ".join(st["voices"][-4:]) or "none yet", ads),
                  num_predict=750, temp=0.75 if attempt == 0 else 0.5)
        plan = parse_plan(raw, cards, ads)
        if plan:
            break
    if not plan:
        note(f"[{cat}] planner output unusable, using fallback outline")
        plan = fallback_plan(cards)
    assign_cards(plan, cards)
    log(f"plan ({time.time() - t0:.0f}s): format='{plan['fmt']}', voice='{plan['voice']}', focus='{plan['focus']}', "
        f"sections={[(s['heading'], s['form'], len(s['cards'])) for s in plan['sections']]}")

    # step 2: write the opening and each section separately
    seen, covered, faq = [], [], []
    body_parts, article_text = [], []
    prev_last = ""
    ad_after = {}
    for name, k in plan["ads"].items():
        ad_after.setdefault(k, name)

    lead_raw = llm(WRITER_SYS, part_prompt(plan, plan["lead"], True, covered, prev_last, False), num_predict=260, temp=0.8)
    lead_text = scrub(clean_part(lead_raw, ""), seen)
    if len(lead_text.split()) < 20:
        note(f"[{cat}] SKIPPED (AI wrote no usable opening): {lead_title}")
        return None
    h, _ = render_part("prose", lead_text)
    body_parts.append(h)
    article_text.append(lead_text)
    covered.append(SENT_SPLIT.split(lead_text)[0][:140])
    prev_last = SENT_SPLIT.split(lead_text)[-1][:160]
    if 0 in ad_after and ad_after[0] in ads:
        body_parts.append(ad_div(ads[ad_after[0]]))

    kept = 0
    for i, sec in enumerate(plan["sections"], 1):
        raw = llm(WRITER_SYS, part_prompt(plan, sec, False, covered, prev_last, kept == 0), num_predict=560, temp=0.8)
        text = scrub(clean_part(raw, sec["heading"]), seen)
        if len(text.split()) < 40:
            log(f"section '{sec['heading']}' too short, dropped")
            continue
        h, f = render_part(sec["form"], text)
        faq += f
        body_parts.append(f"<h2>{inline(sec['heading'])}</h2>{h}")
        article_text.append(sec["heading"] + "\n" + text)
        covered.append(sec["heading"] + ": " + SENT_SPLIT.split(text)[0][:120])
        prev_last = SENT_SPLIT.split(text)[-1][:160]
        kept += 1
        if i in ad_after and ad_after[i] in ads:
            body_parts.append(ad_div(ads[ad_after[i]]))
    log(f"writing finished in {time.time() - t0:.0f}s, sections kept: {kept}")
    if kept < 3:
        note(f"[{cat}] SKIPPED (only {kept} usable sections): {lead_title}")
        return None

    # step 3: title, description, keywords and summary are written last, from the finished article
    full_text = "\n\n".join(article_text)[:5500]
    fin = parse_finish(llm(WRITER_SYS, finish_prompt(full_text, plan["focus"], lead_title), num_predict=420, temp=0.5),
                       lead_title, clean(article_text[0])[:150])
    if is_dup([toks(fin["title"])], st):
        note(f"[{cat}] SKIPPED (generated title duplicates an earlier post): {fin['title']}")
        return None

    # step 4: fact verification against all source material
    src = " ".join([m["title"] + " " + m["summary"] for m in cl] + [i[1] for i in infos] + [h + " " + o for h, o in gn_rel])
    body_soup = BeautifulSoup("".join(body_parts), "lxml")
    fact_check(body_soup, src)
    words = len(body_soup.get_text().split())
    if words < MIN_WORDS:
        note(f"[{cat}] SKIPPED (article only {words} words after checks): {lead_title}")
        return None
    sum_soup = BeautifulSoup(summary_html(fin["summary"], fin["desc"]), "lxml")
    fact_check(sum_soup, src)
    summary_inner = inner_html(sum_soup)
    if len(sum_soup.get_text().split()) < 8:
        summary_inner = f"<p>{esc(fin['desc'])}</p>"

    related = related_block(st, cat, toks(fin["title"]) | {k for kw in fin["kws"] for k in toks(kw)})
    html_out = assemble(cat, fin, summary_inner, inner_html(body_soup), img, chosen, words, related, faq)
    tags = []
    for k in fin["kws"]:
        k = k.replace(",", " ").strip()
        if k and k.lower() != cat.lower() and k.lower() not in [t.lower() for t in tags]:
            tags.append(k)
    return {"title": fin["title"], "html": html_out, "labels": [cat] + tags[:4], "fmt": plan["fmt"],
            "voice": plan["voice"], "words": words}


def process(cat, st, trends):
    seen = set(st["seen"])
    items = [i for i in collect(cat) if i["id"] not in seen]
    if not items:
        note(f"[{cat}] no fresh items in feeds")
        return False
    gn = gn_signals(cat)
    ranked = []
    for c in cluster(items):
        s, srcs, boost, best = score(c, trends, gn)
        ranked.append((s, srcs, boost, best, c))
    ranked.sort(key=lambda x: x[0], reverse=True)

    tried = 0
    for s, srcs, boost, best, cl in ranked:
        if tried >= MAX_CLUSTER_TRIES:
            break
        titles = [m["title"] for m in cl]
        if is_dup([m["tk"] for m in cl], st):
            log(f"[{cat}] already covered: {titles[0]}")
            continue
        tried += 1
        log(f"[{cat}] topic (score {s:.1f}, outlets {srcs}, trend boost {boost:.0f}, "
            f"Google News match {'yes' if best else 'no'}): {titles[0]}")
        res = build_post(cat, cl, st, best)
        ids = [m["id"] for m in cl]
        if not res:
            st["seen"].extend(ids)
            continue
        url = blogger_post(res["title"], res["html"], res["labels"])
        st["seen"].extend(ids)
        st["write_ok"] = True
        st["formats"].append(res["fmt"])
        st["voices"].append(res["voice"])
        record_topic(st, cat, titles[:6], res["title"], url)
        note(f"POSTED [{cat}] ({res['words']} words, structure: {res['fmt']}): {res['title']} -> {url}")
        return True
    return False


def main():
    start = time.time()
    st = load_state()
    preflight(st)
    cats = list(CATEGORIES)
    trends = fetch_trends()
    log("trending terms loaded:", len(trends))
    posted = False
    blogger_failed = False
    for _ in range(len(cats)):
        if time.time() - start > TIME_BUDGET:
            note("Time budget reached, stopping this run")
            break
        cat = cats[st["idx"] % len(cats)]
        try:
            posted = process(cat, st, trends)
        except PostError as ex:
            note(f"BLOGGER ERROR while posting: {ex} | HOW TO FIX: {hint_for(str(ex))}")
            st["write_ok"] = False
            blogger_failed = True
            break
        except Exception as ex:
            note(f"ERROR in {cat}: {ex!r}")
        st["idx"] = (st["idx"] + 1) % len(cats)
        if posted:
            break
    st["fails"] = 0 if posted else st["fails"] + 1
    save_state(st)
    nxt = (not blogger_failed) and st["fails"] < MAX_FAILS
    write_summary()
    write_output(nxt)
    log("next run:", nxt)
    if blogger_failed or st["fails"] >= MAX_FAILS:
        sys.exit(1)


if __name__ == "__main__":
    main()
