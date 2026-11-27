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
MAX_SOURCES = 5            # outlets whose facts are given to the AI
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
    st.setdefault("write_ok", False)
    return st


def save_state(st):
    now = time.time()
    st["topics"] = [t for t in st["topics"] if now - t["t"] < 45 * 86400][-600:]
    st["seen"] = st["seen"][-4000:]
    st["formats"] = st["formats"][-12:]
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
    return " ".join(p for p in ps if len(p) > 50)[:6000], img


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


# ---------------- AI ----------------
RESEARCH_SYS = (
    "You are a meticulous news research assistant. You extract only facts that are explicitly stated in the provided "
    "source excerpts. You never guess and never add outside knowledge."
)

WRITER_SYS = (
    "You are a staff writer at a news explainer website. You write like a smart, warm human journalist who helps readers "
    "truly understand a story. You never invent facts, numbers, quotes or names, and you only use the material provided."
)


def llm(system, user, num_predict=2400, temp=0.65):
    for attempt in range(2):
        try:
            r = requests.post(f"{OLLAMA}/api/chat", json={
                "model": MODEL, "stream": False, "keep_alive": "30m",
                "options": {"temperature": temp, "top_p": 0.92, "repeat_penalty": 1.08, "num_ctx": 10240,
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


def research_prompt(blocks):
    return f"""Read these excerpts from several news outlets about ONE story and write a FACT SHEET for a writer.

{blocks}

Rules: use only facts stated in the excerpts. Put the outlet name in [brackets] after each fact. Keep exact numbers, dates, names and places. No opinions, no guesses.

Output exactly these sections:
WHAT HAPPENED: (2 to 3 lines)
KEY FACTS: (bullets)
BACKGROUND: (bullets)
WHO SAYS WHAT: (bullets, only statements by named people or organisations)
DISAGREEMENTS OR GAPS: (bullets: where outlets differ or details are missing)
WHY IT MATTERS: (bullets, only if supported by the excerpts)"""


def writer_prompt(cat, fact_sheet, excerpts, outlets, gn_lines, recent, ads):
    if ads:
        lines = "\n".join(f"  [[AD:{n}]] = {AD_HINTS[n]}" for n in ads)
        ad_txt = (
            "Optional ad slots. Use only the ones that genuinely fit, each at most once, never before the first "
            "paragraph, never two in a row. Skipping all of them is fine. To place one, put its marker alone on its own line:\n" + lines
        )
    else:
        ad_txt = "There are no ad slots. Do not write any [[AD:...]] markers."
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    return f"""Write a news EXPLAINER post for the "{cat}" section. Today is {today}. This story is trending right now.

FACT SHEET (compiled from these outlets: {outlets}):
{fact_sheet}

SOURCE EXCERPTS:
{excerpts}

OTHER HEADLINES ON THIS STORY (Google News):
{gn_lines}

HOW TO WRITE IT (you decide the shape of the post):
- Choose the best way to explain THIS story yourself. Ideas, not a checklist: a timeline, a question-led explainer, myth vs fact, both sides of the argument, a by-the-numbers breakdown, what changes for ordinary people, a cause-and-effect chain, what we know vs what we don't know yet. Invent a better one if it fits.
- Structures recently used on this site (pick something DIFFERENT): {recent}
- You decide how many sections there are, their order, and their headings. Headings must be specific to the story. A bullet list, short timeline or mini Q&A is allowed only where it truly helps. Do not add a FAQ or "key takeaways" unless this story needs one.
- Write an original opening that fits the story; do not reuse a stock opening style.

MUST-HAVES:
1. SUMMARY: a short box at the very top (2 to 4 short sentences, or 3 to 4 short bullet lines) that tells the reader what the topic is, what happened and why it matters. It must stand on its own.
2. Below it, the full explanation of 900 to 1200 words, drawing on several outlets so the reader gets the complete picture. Name the outlet when attributing ("according to ...").
3. Explain jargon and background in plain words. If outlets disagree or details are missing, say so honestly.
4. Use facts only from the fact sheet and excerpts. Do not invent numbers, dates, names or quotes. Do not put words in quotation marks unless they appear word-for-word in the excerpts.
5. Human voice: vary sentence length, use contractions now and then, be concrete, skip filler. Avoid: "in conclusion", "delve", "landscape", "tapestry", "it's important to note", "in today's fast-paced world", "navigate", "game-changer", "shed light".
6. SEO: choose one focus keyword phrase (2 to 4 words) and use it naturally in the headline, the summary, the first paragraph and at least one <h2>. Headline under 62 characters, specific, no clickbait.
7. Body HTML uses only: <p>, <h2>, <h3>, <ul>, <ol>, <li>, <strong>, <em>. No <h1>, no images, no scripts, no markdown.
8. {ad_txt}

OUTPUT FORMAT (exactly, in this order):
TITLE: <headline>
DESCRIPTION: <meta description, max 150 characters>
KEYWORDS: <focus keyword>, <3 to 5 related keywords>
FORMAT: <3 to 6 word name of the structure you chose>
SUMMARY_HEADING: <a short natural label for the summary box, such as "In short" or "The quick version">
SUMMARY:
<summary text>
---
<body html>"""


def md_to_html(t):
    """Fallback: the small model sometimes answers in markdown instead of HTML."""
    out, in_ul = [], False
    for line in t.splitlines():
        s = line.strip()
        if not s:
            if in_ul:
                out.append("</ul>")
                in_ul = False
            continue
        m = re.match(r"^(#{1,4})\s+(.*)", s)
        if m:
            if in_ul:
                out.append("</ul>")
                in_ul = False
            lvl = 2 if len(m.group(1)) <= 2 else 3
            out.append(f"<h{lvl}>{m.group(2)}</h{lvl}>")
            continue
        m = re.match(r"^[-*\u2022]\s+(.*)", s)
        if m:
            if not in_ul:
                out.append("<ul>")
                in_ul = True
            out.append(f"<li>{m.group(1)}</li>")
            continue
        if in_ul:
            out.append("</ul>")
            in_ul = False
        out.append(s if s.startswith("<") or s.startswith("[[AD:") else f"<p>{s}</p>")
    if in_ul:
        out.append("</ul>")
    return "\n".join(out)


def hdr(name, raw):
    m = re.search(rf"^[ \t]*\**{name}\**[ \t]*:[ \t]*\**[ \t]*(.+?)[ \t]*\**[ \t]*$", raw, re.I | re.M)
    return m.group(1).strip() if m else ""


def summary_html(s, fallback):
    lines = [l.strip() for l in s.strip().splitlines() if l.strip()]
    if not lines:
        return f"<p>{htmlmod.escape(fallback)}</p>"
    bullets = [re.sub(r"^[-*\u2022]\s+", "", l) for l in lines if re.match(r"^[-*\u2022]\s+", l)]
    if len(bullets) >= 2:
        return "<ul>" + "".join(f"<li>{b}</li>" for b in bullets) + "</ul>"
    return "<p>" + " ".join(lines) + "</p>"


def parse_output(raw, fallback_title):
    raw = re.sub(r"```(?:html)?", "", raw).strip()
    title = re.sub(r"[*#\"]", "", hdr("TITLE", raw))[:100]
    if len(title) < 8:
        title = fallback_title[:100]
    desc = re.sub(r"[*#]", "", hdr("DESCRIPTION", raw))[:155]
    kws = [k.strip(" *\"'#") for k in hdr("KEYWORDS", raw).split(",")]
    kws = [k for k in kws if 2 < len(k) <= 40][:6]
    fmt = re.sub(r"[*#\"]", "", hdr("FORMAT", raw))[:60] or "explainer"
    heading = re.sub(r"[*#\"]", "", hdr("SUMMARY_HEADING", raw))[:40] or "In Short"
    m = re.search(r"^[ \t]*\**SUMMARY\**[ \t]*:[ \t]*(.*?)^[ \t]*-{3,}[ \t]*$", raw, re.I | re.M | re.S)
    if m:
        summary, body = m.group(1).strip(), raw[m.end():].strip()
    else:
        idx = list(re.finditer(r"^[ \t]*\**(TITLE|DESCRIPTION|KEYWORDS|FORMAT|SUMMARY_HEADING)\**[ \t]*:.*$", raw, re.I | re.M))
        body = re.sub(r"^\s*-{3,}\s*", "", raw[idx[-1].end():].strip() if idx else raw)
        summary = ""
    if len(body) < 400:
        return None
    if not re.search(r"<(p|h2|h3|ul)\b", body):
        body = md_to_html(body)
    bold = lambda x: re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", x)
    body = bold(body)
    if len(desc) < 20:
        desc = clean(body)[:150]
    return {"title": title, "desc": desc, "kws": kws, "fmt": fmt, "heading": heading,
            "summary": bold(summary_html(summary, desc)), "body": body}


def sanitize(body):
    soup = BeautifulSoup(body, "lxml")
    for t in soup(["script", "style", "iframe", "h1", "form", "link", "meta", "img"]):
        t.decompose()
    for t in soup.find_all(True):
        for a in list(t.attrs):
            if a != "href":
                del t.attrs[a]
        if t.name == "a":
            t["rel"] = "noopener"
            t["target"] = "_blank"
    return soup


NUM = re.compile(r"\d[\d,]*\.?\d*")
QUOTE = re.compile(r'["\u201c]([^"\u201d]{25,})["\u201d]')


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
        if "[[AD:" in txt or not txt:
            continue
        sents = re.split(r"(?<=[.!?])\s+", txt)
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


def faq_pairs(soup):
    out = []
    for h in soup.find_all(["h2", "h3"]):
        q = h.get_text(" ").strip()
        if q.endswith("?"):
            p = h.find_next_sibling()
            if p is not None and p.name == "p":
                a = p.get_text(" ").strip()
                if len(a) > 30:
                    out.append((q, a[:600]))
    return out[:5]


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
            {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": a}} for q, a in faq]})
    return (f'<div class="news-post">{adsense_loader()}{byline}{fig}{box}{body_inner}{related}'
            f'<p style="font-size:13px;color:#555"><em>Sources: {links}</em></p>{ld}</div>')


# ---------------- pipeline ----------------
def build_post(cat, cl, st, best):
    lead = cl[0]["title"]
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
        note(f"[{cat}] SKIPPED (no working image found): {lead}")
        return None

    infos = [i for i in infos if len(i[1]) >= 120][:MAX_SOURCES]
    total = sum(len(i[1]) for i in infos)
    if total < 400:
        note(f"[{cat}] SKIPPED (too little source text, {total} chars): {lead}")
        return None
    chosen = [i[0] for i in infos]

    def block(n, m, facts, limit):
        return f"SOURCE {n} - {m['source']}\nHeadline: {m['title']}\nSummary: {m['summary'][:400]}\nDetails: {facts[:limit]}"

    research_blocks = "\n\n".join(block(n, m, f, 2000) for n, (m, f, _) in enumerate(infos, 1))
    writer_blocks = "\n\n".join(block(n, m, f, 1200) for n, (m, f, _) in enumerate(infos, 1))
    outlets = ", ".join(m["source"] for m in chosen)

    gn_rel = best["rel"] if best else []
    gn_lines = "\n".join(f"- {h} ({o})" for h, o in gn_rel) or "- none"

    # pass 1: research / fact sheet
    t0 = time.time()
    fact_sheet = llm(RESEARCH_SYS, research_prompt(research_blocks), num_predict=900, temp=0.2).strip()
    log(f"research pass: {time.time() - t0:.0f}s, {len(fact_sheet.split())} words")
    if len(fact_sheet) < 200:
        fact_sheet = "(not available - rely on the source excerpts)"

    # pass 2: writing (AI chooses the structure)
    ads = ads_available()
    recent = ", ".join(st["formats"][-6:]) or "none yet"
    t0 = time.time()
    raw = llm(WRITER_SYS, writer_prompt(cat, fact_sheet, writer_blocks, outlets, gn_lines, recent, ads),
              num_predict=3000, temp=0.8)
    log(f"writing pass: {time.time() - t0:.0f}s, {len(raw.split())} words")
    p = parse_output(raw, lead)
    if not p:
        note(f"[{cat}] SKIPPED (AI output unusable): {lead}")
        return None
    if is_dup([toks(p["title"])], st):
        note(f"[{cat}] SKIPPED (generated title duplicates an earlier post): {p['title']}")
        return None

    src = " ".join([m["title"] + " " + m["summary"] for m in cl] + [i[1] for i in infos]
                   + [h + " " + o for h, o in gn_rel])
    body_soup = sanitize(p["body"])
    fact_check(body_soup, src)
    words = len(body_soup.get_text().split())
    if words < MIN_WORDS:
        note(f"[{cat}] SKIPPED (article only {words} words after checks): {lead}")
        return None
    sum_soup = sanitize(p["summary"])
    fact_check(sum_soup, src)
    summary_inner = inner_html(sum_soup)
    if len(sum_soup.get_text().split()) < 8:
        summary_inner = f"<p>{esc(p['desc'])}</p>"

    faq = faq_pairs(body_soup)
    body_inner = place_ads(inner_html(body_soup), ads)
    related = related_block(st, cat, toks(p["title"]) | {k for kw in p["kws"] for k in toks(kw)})
    html_out = assemble(cat, p, summary_inner, body_inner, img, chosen, words, related, faq)
    tags = []
    for k in p["kws"]:
        k = re.sub(r"[,]", " ", k).strip()
        if k and k.lower() != cat.lower() and k.lower() not in [t.lower() for t in tags]:
            tags.append(k)
    return {"title": p["title"], "html": html_out, "labels": [cat] + tags[:4], "fmt": p["fmt"], "words": words}


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
