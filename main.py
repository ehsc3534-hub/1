import calendar, hashlib, json, os, re, sys, time, warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

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
MIN_WORDS = 200
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
    st.setdefault("write_ok", False)
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


def blogger_post(title, content, label):
    r = S.post(
        f"https://www.googleapis.com/blogger/v3/blogs/{blog_id()}/posts/",
        headers={"Authorization": f"Bearer {access_token()}"},
        json={"kind": "blogger#post", "title": title, "content": content, "labels": [label]},
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
    note(f"Token scopes: {scopes if scopes else 'not reported by Google'}")
    if scopes and BLOGGER_SCOPE not in scopes:
        fatal(f"Refresh token scopes are {scopes}, but writing posts needs {BLOGGER_SCOPE}. "
              "HOW TO FIX: create a new refresh token with that scope (not .readonly) and update the REFRESH_TOKEN secret.")
    r = S.get(f"https://www.googleapis.com/blogger/v3/blogs/{blog_id()}", headers=h, timeout=30)
    if r.status_code != 200:
        msg = f"Blogger blog lookup failed {r.status_code}: {r.text[:300]}"
        fatal(f"{msg} | HOW TO FIX: {hint_for(msg)}")
    b = r.json()
    note(f"Blogger connection OK: '{b.get('name')}' ({b.get('url')}), existing posts: {b.get('posts', {}).get('totalItems')}")

    # which blogs can THIS Google account actually write to?
    try:
        ub = S.get("https://www.googleapis.com/blogger/v3/users/self/blogs",
                   params=[("role", "ADMIN"), ("role", "AUTHOR")], headers=h, timeout=30)
        if ub.status_code == 200:
            items = ub.json().get("items", [])
            ids = [str(i.get("id")) for i in items]
            listing = ", ".join(f"{i.get('name')} (id {i.get('id')})" for i in items) or "none"
            note(f"Blogs this Google account can write to: {listing}")
            if blog_id() not in ids:
                fatal(f"The Google account that created your REFRESH_TOKEN is NOT an Admin/Author of blog id {blog_id()} "
                      f"('{b.get('name')}'). It can only write to: {listing}. "
                      "HOW TO FIX: create the refresh token again in OAuth Playground while signed in to the Google account "
                      "that owns this blog (check blogger.com), or add the token's account as Admin in Blogger -> Settings -> "
                      "Permissions and accept the invitation, then update the REFRESH_TOKEN secret.")
        else:
            note(f"Could not list this account's blogs (HTTP {ub.status_code}); continuing with write test")
    except SystemExit:
        raise
    except Exception as ex:
        note(f"Could not list this account's blogs: {ex}")

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
        ct = r.headers.get("content-type", "").lower()
        ok = r.status_code == 200 and (ct.startswith("image") or re.search(r"\.(jpe?g|png|webp)(\?|$)", u.lower()) is not None)
        cl = r.headers.get("content-length")
        if ok and cl and int(cl) < 5000:
            ok = False
        r.close()
        return ok
    except Exception:
        return False


def pick_images(members, cl, pimgs):
    ordered = []
    for m in members:
        ordered += [(m["image"], m["source"]), (m.get("image_raw"), m["source"])]
    ordered += [(p, m["source"]) for m, p in zip(members, pimgs)]
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
    good = [c for c, g in zip(cands, ok) if g]
    out, keys = [], set()
    for u, s in good:
        # avoid using two size-variants of the same picture
        key = re.sub(r"/standard/\d+/|/news/\d+/", "/", u)
        if key in keys:
            continue
        keys.add(key)
        out.append((u, s))
    return out[:2]


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
- 700 to 900 words. Rewrite everything in fresh words; never copy sentences.
- Headings must be natural and specific to the story. Never use labels like "The Hook".
- Attribute claims naturally using only these outlet names: {outlets}. Do not add any number, date, quote or name that is not in the sources.
- Banned phrases: "in conclusion", "delve", "in today's fast-paced world", "landscape", "tapestry", "it is important to note".
- Headline: under 65 characters, specific, main keyword near the start, no clickbait.
- Write the post body in simple HTML tags only: <p>, <h2>, <h3>, <ul>, <li>, <strong>. No <h1>, no images, no scripts, no markdown.
- {ad_txt}

OUTPUT FORMAT (exactly):
TITLE: <headline>
DESCRIPTION: <meta description, max 150 characters>
---
<post html>"""


def llm(system, user):
    for attempt in range(2):
        try:
            r = requests.post(f"{OLLAMA}/api/chat", json={
                "model": MODEL, "stream": False, "keep_alive": "30m",
                "options": {"temperature": 0.65, "top_p": 0.9, "num_ctx": 8192, "num_predict": 2400},
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            }, timeout=1200)
            r.raise_for_status()
            return r.json()["message"]["content"]
        except requests.exceptions.ReadTimeout:
            note("AI took too long (timeout). Use a smaller model or fewer tokens.")
            return ""
        except Exception as ex:
            log("llm error:", ex)
            time.sleep(3)
    return ""


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


def parse_output(raw, fallback_title):
    raw = re.sub(r"```(?:html)?", "", raw).strip()
    head = raw[:700]
    t = re.search(r"title\s*:\s*\**\s*(.+)", head, re.I)
    d = re.search(r"description\s*:\s*\**\s*(.+)", head, re.I)
    start = 0
    if d and (not t or d.start() > t.start()):
        start = d.end()
    elif t:
        start = t.end()
    body = re.sub(r"^\s*-{3,}\s*", "", raw[start:]).strip()
    if len(body) < 200:
        return None
    if not re.search(r"<(p|h2|h3|ul)\b", body):
        body = md_to_html(body)
    body = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", body)
    title = re.sub(r"[*#\"]", "", t.group(1)).strip()[:100] if t else ""
    if len(title) < 8:
        title = fallback_title[:100]
    desc = re.sub(r"[*#]", "", d.group(1)).strip()[:155] if d else ""
    if len(desc) < 20:
        desc = clean(body)[:150]
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


# ---------------- pipeline ----------------
def build_post(cat, cl, st):
    lead = cl[0]["title"]
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
        note(f"[{cat}] SKIPPED (no working image found): {lead}")
        return None

    blocks, total = [], 0
    for n, (m, (text, _)) in enumerate(zip(members, pages), 1):
        facts = (text if len(text) > len(m["summary"]) else m["summary"])[:1800]
        total += len(facts)
        blocks.append(f"SOURCE {n} - {m['source']}\nHeadline: {m['title']}\nSummary: {m['summary'][:400]}\nDetails: {facts}")
    if total < 350:
        note(f"[{cat}] SKIPPED (too little source text, {total} chars): {lead}")
        return None

    src = " ".join([m["title"] + " " + m["summary"] for m in cl] + [p[0] for p in pages])
    outlets = ", ".join(m["source"] for m in members)
    ads = ads_available()
    t0 = time.time()
    raw = llm(SYSTEM, make_prompt(cat, "\n\n".join(blocks), outlets, ads))
    log(f"AI finished in {time.time() - t0:.0f}s, {len(raw.split())} words")
    parsed = parse_output(raw, lead)
    if not parsed:
        note(f"[{cat}] SKIPPED (AI output unusable): {lead}")
        return None
    title, desc, body = parsed

    if is_dup([toks(title)], st):
        note(f"[{cat}] SKIPPED (generated title duplicates an earlier post): {title}")
        return None

    soup = sanitize(body)
    fact_check(soup, src)
    words = len(soup.get_text().split())
    if words < MIN_WORDS:
        note(f"[{cat}] SKIPPED (article only {words} words after checks): {lead}")
        return None
    inner = soup.body.decode_contents() if soup.body else str(soup)
    inner = place_ads(inner, ads)
    return title, assemble(title, desc, inner, imgs, members)


def process(cat, st, trends):
    seen = set(st["seen"])
    items = [i for i in collect(cat) if i["id"] not in seen]
    if not items:
        note(f"[{cat}] no fresh items in feeds")
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
        st["write_ok"] = True
        record_topic(st, cat, titles[:6], res[0])
        note(f"POSTED [{cat}]: {res[0]} -> {url}")
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
