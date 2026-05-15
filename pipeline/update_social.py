#!/usr/bin/env python3
"""
The Ad Podium — World Cup '26 Ad Tracker
Cross-platform pipeline: Instagram + TikTok metadata and comment samples via Apify.

Reads:  pipeline/ads.json      (only ads with instagramUrl or tiktokUrl)
Writes: pipeline/social_data.json  (per-ad, per-platform metrics + comment sample)
        pipeline/spend_log.json    (running monthly Apify spend, for cost discipline)
        pipeline/social_cache/     (per-post 7-day TTL cache, keyed by URL hash)

Why this exists
---------------
YouTube alone is ~3% of the conversation for celebrity-led WC '26 spots —
Instagram's where the volume of comments lives, TikTok's where cuts travel.
A YouTube-only leaderboard structurally underweights brands that are doing
the most culturally interesting work, so we pull cross-platform data and
fold it into the composite score.

Cost discipline
---------------
The Apify free tier is $5/mo. We track ACTUAL spend from each run's compute
units returned by the API and adapt as we approach the cap:
  - Below $3.00:  full sample (100 comments per platform per top-tier ad)
  - $3.00–$4.50: half sample (50 comments)
  - Above $4.50: metadata-only mode (no comment sampling)
  - Hard cap at $5.00: skip remaining runs, log warning

Cache: every Actor run is cached for 7 days. Re-running the script the same
day costs nothing.

Tiered execution: by default only the top-10 ads (by current composite score
in data.json) get full comment sampling; the rest get metadata-only. This is
where the savings come from.

Usage
-----
    APIFY_API_TOKEN=apify_api_... python pipeline/update_social.py [flags]

Flags:
    --dry-run         Don't call Apify. Print what would be done + estimated cost.
    --metadata-only   Skip comment sampling for all ads (cheap mode).
    --force-refresh   Bypass the 7-day cache.
    --tier all|top    Which ads to process. Default: top (top-10 by score).
    --validate-token  One cheap call to confirm auth, then exit.
    --max-ads N       Process at most N ads (testing).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

ROOT          = Path(__file__).resolve().parent.parent
PIPELINE      = ROOT / "pipeline"
ADS_FILE      = PIPELINE / "ads.json"
DATA_FILE     = ROOT / "summer-26-football" / "data.json"  # for tier-by-score ordering
ACTORS_FILE   = PIPELINE / "social_actors.json"
OUTPUT_FILE   = PIPELINE / "social_data.json"
HISTORY_FILE  = PIPELINE / "social_history.json"  # timeseries for velocity
SPEND_FILE    = PIPELINE / "spend_log.json"
CACHE_DIR     = PIPELINE / "social_cache"
CACHE_TTL_DAYS = 7
SOCIAL_HISTORY_RETENTION_DAYS = 30  # prune snapshots older than this

# Cost-discipline thresholds (USD, monthly)
THRESHOLD_FULL_SAMPLE  = 3.00   # below: 100 comments
THRESHOLD_HALF_SAMPLE  = 4.50   # below: 50 comments; above: metadata-only
HARD_CAP               = 5.00   # at this point we stop spending entirely

DEFAULT_FULL_SAMPLE = 100
DEFAULT_HALF_SAMPLE = 50

# Which ads count as "top tier" for full sampling (by composite score from data.json)
TOP_TIER_COUNT = 10

API_BASE = "https://api.apify.com/v2"
RUN_TIMEOUT_SEC = 180  # max wait for an Actor run to finish
HTTP_TIMEOUT = 60


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_key(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m")


def url_hash(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def log(msg: str, *, prefix: str = "  "):
    print(f"{prefix}{msg}", flush=True)


# ---------------------------------------------------------------------------
# Spend tracker
# ---------------------------------------------------------------------------

class SpendTracker:
    """Tracks monthly Apify spend. Reads/writes pipeline/spend_log.json."""

    def __init__(self, path: Path = SPEND_FILE):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if not self.path.exists():
            return {"months": {}, "lastUpdated": now_iso()}
        try:
            return json.loads(self.path.read_text())
        except Exception:
            # corrupted — start fresh, but back up the old file
            backup = self.path.with_suffix(".bak.json")
            self.path.rename(backup)
            return {"months": {}, "lastUpdated": now_iso()}

    def _save(self):
        self.data["lastUpdated"] = now_iso()
        self.path.write_text(json.dumps(self.data, indent=2))

    def month_total(self, month: Optional[str] = None) -> float:
        m = month or month_key()
        return float(self.data["months"].get(m, {}).get("totalUsd", 0.0))

    def add(self, amount_usd: float, *, actor: str, ad_id: str, platform: str):
        m = month_key()
        bucket = self.data["months"].setdefault(m, {"totalUsd": 0.0, "runs": []})
        bucket["totalUsd"] = round(float(bucket["totalUsd"]) + float(amount_usd), 4)
        bucket["runs"].append({
            "ts": now_iso(),
            "actor": actor,
            "adId": ad_id,
            "platform": platform,
            "usd": round(float(amount_usd), 4),
        })
        # cap the runs log so the file doesn't grow unbounded
        if len(bucket["runs"]) > 500:
            bucket["runs"] = bucket["runs"][-500:]
        self._save()

    def sample_size_for(self, total: float) -> int:
        """How many comments we should pull right now given monthly spend so far."""
        if total >= THRESHOLD_HALF_SAMPLE:
            return 0
        if total >= THRESHOLD_FULL_SAMPLE:
            return DEFAULT_HALF_SAMPLE
        return DEFAULT_FULL_SAMPLE

    def can_spend(self, total: float) -> bool:
        return total < HARD_CAP


# ---------------------------------------------------------------------------
# Cache (per-URL, 7-day TTL)
# ---------------------------------------------------------------------------

class Cache:
    def __init__(self, base: Path = CACHE_DIR):
        self.base = base
        self.base.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.base / f"{key}.json"

    def get(self, key: str) -> Optional[dict]:
        p = self._path(key)
        if not p.exists():
            return None
        try:
            entry = json.loads(p.read_text())
        except Exception:
            return None
        ts = entry.get("cachedAt", "")
        try:
            cached_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None
        if datetime.now(timezone.utc) - cached_dt > timedelta(days=CACHE_TTL_DAYS):
            return None
        return entry.get("payload")

    def put(self, key: str, payload: Any):
        self._path(key).write_text(json.dumps({
            "cachedAt": now_iso(),
            "payload": payload,
        }, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Apify client
# ---------------------------------------------------------------------------

class ApifyError(Exception):
    pass


class ApifyClient:
    def __init__(self, token: str):
        if not token:
            raise ApifyError("APIFY_API_TOKEN is not set")
        self.token = token
        self.session = requests.Session()

    def run_actor_sync(self, actor_id: str, input_payload: dict) -> tuple[list[dict], float]:
        """
        Run an Actor synchronously, wait for completion, return (items, costUsd).

        Apify's run-sync-with-dataset-items endpoint blocks until the run finishes
        and returns the dataset items directly. Cost comes from the run object,
        which we fetch separately via the Run ID returned in the response headers.
        """
        # Use run-sync to get items, then look up the run for cost.
        items_url = f"{API_BASE}/acts/{actor_id}/run-sync-get-dataset-items?token={self.token}&timeout={RUN_TIMEOUT_SEC}"
        resp = self.session.post(
            items_url,
            json=input_payload,
            timeout=HTTP_TIMEOUT + RUN_TIMEOUT_SEC,
        )
        if resp.status_code >= 400:
            raise ApifyError(f"{resp.status_code}: {resp.text[:500]}")

        try:
            items = resp.json()
        except Exception:
            raise ApifyError(f"unparseable response: {resp.text[:500]}")
        if not isinstance(items, list):
            # some Actors return {error: ...}; surface it
            raise ApifyError(f"unexpected response shape: {str(items)[:300]}")

        # Cost: we read it from the run header if available, otherwise estimate 0
        # (the cost will still register on the user's Apify dashboard).
        run_id = resp.headers.get("x-apify-actor-run-id")
        cost = 0.0
        if run_id:
            try:
                run_info = self.session.get(
                    f"{API_BASE}/actor-runs/{run_id}?token={self.token}",
                    timeout=HTTP_TIMEOUT,
                )
                if run_info.status_code < 400:
                    run_data = run_info.json().get("data", {})
                    cost = float(run_data.get("usageTotalUsd", 0.0))
            except Exception:
                pass

        return items, cost


# ---------------------------------------------------------------------------
# Per-platform fetchers
# ---------------------------------------------------------------------------

def _load_ig_cookies() -> Optional[list]:
    """
    Load Instagram session cookies from IG_AUTH_COOKIES env var if set.
    Format: JSON array of {name, value, domain} objects. Used to authenticate
    Apify's IG scrapers for age-gated content (alcohol brands, etc.).

    To populate:
      1. Log into instagram.com with a DEDICATED throwaway account (not your
         personal account). DevTools → Application → Cookies → instagram.com.
      2. Copy sessionid, csrftoken, ds_user_id, ig_did, mid, datr values.
      3. Run: python pipeline/setup_ig_cookies.py  (interactive helper)
      4. Or write the JSON directly to ~/.zshrc:
           export IG_AUTH_COOKIES='[{"name":"sessionid","value":"...","domain":".instagram.com"}, ...]'

    Returns None if not set, the parsed list otherwise. Failures are silent —
    the caller falls through to unauthenticated mode if loading fails.
    """
    raw = os.environ.get("IG_AUTH_COOKIES", "").strip()
    if not raw:
        return None
    try:
        cookies = json.loads(raw)
        if not isinstance(cookies, list) or not cookies:
            return None
        return cookies
    except Exception:
        return None


# Apify Actors expose cookie auth under several field names depending on the
# specific Actor. We try them in order — whichever the Actor accepts will work.
COOKIE_FIELD_CANDIDATES = (
    "instagramAccessCookies",   # apify/instagram-scraper (most common)
    "sessionCookies",
    "cookies",
)


def _build_input(template: dict, url: str, sample_size: Optional[int]) -> dict:
    """Substitute __URL__ in the input template, set the sample size if applicable."""
    out = json.loads(json.dumps(template))  # deep copy

    def replace(node):
        if isinstance(node, dict):
            return {k: replace(v) for k, v in node.items()}
        if isinstance(node, list):
            return [replace(v) for v in node]
        if node == "__URL__":
            return url
        return node

    out = replace(out)
    if sample_size is not None:
        # Common keys across Actors. Set whichever exists in the template.
        for key in ("resultsLimit", "commentsPerPost", "resultsPerPage"):
            if key in out:
                out[key] = sample_size
                break

    # If IG cookies are set in the env, inject them into the input so the Actor
    # can authenticate and access age-gated content (alcohol brands, etc.).
    # We inject under the most-common field name; if a specific Actor needs a
    # different one, add the appropriate key to the template in social_actors.json.
    cookies = _load_ig_cookies()
    if cookies and "directUrls" in out:  # IG path (TikTok template uses postURLs)
        # Add to all candidate field names — Apify Actors ignore unknown fields,
        # so this safely accommodates whichever name the Actor expects.
        for field in COOKIE_FIELD_CANDIDATES:
            out[field] = cookies
    return out


def parse_instagram_metadata(items: list[dict]) -> dict:
    """Normalize Instagram post-detail response into our schema.

    Important: Instagram's API returns two distinct view fields and they mean
    different things for collab posts:
      - `videoPlayCount` — the combined number Instagram surfaces publicly.
         For a Messi-Adidas collab post this is ~62M.
      - `videoViewCount` — the brand-owner-side number, lower. Same post: ~17M.
    Prefer `videoPlayCount` because it matches what the audience actually sees
    in Instagram's UI (and matches the editorial intent of measuring reach).
    """
    if not items:
        return {}
    item = items[0]
    return {
        "platform": "instagram",
        "url": item.get("url") or item.get("inputUrl") or "",
        "views": item.get("videoPlayCount") or item.get("videoViewCount") or item.get("playsCount"),
        # Surface both fields for transparency / debugging — collab posts diverge wildly
        "_videoPlayCount":  item.get("videoPlayCount"),
        "_videoViewCount":  item.get("videoViewCount"),
        "likes": item.get("likesCount") or item.get("likes"),
        "commentCount": item.get("commentsCount") or item.get("comments"),
        "publishedAt": item.get("timestamp") or item.get("takenAtTimestamp"),
        "caption": (item.get("caption") or "")[:500],
        "ownerUsername": item.get("ownerUsername") or item.get("ownerHandle"),
        "fetchedAt": now_iso(),
    }


def parse_instagram_comments(items: list[dict], limit: int) -> list[dict]:
    """Normalize Instagram comment response."""
    out = []
    for c in items[:limit]:
        text = c.get("text") or c.get("comment") or ""
        if not text:
            continue
        out.append({
            "id": c.get("id") or c.get("commentId"),
            "text": text,
            "likes": int(c.get("likesCount") or c.get("likes") or 0),
            "owner": (c.get("ownerUsername") or c.get("owner") or "")[:80],
            "timestamp": c.get("timestamp") or c.get("createdAt"),
        })
    return out


def parse_tiktok_metadata(items: list[dict]) -> dict:
    if not items:
        return {}
    item = items[0]
    return {
        "platform": "tiktok",
        "url": item.get("webVideoUrl") or item.get("url") or "",
        "views": item.get("playCount") or item.get("plays"),
        "likes": item.get("diggCount") or item.get("likes"),
        "commentCount": item.get("commentCount") or item.get("comments"),
        "shares": item.get("shareCount") or item.get("shares"),
        "publishedAt": item.get("createTimeISO") or item.get("createTime"),
        "caption": (item.get("text") or "")[:500],
        "ownerUsername": (item.get("authorMeta") or {}).get("name"),
        "fetchedAt": now_iso(),
    }


def parse_tiktok_comments(items: list[dict], limit: int) -> list[dict]:
    out = []
    for c in items[:limit]:
        text = c.get("text") or c.get("comment") or ""
        if not text:
            continue
        out.append({
            "id": c.get("cid") or c.get("commentId") or c.get("id"),
            "text": text,
            "likes": int(c.get("diggCount") or c.get("likes") or 0),
            "owner": (c.get("uniqueId") or c.get("user") or "")[:80],
            "timestamp": c.get("createTime"),
        })
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def load_actors() -> dict:
    return json.loads(ACTORS_FILE.read_text())


def rank_ads_by_score() -> list[str]:
    """Return ad IDs ordered by current composite score (data.json), best first."""
    if not DATA_FILE.exists():
        return []
    try:
        data = json.loads(DATA_FILE.read_text())
    except Exception:
        return []
    # data.json's ads don't have score (computed client-side), so we approximate:
    # rank by views as a stand-in. Good enough for top-10 selection.
    ads = data.get("ads", [])
    ads_sorted = sorted(ads, key=lambda a: int(a.get("views") or 0), reverse=True)
    return [a["id"] for a in ads_sorted]


def _social_urls(ad: dict, platform: str) -> list[str]:
    """
    Return the list of URLs configured for a platform on an ad — brand-owned
    posts AND paid talent reposts together. Apify fetches both so the totals
    on the leaderboard reflect the campaign's full paid distribution.

    Schema supports:
      - `instagramUrls`/`tiktokUrls`         — brand-owned posts (list of str)
      - `instagramUrl`/`tiktokUrl`           — singular, kept for backward compat
      - `talentIgUrls`/`talentTtUrls`        — paid talent posts (list of objects
                                                with shape {"url": ..., "talent": ...})
    """
    key_plural   = f"{platform}Urls"
    key_singular = f"{platform}Url"
    urls = ad.get(key_plural) or []
    if isinstance(urls, str):  # tolerate misuse
        urls = [urls]
    single = ad.get(key_singular)
    if single and single not in urls:
        urls = list(urls) + [single]

    # Add talent posts. Stored as a separate array of objects with talent
    # attribution; here we flatten to just the URLs so the fetch loop processes
    # them identically. The talent attribution is preserved in ads.json for
    # future per-post display in the expand row.
    talent_key = "talentIgUrls" if platform == "instagram" else "talentTtUrls"
    for item in (ad.get(talent_key) or []):
        u = item.get("url") if isinstance(item, dict) else item
        if u and u not in urls:
            urls.append(u)

    return [u for u in urls if u]


def _has_any_social(ad: dict) -> bool:
    return bool(_social_urls(ad, "instagram")) or bool(_social_urls(ad, "tiktok"))


def _talent_for_url(ad: dict, platform: str, url: str) -> str | None:
    """If this URL was added as a paid talent post, return the talent's name.
    Otherwise None (i.e. it's a brand-owned post)."""
    talent_key = "talentIgUrls" if platform == "instagram" else "talentTtUrls"
    for item in (ad.get(talent_key) or []):
        if isinstance(item, dict) and item.get("url") == url:
            return item.get("talent")
    return None


def select_ads_for_run(all_ads: list[dict], tier: str, max_ads: Optional[int]) -> tuple[list[dict], list[dict]]:
    """
    Split ads into (full_sample_targets, metadata_only_targets) based on tier choice.
    Only returns ads that have at least one IG or TT URL configured.
    """
    have_social = [a for a in all_ads if _has_any_social(a)]
    if max_ads:
        have_social = have_social[:max_ads]

    if tier == "all":
        return have_social, []

    # tier == "top" — top-N by score, rest get metadata-only
    ranking = rank_ads_by_score()
    pos = {ad_id: i for i, ad_id in enumerate(ranking)}

    def rank_of(ad):
        return pos.get(ad["id"], 9999)

    have_social.sort(key=rank_of)
    return have_social[:TOP_TIER_COUNT], have_social[TOP_TIER_COUNT:]


def _manual_data_for_url(ad: dict, platform: str, url: str) -> Optional[dict]:
    """
    Return manually-entered metrics for a URL, if any.

    Schema in ads.json (optional, per-ad):
        "manualIgData": [
          {"url": "https://...", "views": 4800000, "likes": 43600, "comments": 265},
          ...
        ]
        "manualTtData": [ ... ]  (same shape)

    This exists for content Apify can't scrape — primarily age-gated alcohol
    brands where Instagram returns "Restricted profile" regardless of cookies.
    Manual entries take precedence over Apify-scraped values: if a manual
    entry exists for a URL, the Apify call is skipped entirely (saving cost).
    """
    key = "manualIgData" if platform == "instagram" else "manualTtData"
    for entry in (ad.get(key) or []):
        if entry.get("url") == url:
            return {
                "platform": platform,
                "url": url,
                "views": entry.get("views"),
                "likes": entry.get("likes"),
                "commentCount": entry.get("comments"),
                "_source": "manual",
                "fetchedAt": now_iso(),
            }
    return None


def _fetch_one_metadata(platform: str, url: str, *, client, actors, cache,
                        spend, dry_run, force_refresh, ad_id) -> tuple[dict | None, list[str], bool]:
    """Fetch metadata for one URL. Returns (parsed, errors, fromCache)."""
    meta_actor = actors[platform]["metadata"]["actor"]
    meta_input = _build_input(actors[platform]["metadata"]["input"], url, None)
    cache_key  = f"{platform}_meta_{url_hash(url)}"

    cached = None if force_refresh else cache.get(cache_key)
    if cached is not None:
        return cached, [], True
    if dry_run:
        return {"_dryRun": True, "url": url}, [], False
    if not spend.can_spend(spend.month_total()):
        return None, ["hard cap reached, skipping"], False
    try:
        items, cost = client.run_actor_sync(meta_actor, meta_input)
        parser = parse_instagram_metadata if platform == "instagram" else parse_tiktok_metadata
        parsed = parser(items)
        cache.put(cache_key, parsed)
        spend.add(cost, actor=meta_actor, ad_id=ad_id, platform=platform)
        return parsed, [], False
    except ApifyError as e:
        return None, [f"metadata: {e}"], False


def _fetch_one_comments(platform: str, url: str, sample_size: int, *,
                        client, actors, cache, spend, dry_run, force_refresh, ad_id) -> tuple[list[dict], list[str], bool]:
    cmt_actor = actors[platform]["comments"]["actor"]
    cmt_input = _build_input(actors[platform]["comments"]["input"], url, sample_size)
    cache_key = f"{platform}_cmt_{url_hash(url)}_n{sample_size}"

    cached = None if force_refresh else cache.get(cache_key)
    if cached is not None:
        return cached, [], True
    if dry_run:
        return [], [], False
    if not spend.can_spend(spend.month_total()):
        return [], ["hard cap reached, skipping comments"], False
    try:
        items, cost = client.run_actor_sync(cmt_actor, cmt_input)
        parser = parse_instagram_comments if platform == "instagram" else parse_tiktok_comments
        comments = parser(items, sample_size)
        cache.put(cache_key, comments)
        spend.add(cost, actor=cmt_actor, ad_id=ad_id, platform=platform)
        return comments, [], False
    except ApifyError as e:
        return [], [f"comments: {e}"], False


def _aggregate_metadata(per_url_meta: list[dict]) -> dict:
    """Sum views/likes/commentCount across cutdowns of the same campaign."""
    agg = {"views": 0, "likes": 0, "commentCount": 0, "shares": 0}
    nonzero = False
    for m in per_url_meta:
        if not m or m.get("_dryRun"):
            continue
        for k in ("views", "likes", "commentCount", "shares"):
            v = m.get(k)
            if v:
                agg[k] += int(v)
                nonzero = True
    if not nonzero:
        return {}
    # Surface counts for transparency
    agg["urlCount"] = len([m for m in per_url_meta if m and not m.get("_dryRun")])
    agg["fetchedAt"] = now_iso()
    return agg


def process_ad(
    ad: dict,
    *,
    client: Optional[ApifyClient],
    actors: dict,
    cache: Cache,
    spend: SpendTracker,
    sample_size: int,
    metadata_only: bool,
    force_refresh: bool,
    dry_run: bool,
) -> dict:
    """
    Process one ad. Iterates over each URL configured for each platform
    (supports cutdown aggregation), fetches metadata + comments per URL,
    and writes both per-URL records and an aggregated total per platform.
    """
    result = {"adId": ad["id"], "brand": ad["brand"], "platforms": {}}

    for platform in ("instagram", "tiktok"):
        urls = _social_urls(ad, platform)
        if not urls:
            continue

        per_url = []          # per-URL detail (transparency)
        all_comments = []     # comments deduped across URLs
        seen_ids = set()
        errors  = []
        any_cache_hit = False

        for url in urls:
            # Manual override wins. If the ad has manualIgData/manualTtData with
            # an entry for this URL, use those numbers directly and skip Apify
            # entirely (saves cost AND works for age-gated alcohol content).
            talent = _talent_for_url(ad, platform, url)
            manual = _manual_data_for_url(ad, platform, url)
            if manual:
                entry = {"url": url, "metadata": manual, "commentSample": 0}
                if talent: entry["talent"] = talent
                per_url.append(entry)
                talent_tag = f" [talent: {talent}]" if talent else ""
                log(f"  {ad['id']:7s} {platform[:2].upper()} {url[-22:]}  v={manual.get('views')} l={manual.get('likes')} c={manual.get('commentCount')}  (manual){talent_tag}")
                continue

            meta, m_errs, m_cache = _fetch_one_metadata(
                platform, url, client=client, actors=actors, cache=cache,
                spend=spend, dry_run=dry_run, force_refresh=force_refresh,
                ad_id=ad["id"],
            )
            errors.extend([f"{url}: {e}" for e in m_errs])
            if m_cache: any_cache_hit = True

            cmts: list[dict] = []
            if not metadata_only and sample_size > 0:
                cmts, c_errs, c_cache = _fetch_one_comments(
                    platform, url, sample_size,
                    client=client, actors=actors, cache=cache,
                    spend=spend, dry_run=dry_run, force_refresh=force_refresh,
                    ad_id=ad["id"],
                )
                errors.extend([f"{url}: {e}" for e in c_errs])
                if c_cache: any_cache_hit = True
                for c in cmts:
                    cid = c.get("id")
                    if cid and cid in seen_ids:
                        continue
                    if cid: seen_ids.add(cid)
                    all_comments.append(c)

            entry = {"url": url, "metadata": meta or {}, "commentSample": len(cmts)}
            if talent: entry["talent"] = talent
            per_url.append(entry)
            if meta and not meta.get("_dryRun"):
                v = meta.get("views"); l = meta.get("likes"); c = meta.get("commentCount")
                talent_tag = f" [talent: {talent}]" if talent else ""
                log(f"  {ad['id']:7s} {platform[:2].upper()} {url[-22:]}  v={v} l={l} c={c}{talent_tag}")

        # Aggregate across all URLs for this platform
        agg = _aggregate_metadata([p["metadata"] for p in per_url])

        if agg:
            log(f"  {ad['id']:7s} {platform[:2].upper()} TOTAL across {agg.get('urlCount',0)} url(s): "
                f"views={agg.get('views')} likes={agg.get('likes')} comments={agg.get('commentCount')}")

        result["platforms"][platform] = {
            "metadata":   agg,
            "perUrl":     per_url,            # transparency: which URL contributed what
            "comments":   all_comments,        # deduplicated across URLs
            "fromCache":  any_cache_hit,
            "errors":     errors,
        }

    return result


def write_output(results: list[dict]):
    """
    Merge new results into the existing social_data.json — do NOT overwrite.
    Earlier behaviour was to clobber the file with whatever this run produced,
    so a partial run (e.g. `--only mch-01`) silently wiped every other ad's
    social metrics. The merge below keeps previous entries intact.

    Also appends today's snapshot to social_history.json for velocity tracking.
    """
    existing_by_id = {}
    if OUTPUT_FILE.exists():
        try:
            prev = json.loads(OUTPUT_FILE.read_text())
            for ad in prev.get("ads", []):
                existing_by_id[ad["adId"]] = ad
        except Exception:
            pass

    # Overwrite entries for ads we processed this run
    for r in results:
        existing_by_id[r["adId"]] = r

    merged_ads = list(existing_by_id.values())
    payload = {
        "generated": now_iso(),
        "adCount": len(merged_ads),
        "ads": merged_ads,
    }
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False))

    # Append today's totals to the velocity history file
    _append_to_social_history(merged_ads)


def _append_to_social_history(ads: list[dict]):
    """
    Append a fresh snapshot of per-ad cross-platform totals to social_history.json,
    then prune anything older than SOCIAL_HISTORY_RETENTION_DAYS.

    The front-end reads this file to compute trailing-7-day cross-platform velocity.
    Snapshot shape (one entry per pipeline run):
        {
          "timestamp": "2026-05-15T17:00:00Z",
          "ads": [
            {"adId":"adi-01", "ig":{"views":152937285,"likes":...,"comments":...},
                              "tt":{"views":5100000,"likes":...,"comments":...}}
          ]
        }
    Only ads that have populated metadata on at least one platform are recorded.
    Ads without any usable data are omitted from the snapshot rather than recording
    zeros, which would distort velocity if they later appear with real numbers.
    """
    snapshot_ads = []
    for ad in ads:
        platforms = ad.get("platforms") or {}
        ig_meta   = (platforms.get("instagram") or {}).get("metadata") or {}
        tt_meta   = (platforms.get("tiktok")    or {}).get("metadata") or {}
        ig_views  = ig_meta.get("views")
        tt_views  = tt_meta.get("views")
        # Only record platforms that actually have a view count
        entry = {"adId": ad["adId"]}
        if ig_views is not None and ig_views > 0:
            entry["ig"] = {
                "views":    ig_views,
                "likes":    ig_meta.get("likes") or 0,
                "comments": ig_meta.get("commentCount") or 0,
            }
        if tt_views is not None and tt_views > 0:
            entry["tt"] = {
                "views":    tt_views,
                "likes":    tt_meta.get("likes") or 0,
                "comments": tt_meta.get("commentCount") or 0,
            }
        # Skip entries with no useful data — recording zeros would corrupt velocity
        if "ig" in entry or "tt" in entry:
            snapshot_ads.append(entry)

    if not snapshot_ads:
        return

    history = []
    if HISTORY_FILE.exists():
        try:
            history = json.loads(HISTORY_FILE.read_text())
        except Exception:
            history = []

    history.append({
        "timestamp": now_iso(),
        "ads": snapshot_ads,
    })

    # Prune snapshots older than retention window
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SOCIAL_HISTORY_RETENTION_DAYS)).isoformat()
    history = [s for s in history if s.get("timestamp", "") >= cutoff]

    HISTORY_FILE.write_text(json.dumps(history, indent=2, ensure_ascii=False))
    log(f"  ↪ social_history.json: appended snapshot ({len(snapshot_ads)} ads), {len(history)} snapshots retained", prefix="")

    # With the new snapshot appended, compute trailing-window deltas per ad/platform
    # and bake them into social_data.json so the front-end can read viewsLast7Days
    # without needing to fetch + parse the history file itself.
    _inject_trailing_window_into_output(history)


VELOCITY_WINDOW_DAYS_SOCIAL = 7


def _find_snapshot_n_days_ago(history: list[dict], target_days: int) -> dict | None:
    """
    Return the snapshot closest to `target_days` ago. Uses the one within
    ±2 days of the target if available; returns None otherwise (front-end
    falls back to lifetime average).
    """
    if not history:
        return None
    target = datetime.now(timezone.utc) - timedelta(days=target_days)
    best = None
    best_gap = timedelta(days=999)
    for snap in history:
        try:
            ts = datetime.fromisoformat(snap["timestamp"].replace("Z", "+00:00"))
        except Exception:
            continue
        gap = abs(ts - target)
        if gap < best_gap:
            best = snap
            best_gap = gap
    # Only return if it's within ±2 days of target — wider gaps make velocity meaningless
    if best and best_gap <= timedelta(days=2):
        return best
    return None


def _inject_trailing_window_into_output(history: list[dict]):
    """
    For each ad in social_data.json, compute trailing-7-day delta per platform
    (IG + TT) and store as `viewsLast7Days` inside each platform's metadata.

    Front-end will combine this with the YouTube `viewsLast7Days` field
    (already produced by update_metrics.py) for cross-platform velocity.
    """
    if not OUTPUT_FILE.exists():
        return

    try:
        data = json.loads(OUTPUT_FILE.read_text())
    except Exception:
        return

    older = _find_snapshot_n_days_ago(history, VELOCITY_WINDOW_DAYS_SOCIAL)
    if not older:
        # Not enough history yet — clear any existing fields so we don't show stale numbers
        for ad in data.get("ads", []):
            for plat_key in ("instagram", "tiktok"):
                plat = (ad.get("platforms") or {}).get(plat_key) or {}
                meta = plat.get("metadata")
                if meta and "viewsLast7Days" in meta:
                    meta.pop("viewsLast7Days", None)
                    meta.pop("velocityWindowDays", None)
        OUTPUT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        log(f"  ↪ Not enough history yet for trailing-7d velocity (need a snapshot ~7 days old)", prefix="")
        return

    # Map adId → {ig_views, tt_views} from the N-days-ago snapshot
    older_by_id = {}
    for entry in older.get("ads", []):
        older_by_id[entry["adId"]] = {
            "ig": (entry.get("ig") or {}).get("views"),
            "tt": (entry.get("tt") or {}).get("views"),
        }

    # Actual elapsed days between "now" and the older snapshot (may be 6.x or 7.x)
    try:
        older_ts = datetime.fromisoformat(older["timestamp"].replace("Z", "+00:00"))
        elapsed_days = max(1.0, (datetime.now(timezone.utc) - older_ts).total_seconds() / 86400.0)
    except Exception:
        elapsed_days = VELOCITY_WINDOW_DAYS_SOCIAL

    annotated = 0
    for ad in data.get("ads", []):
        ad_id = ad.get("adId")
        prev = older_by_id.get(ad_id, {})
        for plat_key, prev_key in (("instagram", "ig"), ("tiktok", "tt")):
            plat = (ad.get("platforms") or {}).get(plat_key) or {}
            meta = plat.get("metadata")
            if not meta:
                continue
            current = meta.get("views")
            previous = prev.get(prev_key)
            if current is None or previous is None:
                # Either no current data or no historical baseline — leave fields blank
                meta.pop("viewsLast7Days", None)
                meta.pop("velocityWindowDays", None)
                continue
            delta = max(0, int(current) - int(previous))
            meta["viewsLast7Days"] = delta
            meta["velocityWindowDays"] = round(elapsed_days, 2)
            annotated += 1

    OUTPUT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    log(f"  ↪ Trailing-{VELOCITY_WINDOW_DAYS_SOCIAL}d velocity annotated on {annotated} platform entries (window: {elapsed_days:.1f} days)", prefix="")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pull Instagram + TikTok metrics + comment samples via Apify."
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Don't call Apify; print plan + estimated cost.")
    p.add_argument("--metadata-only", action="store_true",
                   help="Skip comment sampling for all ads (cheap mode).")
    p.add_argument("--force-refresh", action="store_true",
                   help="Bypass the 7-day cache.")
    p.add_argument("--tier", choices=["all", "top"], default="top",
                   help="Which ads get full sampling. Default: top (top-10 by views).")
    p.add_argument("--validate-token", action="store_true",
                   help="Make one cheap call to verify auth, then exit.")
    p.add_argument("--max-ads", type=int, default=None,
                   help="Process at most N ads (testing).")
    p.add_argument("--only", type=str, default=None,
                   help="Comma-separated ad IDs to process (overrides --tier and --max-ads). "
                        "e.g. --only bud-01,jim-01,don-01 to test the alcohol brands.")
    return p.parse_args()


def validate_token(client: ApifyClient) -> int:
    """Hit the user's account info endpoint — costs nothing."""
    try:
        r = client.session.get(f"{API_BASE}/users/me?token={client.token}", timeout=HTTP_TIMEOUT)
        if r.status_code < 400:
            data = r.json().get("data", {})
            log(f"OK — Apify auth working. user={data.get('username')} plan={data.get('plan')}", prefix="")
            return 0
        log(f"FAIL — {r.status_code}: {r.text[:300]}", prefix="")
        return 1
    except Exception as e:
        log(f"FAIL — {e}", prefix="")
        return 1


def main() -> int:
    args = parse_args()
    token = os.environ.get("APIFY_API_TOKEN", "")

    if args.validate_token:
        if not token:
            log("APIFY_API_TOKEN is not set in environment.", prefix="")
            return 2
        return validate_token(ApifyClient(token))

    actors = load_actors()
    spend  = SpendTracker()
    cache  = Cache()
    ads    = json.loads(ADS_FILE.read_text())

    if args.only:
        # Override tier/max-ads: process exactly the ad IDs the user named.
        wanted_ids = {x.strip() for x in args.only.split(",") if x.strip()}
        full_targets = [a for a in ads if a['id'] in wanted_ids and _has_any_social(a)]
        missing = wanted_ids - {a['id'] for a in full_targets}
        if missing:
            log(f"⚠  Requested IDs not found or have no social URLs: {sorted(missing)}", prefix="")
        meta_only_targets = []
    else:
        full_targets, meta_only_targets = select_ads_for_run(ads, args.tier, args.max_ads)

    monthly = spend.month_total()
    sample_size = spend.sample_size_for(monthly)
    if args.metadata_only:
        sample_size = 0

    auth_mode = "ENABLED — using IG session cookies" if _load_ig_cookies() else "disabled — anonymous scraping"
    log("", prefix="")
    log(f"Apify monthly spend so far: ${monthly:.2f} (cap ${HARD_CAP:.2f})", prefix="")
    log(f"Adaptive sample size:       {sample_size} comments per platform", prefix="")
    log(f"Full-sample ads:            {len(full_targets)}", prefix="")
    log(f"Metadata-only ads:          {len(meta_only_targets)}", prefix="")
    log(f"IG authenticated mode:      {auth_mode}", prefix="")
    log(f"Dry run:                    {args.dry_run}", prefix="")
    log("", prefix="")

    if not args.dry_run and not token:
        log("APIFY_API_TOKEN is not set. Aborting (use --dry-run to plan without spending).", prefix="")
        return 2

    client = None if args.dry_run else ApifyClient(token)
    results = []

    log("=== Full-sample tier ===", prefix="")
    for ad in full_targets:
        try:
            r = process_ad(ad, client=client, actors=actors, cache=cache, spend=spend,
                           sample_size=sample_size, metadata_only=args.metadata_only,
                           force_refresh=args.force_refresh, dry_run=args.dry_run)
            results.append(r)
        except Exception as e:
            log(f"  {ad.get('id')} FAILED: {e}")

    log("", prefix="")
    log("=== Metadata-only tier ===", prefix="")
    for ad in meta_only_targets:
        try:
            r = process_ad(ad, client=client, actors=actors, cache=cache, spend=spend,
                           sample_size=0, metadata_only=True,
                           force_refresh=args.force_refresh, dry_run=args.dry_run)
            results.append(r)
        except Exception as e:
            log(f"  {ad.get('id')} FAILED: {e}")

    if not args.dry_run:
        write_output(results)
        log("", prefix="")
        log(f"✓ Wrote {OUTPUT_FILE.relative_to(ROOT)}", prefix="")

    log(f"✓ Apify monthly spend now: ${spend.month_total():.4f}", prefix="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
