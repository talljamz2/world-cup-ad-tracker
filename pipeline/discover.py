#!/usr/bin/env python3
"""
The Ad Podium — Discovery pipeline.

Combines two candidate sources into a unified review queue:

  1. Brand-channel watch — recent uploads on each watchlist channel,
     scored against tournament keyword tiers. Catches guerrilla / non-FIFA
     marketers (Nike, Pepsi, etc.) the moment they post, even when they
     can't legally use FIFA marks.

  2. Tracker scrape — YouTube embeds extracted from The Drum and Brand
     Innovators 2026 ad trackers. The press tier — what the industry
     itself has flagged as worth covering.

Outputs:
  pipeline/candidates.json   — ordered review queue, highest-confidence first
  pipeline/.channel_cache.json — resolved @handle → channelId mappings (transient)

Quota cost per run: ~120 units first run (resolve + fetch), ~60 units subsequent
runs (resolves are cached). Free tier is 10,000 units/day.

Usage:
    YOUTUBE_API_KEY=... python pipeline/discover.py
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import requests


API_BASE = "https://www.googleapis.com/youtube/v3"
ROOT = Path(__file__).resolve().parent.parent
WATCHLIST_FILE = ROOT / "pipeline" / "watchlist.json"
KEYWORDS_FILE  = ROOT / "pipeline" / "keywords.json"
ADS_FILE       = ROOT / "pipeline" / "ads.json"
REJECTED_FILE  = ROOT / "pipeline" / "rejected.json"
CANDIDATES_FILE = ROOT / "pipeline" / "candidates.json"
SUGGESTIONS_FILE = ROOT / "pipeline" / "channel_suggestions.json"
CHANNEL_CACHE    = ROOT / "pipeline" / ".channel_cache.json"
CHANNEL_DENYLIST = ROOT / "pipeline" / "channel_denylist.json"

# How many recent uploads to inspect per channel (Phase 1).
RECENT_VIDEO_LIMIT = 20

# Phase 3 (search): only consider videos published in the last N days.
SEARCH_RECENT_DAYS = 30

# Brand-likeness heuristic threshold for unknown channels surfaced via search.
SUGGESTION_MIN_SUBSCRIBERS = 50_000

TRACKER_SOURCES = [
    {"name": "thedrum",         "url": "https://www.thedrum.com/news/world-cup-2026-watch-all-the-latest-ads"},
    {"name": "brandinnovators", "url": "https://brand-innovators.com/brand-innovators-fifa-world-cup-ad-tracker-2026/"},
    {"name": "adsoftheworld",   "url": "https://www.adsoftheworld.com/collections/2026-fifa-world-cup"},
    {"name": "system1",         "url": "https://system1group.com/blog/worldcup2026tracker"},
]

# Title and channel patterns that mark a search result as obvious noise.
# Tunable — extend as new false positives surface.
NOISE_TITLE_PATTERNS = [
    r"\bhighlights?\b",
    r"\brecap\b",
    r"\bfull match\b",
    r"\breaction\b",
    r"\bwatching\b",
    r"\blive stream\b",
    r"\bpredict",
    r"\btop \d+\b",
    r"\bvs\.?\b",
    r"\bbreakdown\b",
    r"\btactical analysis\b",
]
NOISE_CHANNEL_PATTERNS = [
    r"\bnews\b",
    r"\bhighlights\b",
    r"\bfan tv\b",
    r"\bsports tv\b",
    r"\bfooty\b",
    r"\bpundit",
    r"\bbreakdown\b",
    r"\bpredictions?\b",
]

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

YOUTUBE_ID_PATTERNS = [
    re.compile(r"youtube\.com/(?:watch\?v=|embed/)([A-Za-z0-9_-]{11})"),
    re.compile(r"youtu\.be/([A-Za-z0-9_-]{11})"),
]


# ---------------------------------------------------------------------------
# YouTube API
# ---------------------------------------------------------------------------

def load_channel_cache() -> dict:
    if CHANNEL_CACHE.exists():
        try:
            return json.loads(CHANNEL_CACHE.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def save_channel_cache(cache: dict) -> None:
    CHANNEL_CACHE.write_text(json.dumps(cache, indent=2))


def resolve_channel(handle: str, api_key: str, cache: dict) -> dict | None:
    """Resolve @handle → {channelId, channelTitle, uploadsPlaylistId}.
    Cached on disk to avoid burning quota on repeat lookups."""
    if handle in cache:
        return cache[handle]

    handle_clean = handle.lstrip("@")
    r = requests.get(
        f"{API_BASE}/channels",
        params={
            "part": "id,contentDetails,snippet",
            "forHandle": f"@{handle_clean}",
            "key": api_key,
        },
        timeout=30,
    )
    if r.status_code != 200:
        cache[handle] = None
        return None
    items = r.json().get("items", [])
    if not items:
        cache[handle] = None
        return None
    item = items[0]
    result = {
        "channelId": item["id"],
        "channelTitle": item["snippet"]["title"],
        "uploadsPlaylistId": item["contentDetails"]["relatedPlaylists"]["uploads"],
    }
    cache[handle] = result
    return result


def fetch_recent_uploads(uploads_playlist_id: str, api_key: str,
                         max_results: int = RECENT_VIDEO_LIMIT) -> list[dict]:
    """Returns up to `max_results` recent videos from a channel's uploads playlist."""
    r = requests.get(
        f"{API_BASE}/playlistItems",
        params={
            "part": "snippet,contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": min(max_results, 50),
            "key": api_key,
        },
        timeout=30,
    )
    if r.status_code != 200:
        return []
    out = []
    for item in r.json().get("items", []):
        s = item["snippet"]
        out.append({
            "videoId": item["contentDetails"]["videoId"],
            "title": s.get("title", ""),
            "description": s.get("description", ""),
            "publishedAt": s.get("publishedAt", ""),
            "channelTitle": s.get("channelTitle", ""),
        })
    return out


# ---------------------------------------------------------------------------
# Keyword scoring
# ---------------------------------------------------------------------------

def parse_published_date(iso_ts: str) -> date | None:
    if not iso_ts:
        return None
    try:
        return datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def score_video(video: dict, keywords: dict) -> tuple | None:
    """Return (confidence, reason, hits) or None.

    Confidence tiers:
      high   — explicit FIFA-trademarked terms (only sponsors will use these)
      medium — tournament-coded language without trademarks (anyone can use)
      low    — football vocabulary OR a tournament-bound player name, AND
               published inside the tournament time window
    """
    text = (video.get("title", "") + " " + video.get("description", "")).lower()

    # Negative keywords — drop highlights / press / podcast clips before scoring
    if any(neg.lower() in text for neg in keywords.get("_negativeKeywords", [])):
        return None

    high_hits = [k for k in keywords["high"] if k.lower() in text]
    if high_hits:
        return ("high", "explicit_fifa", high_hits)

    med_hits = [k for k in keywords["medium"] if k.lower() in text]
    if med_hits:
        return ("medium", "tournament_coded", med_hits)

    pub_date = parse_published_date(video.get("publishedAt"))
    if pub_date is None:
        return None
    win = keywords["tournamentWindow"]
    if not (date.fromisoformat(win["start"]) <= pub_date <= date.fromisoformat(win["end"])):
        return None

    football_hits = [k for k in keywords["footballTerms"] if k.lower() in text]
    player_hits = [k for k in keywords["playerNames"] if k.lower() in text]
    if football_hits or player_hits:
        return ("low", "football_in_window", football_hits + player_hits)

    return None


# ---------------------------------------------------------------------------
# Tracker scraping
# ---------------------------------------------------------------------------

def scrape_tracker(source: dict) -> list[dict]:
    """Pull YouTube IDs and surrounding text context from an ad-tracker page."""
    try:
        r = requests.get(source["url"], headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  ⚠  Failed to fetch {source['name']}: {e}", file=sys.stderr)
        return []

    html = r.text
    found_ids: set[str] = set()
    for pattern in YOUTUBE_ID_PATTERNS:
        for m in pattern.finditer(html):
            found_ids.add(m.group(1))

    out = []
    for vid_id in found_ids:
        ctx_match = re.search(
            r"(.{0,800})(?:youtube\.com/(?:watch\?v=|embed/)|youtu\.be/)" + re.escape(vid_id),
            html,
            re.DOTALL,
        )
        ctx_clean = ""
        if ctx_match:
            ctx_text = ctx_match.group(1)
            ctx_text = re.sub(r"<[^>]+>", " ", ctx_text)
            ctx_text = re.sub(r"\s+", " ", ctx_text).strip()
            ctx_clean = ctx_text[-400:]
        out.append({"videoId": vid_id, "context": ctx_clean})
    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fuzzy_match_brand(brand: str, channel_title: str) -> bool:
    """Loose alphanumeric containment check between brand name and channel title."""
    if not channel_title:
        return False
    a = re.sub(r"[^a-z0-9]", "", brand.lower())
    b = re.sub(r"[^a-z0-9]", "", channel_title.lower())
    return bool(a) and bool(b) and (a in b or b in a)


def load_skip_ids() -> set[str]:
    """Return the set of YouTube IDs we already track or have rejected."""
    skip: set[str] = set()
    if ADS_FILE.exists():
        for ad in json.loads(ADS_FILE.read_text()):
            if ad.get("youtubeId"):
                skip.add(ad["youtubeId"])
    if REJECTED_FILE.exists():
        try:
            rejected = json.loads(REJECTED_FILE.read_text())
            for r in rejected:
                if isinstance(r, dict) and r.get("youtubeId"):
                    skip.add(r["youtubeId"])
        except json.JSONDecodeError:
            pass
    return skip


# ---------------------------------------------------------------------------
# Phase 3 helpers — YouTube search
# ---------------------------------------------------------------------------

def search_youtube(query: str, api_key: str, published_after_iso: str,
                   max_results: int = 50) -> list[dict]:
    """Run a YouTube `search.list` query. Costs 100 quota units per call.
    Returns up to `max_results` recent video matches."""
    r = requests.get(
        f"{API_BASE}/search",
        params={
            "part": "snippet",
            "q": query,
            "type": "video",
            "publishedAfter": published_after_iso,
            "maxResults": min(max_results, 50),
            "order": "date",
            "key": api_key,
        },
        timeout=30,
    )
    if r.status_code != 200:
        return []
    out = []
    for item in r.json().get("items", []):
        if "videoId" not in item.get("id", {}):
            continue
        s = item["snippet"]
        out.append({
            "videoId": item["id"]["videoId"],
            "channelId": s.get("channelId"),
            "channelTitle": s.get("channelTitle", ""),
            "title": s.get("title", ""),
            "description": s.get("description", ""),
            "publishedAt": s.get("publishedAt", ""),
        })
    return out


def get_channel_details(channel_id: str, api_key: str, cache: dict) -> dict | None:
    """Fetch channel description and stats. 1 quota unit per uncached call."""
    if channel_id in cache:
        return cache[channel_id]
    r = requests.get(
        f"{API_BASE}/channels",
        params={"part": "snippet,statistics", "id": channel_id, "key": api_key},
        timeout=30,
    )
    if r.status_code != 200:
        cache[channel_id] = None
        return None
    items = r.json().get("items", [])
    if not items:
        cache[channel_id] = None
        return None
    item = items[0]
    info = {
        "channelId": item["id"],
        "title": item["snippet"]["title"],
        "description": item["snippet"].get("description", ""),
        "customUrl": item["snippet"].get("customUrl"),
        "subscriberCount": int(item["statistics"].get("subscriberCount", 0)),
        "videoCount": int(item["statistics"].get("videoCount", 0)),
        "viewCount": int(item["statistics"].get("viewCount", 0)),
    }
    cache[channel_id] = info
    return info


def looks_like_noise(title: str, channel_title: str) -> bool:
    """Cheap regex pass over title + channel name to drop highlights / news / fan content
    before we spend quota on channel-detail lookups."""
    text = (title or "").lower()
    if any(re.search(p, text) for p in NOISE_TITLE_PATTERNS):
        return True
    chan = (channel_title or "").lower()
    if any(re.search(p, chan) for p in NOISE_CHANNEL_PATTERNS):
        return True
    return False


def is_brand_like(channel: dict | None) -> bool:
    """Heuristic: would this channel plausibly be a brand we'd want to track?"""
    if not channel:
        return False
    title = (channel.get("title") or "").lower()
    if any(b in title for b in ["news", "highlights", "fan tv", "footy", "predictions", "tactical"]):
        return False
    if (channel.get("subscriberCount") or 0) < SUGGESTION_MIN_SUBSCRIBERS:
        return False
    return True


def known_channelid_to_brand(watchlist: list[dict], cache: dict) -> dict[str, dict]:
    """Reverse-map: channelId → {brand, tier, category, rivalOf} for watchlist entries
    we've already resolved in Phase 1's channel cache."""
    out: dict[str, dict] = {}
    for entry in watchlist:
        for handle in entry.get("channels", []):
            ch = cache.get(handle)
            if isinstance(ch, dict) and ch.get("channelId"):
                out[ch["channelId"]] = {
                    "brand": entry["brand"],
                    "tier": entry["tier"],
                    "category": entry["category"],
                    "rivalOf": entry.get("rivalOf"),
                }
    return out


def load_channel_denylist() -> set[str]:
    """ChannelIds explicitly rejected as not-a-brand. Never re-suggested."""
    if not CHANNEL_DENYLIST.exists():
        return set()
    try:
        data = json.loads(CHANNEL_DENYLIST.read_text())
        return {entry["channelId"] for entry in data if entry.get("channelId")}
    except (json.JSONDecodeError, KeyError, TypeError):
        return set()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        print("error: YOUTUBE_API_KEY not set in environment", file=sys.stderr)
        return 1

    watchlist = json.loads(WATCHLIST_FILE.read_text())
    keywords  = json.loads(KEYWORDS_FILE.read_text())
    skip_ids  = load_skip_ids()
    cache     = load_channel_cache()

    print(f"Watchlist: {len(watchlist)} brands, {sum(len(b.get('channels', [])) for b in watchlist)} channels")
    print(f"Already known (ads.json + rejected.json): {len(skip_ids)} videoIds will be skipped")
    print()

    candidates: dict[str, dict] = {}  # videoId → candidate

    # --- Phase 1: brand-channel watch ---
    print("=== Brand-channel watch ===")
    for brand in watchlist:
        any_videos = 0
        for handle in brand.get("channels", []):
            try:
                ch = resolve_channel(handle, api_key, cache)
            except requests.RequestException as e:
                print(f"  ⚠  {brand['brand']} {handle}: resolve failed ({e})")
                continue
            if not ch:
                continue
            try:
                videos = fetch_recent_uploads(ch["uploadsPlaylistId"], api_key)
            except requests.RequestException as e:
                print(f"  ⚠  {brand['brand']} {handle}: fetch failed ({e})")
                continue
            for v in videos:
                any_videos += 1
                if v["videoId"] in skip_ids:
                    continue
                scored = score_video(v, keywords)
                if scored is None:
                    continue
                confidence, reason, hits = scored
                cand = candidates.get(v["videoId"]) or {
                    "videoId": v["videoId"],
                    "title": v["title"],
                    "description": (v["description"] or "")[:300],
                    "channelTitle": v["channelTitle"],
                    "publishedAt": v["publishedAt"],
                    "expectedBrand": brand["brand"],
                    "tier": brand["tier"],
                    "category": brand["category"],
                    "rivalOf": brand.get("rivalOf"),
                    "watchUrl": f"https://www.youtube.com/watch?v={v['videoId']}",
                    "thumbnailUrl": f"https://i.ytimg.com/vi/{v['videoId']}/hqdefault.jpg",
                    "channelMatch": fuzzy_match_brand(brand["brand"], v["channelTitle"]),
                    "confidence": confidence,
                    "matchReason": reason,
                    "matchedKeywords": hits,
                    "sources": [],
                }
                tag = f"channel_watch:{brand['brand']}"
                if tag not in cand["sources"]:
                    cand["sources"].append(tag)
                # Take the strongest confidence signal seen so far.
                rank = {"high": 3, "medium": 2, "low": 1}
                if rank.get(confidence, 0) > rank.get(cand["confidence"], 0):
                    cand["confidence"] = confidence
                    cand["matchReason"] = reason
                    cand["matchedKeywords"] = hits
                candidates[v["videoId"]] = cand
        if any_videos:
            print(f"  ✓ {brand['brand']:<22} → scanned {any_videos} recent uploads")
        else:
            print(f"  ·  {brand['brand']:<22} → no uploads found (handle may be wrong)")

    save_channel_cache(cache)

    # --- Phase 2: tracker scrape ---
    print()
    print("=== Tracker scrape ===")
    for source in TRACKER_SOURCES:
        results = scrape_tracker(source)
        print(f"  {source['name']}: {len(results)} videoIds extracted")
        for r in results:
            vid_id = r["videoId"]
            if vid_id in skip_ids:
                continue
            existing = candidates.get(vid_id)
            tag = f"tracker:{source['name']}"
            if existing:
                if tag not in existing["sources"]:
                    existing["sources"].append(tag)
            else:
                candidates[vid_id] = {
                    "videoId": vid_id,
                    "title": "(unknown — discovered via tracker only)",
                    "description": (r["context"] or "")[-200:],
                    "channelTitle": "",
                    "publishedAt": "",
                    "expectedBrand": "(extract from context)",
                    "tier": None,
                    "category": None,
                    "rivalOf": None,
                    "watchUrl": f"https://www.youtube.com/watch?v={vid_id}",
                    "thumbnailUrl": f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg",
                    "channelMatch": None,
                    "confidence": "tracker_only",
                    "matchReason": f"Listed by {source['name']}",
                    "matchedKeywords": [],
                    "sources": [tag],
                }

    # --- Phase 3: YouTube search (broad keyword sweep) ---
    print()
    print("=== YouTube search ===")
    search_queries = keywords.get("searchQueries", [])
    suggestions: dict[str, dict] = {}
    if not search_queries:
        print("  (no searchQueries configured in keywords.json — skipping)")
    else:
        from datetime import timedelta
        published_after = (datetime.now(timezone.utc) - timedelta(days=SEARCH_RECENT_DAYS))\
            .isoformat(timespec="seconds").replace("+00:00", "Z")
        known = known_channelid_to_brand(watchlist, cache)
        denylist = load_channel_denylist()
        ch_details_cache: dict[str, dict] = {}

        for query in search_queries:
            try:
                results = search_youtube(query, api_key, published_after)
            except requests.RequestException as e:
                print(f"  ⚠  search '{query}' failed: {e}", file=sys.stderr)
                continue
            print(f"  search '{query}': {len(results)} results")
            for r in results:
                vid = r["videoId"]
                if vid in skip_ids:
                    continue
                if looks_like_noise(r["title"], r["channelTitle"]):
                    continue
                cid = r["channelId"]
                if not cid:
                    continue

                if cid in known:
                    # Channel is on the watchlist — fold into the candidates queue
                    info = known[cid]
                    cand = candidates.get(vid) or {
                        "videoId": vid,
                        "title": r["title"],
                        "description": (r["description"] or "")[:300],
                        "channelTitle": r["channelTitle"],
                        "publishedAt": r["publishedAt"],
                        "expectedBrand": info["brand"],
                        "tier": info["tier"],
                        "category": info["category"],
                        "rivalOf": info["rivalOf"],
                        "watchUrl": f"https://www.youtube.com/watch?v={vid}",
                        "thumbnailUrl": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
                        "channelMatch": True,
                        "confidence": "high",
                        "matchReason": "search_known_brand",
                        "matchedKeywords": [query],
                        "sources": [],
                    }
                    tag = f"search:{query[:40]}"
                    if tag not in cand["sources"]:
                        cand["sources"].append(tag)
                    candidates[vid] = cand
                    continue

                # Unknown channel — does it look brand-shaped?
                if cid in denylist:
                    continue
                ch_details = get_channel_details(cid, api_key, ch_details_cache)
                if not is_brand_like(ch_details):
                    continue

                if cid not in suggestions:
                    suggestions[cid] = {
                        "channelId": cid,
                        "channelTitle": r["channelTitle"],
                        "channelUrl": f"https://www.youtube.com/channel/{cid}",
                        "channelHandle": (ch_details or {}).get("customUrl"),
                        "subscriberCount": (ch_details or {}).get("subscriberCount", 0),
                        "videoCount": (ch_details or {}).get("videoCount", 0),
                        "channelDescription": ((ch_details or {}).get("description") or "")[:280],
                        "discoveredViaQueries": [],
                        "exampleVideos": [],
                    }
                if query not in suggestions[cid]["discoveredViaQueries"]:
                    suggestions[cid]["discoveredViaQueries"].append(query)
                if not any(v["videoId"] == vid for v in suggestions[cid]["exampleVideos"]):
                    suggestions[cid]["exampleVideos"].append({
                        "videoId": vid,
                        "title": r["title"],
                        "publishedAt": r["publishedAt"],
                        "watchUrl": f"https://www.youtube.com/watch?v={vid}",
                        "thumbnailUrl": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
                    })

    # --- Phase 4: cross-source confidence boost ---
    for cand in candidates.values():
        kinds = {s.split(":", 1)[0] for s in cand["sources"]}
        # Two distinct kinds (channel_watch / tracker / search) all pointing at the same
        # videoId is strong agreement — promote to high.
        if len(kinds) >= 2 and cand["confidence"] != "high":
            cand["confidence"] = "high"
            cand["matchReason"] = (cand["matchReason"] or "") + "; cross_source_match"

    # --- Output ---
    rank = {"high": 0, "medium": 1, "low": 2, "tracker_only": 3}
    ordered = sorted(
        candidates.values(),
        key=lambda c: (rank.get(c["confidence"], 9), c.get("publishedAt", "") or ""),
    )

    summary = {
        "high":         sum(1 for c in ordered if c["confidence"] == "high"),
        "medium":       sum(1 for c in ordered if c["confidence"] == "medium"),
        "low":          sum(1 for c in ordered if c["confidence"] == "low"),
        "tracker_only": sum(1 for c in ordered if c["confidence"] == "tracker_only"),
    }

    out = {
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "candidateCount": len(ordered),
        "byConfidence": summary,
        "candidates": ordered,
    }
    CANDIDATES_FILE.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print()
    print(f"✓ Wrote {CANDIDATES_FILE.relative_to(ROOT)}")
    print(f"  total: {len(ordered)}  |  high: {summary['high']}  med: {summary['medium']}  "
          f"low: {summary['low']}  tracker_only: {summary['tracker_only']}")

    # --- Channel suggestions (Phase 3 byproduct) ---
    if suggestions:
        suggestion_list = sorted(
            suggestions.values(),
            key=lambda s: -(s.get("subscriberCount") or 0),
        )
        sugg_out = {
            "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "suggestionCount": len(suggestion_list),
            "suggestions": suggestion_list,
        }
        SUGGESTIONS_FILE.write_text(json.dumps(sugg_out, indent=2, ensure_ascii=False))
        print(f"✓ Wrote {SUGGESTIONS_FILE.relative_to(ROOT)} ({len(suggestion_list)} new channels suggested for review)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
