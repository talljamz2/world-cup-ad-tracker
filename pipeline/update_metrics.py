#!/usr/bin/env python3
"""
The Ad Podium — World Cup '26 Ad Tracker
Pipeline: pull live YouTube metrics + compute like-weighted comment sentiment.

Reads:  pipeline/ads.json  (static metadata, hand-curated)
Writes: data.json          (live stats, sentiment, ready for the page)

Cost per run: ~2 quota units per ad (1 for stats batch, 1 for comments).
At 12 ads × hourly = ~600 units/day, well inside the free 10,000-unit quota.

Usage:
    YOUTUBE_API_KEY=... python pipeline/update_metrics.py
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


API_BASE = "https://www.googleapis.com/youtube/v3"
ROOT = Path(__file__).resolve().parent.parent
ADS_FILE = ROOT / "pipeline" / "ads.json"
OUT_FILE = ROOT / "data.json"
HISTORY_FILE = ROOT / "pipeline" / "history.json"

# Retain 30 days of snapshots; older entries pruned on each run.
HISTORY_RETENTION_DAYS = 30

# Trailing-window for "views in last N days" velocity computation.
VELOCITY_WINDOW_DAYS = 7

# How many comments to pull per video. Default 500 means we paginate up to 5
# pages from the relevance-ranked list and another 2 pages from the time-ranked
# list, dedupe by comment ID, and feed VADER the union. Cost: ~7 quota units
# per video (free tier is 10,000/day, current hourly run uses well under 200).
COMMENT_SAMPLE_SIZE = 500

# Mix ratio: ~60% from relevance (the loud / popular signal), ~40% from time
# (newest, captures recent reactions + coordinated campaigns the relevance
# ranker filters out).
RELEVANCE_SHARE = 0.60

# Below this many valid comments, sentiment is suppressed (low confidence).
MIN_SENTIMENT_SAMPLE = 20

# Spam / low-signal comment filters.
MIN_TOKENS = 5
SPAM_PATTERNS = [
    re.compile(r"http[s]?://", re.I),
    re.compile(r"\b(subscribe|sub4sub|sub2sub)\b", re.I),
    re.compile(r"check( out)? my (channel|videos|content)", re.I),
    re.compile(r"^first!*$", re.I),
    re.compile(r"^\W+$"),  # emoji / punctuation only
]


# ---------------------------------------------------------------------------
# YouTube Data API
# ---------------------------------------------------------------------------

def api_get(path: str, params: dict, api_key: str) -> dict:
    """Single GET to the YouTube Data API. Raises on HTTP error."""
    params = {**params, "key": api_key}
    r = requests.get(f"{API_BASE}/{path}", params=params, timeout=30)
    if r.status_code == 403:
        # Could be quota exceeded or comments disabled — caller decides.
        return {"error": r.json()}
    r.raise_for_status()
    return r.json()


def parse_iso_duration(iso: str | None) -> int | None:
    """ISO 8601 duration to seconds. PT1M30S → 90, PT2H5M → 7500, PT45S → 45."""
    if not iso:
        return None
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if not m:
        return None
    h, mn, s = (int(g or 0) for g in m.groups())
    return h * 3600 + mn * 60 + s


def fetch_video_stats(video_ids: list[str], api_key: str) -> dict[str, dict]:
    """
    Batch-fetch stats for up to 50 videos in a single API call (1 quota unit).
    Returns { videoId: {views, likes, comments, published, channelId,
                        durationSeconds, ...} }.
    """
    out: dict[str, dict] = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = api_get(
            "videos",
            {"part": "statistics,snippet,contentDetails", "id": ",".join(batch)},
            api_key,
        )
        if "error" in resp:
            raise RuntimeError(f"videos.list failed: {resp['error']}")
        for item in resp.get("items", []):
            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})
            content = item.get("contentDetails", {})
            out[item["id"]] = {
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
                "published": snippet.get("publishedAt"),
                "channelId": snippet.get("channelId"),
                "channelTitle": snippet.get("channelTitle"),
                "durationSeconds": parse_iso_duration(content.get("duration")),
            }
    return out


def _fetch_comments_page(video_id: str, api_key: str, *,
                          order: str, page_token: str | None) -> tuple[list[dict], str | None]:
    """Single page of commentThreads. Returns (items, nextPageToken)."""
    params = {
        "part": "snippet",
        "videoId": video_id,
        "order": order,           # "relevance" or "time"
        "maxResults": 100,
        "textFormat": "plainText",
    }
    if page_token:
        params["pageToken"] = page_token
    resp = api_get("commentThreads", params, api_key)
    if "error" in resp:
        # 403 here usually means comments disabled. Treat as empty.
        return [], None
    items = []
    for item in resp.get("items", []):
        comment_id = item.get("id")
        s = item["snippet"]["topLevelComment"]["snippet"]
        author_channel = (s.get("authorChannelId") or {}).get("value")
        items.append({
            "id": comment_id,
            "text": s.get("textOriginal") or s.get("textDisplay", ""),
            "likes": int(s.get("likeCount", 0)),
            "authorChannelId": author_channel,
        })
    return items, resp.get("nextPageToken")


def _paginate_comments(video_id: str, api_key: str, *,
                        order: str, target: int) -> list[dict]:
    """Page through commentThreads until we have `target` items or run out."""
    out: list[dict] = []
    token: str | None = None
    while len(out) < target:
        page, token = _fetch_comments_page(video_id, api_key, order=order, page_token=token)
        if not page:
            break
        out.extend(page)
        if not token:
            break
    return out[:target]


def fetch_top_comments(video_id: str, api_key: str,
                       max_results: int = COMMENT_SAMPLE_SIZE) -> list[dict]:
    """
    Stratified comment sample for sentiment analysis.

    We blend YouTube's two orderings to fight selection bias:
      - 'relevance' returns YouTube's top picks (high-engagement comments,
         loud-signal heavy). Default 60% of the sample.
      - 'time' returns newest-first (catches recent reactions and any
         coordinated comment campaigns the relevance ranker filters out).
         Default 40% of the sample.

    The two sets are merged and deduplicated by comment ID. The result is a
    sample that's both larger (default 500 vs the old 100) and broader in
    distribution than the previous relevance-only pull.

    Cost: ~ceil(max_results/100) quota units per ordering, so 5+2=7 units per
    video at the 500/300+200 split. Free quota is 10,000/day.
    """
    if max_results <= 100:
        # Tiny request — single page from relevance is fine
        items, _ = _fetch_comments_page(video_id, api_key, order="relevance", page_token=None)
        return items[:max_results]

    relevance_target = int(max_results * RELEVANCE_SHARE)
    time_target      = max_results - relevance_target

    relevance_pool = _paginate_comments(video_id, api_key, order="relevance", target=relevance_target)
    time_pool      = _paginate_comments(video_id, api_key, order="time",      target=time_target)

    # Dedupe by comment id — relevance and time can overlap on small videos.
    seen: set[str] = set()
    merged: list[dict] = []
    for c in relevance_pool + time_pool:
        cid = c.get("id")
        if cid and cid in seen:
            continue
        if cid:
            seen.add(cid)
        merged.append(c)
    return merged


# ---------------------------------------------------------------------------
# Sentiment
# ---------------------------------------------------------------------------

def is_spam(text: str) -> bool:
    """Drop very short, URL-bearing, or bot-pattern comments."""
    if not text:
        return True
    if len(text.split()) < MIN_TOKENS:
        return True
    return any(p.search(text) for p in SPAM_PATTERNS)


def weighted_sentiment(comments: list[dict],
                       channel_id: str | None) -> tuple[float | None, int]:
    """
    Compute like-weighted Net Sentiment as a 0..100 score.

    Each comment c is scored by VADER (compound, range -1..+1), then weighted
    by w = 1 + ln(1 + likes). Channel-staff comments are excluded; spam/short
    comments are dropped. Returns (score, valid_sample_size). If sample size
    is below MIN_SENTIMENT_SAMPLE, returns (None, sample_size).
    """
    analyzer = SentimentIntensityAnalyzer()
    valid = []
    for c in comments:
        if channel_id and c.get("authorChannelId") == channel_id:
            continue  # skip the brand's own replies
        if is_spam(c["text"]):
            continue
        valid.append(c)

    if len(valid) < MIN_SENTIMENT_SAMPLE:
        return None, len(valid)

    total_weighted = 0.0
    total_weight = 0.0
    for c in valid:
        compound = analyzer.polarity_scores(c["text"])["compound"]  # -1..+1
        weight = 1.0 + math.log(1.0 + c["likes"])
        total_weighted += compound * weight
        total_weight += weight

    weighted_avg = total_weighted / total_weight  # -1..+1
    score = (weighted_avg + 1.0) / 2.0 * 100.0    #  0..100
    return round(score, 1), len(valid)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def days_since(iso_ts: str | None) -> float:
    """Days since publication (min 1 to avoid divide-by-zero on day-of)."""
    if not iso_ts:
        return 1.0
    published = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    delta = datetime.now(timezone.utc) - published
    return max(delta.total_seconds() / 86400.0, 1.0)


# ---------------------------------------------------------------------------
# History — snapshot views/likes/comments per ad on every run, build a
# rolling time series so we can compute proper trailing-window velocity
# rather than the lifetime-average proxy.
# ---------------------------------------------------------------------------

def load_history() -> list[dict]:
    """Returns list of {timestamp, ads: [{youtubeId, views, likes, comments}]}."""
    if not HISTORY_FILE.exists():
        return []
    try:
        return json.loads(HISTORY_FILE.read_text())
    except json.JSONDecodeError:
        return []


def prune_history(history: list[dict], retention_days: int = HISTORY_RETENTION_DAYS) -> list[dict]:
    """Drop snapshots older than retention_days."""
    cutoff = datetime.now(timezone.utc).timestamp() - retention_days * 86400
    out = []
    for snap in history:
        try:
            ts = datetime.fromisoformat(snap["timestamp"].replace("Z", "+00:00")).timestamp()
            if ts >= cutoff:
                out.append(snap)
        except (KeyError, ValueError):
            pass
    return out


def find_snapshot_n_days_ago(history: list[dict], target_days: float) -> dict | None:
    """Return the snapshot closest to (now - target_days), or None if no
    snapshot exists within ±2 days of target."""
    if not history:
        return None
    target_ts = datetime.now(timezone.utc).timestamp() - target_days * 86400
    best = None
    best_delta = float("inf")
    for snap in history:
        try:
            ts = datetime.fromisoformat(snap["timestamp"].replace("Z", "+00:00")).timestamp()
            delta = abs(ts - target_ts)
            if delta < best_delta:
                best_delta = delta
                best = snap
        except (KeyError, ValueError):
            continue
    # Only count it if within 2 days of target (avoid using a 1-day-old snapshot
    # as a "7 days ago" reference)
    if best and best_delta <= 2 * 86400:
        return best
    return None


def views_in_window(youtube_id: str, current_views: int,
                    history: list[dict], window_days: float = VELOCITY_WINDOW_DAYS) -> int | None:
    """Returns delta in views over the last `window_days` if we have a
    historical snapshot from that far back. Returns None when there's no
    matching snapshot — caller falls back to lifetime average."""
    snap = find_snapshot_n_days_ago(history, window_days)
    if not snap:
        return None
    for entry in snap.get("ads", []):
        if entry.get("youtubeId") == youtube_id:
            past_views = entry.get("views")
            if past_views is not None:
                return max(current_views - past_views, 0)
    return None


def append_snapshot(history: list[dict], stats: dict[str, dict]) -> list[dict]:
    """Add the current run's data as a new snapshot."""
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ads": [
            {"youtubeId": vid, "views": s["views"], "likes": s["likes"], "comments": s["comments"]}
            for vid, s in stats.items()
        ],
    }
    return history + [snapshot]


def pretty_int(n: int) -> str:
    return f"{n:,}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        print("error: YOUTUBE_API_KEY not set in environment", file=sys.stderr)
        return 1

    ads = json.loads(ADS_FILE.read_text())
    # Fetch metrics for every YouTube video (primary + cutdowns).
    # Cutdown ids live on ad.youtubeCutdownIds — they're shorter variants of
    # the same campaign whose views/likes/comments we aggregate into the
    # parent ad's totals.
    video_ids = []
    for a in ads:
        if a.get("youtubeId"):
            video_ids.append(a["youtubeId"])
        for cid in (a.get("youtubeCutdownIds") or []):
            video_ids.append(cid)
    video_ids = list(dict.fromkeys(video_ids))  # dedupe, preserve order
    print(f"Fetching stats for {len(video_ids)} videos (primary + cutdowns)...")
    stats = fetch_video_stats(video_ids, api_key)

    history = load_history()
    if history:
        print(f"Loaded {len(history)} historical snapshot(s) for trailing-window velocity")

    enriched = []
    for ad in ads:
        vid = ad.get("youtubeId")
        if not vid or vid not in stats:
            print(f"  ⚠  {ad['brand']}: missing stats")
            enriched.append({**ad, "missing": True})
            continue

        s = stats[vid]
        comments = fetch_top_comments(vid, api_key)
        sentiment_score, sample_size = weighted_sentiment(
            comments, s.get("channelId")
        )

        # Aggregate primary + cutdowns. The primary ad's `published` and
        # `channelTitle` represent the campaign — cutdowns are typically the
        # same channel and adjacent dates.
        cut_ids = ad.get("youtubeCutdownIds") or []
        cut_stats = [stats[c] for c in cut_ids if c in stats]
        total_views    = s["views"]    + sum(c["views"]    for c in cut_stats)
        total_likes    = s["likes"]    + sum(c["likes"]    for c in cut_stats)
        total_comments = s["comments"] + sum(c["comments"] for c in cut_stats)

        velocity = int(total_views / max(1, days_since(s["published"])))
        # Trailing-window velocity from history snapshots — cleaner signal
        # than lifetime average. None when there's no snapshot yet.
        # NOTE: views_in_window operates on the primary id only; we may want
        # to expand it to sum cutdowns too once a few daily snapshots accrue.
        views_last_window = views_in_window(vid, total_views, history,
                                            window_days=VELOCITY_WINDOW_DAYS)

        # Per-cut detail for the expand row — same shape as IG/TT cut data.
        cut_details = []
        for cid in cut_ids:
            cs = stats.get(cid)
            if not cs:
                continue
            cut_details.append({
                "videoId": cid,
                "title": cs.get("title", ""),
                "views": cs["views"],
                "likes": cs["likes"],
                "comments": cs["comments"],
                "published": cs.get("published"),
                "durationSeconds": cs.get("durationSeconds"),
                "watchUrl": f"https://www.youtube.com/watch?v={cid}",
            })

        enriched.append({
            **ad,
            "views": total_views,
            "likes": total_likes,
            "comments": total_comments,
            "published": s["published"],
            "channelTitle": s["channelTitle"],
            "durationSeconds": s.get("durationSeconds"),
            "weightedSentiment": sentiment_score,
            "sentimentSampleSize": sample_size,
            "velocityViewsPerDay": velocity,
            "viewsLast7Days": views_last_window,
            "velocityWindowDays": VELOCITY_WINDOW_DAYS if views_last_window is not None else None,
            # Per-cut detail for the page to surface in the expand row.
            "youtubeCutdownCount": len(cut_details),
            "youtubeCutdowns": cut_details,
            # The primary's own metrics, isolated, in case the page wants to
            # show "hero film: X views / total campaign: Y views" later.
            "primary": {
                "videoId": vid,
                "views": s["views"],
                "likes": s["likes"],
                "comments": s["comments"],
            },
        })

        cut_note = f" + {len(cut_details)} cutdown{'s' if len(cut_details) != 1 else ''}" if cut_details else ""
        sent_str = f"{sentiment_score} (n={sample_size})" if sentiment_score else f"— (n={sample_size})"
        print(f"  ✓ {ad['brand']:<15} views={pretty_int(total_views):>12}{cut_note}  "
              f"likes={pretty_int(total_likes):>10}  sentiment={sent_str}")

        # Gentle pacing — well under any rate limit, just polite.
        time.sleep(0.1)

    out = {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ads": enriched,
        "methodology": {
            "weights": {
                "reach": 0.40,
                "resonance": 0.20,
                "sentiment": 0.25,
                "velocity": 0.15,
            },
            "sentimentModel": "VADER (compound, -1..+1) → like-weighted mean → normalized 0..100",
            "sentimentSampleMin": MIN_SENTIMENT_SAMPLE,
            "commentSampleSize": COMMENT_SAMPLE_SIZE,
        },
    }
    OUT_FILE.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"\n✓ Wrote {OUT_FILE.relative_to(ROOT)}")

    # Append this run to the history file and prune old snapshots.
    history = append_snapshot(history, stats)
    history = prune_history(history)
    HISTORY_FILE.write_text(json.dumps(history, indent=2, ensure_ascii=False))
    print(f"✓ History: {len(history)} snapshot(s) retained "
          f"({HISTORY_RETENTION_DAYS}-day rolling window)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
