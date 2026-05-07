#!/usr/bin/env python3
"""
The Pitch — World Cup '26 Ad Tracker
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

# How many top comments to pull per video (max 100 per API call).
COMMENT_SAMPLE_SIZE = 100

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


def fetch_video_stats(video_ids: list[str], api_key: str) -> dict[str, dict]:
    """
    Batch-fetch stats for up to 50 videos in a single API call (1 quota unit).
    Returns { videoId: {views, likes, comments, published, channelId, ...} }.
    """
    out: dict[str, dict] = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = api_get(
            "videos",
            {"part": "statistics,snippet", "id": ",".join(batch)},
            api_key,
        )
        if "error" in resp:
            raise RuntimeError(f"videos.list failed: {resp['error']}")
        for item in resp.get("items", []):
            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})
            out[item["id"]] = {
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
                "published": snippet.get("publishedAt"),
                "channelId": snippet.get("channelId"),
                "channelTitle": snippet.get("channelTitle"),
            }
    return out


def fetch_top_comments(video_id: str, api_key: str,
                       max_results: int = COMMENT_SAMPLE_SIZE) -> list[dict]:
    """
    Pull top-level comments ordered by 'relevance' (YouTube's top-comments order,
    which weights heavily by likes). Returns up to max_results comments.
    """
    resp = api_get(
        "commentThreads",
        {
            "part": "snippet",
            "videoId": video_id,
            "order": "relevance",
            "maxResults": min(max_results, 100),
            "textFormat": "plainText",
        },
        api_key,
    )
    if "error" in resp:
        # 403 here usually means comments disabled. Treat as empty.
        return []
    out = []
    for item in resp.get("items", []):
        s = item["snippet"]["topLevelComment"]["snippet"]
        author_channel = (s.get("authorChannelId") or {}).get("value")
        out.append({
            "text": s.get("textOriginal") or s.get("textDisplay", ""),
            "likes": int(s.get("likeCount", 0)),
            "authorChannelId": author_channel,
        })
    return out


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
    video_ids = [a["youtubeId"] for a in ads if a.get("youtubeId")]
    print(f"Fetching stats for {len(video_ids)} videos...")
    stats = fetch_video_stats(video_ids, api_key)

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

        velocity = int(s["views"] / days_since(s["published"]))

        enriched.append({
            **ad,
            "views": s["views"],
            "likes": s["likes"],
            "comments": s["comments"],
            "published": s["published"],
            "channelTitle": s["channelTitle"],
            "weightedSentiment": sentiment_score,
            "sentimentSampleSize": sample_size,
            "velocityViewsPerDay": velocity,
        })

        sent_str = f"{sentiment_score} (n={sample_size})" if sentiment_score else f"— (n={sample_size})"
        print(f"  ✓ {ad['brand']:<15} views={pretty_int(s['views']):>12}  "
              f"likes={pretty_int(s['likes']):>10}  sentiment={sent_str}")

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
    return 0


if __name__ == "__main__":
    sys.exit(main())
