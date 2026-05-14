#!/usr/bin/env python3
"""
Daily refresh issue builder — Step 3 of productization.

Reads pipeline/ads.json, finds all URLs that require manual refresh
(currently: every URL in manualIgData / manualTtData arrays, since
those are age-gated / restricted-profile posts that Apify can't scrape),
and creates a GitHub Issue with a prioritized checklist.

User receives an email notification, taps the link, updates the numbers
either by replying to the issue (parsed by Step 6) or via the CLI
prompter (Step 4).

Closes any previous still-open "Daily refresh" issue first so old ones
don't pile up.

Runs in GitHub Actions. Required environment:
  GITHUB_TOKEN      — provided automatically by the workflow runner
  GITHUB_REPOSITORY — set automatically (e.g. "talljamz2/world-cup-ad-tracker")

Local testing:
  GITHUB_TOKEN=ghp_... GITHUB_REPOSITORY=talljamz2/world-cup-ad-tracker \\
    python pipeline/build_refresh_issue.py
"""

from __future__ import annotations
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
ADS_FILE = ROOT / "pipeline" / "ads.json"
DATA_FILE = ROOT / "data.json"

# In v1 we list every manual URL daily. Once Step 4 (CLI prompter) lands
# and writes lastCheckedAt per entry, we can switch to: only list URLs not
# checked in the last 24h (or 7d for older content). For now, show all so
# nothing slips through the cracks.

ISSUE_TITLE_PREFIX = "🔄 Daily refresh"


def fmt_num(n: int | None) -> str:
    """Compact number formatting for the issue body (4.8M, 43.6K, etc)."""
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M".rstrip("0").rstrip(".")
    if n >= 1_000:
        return f"{n / 1_000:.1f}K".rstrip("0").rstrip(".")
    return str(n)


def url_handle(url: str) -> str:
    """Return the short identifier for a social URL — the reel ID for IG,
    the numeric post ID for TT. Used as the parsing key in user replies."""
    # IG: /reel/DXqnj_MImxf/ → DXqnj_MImxf
    # TT: /video/7637057954289618198 → 7637057954289618198
    path = urlparse(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1] if path else url


def days_since(iso_dt: str | None) -> int:
    """Days since an ISO date/datetime string. Tolerates 'YYYY-MM-DD' and
    'YYYY-MM-DDTHH:MM:SSZ' from YouTube. Unknown → 9999 (treated as ancient)."""
    if not iso_dt:
        return 9999
    try:
        # Strip Zulu suffix; tolerate timezone-less strings.
        s = iso_dt.replace("Z", "+00:00")
        d = datetime.fromisoformat(s).date()
        return (datetime.now(timezone.utc).date() - d).days
    except (ValueError, TypeError):
        return 9999


def load_published_dates() -> dict[str, str]:
    """Map adId → YouTube `published` timestamp, sourced from data.json.
    Used to estimate post age. Missing entries fall back to 'unknown'."""
    if not DATA_FILE.exists():
        return {}
    data = json.loads(DATA_FILE.read_text())
    ads = data.get("ads", []) if isinstance(data, dict) else data
    return {a["id"]: a.get("published") for a in ads if a.get("published")}


def collect_refresh_targets(ads: list[dict], published_dates: dict[str, str]) -> list[dict]:
    """Return one entry per URL needing manual refresh. All of them, every day."""
    out = []
    for ad in ads:
        pub = published_dates.get(ad["id"])
        age = days_since(pub)
        released_label = pub.split("T")[0] if pub else "—"
        for platform, key in (("instagram", "manualIgData"), ("tiktok", "manualTtData")):
            for entry in (ad.get(key) or []):
                out.append({
                    "adId": ad["id"],
                    "brand": ad["brand"],
                    "released": released_label,
                    "ageDays": age,
                    "platform": platform,
                    "platformLabel": "IG" if platform == "instagram" else "TT",
                    "url": entry["url"],
                    "handle": url_handle(entry["url"]),
                    "views": entry.get("views"),
                    "likes": entry.get("likes"),
                    "comments": entry.get("comments"),
                })
    # Newest posts first — they move fastest, so prioritize them.
    out.sort(key=lambda e: (e["ageDays"], e["brand"], e["platform"], e["handle"]))
    return out


def build_issue_body(items: list[dict], date_str: str) -> tuple[str, str]:
    """Compose the title and markdown body. Returns (title, body)."""
    total = len(items)
    title = f"{ISSUE_TITLE_PREFIX} — {total} URL{'s' if total != 1 else ''} ({date_str})"

    lines = []
    lines.append(f"_{total} URLs need manual refresh today._ "
                 "These are age-gated alcohol brands and restricted-profile posts "
                 "that Apify can't scrape directly. Sorted newest-first (those move fastest).")
    lines.append("")
    lines.append("**How to update:** open each post on your phone, copy the "
                 "current views/likes/comments, then reply to this issue with "
                 "one line per URL in this exact format:")
    lines.append("")
    lines.append("```")
    lines.append("ad-id IG|TT handle v=N l=N c=N")
    lines.append("```")
    lines.append("")
    lines.append("Example reply:")
    lines.append("```")
    lines.append("bud-01 IG DXqnj_MImxf v=5100000 l=45200 c=280")
    lines.append("mch-01 TT 7638989476273982733 v=160000 l=510 c=22")
    lines.append("```")
    lines.append("")
    lines.append("Reply via the GitHub mobile app or directly to the "
                 "notification email. Numbers can include commas or M/K suffixes "
                 "(`v=5.1M`, `l=45,200`) — the parser is forgiving. "
                 "Skip any URL whose numbers haven't moved much; partial updates are fine.")
    lines.append("")

    if items:
        lines.extend(_render_url_block(items))
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("_Generated automatically by `.github/workflows/daily-refresh.yml`. "
                 "Closing this issue is fine — tomorrow's run will open a fresh one._")

    return title, "\n".join(lines)


def _render_url_block(items: list[dict]) -> list[str]:
    """Render a list of refresh targets, grouped by ad (preserves sort order)."""
    lines: list[str] = []
    current_ad = None
    for it in items:
        if it["adId"] != current_ad:
            current_ad = it["adId"]
            lines.append("")
            age_label = f"{it['ageDays']}d old" if it["ageDays"] < 9999 else "age unknown"
            lines.append(f"### `{it['adId']}` {it['brand']} · YouTube released {it['released']} · {age_label}")
        v = fmt_num(it["views"])
        l = fmt_num(it["likes"])
        c = fmt_num(it["comments"])
        lines.append(
            f"- [ ] `{it['adId']} {it['platformLabel']} {it['handle']}` — "
            f"current: v={v} l={l} c={c} · "
            f"[open ↗]({it['url']})"
        )
    return lines


def close_previous_issues(repo: str, token: str, current_date: str) -> int:
    """Close any open issues whose title starts with the daily refresh prefix
    AND don't match today's date string. Returns number closed."""
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    r = requests.get(
        f"https://api.github.com/repos/{repo}/issues",
        headers=headers,
        params={"state": "open", "per_page": 50},
    )
    r.raise_for_status()
    closed = 0
    for issue in r.json():
        if not issue["title"].startswith(ISSUE_TITLE_PREFIX):
            continue
        if current_date in issue["title"]:
            continue  # this is today's, leave it alone
        num = issue["number"]
        requests.post(
            f"https://api.github.com/repos/{repo}/issues/{num}/comments",
            headers=headers,
            json={"body": "Superseded by today's refresh issue. Closing automatically."},
        )
        requests.patch(
            f"https://api.github.com/repos/{repo}/issues/{num}",
            headers=headers,
            json={"state": "closed"},
        )
        closed += 1
    return closed


def create_issue(repo: str, token: str, title: str, body: str) -> dict:
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    r = requests.post(
        f"https://api.github.com/repos/{repo}/issues",
        headers=headers,
        json={"title": title, "body": body},
    )
    r.raise_for_status()
    return r.json()


def main() -> int:
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repo:
        print("ERROR: GITHUB_TOKEN and GITHUB_REPOSITORY must be set.", file=sys.stderr)
        return 2

    ads = json.loads(ADS_FILE.read_text())
    published = load_published_dates()
    today = datetime.now(timezone.utc).date()
    today_str = today.isoformat()

    items = collect_refresh_targets(ads, published)

    if not items:
        print(f"No URLs need manual refresh today ({today_str}). Skipping issue creation.")
        # Still close any stale issues from previous days.
        closed = close_previous_issues(repo, token, today_str)
        if closed:
            print(f"Closed {closed} stale refresh issue(s).")
        return 0

    title, body = build_issue_body(items, today_str)

    # Close yesterday's issue (and any older still-open ones).
    closed = close_previous_issues(repo, token, today_str)
    if closed:
        print(f"Closed {closed} previous refresh issue(s).")

    issue = create_issue(repo, token, title, body)
    print(f"✓ Created #{issue['number']}: {issue['title']}")
    print(f"  {issue['html_url']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
