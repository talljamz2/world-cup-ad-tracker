#!/usr/bin/env python3
"""
Daily discovery issue builder — Step 2 of productization.

Reads pipeline/candidates.json (produced by discover.py) and creates a
GitHub Issue listing new high-confidence candidates the user should
triage. Excludes anything already tracked in ads.json or already in
rejected.json so the daily issue is always actionable.

User receives an email notification. To act on a candidate:
  - Track it: edit pipeline/ads.json to add a new entry, push.
    The YouTube cron will pick up metrics within the hour.
  - Reject it: edit pipeline/rejected.json to add the videoId,
    so it won't reappear in future discovery issues.

(A future v2 may add a reply parser that handles `add` / `skip`
commands. For now, manual JSON editing is the path.)

Runs in GitHub Actions. Required env:
  GITHUB_TOKEN      — provided automatically by the workflow runner
  GITHUB_REPOSITORY — set automatically (e.g. "talljamz2/world-cup-ad-tracker")
"""

from __future__ import annotations
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
CANDIDATES_FILE = ROOT / "pipeline" / "candidates.json"
ADS_FILE = ROOT / "pipeline" / "ads.json"
REJECTED_FILE = ROOT / "pipeline" / "rejected.json"

# Start strict: only surface confidence='high' candidates. discover.py's
# medium tier has too many false positives for daily review. We can loosen
# this to include medium once the trade-off is calibrated.
CONFIDENCE_INCLUDE = {"high"}

ISSUE_TITLE_PREFIX = "🆕 Discovery candidates"


def load_tracked_video_ids() -> set[str]:
    """videoIds already on the leaderboard — never suggest them again."""
    if not ADS_FILE.exists():
        return set()
    ads = json.loads(ADS_FILE.read_text())
    return {a["youtubeId"] for a in ads if a.get("youtubeId")}


def load_rejected_video_ids() -> set[str]:
    """videoIds the user has explicitly rejected — keep them out of the issue."""
    if not REJECTED_FILE.exists():
        return set()
    r = json.loads(REJECTED_FILE.read_text())
    entries = r if isinstance(r, list) else r.get("rejected", [])
    return {e.get("youtubeId") or e.get("videoId") for e in entries if (e.get("youtubeId") or e.get("videoId"))}


def select_candidates() -> list[dict]:
    """Return today's actionable candidates: high-confidence, not tracked, not rejected."""
    if not CANDIDATES_FILE.exists():
        return []
    data = json.loads(CANDIDATES_FILE.read_text())
    all_c = data.get("candidates", []) if isinstance(data, dict) else data

    tracked = load_tracked_video_ids()
    rejected = load_rejected_video_ids()

    out = []
    for cd in all_c:
        if cd.get("confidence") not in CONFIDENCE_INCLUDE:
            continue
        vid = cd.get("videoId")
        if not vid or vid in tracked or vid in rejected:
            continue
        out.append(cd)

    # Sort by published date desc (newest first), then by brand.
    def sort_key(cd: dict):
        pub = cd.get("publishedAt", "")
        return (pub if pub else "0000", cd.get("expectedBrand", ""))

    out.sort(key=sort_key, reverse=True)
    return out


def build_issue_body(candidates: list[dict], date_str: str) -> tuple[str, str]:
    n = len(candidates)
    title = f"{ISSUE_TITLE_PREFIX} — {n} new ad{'s' if n != 1 else ''} ({date_str})"

    lines: list[str] = []
    lines.append(
        f"_{n} new high-confidence candidate{'s' if n != 1 else ''} discovered by `discover.py`._ "
        "These are YouTube videos that match World Cup brand-creative signals and "
        "aren't yet tracked or rejected. Review each (~30 sec) and decide:"
    )
    lines.append("")
    lines.append("- **Worth tracking?** Edit `pipeline/ads.json` to add a new entry with the videoId, "
                 "tier (Official Partner / Other), brandColor, title, and description. The next hourly "
                 "YouTube cron will pull metrics automatically.")
    lines.append("- **Noise / not relevant?** Edit `pipeline/rejected.json` to add the videoId with a "
                 "reason — it'll never appear in future discovery issues.")
    lines.append("")
    lines.append("Sorted newest-first.")
    lines.append("")

    for cd in candidates:
        title_line = cd.get("title", "(no title)").strip() or "(no title)"
        if len(title_line) > 120:
            title_line = title_line[:117] + "…"
        brand = cd.get("expectedBrand") or "—"
        channel = cd.get("channelTitle") or "—"
        verified = "✓ verified channel match" if cd.get("channelMatch") else "⚠ channel name doesn't match expected brand"
        pub = (cd.get("publishedAt") or "—")[:10]
        reason = cd.get("matchReason") or "—"
        keywords = cd.get("matchedKeywords") or []
        sources = cd.get("sources") or []
        vid = cd.get("videoId", "")

        lines.append(f"### `{vid}` · **{brand}**")
        lines.append("")
        lines.append(f"> {title_line}")
        lines.append("")
        lines.append(f"- **Channel:** {channel} · {verified}")
        lines.append(f"- **Published:** {pub}")
        lines.append(f"- **Confidence:** high · {reason}" + (f" · keywords: {', '.join(keywords[:6])}" if keywords else ""))
        if sources:
            lines.append(f"- **Source:** {' · '.join(sources[:3])}")
        lines.append(f"- **Watch:** [{vid} on YouTube ↗](https://www.youtube.com/watch?v={vid})")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("_Generated automatically by `.github/workflows/discovery.yml`. "
                 "Closing this issue is fine — tomorrow's run will open a fresh one if there are new finds._")

    return title, "\n".join(lines)


def close_previous_issues(repo: str, token: str, today: str) -> int:
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
        if today in issue["title"]:
            continue
        num = issue["number"]
        requests.post(
            f"https://api.github.com/repos/{repo}/issues/{num}/comments",
            headers=headers,
            json={"body": "Superseded by today's discovery issue. Closing automatically."},
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

    today_str = datetime.now(timezone.utc).date().isoformat()
    candidates = select_candidates()

    if not candidates:
        print(f"No new high-confidence candidates today ({today_str}). Skipping issue creation.")
        closed = close_previous_issues(repo, token, today_str)
        if closed:
            print(f"Closed {closed} stale discovery issue(s).")
        return 0

    title, body = build_issue_body(candidates, today_str)
    closed = close_previous_issues(repo, token, today_str)
    if closed:
        print(f"Closed {closed} previous discovery issue(s).")
    issue = create_issue(repo, token, title, body)
    print(f"✓ Created #{issue['number']}: {issue['title']}")
    print(f"  {issue['html_url']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
