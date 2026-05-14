#!/usr/bin/env python3
"""
Reply-to-issue parser — Step 6 of productization.

Reads a comment posted to a "🔄 Daily refresh" issue and applies any
structured update lines to pipeline/ads.json.

Expected line format (one per line, anywhere in the comment):
    ad-id IG|TT handle v=N l=N c=N

Examples:
    bud-01 IG DXqnj_MImxf v=5100000 l=45200 c=280
    mch-01 TT 7638989476273982733 v=160K l=510 c=22
    don-01 ig DWowE3-CJbA v=4.8M l=6,700 c=160

The parser is forgiving on number formats: commas (`45,200`), K/M suffixes
(`5.1M`, `160K`), and case-insensitive platform labels are all accepted.
Lines that don't match are silently ignored (so users can add freeform
prose before/after their data lines without breaking anything).

Triggered by: .github/workflows/refresh-reply.yml on issue_comment events.

Required environment:
  COMMENT_BODY      — the GitHub comment text to parse (set by workflow)
Optional:
  GITHUB_OUTPUT     — if set, writes a Markdown summary for the workflow to
                      use as the confirmation reply (set by GitHub Actions)
"""

from __future__ import annotations
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ADS_FILE = ROOT / "pipeline" / "ads.json"

# Permissive line regex:
#   group(1) ad id, group(2) platform (IG|TT, any case),
#   group(3) handle (alphanumerics, underscore, dash),
#   groups 4-6 view/like/comment values (digits with optional commas/.KkMm)
LINE_RE = re.compile(
    r"^\s*"
    r"([a-zA-Z]{2,5}-\d+)\s+"           # ad id (e.g. bud-01, mch-02)
    r"(IG|TT|ig|tt|Ig|Tt)\s+"           # platform label
    r"([A-Za-z0-9_\-]+)\s+"             # post handle
    r"v\s*=\s*([\d.,KkMm]+)\s+"
    r"l\s*=\s*([\d.,KkMm]+)\s+"
    r"c\s*=\s*([\d.,KkMm]+)\s*$",
    re.MULTILINE,
)


def parse_num(s: str) -> int | None:
    """Parse '5.1M', '45,200', '160K', '12345' into an int. None on failure."""
    s = s.strip().replace(",", "").replace("_", "").lower()
    if not s:
        return None
    mult = 1
    if s.endswith("m"):
        mult, s = 1_000_000, s[:-1]
    elif s.endswith("k"):
        mult, s = 1_000, s[:-1]
    try:
        return int(round(float(s) * mult))
    except ValueError:
        return None


def fmt_num(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M".rstrip("0").rstrip(".")
    if n >= 1_000:
        return f"{n / 1_000:.1f}K".rstrip("0").rstrip(".")
    return str(n)


def find_manual_entry(ad: dict, platform: str, handle: str) -> dict | None:
    """Locate the manual-data entry for this URL handle. Match is by handle
    substring within the URL — works for both IG (/reel/<handle>) and TT
    (/video/<handle>)."""
    key = "manualIgData" if platform == "IG" else "manualTtData"
    for entry in (ad.get(key) or []):
        if handle in entry.get("url", ""):
            return entry
    return None


def apply_updates(ads: list[dict], comment_body: str) -> dict:
    """Walk the comment body, find structured lines, apply matching updates.
    Returns a summary dict with counts and a per-update detail list."""
    updates = []     # successfully applied
    not_found = []   # parsed correctly but no matching ad/URL
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    ads_by_id = {a["id"]: a for a in ads}

    for match in LINE_RE.finditer(comment_body):
        ad_id, plat_raw, handle, v_raw, l_raw, c_raw = match.groups()
        ad_id = ad_id.lower()
        platform = plat_raw.upper()  # IG or TT

        ad = ads_by_id.get(ad_id)
        if not ad:
            not_found.append({"reason": "unknown ad-id", "line": match.group(0).strip()})
            continue

        entry = find_manual_entry(ad, platform, handle)
        if not entry:
            not_found.append({
                "reason": f"no manualIg/TtData entry matching handle '{handle}' on {ad_id}",
                "line": match.group(0).strip(),
            })
            continue

        new_v = parse_num(v_raw)
        new_l = parse_num(l_raw)
        new_c = parse_num(c_raw)
        if None in (new_v, new_l, new_c):
            not_found.append({
                "reason": "couldn't parse one of v/l/c",
                "line": match.group(0).strip(),
            })
            continue

        old = {"views": entry.get("views"), "likes": entry.get("likes"), "comments": entry.get("comments")}
        entry["views"] = new_v
        entry["likes"] = new_l
        entry["comments"] = new_c
        entry["lastCheckedAt"] = now

        updates.append({
            "adId": ad_id,
            "brand": ad["brand"],
            "platform": platform,
            "handle": handle,
            "url": entry["url"],
            "before": old,
            "after": {"views": new_v, "likes": new_l, "comments": new_c},
        })

    return {"updated": updates, "skipped": not_found, "now": now}


def render_summary_md(summary: dict) -> str:
    updated = summary["updated"]
    skipped = summary["skipped"]
    lines = []

    if updated:
        lines.append(f"✓ Updated **{len(updated)}** URL{'s' if len(updated) != 1 else ''}:")
        lines.append("")
        for u in updated:
            b, a = u["before"], u["after"]
            def delta(k):
                return f"{fmt_num(b.get(k))} → **{fmt_num(a.get(k))}**"
            lines.append(
                f"- `{u['adId']} {u['platform']} {u['handle']}` ({u['brand']}): "
                f"v {delta('views')} · l {delta('likes')} · c {delta('comments')}"
            )
        lines.append("")
    else:
        lines.append("No URLs were updated (no lines matched the expected format, or no entries were found in `ads.json`).")
        lines.append("")

    if skipped:
        lines.append(f"⚠ Couldn't process **{len(skipped)}** line{'s' if len(skipped) != 1 else ''}:")
        lines.append("")
        for s in skipped:
            lines.append(f"- _{s['reason']}_ — `{s['line']}`")
        lines.append("")

    return "\n".join(lines).strip()


def main() -> int:
    comment = os.environ.get("COMMENT_BODY")
    if comment is None:
        print("ERROR: COMMENT_BODY env var not set", file=sys.stderr)
        return 2

    ads = json.loads(ADS_FILE.read_text())
    summary = apply_updates(ads, comment)

    # Persist ads.json only if there were real updates.
    if summary["updated"]:
        ADS_FILE.write_text(json.dumps(ads, indent=2, ensure_ascii=False) + "\n")
        print(f"✓ Wrote {ADS_FILE.relative_to(ROOT)} with {len(summary['updated'])} update(s).")
    else:
        print("No updates applied.")

    md = render_summary_md(summary)
    print()
    print("--- summary markdown ---")
    print(md)

    # Write summary to a file the workflow can pass to `gh issue comment --body-file`.
    # Avoids bash interpolation issues with markdown backticks/quotes.
    summary_path = ROOT / ".github" / "_refresh_reply_summary.md"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(md + "\n")

    # Set workflow output flags.
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write("did_update=" + ("true" if summary["updated"] else "false") + "\n")
            f.write(f"summary_path={summary_path.relative_to(ROOT)}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
