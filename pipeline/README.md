# Pipeline

The Ad Podium is a static site backed by two scheduled Python jobs:

1. **`update_metrics.py`** — pulls live YouTube metrics + comment sentiment.
   Writes `data.json` at the repo root.
2. **`update_social.py`** — pulls Instagram + TikTok metrics + comment samples
   via Apify Actors. Writes `pipeline/social_data.json`.

The page reads both files on load.

## What's in here

| File                       | Purpose                                                     |
|----------------------------|-------------------------------------------------------------|
| `ads.json`                 | Source-of-truth metadata: brand, agency, title, YouTube ID, IG/TT URLs, description. Edit this to add/remove ads. |
| `update_metrics.py`        | YouTube stats + like-weighted sentiment → `data.json`.      |
| `update_social.py`         | Instagram + TikTok via Apify → `social_data.json`.          |
| `social_actors.json`       | Apify Actor IDs + estimated pricing (editable).             |
| `discover.py`              | Candidate finder: channel watch + tracker scrape + YouTube search → `candidates.json`. |
| `watchlist.json`           | Channels to monitor for new uploads.                        |
| `keywords.json`            | High/medium-confidence terms + negative filters.            |
| `rejected.json`            | Manual blocklist of YouTube IDs we've decided against.      |
| `requirements.txt`         | Python deps (`requests`, `vaderSentiment`).                 |

## Running locally

Activate venv first:

```bash
cd "/path/to/World Cup Ad Tracker"
source .venv/bin/activate
```

YouTube refresh:

```bash
export YOUTUBE_API_KEY=AIza...your_key
python pipeline/update_metrics.py
```

Social refresh (Instagram + TikTok):

```bash
export APIFY_API_TOKEN=apify_api_...your_token
python pipeline/update_social.py             # default: top-10 by views, full sampling
python pipeline/update_social.py --dry-run   # plan only, no spending
python pipeline/update_social.py --metadata-only   # cheap mode (no comments)
python pipeline/update_social.py --validate-token  # confirm auth works
```

## Getting credentials

### YouTube API key
1. <https://console.cloud.google.com> → create a project (free).
2. APIs & Services → Library → enable **YouTube Data API v3**.
3. Credentials → Create credentials → API key.
4. Restrict it (Application restrictions → HTTP referrers or IP; API restrictions → YouTube Data API v3 only).

Default quota: 10,000 units/day. This pipeline costs ~2 units per ad per run.

### Apify API token
1. Sign up free at <https://apify.com>.
2. Console → Settings → Integrations → Personal API tokens → copy.
3. Add to `~/.zshrc`:
   ```bash
   export APIFY_API_TOKEN=apify_api_xxxxxxxxxxxxxxxxxxxx
   ```

Free plan: $5 USD/month in platform credits.

## Cost discipline (Apify)

`update_social.py` tracks every Actor run's cost from the API response and
writes the running monthly total to `pipeline/spend_log.json` (gitignored —
machine-local). It adapts as we approach the cap:

| Monthly spend so far | Sample size per platform |
|----------------------|--------------------------|
| Below $3.00          | 100 comments             |
| $3.00 – $4.50        | 50 comments              |
| Above $4.50          | 0 (metadata only)        |
| At $5.00             | hard cap, all runs skipped |

By default only the **top-10 ads by views** get full sampling; the rest get
metadata-only. A 7-day cache means re-running the same day is free.

To swap Actors (if Meta breaks something or a cheaper option appears), edit
`pipeline/social_actors.json` — Actor IDs and input templates live there.
The script reads pricing as a sanity-check only; actual spend comes from
each run's `usageTotalUsd` reported by Apify.

## Sentiment

`data.json` contains, per ad, the live YouTube metrics plus:

- `weightedSentiment` — 0..100 score. Each comment in the top-100 sample is
  scored by VADER (compound, -1..+1), weighted by `1 + ln(1 + likes)`,
  averaged, normalized to 0..100. Filters out channel-staff replies, URLs,
  very short comments, and obvious bot patterns.
- `sentimentSampleSize` — comments that passed filtering. Below `MIN_SENTIMENT_SAMPLE`
  (default 20), `weightedSentiment` is `null` so the score isn't read off noise.
- `velocityViewsPerDay` — lifetime average. With history snapshots, the
  trailing-7-day rate is also computed and used in preference.

The page combines all signals into a composite:

```
Score = 0.40 × Reach          (log10 views, normalized)
      + 0.20 × Resonance      (engagement rate)
      + 0.10 × Sentiment      (weighted comment score)
      + 0.30 × Velocity       (trailing-7d rate, log10)
```

When sentiment is null on an ad, weights renormalize so it isn't punished
for having too few comments.

## Cross-platform reach (in progress)

`social_data.json` carries per-ad, per-platform metrics. Schema:

```jsonc
{
  "generated": "2026-05-08T22:00:00Z",
  "ads": [
    {
      "adId": "adi-01",
      "brand": "Adidas",
      "platforms": {
        "instagram": {
          "metadata": { "views": 38000000, "likes": 2900000, "commentCount": 25000, ... },
          "comments": [ { "text": "...", "likes": 412, ... }, ... ],
          "fromCache": false
        },
        "tiktok": { ... }
      }
    }
  ]
}
```

Once enough ads have IG/TT URLs populated in `ads.json`, the front-end will
fold per-platform reach into a normalized cross-platform Reach score
(YouTube 50% / Instagram 30% / TikTok 20% as a starting weight).

## Automation

`.github/workflows/update.yml` runs `update_metrics.py` hourly via GitHub
Actions. To enable:

1. Settings → Secrets and variables → Actions → New repository secret
2. Name: `YOUTUBE_API_KEY`, value: your key
3. Push — the schedule activates.

`update_social.py` is run manually for now (Apify cost discipline benefits
from a human-in-the-loop). Once spend behavior is well-understood, we can
add a daily/weekly workflow with `APIFY_API_TOKEN` stored alongside.
