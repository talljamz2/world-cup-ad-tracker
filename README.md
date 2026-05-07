# The Pitch — World Cup '26 Ad Tracker

A live leaderboard of FIFA World Cup 2026 brand advertising, ranked on a
transparent four-factor composite score. Built for marketing, creative, and
strategy execs who want to see who's actually winning the tournament's
branding battle — not just whose campaign got the loudest press release.

## Architecture

Static HTML page + tiny scheduled Python job. Cheap, fast, no servers.

```
index.html              → the page (single file, vanilla JS, no build step)
data.json               → live stats + sentiment, written by the pipeline
pipeline/
  ads.json              → static metadata (brand, agency, title, YouTube ID)
  update_metrics.py     → pulls YouTube stats, scores comment sentiment
  requirements.txt      → Python deps (requests, vaderSentiment)
  README.md             → setup details
.github/workflows/
  update.yml            → runs the pipeline hourly, commits data.json
```

## Methodology

The Composite Score combines four normalized factors:

| Factor    | Weight | What it measures                                      |
|-----------|--------|-------------------------------------------------------|
| Reach     | 40%    | YouTube views                                         |
| Resonance | 20%    | Engagement rate — (likes + comments) ÷ views          |
| Sentiment | 25%    | Like-weighted sentiment of the top 100 comments       |
| Velocity  | 15%    | Views per day since publish                           |

Sentiment is the differentiator. Each comment in the top-100 sample is scored
with VADER (compound, -1 to +1), then weighted by `1 + ln(1 + likes)` so the
audience-endorsed reactions carry more influence than the average commenter.
Normalized to 0–100. When fewer than 20 valid comments survive filtering
(channel-staff replies, URLs, bot patterns), the sentiment factor is dropped
for that ad and the remaining weights are renormalized.

## Running locally

```bash
pip install -r pipeline/requirements.txt
export YOUTUBE_API_KEY=your_key_here
python3 pipeline/update_metrics.py     # writes data.json
python3 -m http.server 8000            # serve the static site
# open http://localhost:8000
```

The page works without `data.json` too — it falls back to inline sample data,
so you can preview the layout before any keys are wired up.

## Production

GitHub Actions runs `pipeline/update_metrics.py` every hour, commits any
change to `data.json`, and the deployed static site picks it up on next
visit. Secret `YOUTUBE_API_KEY` lives in repo Settings → Secrets and
variables → Actions.

Quota cost: ~24 YouTube API units per run, ~600/day on the hourly schedule.
The free quota is 10,000/day.
