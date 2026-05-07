# Pipeline

The Pitch is a static site backed by a tiny scheduled Python job. The job pulls
live YouTube metrics (views, likes, comments, publish date) plus comment
sentiment for each tracked ad, and writes the results to `data.json` at the
repo root. The page reads from `data.json` on load.

## What's in here

| File                   | Purpose                                                     |
|------------------------|-------------------------------------------------------------|
| `ads.json`             | Source of truth for static metadata: brand, agency, title, YouTube ID, description. Edit this to add or remove ads. |
| `update_metrics.py`    | Pulls live stats + computes like-weighted sentiment. Writes `data.json`. |
| `requirements.txt`     | Python deps (`requests`, `vaderSentiment`).                 |

## Running locally

```bash
pip install -r pipeline/requirements.txt
export YOUTUBE_API_KEY=your_key_here
python pipeline/update_metrics.py
```

You'll see a row per ad with views, likes, and the like-weighted sentiment
score (plus the valid sample size). `data.json` is rewritten in place at the
repo root.

## Getting a YouTube API key

1. Go to <https://console.cloud.google.com>, create a project (free).
2. APIs & Services → Library → enable **YouTube Data API v3**.
3. APIs & Services → Credentials → Create credentials → API key.
4. Restrict the key (recommended): Application restrictions → IP addresses or
   HTTP referrers; API restrictions → YouTube Data API v3 only.

The default daily quota is 10,000 units. This pipeline costs ~2 units per ad
per run, so the full 12-ad refresh is ~24 units. Hourly = ~600 units/day,
well inside the free tier.

## Methodology

`data.json` contains, per ad, the live YouTube metrics plus:

* `weightedSentiment` — 0..100 score. Each comment in the top-100 sample is
  scored by VADER (compound, -1..+1), weighted by `1 + ln(1 + likes)`,
  averaged, and normalized to 0..100. Channel-staff replies, URLs, very short
  comments, and obvious bot patterns are filtered before scoring.
* `sentimentSampleSize` — number of comments that passed filtering. Below
  the minimum (default 20), `weightedSentiment` is `null` to suppress noisy
  reads on thinly-commented videos.
* `velocityViewsPerDay` — `views ÷ days_since_publish`. The "trajectory"
  signal in the score formula.

The page combines these into a single composite:

```
Score = 0.40 × Reach
      + 0.20 × Resonance
      + 0.25 × Sentiment
      + 0.15 × Velocity
        (each factor normalized against the leader in the dataset, × 100)
```

Want to retune? Adjust the weights in `methodology.weights` in `data.json`,
or in the `WEIGHTS` constant at the top of the script in `index.html`.

## Automation

The repo includes `.github/workflows/update.yml`, which runs the script
hourly via GitHub Actions and commits any change to `data.json`. To enable:

1. Repo Settings → Secrets and variables → Actions → New repository secret
2. Name: `YOUTUBE_API_KEY`, value: your key
3. Push to `main` — the schedule activates automatically. Trigger manually
   from the Actions tab to verify.

If you'd rather run it locally on a cron, the same script works there.
