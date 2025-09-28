# Real Estate Listing Pipeline

## 📌 Overview
A data pipeline to collect, parse, and structure real estate listings from **Zillow** and **Redfin** using [Firecrawl](https://firecrawl.dev).  
The pipeline scrapes search results per ZIP, extracts hidden JSON-LD/JS state, normalizes fields, and writes clean JSON tables.

## 📂 Project Structure
src/
extractors.py # regex + JSON-LD + hidden JS parsers
parsers.py # core detail-page parsing
steps/
search_links.py # collect detail links from search pages
fetch_details.py # fetch + parse detail pages
firecrawl_client.py # wrapper around Firecrawl SDK
pipeline.py # main pipeline runner
schemas.py # pydantic schemas for tables
settings.py # config, paths, env helpers
utils.py # misc utils (safe_int, write_json…)
data/
batches/ # normal full runs
test_runs/ # smaller test runs
config/
listings_config.json # areas & seeds (Zillow/Redfin URLs, ZIPs)


## ⚙️ Requirements
- Python 3.9+
- Install dependencies:
  ```bash
  pip install -r requirements.txt


Create .env with:

FIRECRAWL_API_KEY=fc-xxxxxxx

▶️ Running Full Pipeline

Run the whole search → detail → parse → write JSON batch:

python -m src.pipeline


Outputs under:

data/batches/<batch_id>/
  ├── search_links.json   # counts of collected detail links
  ├── detail_urls.json    # all detail URLs
  ├── raw/                # raw HTML & markdown for each listing
  ├── properties.json
  ├── listings.json
  ├── media.json
  ├── price_history.json
  ├── qa_rejects.json     # rejected listings w/ reasons
  └── batch_summary.json

🧪 Running a Test Subset

For quick experiments, use the helper script:

python test_run_subset.py -n 5


Collects only first ZIP from config and limits to 5 listings.

Writes results under:

data/test_runs/test-<timestamp>/
  ├── raw/
  ├── properties.json
  ├── listings.json
  ├── media.json
  ├── price_history.json
  └── summary.json

Options

-n N: number of listings to fetch (default=3).

The script always fetches with onlyMainContent=False to preserve hidden JS state.

✅ Validation & Rejections

Listings missing location, price, or all specs (beds/baths/area) are rejected and logged in qa_rejects.json.

Pydantic models (schemas.py) validate fields:

postal_code must be numeric string.

list_price must be positive.

🔑 Config Structure

config/listings_config.json example:

{
  "run": {
    "request_timeout_sec": 30,
    "sleep_range_sec": [1.2, 2.8],
    "per_zip_limit": 20
  },
  "crawl_method": "firecrawl_v1",
  "seeds": {
    "zillow": { "zip_search": "https://www.zillow.com/homes/{ZIP}_rb/" },
    "redfin": { "zip_search": "https://www.redfin.com/zip/{ZIP}" }
  },
  "areas": [
    {
      "city": "New York",
      "state": "NY",
      "zips": ["10003", "10011", "10012"]
    }
  ]
}


per_zip_limit: optional, cap the number of detail links per ZIP.

Add/remove ZIP codes under areas.

🛠️ Tips

Start with small test runs (test_run_subset.py) before full batches.

Inspect qa_rejects.json to understand why listings were skipped.

Raw HTML (raw/) is always stored for debugging parsing failures.

