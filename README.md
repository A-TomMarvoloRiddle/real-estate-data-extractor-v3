# Real Estate Listing Optimization

## 📌 Overview
This project is the data crawling section of **Fellowship.AI (Sept 2025)**.  
The goal is to build an intelligent pipeline that scrapes property listings from **Zillow** and **Redfin**, parses structured data, and later runs AI-powered analysis for pricing, content quality, and optimization recommendations.  

The platform aims to help real estate agents identify **mispriced or poorly optimized listings** and provide **actionable improvements** to reduce time-on-market.

---


## 📂 Project Structure
```
REAL-ESTATE-LISTING-OPTIMIZATION/
│── config/
│   ├── raw/                      #contains the raw data (outputs from 1.1) as csv files
│       ├── top_regions_with_listing_count_and_median_price.csv
|       ├── top_zipcodes_per_city.csv
│   ├── redfin_city_ids.json      
│   ├── listings_config.json      #the file contains the listings to search for
│── data/                         #code outputs organized in batch folders
│── src/
│   ├── __init__.py
│   ├── batch.py                  #Initialize a new batch with ID, folders, and seed search pages
│   ├── extract_search.py         #extracts listings urls "details pages" from search pages
│   ├── fc_extract_adapted.py     #Firecrawl-based extract, emits JSON (arrays) for all schema tables.
│   ├── fetch.py                  #fetches search pages(raw html + meta data) from the seed files
│   ├── parse_detail.py           #parse details of search pages into structured JSON format
│   ├── pipeline.py               #orchestrate fetching details & parsing using existing modules.
│   ├── settings.py               #centraalized settings and helpers
│── tools/ 
│   ├── build_listings_config.py  #build config/listings_config.json based on config/raw files
│── .env                          #environment variables "FIRECRAWL_API_KEY"
│── requirements.txt              #python requirements to run this project
│── README.md                     #this file
```

---

## ✅ Current Progress

- **Schema-driven structured data**: Address, price, beds, baths, area, description, images, and more.
- **Batch pipeline**: Fetch → Parse → Run orchestration ([src/pipeline.py](src/pipeline.py)).
- **Raw HTML/JSON storage**: All source files saved for reproducibility.
- **Robust parsing**: Multi-strategy extraction from Redfin/Zillow, with schema.org and regex fallbacks.
- **Configurable areas/zips**: Easily add new cities/zips via [config/listings_config.json](config/listings_config.json).

---

## ⚙️ Usage
Run from the project root:

```bash
# Step 1: Initialize batch 
python -m src.batch
#generates a new batch folder in the data folder, containing qa, raw, structured folders, and structured/seed_search_pages.json file

# Step 1: Fetch search pages 
python -m src.fetch
# generates raw html and meta data files for search pages saved in data/batches/<BATCH ID>/raw

# Step 2: Fetch detail pages
python -m src.extract_search --n 10
#extracts listing urls "details pages" from search pages, save results in data/batches/<BATCH ID>/structured/listing_urls.json

#Step 3: Fetch details of n urls "detail pages"
-m src.pipeline fetch-details --n 50 
#fetchs details of listin urls, save raw HTML and meta data json files in data/batches/<batch_id>/raw/ 

#Step 4: Parse details of fetched urls
python -m src.pipeline parse-details --limit 50 --mode raw
#extract data from urls and generates json files for each listing

#Step 5: Creates JSON files for each table
python -m src.pipeline parse-details --limit 50 --mode adapted
# results in JSON files{listings, agents, properities..} "each represent a table to be in the DB" saved in data/batches/<batch_id>/structured/

#Step 3: (Step 3, 4, and 5 can be executed in one line: )
python -m src.pipeline run --n 50 --limit 80
```

Outputs are stored in `data/batches/{batch_id}/`:

- `raw/` — raw HTML/JSON snapshots of listing pages
- `structured/` — parsed structured JSON files

---

## 📑 Data Specification

- **Identifiers**: listing_id, platform_id, source_url, batch_id, scraped_timestamp
- **Address**: street, unit, city, state, postal_code
- **Property Attributes**: beds, baths, interior_area_sqft, lot_sqft, year_built, condition
- **Listing Info**: status, list_date, days_on_market, list_price, price_per_sqft
- **Media**: photos, videos, floorplans
- **Description**: textual content
- **Market Signals**: views, saves, share_count
- **Deduplication**: possible_duplicate, duplicate_candidates

See [`config/listings_config.json`](config/listings_config.json) for the full schema.

---
---

## 📝 References

- `src/pipeline.py` — main orchestration
- `src/fetch.py` — fetching logic
- `src/parse_detail.py` — parsing logic
- `config/listings_config.json` — schema and area/zips config

---
