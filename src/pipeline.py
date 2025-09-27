# pipeline.py
# Project CLI to orchestrate crawling and parsing
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# local modules
import crawl
import parse


def cmd_init_batch(args):
    bd = crawl.init_batch()
    print(f"[pipeline] batch created: {bd}")


def cmd_fetch_search(args):
    bd, saved = crawl.fetch_search_pages(limit=args.limit, overwrite=args.overwrite)
    print(f"[pipeline] fetch-search done. batch={bd} saved={saved}")


def cmd_extract_urls(args):
    bd, count = crawl.extract_listing_urls()
    print(f"[pipeline] extract-urls done. batch={bd} urls={count}")


def cmd_fetch_details(args):
    bd, res = crawl.fetch_detail_pages(n=args.n, overwrite=args.overwrite)
    print(
        f"[pipeline] fetch-details done. batch={bd} "
        f"total={res.total} saved={res.saved} skipped={res.skipped} errors={res.errors}"
    )


def cmd_run_full_search(args):
    bd = crawl.run_full_search(limit=args.limit, overwrite=args.overwrite)
    print(f"[pipeline] run-full-search done. batch={bd}")


def cmd_parse_details(args):
    mode = args.mode.lower()
    if mode == "raw":
        bd, summary, _ = parse.parse_all_details(limit=args.limit, save_individual=True)
        print(
            f"[pipeline] parse-details (raw) done. batch={bd} "
            f"files={summary.total_files} parsed_ok={summary.parsed_ok} "
            f"saved_individual={summary.saved_individual}"
        )
    else:
        bd, summary = parse.parse_details_and_adapt(limit=args.limit)
        print(
            f"[pipeline] parse-details (adapted) done. batch={bd} "
            f"files={summary.total_files} parsed_ok={summary.parsed_ok} "
            f"saved_individual={summary.saved_individual}. "
            f"→ wrote per-table JSONL under structured/."
        )


def cmd_fc_extract(args):
    bd, stats = parse.firecrawl_extract(limit=args.limit, delay_sec=args.delay)
    print(
        f"[pipeline] fc-extract done. batch={bd} "
        f"ok={stats.get('ok')} error={stats.get('error')} total={stats.get('total')} "
        f"→ wrote per-table JSONL under structured/."
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipeline.py",
        description="Crawl & Parse CLI (batches/search/details/adapted tables)",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # init-batch
    sp = sub.add_parser("init-batch", help="Create a new batch and seed search pages")
    sp.set_defaults(func=cmd_init_batch)

    # fetch-search
    sp = sub.add_parser("fetch-search", help="Fetch seed search pages into raw/search")
    sp.add_argument("--limit", type=int, default=None, help="Max number of seed pages to fetch")
    sp.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    sp.set_defaults(func=cmd_fetch_search)

    # extract-urls
    sp = sub.add_parser("extract-urls", help="Extract listing detail URLs from raw/search HTML")
    sp.set_defaults(func=cmd_extract_urls)

    # fetch-details
    sp = sub.add_parser("fetch-details", help="Fetch detail pages listed in detail_urls.json into raw/detail")
    sp.add_argument("--n", type=int, default=None, help="Max number of detail URLs to fetch")
    sp.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    sp.set_defaults(func=cmd_fetch_details)

    # run-full-search (init → fetch-search → extract-urls)
    sp = sub.add_parser("run-full-search", help="Init (if needed) + fetch-search + extract-urls")
    sp.add_argument("--limit", type=int, default=None, help="Max number of seed pages to fetch")
    sp.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    sp.set_defaults(func=cmd_run_full_search)

    # parse-details
    sp = sub.add_parser("parse-details", help="Parse raw/detail HTML and optionally write adapted tables")
    sp.add_argument("--limit", type=int, default=None, help="Max number of files to parse")
    sp.add_argument(
        "--mode",
        choices=["raw", "adapted"],
        default="adapted",
        help="raw: write only parsed JSONs; adapted: also emit per-table JSONL under structured/",
    )
    sp.set_defaults(func=cmd_parse_details)

    # fc-extract
    sp = sub.add_parser("fc-extract", help="Direct extraction to per-table JSONL via Firecrawl")
    sp.add_argument("--limit", type=int, default=None, help="Max number of detail URLs to extract")
    sp.add_argument("--delay", type=float, default=None, help="Sleep seconds between API calls")
    sp.set_defaults(func=cmd_fc_extract)

    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
