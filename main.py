#!/usr/bin/env python3
"""
Multi-Source Funded Companies Scraper

Fetches recently funded US companies from:
1. SEC Form D filings (official regulatory data)
2. TechCrunch (via Perplexity AI search)
3. VentureBeat (via Perplexity AI search)
4. CB Insights (via Perplexity AI search)
5. PitchBook (via Perplexity AI search)
6. Founder Collective Portfolio (via Perplexity AI search)

Then deduplicates, enriches with website data, and sends to Clay webhook.
"""

import sys
import argparse
from datetime import datetime

# Import source modules
from sources.sec_api import fetch_sec_form_d_filings
from sources.perplexity_news import fetch_funding_news_from_all_sources

# Import utility modules
from utils.deduplication import deduplicate_companies, get_dedup_stats
from utils.enrichment import enrich_with_websites, batch_enrich_websites
from utils.clay_webhook import send_to_clay, test_clay_webhook, generate_summary_report

import config


def main(
    skip_sec: bool = False,
    skip_news: bool = False,
    skip_enrichment: bool = False,
    max_website_lookups: int = 50,
    dry_run: bool = False,
    test_webhook: bool = False
):
    """
    Main orchestration function.
    
    Args:
        skip_sec: Skip SEC Form D API fetching.
        skip_news: Skip news source scraping via Perplexity.
        skip_enrichment: Skip website enrichment.
        max_website_lookups: Maximum number of website lookups to perform.
        dry_run: If True, don't send to Clay (just show what would be sent).
        test_webhook: If True, only test the Clay webhook and exit.
    """
    start_time = datetime.now()
    
    print("\n" + "=" * 60)
    print("MULTI-SOURCE FUNDED COMPANIES SCRAPER")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Test webhook only mode
    if test_webhook:
        success = test_clay_webhook()
        sys.exit(0 if success else 1)
    
    # Show configuration
    print(f"\nConfiguration:")
    print(f"  Lookback period: {config.LOOKBACK_DAYS} days")
    print(f"  Skip SEC Form D: {skip_sec}")
    print(f"  Skip news sources: {skip_news}")
    print(f"  Skip website enrichment: {skip_enrichment}")
    print(f"  Max website lookups: {max_website_lookups}")
    print(f"  Dry run: {dry_run}")
    
    # Collect companies from all sources
    sec_companies = []
    news_companies = []
    
    # Fetch from SEC Form D
    if not skip_sec:
        try:
            sec_companies = fetch_sec_form_d_filings()
        except Exception as e:
            print(f"\nError fetching SEC Form D filings: {e}")
            print("Continuing with news sources...")
    else:
        print("\n  Skipping SEC Form D fetching...")
    
    # Fetch from news sources via Perplexity
    if not skip_news:
        try:
            news_companies = fetch_funding_news_from_all_sources()
        except Exception as e:
            print(f"\nError fetching news sources: {e}")
            print("Continuing with SEC data only...")
    else:
        print("\n  Skipping news source fetching...")
    
    # Combine all companies
    all_companies = sec_companies + news_companies
    
    if not all_companies:
        print("\nNo companies found from any source. Exiting.")
        return
    
    print(f"\nTotal companies collected: {len(all_companies)}")
    print(f"  From SEC Form D: {len(sec_companies)}")
    print(f"  From news sources: {len(news_companies)}")
    
    # Deduplicate
    deduped_companies = deduplicate_companies(all_companies)
    
    # Show deduplication stats
    stats = get_dedup_stats(all_companies, deduped_companies)
    print(f"\nDeduplication stats:")
    print(f"  Duplicates removed: {stats['duplicates_removed']}")
    print(f"  Companies with merged sources: {stats['companies_with_merged_sources']}")
    
    # Enrich with websites
    if not skip_enrichment and deduped_companies:
        try:
            deduped_companies = batch_enrich_websites(deduped_companies, max_lookups=max_website_lookups)
        except Exception as e:
            print(f"\nError enriching websites: {e}")
            print("Continuing without website enrichment...")
    elif skip_enrichment:
        print("\n  Skipping website enrichment...")
    
    # Count companies with websites
    with_websites = sum(1 for c in deduped_companies if c.get("company_website"))
    print(f"\nCompanies with websites: {with_websites}/{len(deduped_companies)}")
    
    # Send to Clay
    if dry_run:
        print("\n  DRY RUN - Not sending to Clay")
        print(f"  Would send {len(deduped_companies)} companies")
        
        # Show sample of what would be sent
        print("\n  Sample companies:")
        for company in deduped_companies[:5]:
            print(f"    - {company['company_name']}")
            if company.get('company_website'):
                print(f"      Website: {company['company_website']}")
            if company.get('funding_amount'):
                print(f"      Funding: ${company['funding_amount']:,}")
            print(f"      Source: {company.get('source', company.get('sources', 'Unknown'))}")
        
        successful, failed = len(deduped_companies), 0
    else:
        successful, failed = send_to_clay(deduped_companies)
    
    # Generate and print summary
    summary = generate_summary_report(
        sec_companies,
        news_companies,
        deduped_companies,
        successful,
        failed
    )
    print(summary)
    
    # Calculate runtime
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\nCompleted at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total runtime: {duration}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape recently funded companies and send to Clay webhook."
    )
    
    parser.add_argument(
        "--skip-sec",
        action="store_true",
        help="Skip SEC Form D API fetching"
    )
    
    parser.add_argument(
        "--skip-news",
        action="store_true",
        help="Skip news source scraping via Perplexity"
    )
    
    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Skip website enrichment step"
    )
    
    parser.add_argument(
        "--max-website-lookups",
        type=int,
        default=50,
        help="Maximum number of website lookups (default: 50)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't send to Clay, just show what would be sent"
    )
    
    parser.add_argument(
        "--test-webhook",
        action="store_true",
        help="Only test the Clay webhook connection and exit"
    )
    
    args = parser.parse_args()
    
    main(
        skip_sec=args.skip_sec,
        skip_news=args.skip_news,
        skip_enrichment=args.skip_enrichment,
        max_website_lookups=args.max_website_lookups,
        dry_run=args.dry_run,
        test_webhook=args.test_webhook
    )

