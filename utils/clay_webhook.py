"""
Clay webhook integration module.
Sends company data to Clay via webhook with batching and error handling.
"""

import requests
import time
import json
from typing import List, Dict, Any, Tuple

import config


def send_to_clay(companies: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Send companies to Clay webhook (one at a time or in batches based on config).
    
    Args:
        companies: List of company dictionaries to send.
        
    Returns:
        Tuple of (successful_count, failed_count)
    """
    print(f"\n{'='*60}")
    print("Sending companies to Clay webhook...")
    print(f"{'='*60}")
    
    if not companies:
        print("  No companies to send")
        return 0, 0
    
    print(f"  Total companies to send: {len(companies)}")
    
    is_single_mode = config.CLAY_BATCH_SIZE == 1
    if is_single_mode:
        print(f"  Mode: Sending one company at a time")
    else:
        print(f"  Batch size: {config.CLAY_BATCH_SIZE}")
    
    successful = 0
    failed = 0
    
    # Send in batches (or one at a time if CLAY_BATCH_SIZE == 1)
    for i in range(0, len(companies), config.CLAY_BATCH_SIZE):
        batch = companies[i:i + config.CLAY_BATCH_SIZE]
        current_num = i + 1
        
        if is_single_mode:
            company_name = batch[0].get("company_name", "Unknown")
            print(f"  [{current_num}/{len(companies)}] Sending: {company_name}")
        else:
            batch_num = (i // config.CLAY_BATCH_SIZE) + 1
            total_batches = (len(companies) + config.CLAY_BATCH_SIZE - 1) // config.CLAY_BATCH_SIZE
            print(f"  Sending batch {batch_num}/{total_batches} ({len(batch)} companies)...")
        
        # Prepare payload for Clay
        payload = _prepare_clay_payload(batch)
        
        success = _send_batch_to_clay(payload)
        
        if success:
            successful += len(batch)
            if not is_single_mode:
                print(f"    Sent successfully")
        else:
            failed += len(batch)
            if is_single_mode:
                print(f"    FAILED")
            else:
                print(f"    Batch failed")
        
        # Small delay between sends
        if i + config.CLAY_BATCH_SIZE < len(companies):
            time.sleep(0.2)  # 200ms delay between individual sends
    
    print(f"\n  Summary:")
    print(f"    Successful: {successful}")
    print(f"    Failed: {failed}")
    
    return successful, failed


def _prepare_clay_payload(companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare company data for Clay webhook.
    Cleans and formats the data according to Clay's expected format.
    """
    cleaned = []
    
    for company in companies:
        # Build a clean record with only relevant fields
        record = {
            "company_name": company.get("company_name", ""),
            "company_website": company.get("company_website"),
            "funding_amount": company.get("funding_amount"),
            "funding_round": company.get("funding_round", ""),
            "investors": _format_list(company.get("investors", [])),
            "industry": company.get("industry", ""),
            "location": company.get("location", ""),
            "founding_year": company.get("founding_year"),
            "source": _format_source(company),
            "announcement_date": company.get("announcement_date", ""),
            "description": company.get("description", ""),
            "ceo_name": company.get("ceo_name"),
            "executives": _format_list(company.get("executives", [])),
            "phone": company.get("phone", ""),
            "linkedin_url": company.get("linkedin_url"),
            "sec_filing_url": company.get("sec_filing_url"),
        }
        
        # Remove None values for cleaner payload
        record = {k: v for k, v in record.items() if v is not None}
        
        cleaned.append(record)
    
    return cleaned


def _format_list(items: List[str]) -> str:
    """
    Format a list of items as a comma-separated string.
    """
    if not items:
        return ""
    return ", ".join(str(item) for item in items if item)


def _format_source(company: Dict[str, Any]) -> str:
    """
    Format the source field, handling merged sources.
    """
    sources = company.get("sources", [])
    if sources:
        return ", ".join(sources)
    return company.get("source", "Unknown")


def _send_batch_to_clay(payload: List[Dict[str, Any]]) -> bool:
    """
    Send a batch of companies to Clay webhook with retry logic.
    """
    headers = {
        "Content-Type": "application/json"
    }
    
    # If sending a single company, unwrap from array to send as single object
    data_to_send = payload[0] if len(payload) == 1 else payload
    
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.post(
                config.CLAY_WEBHOOK_URL,
                json=data_to_send,
                headers=headers,
                timeout=30
            )
            
            # Clay webhooks typically return 200 or 201 on success
            if response.status_code in [200, 201, 202]:
                return True
            else:
                print(f"    Attempt {attempt}: HTTP {response.status_code}")
                if response.text:
                    print(f"    Response: {response.text[:200]}")
                    
        except requests.exceptions.RequestException as e:
            print(f"    Attempt {attempt}: Error - {e}")
        
        if attempt < config.MAX_RETRIES:
            time.sleep(config.RETRY_DELAY * attempt)
    
    return False


def send_single_to_clay(company: Dict[str, Any]) -> bool:
    """
    Send a single company to Clay webhook.
    Useful for testing or one-off sends.
    
    Args:
        company: Company dictionary to send.
        
    Returns:
        True if successful, False otherwise.
    """
    payload = _prepare_clay_payload([company])
    return _send_batch_to_clay(payload)


def test_clay_webhook() -> bool:
    """
    Test the Clay webhook connection with a test payload.
    
    Returns:
        True if webhook is reachable and accepting data.
    """
    print("Testing Clay webhook connection...")
    
    test_payload = [{
        "company_name": "Test Company",
        "company_website": "https://test.com",
        "funding_amount": 1000000,
        "funding_round": "Test",
        "source": "Test",
        "description": "This is a test record from the SEC scraper."
    }]
    
    try:
        response = requests.post(
            config.CLAY_WEBHOOK_URL,
            json=test_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code in [200, 201, 202]:
            print(f"  Webhook test successful (HTTP {response.status_code})")
            return True
        else:
            print(f"  Webhook test failed (HTTP {response.status_code})")
            print(f"  Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"  Webhook test error: {e}")
        return False


def generate_summary_report(
    sec_companies: List[Dict[str, Any]],
    news_companies: List[Dict[str, Any]],
    deduped_companies: List[Dict[str, Any]],
    successful: int,
    failed: int
) -> str:
    """
    Generate a summary report of the scraping run.
    """
    report = []
    report.append("\n" + "=" * 60)
    report.append("SCRAPING RUN SUMMARY")
    report.append("=" * 60)
    report.append(f"\nData Sources:")
    report.append(f"  SEC Form D filings: {len(sec_companies)}")
    report.append(f"  News sources: {len(news_companies)}")
    report.append(f"    - TechCrunch")
    report.append(f"    - VentureBeat")
    report.append(f"    - CB Insights")
    report.append(f"    - PitchBook")
    report.append(f"    - Founder Collective")
    report.append(f"\nProcessing:")
    report.append(f"  Total before dedup: {len(sec_companies) + len(news_companies)}")
    report.append(f"  After deduplication: {len(deduped_companies)}")
    report.append(f"  Duplicates removed: {len(sec_companies) + len(news_companies) - len(deduped_companies)}")
    report.append(f"\nClay Webhook:")
    report.append(f"  Successfully sent: {successful}")
    report.append(f"  Failed: {failed}")
    report.append("=" * 60)
    
    return "\n".join(report)


if __name__ == "__main__":
    # Test the webhook
    test_clay_webhook()

