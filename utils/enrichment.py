"""
Website enrichment module using Perplexity AI.
Finds company websites for records that don't have them.
"""

import requests
import time
import re
from typing import List, Dict, Any, Optional

import config


def enrich_with_websites(companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enrich company records with website URLs using Perplexity AI.
    Only queries for companies that don't already have a website.
    
    Args:
        companies: List of company dictionaries.
        
    Returns:
        List of companies with website data enriched.
    """
    print(f"\n{'='*60}")
    print("Enriching companies with website data...")
    print(f"{'='*60}")
    
    # Count companies needing website lookup
    need_website = [c for c in companies if not c.get("company_website")]
    print(f"  Companies needing website lookup: {len(need_website)}/{len(companies)}")
    
    enriched_count = 0
    
    for i, company in enumerate(companies):
        if company.get("company_website"):
            continue  # Already has website
        
        company_name = company.get("company_name", "")
        industry = company.get("industry", "")
        location = company.get("location", "")
        
        if not company_name:
            continue
        
        # Rate limiting
        if i > 0:
            time.sleep(config.PERPLEXITY_API_DELAY)
        
        print(f"  [{i+1}/{len(need_website)}] Looking up website for: {company_name}")
        
        website = _find_company_website(company_name, industry, location)
        
        if website:
            company["company_website"] = website
            enriched_count += 1
            print(f"    Found: {website}")
        else:
            print(f"    Not found")
    
    print(f"  Enriched {enriched_count} companies with websites")
    
    return companies


def _find_company_website(company_name: str, industry: str = "", location: str = "") -> Optional[str]:
    """
    Find a company's website using Perplexity AI.
    """
    # Build context for the search
    context_parts = []
    if industry:
        context_parts.append(f"in the {industry} industry")
    if location:
        context_parts.append(f"based in {location}")
    
    context = " ".join(context_parts) if context_parts else "that recently raised funding"
    
    prompt = f"""What is the official company website URL for "{company_name}" {context}?

Return ONLY the website URL, nothing else. If you cannot find the official website, return "NOT_FOUND".

Example response: https://www.example.com"""

    response = _make_perplexity_request(prompt)
    
    if response:
        website = _extract_website_from_response(response)
        if website:
            return website
    
    return None


def _make_perplexity_request(prompt: str) -> Optional[str]:
    """
    Make a request to Perplexity AI API.
    """
    headers = {
        "Authorization": f"Bearer {config.PERPLEXITY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "sonar",
        "messages": [
            {
                "role": "system",
                "content": "You are a research assistant that finds company websites. Return only the URL, nothing else."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.1,
        "max_tokens": 200
    }
    
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.post(
                config.PERPLEXITY_API_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content
            
        except requests.exceptions.RequestException as e:
            if attempt < config.MAX_RETRIES:
                time.sleep(config.RETRY_DELAY * attempt)
            else:
                print(f"    API error: {e}")
    
    return None


def _extract_website_from_response(response: str) -> Optional[str]:
    """
    Extract and validate a website URL from the Perplexity response.
    """
    if not response:
        return None
    
    response = response.strip()
    
    # Check for "not found" responses
    not_found_patterns = ["not_found", "not found", "unable to find", "could not find", "n/a", "unknown"]
    if any(pattern in response.lower() for pattern in not_found_patterns):
        return None
    
    # Try to extract URL from response
    url_pattern = r'https?://[^\s<>"\')\]]+(?:\.[^\s<>"\')\]]+)+'
    matches = re.findall(url_pattern, response)
    
    if matches:
        url = matches[0]
        # Clean up trailing punctuation
        url = url.rstrip('.,;:')
        
        # Validate URL
        if _is_valid_website(url):
            return url
    
    # If no URL found but response looks like a domain, add https://
    domain_pattern = r'^(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z]{2,})+)$'
    domain_match = re.match(domain_pattern, response.strip())
    if domain_match:
        url = f"https://{response.strip()}"
        if _is_valid_website(url):
            return url
    
    return None


def _is_valid_website(url: str) -> bool:
    """
    Basic validation of a website URL.
    """
    if not url:
        return False
    
    # Must start with http:// or https://
    if not url.startswith(('http://', 'https://')):
        return False
    
    # Must have a valid TLD
    tld_pattern = r'\.[a-zA-Z]{2,}(?:/|$)'
    if not re.search(tld_pattern, url):
        return False
    
    # Exclude common non-company domains
    excluded_domains = [
        'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
        'youtube.com', 'crunchbase.com', 'pitchbook.com', 'bloomberg.com',
        'sec.gov', 'wikipedia.org', 'google.com', 'bing.com'
    ]
    
    for domain in excluded_domains:
        if domain in url.lower():
            return False
    
    return True


def batch_enrich_websites(companies: List[Dict[str, Any]], max_lookups: int = 50) -> List[Dict[str, Any]]:
    """
    Enrich websites with a limit on the number of API calls.
    Useful for large datasets to control API usage.
    
    Args:
        companies: List of company dictionaries.
        max_lookups: Maximum number of website lookups to perform.
        
    Returns:
        List of companies with website data enriched (up to max_lookups).
    """
    print(f"\n{'='*60}")
    print(f"Batch enriching websites (max {max_lookups} lookups)...")
    print(f"{'='*60}")
    
    lookup_count = 0
    enriched_count = 0
    
    for company in companies:
        if company.get("company_website"):
            continue
        
        if lookup_count >= max_lookups:
            remaining = sum(1 for c in companies if not c.get("company_website"))
            print(f"  Reached max lookups. {remaining} companies still without websites.")
            break
        
        company_name = company.get("company_name", "")
        if not company_name:
            continue
        
        # Rate limiting
        if lookup_count > 0:
            time.sleep(config.PERPLEXITY_API_DELAY)
        
        lookup_count += 1
        print(f"  [{lookup_count}/{max_lookups}] Looking up: {company_name}")
        
        website = _find_company_website(
            company_name,
            company.get("industry", ""),
            company.get("location", "")
        )
        
        if website:
            company["company_website"] = website
            enriched_count += 1
            print(f"    Found: {website}")
    
    print(f"  Completed {lookup_count} lookups, enriched {enriched_count} companies")
    
    return companies


if __name__ == "__main__":
    # Test the module
    test_companies = [
        {"company_name": "OpenAI", "industry": "Artificial Intelligence", "location": "San Francisco, CA"},
        {"company_name": "Stripe", "industry": "Fintech", "location": "San Francisco, CA"},
    ]
    
    enriched = enrich_with_websites(test_companies)
    for c in enriched:
        print(f"{c['company_name']}: {c.get('company_website', 'Not found')}")

