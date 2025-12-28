"""
Perplexity AI-based news scraper for funding announcements.
Searches TechCrunch, VentureBeat, CB Insights, PitchBook, and Founder Collective.
"""

import requests
import time
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import config


# Prompts for each news source
NEWS_SOURCE_PROMPTS = {
    "TechCrunch": """Search TechCrunch for US-based startup funding announcements from the last 30 days.
List each company that raised funding with:
- Company name
- Funding amount (in USD)
- Funding round (Seed, Series A, B, C, etc.)
- Lead investors
- Company industry/sector
- Brief company description
- Headquarters location

Return ONLY a valid JSON array with objects containing these exact fields:
{
  "company_name": "string",
  "funding_amount": number or null,
  "funding_round": "string",
  "investors": ["array of investor names"],
  "industry": "string",
  "description": "string",
  "location": "string"
}

Include at least 10-15 companies if available. Return ONLY the JSON array, no other text.""",

    "VentureBeat": """Search VentureBeat for venture capital and startup funding announcements from the last 30 days for US-based companies.
List each company that announced funding with:
- Company name
- Funding amount (in USD)
- Funding round type
- Investors
- Industry/sector
- Brief description
- Location

Return ONLY a valid JSON array with objects containing these exact fields:
{
  "company_name": "string",
  "funding_amount": number or null,
  "funding_round": "string",
  "investors": ["array of investor names"],
  "industry": "string",
  "description": "string",
  "location": "string"
}

Include at least 10-15 companies if available. Return ONLY the JSON array, no other text.""",

    "CB Insights": """Search CB Insights for recent startup funding rounds and deals from the last 30 days for US-based companies.
List companies that received funding with:
- Company name
- Funding amount
- Round type (Seed, Series A, B, etc.)
- Investors
- Industry
- Description
- Location

Return ONLY a valid JSON array with objects containing these exact fields:
{
  "company_name": "string",
  "funding_amount": number or null,
  "funding_round": "string",
  "investors": ["array of investor names"],
  "industry": "string",
  "description": "string",
  "location": "string"
}

Include at least 10-15 companies if available. Return ONLY the JSON array, no other text.""",

    "PitchBook": """Search PitchBook and related news for recent private equity and venture capital deals from the last 30 days involving US-based companies.
List companies that raised funding with:
- Company name
- Deal size/funding amount
- Round type
- Investors
- Sector/Industry
- Description
- Location

Return ONLY a valid JSON array with objects containing these exact fields:
{
  "company_name": "string",
  "funding_amount": number or null,
  "funding_round": "string",
  "investors": ["array of investor names"],
  "industry": "string",
  "description": "string",
  "location": "string"
}

Include at least 10-15 companies if available. Return ONLY the JSON array, no other text.""",

    "Founder Collective Portfolio": """Search for recent funding announcements from Founder Collective portfolio companies and other notable early-stage startups from the last 30 days.
Focus on US-based companies that announced funding rounds.
List companies with:
- Company name
- Funding amount
- Round type
- Investors
- Industry
- Description
- Location

Return ONLY a valid JSON array with objects containing these exact fields:
{
  "company_name": "string",
  "funding_amount": number or null,
  "funding_round": "string",
  "investors": ["array of investor names"],
  "industry": "string",
  "description": "string",
  "location": "string"
}

Include at least 5-10 companies if available. Return ONLY the JSON array, no other text."""
}


def fetch_funding_news_from_all_sources() -> List[Dict[str, Any]]:
    """
    Fetch funding news from all configured news sources using Perplexity AI.
    
    Returns:
        List of normalized company dictionaries from all sources.
    """
    print(f"\n{'='*60}")
    print("Fetching funding news via Perplexity AI...")
    print(f"{'='*60}")
    
    all_companies = []
    
    for source_name, prompt in NEWS_SOURCE_PROMPTS.items():
        print(f"\n  Searching {source_name}...")
        
        companies = _fetch_from_source(source_name, prompt)
        
        if companies:
            print(f"    Found {len(companies)} companies from {source_name}")
            all_companies.extend(companies)
        else:
            print(f"    No companies found from {source_name}")
        
        # Rate limiting between requests
        time.sleep(config.PERPLEXITY_API_DELAY)
    
    print(f"\n  Total companies from news sources: {len(all_companies)}")
    return all_companies


def _fetch_from_source(source_name: str, prompt: str) -> List[Dict[str, Any]]:
    """
    Fetch funding news from a single source using Perplexity AI.
    """
    response = _make_perplexity_request(prompt)
    
    if response is None:
        return []
    
    # Parse the response
    companies = _parse_perplexity_response(response, source_name)
    
    return companies


def _make_perplexity_request(prompt: str) -> Optional[str]:
    """
    Make a request to Perplexity AI API with retry logic.
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
                "content": "You are a financial research assistant that finds and reports on startup funding announcements. Always return data in valid JSON format."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.1,
        "max_tokens": 4000
    }
    
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.post(
                config.PERPLEXITY_API_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content
            
        except requests.exceptions.RequestException as e:
            print(f"    Attempt {attempt}/{config.MAX_RETRIES} failed: {e}")
            if attempt < config.MAX_RETRIES:
                time.sleep(config.RETRY_DELAY * attempt)
    
    return None


def _parse_perplexity_response(response: str, source_name: str) -> List[Dict[str, Any]]:
    """
    Parse Perplexity AI response and extract company data.
    """
    companies = []
    
    try:
        # Try to extract JSON from the response
        json_str = _extract_json_from_text(response)
        
        if not json_str:
            print(f"    Could not extract JSON from response")
            return []
        
        data = json.loads(json_str)
        
        # Handle both array and object with array property
        if isinstance(data, dict):
            # Look for an array property
            for key in ["companies", "results", "data", "funding_rounds"]:
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                # If still a dict, wrap in list
                data = [data]
        
        if not isinstance(data, list):
            print(f"    Response is not a list: {type(data)}")
            return []
        
        # Normalize each company
        for item in data:
            if isinstance(item, dict):
                company = _normalize_news_company(item, source_name)
                if company:
                    companies.append(company)
        
    except json.JSONDecodeError as e:
        print(f"    JSON parse error: {e}")
    except Exception as e:
        print(f"    Error parsing response: {e}")
    
    return companies


def _extract_json_from_text(text: str) -> Optional[str]:
    """
    Extract JSON array or object from text that may contain other content.
    """
    # First, try to parse the entire text as JSON
    text = text.strip()
    
    # Remove markdown code blocks if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```json or ```) and last line (```)
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    
    # Try direct parse
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass
    
    # Try to find JSON array
    array_match = re.search(r'\[[\s\S]*\]', text)
    if array_match:
        try:
            json.loads(array_match.group())
            return array_match.group()
        except json.JSONDecodeError:
            pass
    
    # Try to find JSON object
    object_match = re.search(r'\{[\s\S]*\}', text)
    if object_match:
        try:
            json.loads(object_match.group())
            return object_match.group()
        except json.JSONDecodeError:
            pass
    
    return None


def _normalize_news_company(item: Dict[str, Any], source_name: str) -> Optional[Dict[str, Any]]:
    """
    Normalize a company from news source into our standard format.
    """
    try:
        company_name = item.get("company_name", "").strip()
        if not company_name:
            return None
        
        # Extract funding amount
        funding_amount = item.get("funding_amount")
        if isinstance(funding_amount, str):
            # Try to parse string amounts like "$10M" or "10 million"
            funding_amount = _parse_funding_amount(funding_amount)
        
        # Extract investors
        investors = item.get("investors", [])
        if isinstance(investors, str):
            # Split comma-separated string
            investors = [inv.strip() for inv in investors.split(",") if inv.strip()]
        
        return {
            "company_name": company_name,
            "company_website": None,  # Will be enriched later
            "funding_amount": funding_amount,
            "amount_sold": None,
            "funding_round": item.get("funding_round", "Unknown"),
            "investors": investors if isinstance(investors, list) else [],
            "industry": item.get("industry", ""),
            "location": item.get("location", ""),
            "founding_year": None,
            "source": source_name,
            "announcement_date": datetime.now().strftime("%Y-%m-%d"),
            "description": item.get("description", ""),
            "ceo_name": None,
            "executives": [],
            "phone": "",
            "linkedin_url": None,
            "sec_filing_url": None,
            "total_investors": len(investors) if isinstance(investors, list) else 0
        }
        
    except Exception as e:
        print(f"    Error normalizing company: {e}")
        return None


def _parse_funding_amount(amount_str: str) -> Optional[int]:
    """
    Parse funding amount strings like "$10M", "10 million", "$1.5B" into integers.
    """
    if not amount_str:
        return None
    
    amount_str = amount_str.lower().replace(",", "").replace("$", "").strip()
    
    # Handle "undisclosed" or similar
    if "undisclosed" in amount_str or "unknown" in amount_str:
        return None
    
    multipliers = {
        "k": 1_000,
        "thousand": 1_000,
        "m": 1_000_000,
        "million": 1_000_000,
        "mm": 1_000_000,
        "b": 1_000_000_000,
        "billion": 1_000_000_000
    }
    
    try:
        # Extract numeric part
        number_match = re.search(r'[\d.]+', amount_str)
        if not number_match:
            return None
        
        number = float(number_match.group())
        
        # Find multiplier
        for suffix, mult in multipliers.items():
            if suffix in amount_str:
                return int(number * mult)
        
        # If no multiplier found, assume it's already a full number
        return int(number)
        
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    # Test the module
    companies = fetch_funding_news_from_all_sources()
    print(f"\nFetched {len(companies)} companies from news sources")
    if companies:
        print("\nFirst company:")
        print(json.dumps(companies[0], indent=2, default=str))

