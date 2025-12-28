"""
SEC Form D API integration module.
Fetches recently filed Form D offerings from SEC EDGAR.
"""

import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import config


def fetch_sec_form_d_filings() -> List[Dict[str, Any]]:
    """
    Fetch all Form D filings from the last LOOKBACK_DAYS days.
    Paginates through all results automatically.
    
    Returns:
        List of normalized company dictionaries.
    """
    print(f"\n{'='*60}")
    print("Fetching SEC Form D filings...")
    print(f"{'='*60}")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=config.LOOKBACK_DAYS)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    # Build Lucene query for date range
    query = f"filedAt:[{start_date_str} TO *]"
    
    all_companies = []
    offset = 0
    total_fetched = 0
    
    while True:
        payload = {
            "query": query,
            "from": str(offset),
            "size": str(config.SEC_API_PAGE_SIZE),
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        
        try:
            response = _make_sec_api_request(payload)
            
            if response is None:
                print("  Failed to fetch from SEC API after retries")
                break
            
            offerings = response.get("offerings", [])
            total_count = response.get("total", {}).get("value", 0)
            
            if not offerings:
                break
            
            # Normalize each offering into our standard format
            for offering in offerings:
                company = _normalize_sec_offering(offering)
                if company:
                    all_companies.append(company)
            
            total_fetched += len(offerings)
            print(f"  Fetched {total_fetched}/{total_count} filings...")
            
            # Check if we've fetched all results
            if total_fetched >= total_count:
                break
            
            offset += config.SEC_API_PAGE_SIZE
            time.sleep(config.SEC_API_DELAY)
            
        except Exception as e:
            print(f"  Error fetching SEC filings: {e}")
            break
    
    print(f"  Total companies from SEC Form D: {len(all_companies)}")
    return all_companies


def _make_sec_api_request(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Make a request to the SEC Form D API with retry logic.
    """
    headers = {
        "Authorization": config.SEC_API_KEY,
        "Content-Type": "application/json"
    }
    
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.post(
                config.SEC_FORM_D_ENDPOINT,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"  Attempt {attempt}/{config.MAX_RETRIES} failed: {e}")
            if attempt < config.MAX_RETRIES:
                time.sleep(config.RETRY_DELAY * attempt)
    
    return None


def _normalize_sec_offering(offering: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a SEC Form D offering into our standard company format.
    """
    try:
        primary_issuer = offering.get("primaryIssuer", {})
        offering_data = offering.get("offeringData", {})
        
        # Extract company name
        company_name = primary_issuer.get("entityName", "").strip()
        if not company_name:
            return None
        
        # Extract address
        address_obj = primary_issuer.get("issuerAddress", {})
        address_parts = [
            address_obj.get("street1", ""),
            address_obj.get("street2", ""),
            address_obj.get("city", ""),
            address_obj.get("stateOrCountryDescription", ""),
            address_obj.get("zipCode", "")
        ]
        location = ", ".join(part for part in address_parts if part).strip(", ")
        
        # Extract industry
        industry_group = offering_data.get("industryGroup", {})
        industry = industry_group.get("industryGroupType", "")
        
        # Extract investment fund type if applicable
        fund_info = industry_group.get("investmentFundInfo", {})
        fund_type = fund_info.get("investmentFundType", "")
        if fund_type:
            industry = f"{industry} - {fund_type}"
        
        # Extract offering amounts
        sales_amounts = offering_data.get("offeringSalesAmounts", {})
        total_offering = sales_amounts.get("totalOfferingAmount", 0)
        amount_sold = sales_amounts.get("totalAmountSold", 0)
        
        # Handle indefinite amounts (-1)
        if total_offering == -1:
            total_offering = None
        if amount_sold == -1:
            amount_sold = None
        
        # Extract funding round type from securities offered
        securities = offering_data.get("typesOfSecuritiesOffered", {})
        funding_round = _determine_funding_round(securities)
        
        # Extract executives/related persons
        related_persons = offering.get("relatedPersonsList", {}).get("relatedPersonInfo", [])
        executives = _extract_executives(related_persons)
        
        # Extract investors info
        investors_data = offering_data.get("investors", {})
        total_investors = investors_data.get("totalNumberAlreadyInvested", 0)
        
        # Build SEC filing URL
        accession_no = offering.get("accessionNo", "")
        sec_filing_url = None
        if accession_no:
            # Format: 0001234567-24-000001 -> 000123456724000001
            formatted_accession = accession_no.replace("-", "")
            cik = primary_issuer.get("cik", "").lstrip("0")
            if cik:
                sec_filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=D&dateb=&owner=include&count=40"
        
        # Extract year of incorporation as founding year
        year_of_inc = primary_issuer.get("yearOfInc", {})
        founding_year = None
        if year_of_inc.get("value"):
            try:
                founding_year = int(year_of_inc.get("value"))
            except (ValueError, TypeError):
                pass
        
        return {
            "company_name": company_name,
            "company_website": None,  # Will be enriched later
            "funding_amount": total_offering,
            "amount_sold": amount_sold,
            "funding_round": funding_round,
            "investors": [],  # SEC filings don't typically include investor names
            "industry": industry,
            "location": location,
            "founding_year": founding_year,
            "source": "SEC Form D",
            "announcement_date": offering.get("filedAt", "")[:10],  # Just the date part
            "description": f"{primary_issuer.get('entityType', '')} incorporated in {primary_issuer.get('jurisdictionOfInc', '')}",
            "ceo_name": executives[0] if executives else None,
            "executives": executives,
            "phone": primary_issuer.get("issuerPhoneNumber", ""),
            "linkedin_url": None,
            "sec_filing_url": sec_filing_url,
            "total_investors": total_investors
        }
        
    except Exception as e:
        print(f"  Error normalizing offering: {e}")
        return None


def _determine_funding_round(securities: Dict[str, Any]) -> str:
    """
    Determine the funding round type based on securities offered.
    """
    types = []
    
    if securities.get("isEquityType"):
        types.append("Equity")
    if securities.get("isDebtType"):
        types.append("Debt")
    if securities.get("isPooledInvestmentFundType"):
        types.append("Pooled Investment Fund")
    if securities.get("isOptionToAcquireType"):
        types.append("Options")
    if securities.get("isSecurityToBeAcquiredType"):
        types.append("Security to be Acquired")
    if securities.get("isOtherType"):
        desc = securities.get("descriptionOfOtherType", "Other")
        types.append(desc if desc else "Other")
    
    return ", ".join(types) if types else "Unknown"


def _extract_executives(related_persons: List[Dict[str, Any]]) -> List[str]:
    """
    Extract executive names from related persons list.
    """
    executives = []
    
    for person in related_persons[:5]:  # Limit to first 5
        name_obj = person.get("relatedPersonName", {})
        first = name_obj.get("firstName", "")
        middle = name_obj.get("middleName", "")
        last = name_obj.get("lastName", "")
        
        full_name = " ".join(part for part in [first, middle, last] if part).strip()
        
        relationships = person.get("relatedPersonRelationshipList", {}).get("relationship", [])
        if relationships and full_name:
            title = relationships[0] if relationships else ""
            executives.append(f"{full_name} - {title}" if title else full_name)
        elif full_name:
            executives.append(full_name)
    
    return executives


if __name__ == "__main__":
    # Test the module
    companies = fetch_sec_form_d_filings()
    print(f"\nFetched {len(companies)} companies")
    if companies:
        print("\nFirst company:")
        import json
        print(json.dumps(companies[0], indent=2, default=str))

