"""
Deduplication module for company records.
Uses fuzzy matching to identify and merge duplicate companies.
"""

import re
from typing import List, Dict, Any, Tuple
from rapidfuzz import fuzz


# Suffixes to remove when normalizing company names
COMPANY_SUFFIXES = [
    r"\s*,?\s*inc\.?$",
    r"\s*,?\s*llc\.?$",
    r"\s*,?\s*ltd\.?$",
    r"\s*,?\s*corp\.?$",
    r"\s*,?\s*corporation$",
    r"\s*,?\s*incorporated$",
    r"\s*,?\s*limited$",
    r"\s*,?\s*l\.?l\.?c\.?$",
    r"\s*,?\s*company$",
    r"\s*,?\s*co\.?$",
    r"\s*,?\s*holdings?$",
    r"\s*,?\s*group$",
    r"\s*,?\s*partners?$",
    r"\s*,?\s*ventures?$",
    r"\s*,?\s*capital$",
    r"\s*,?\s*fund$",
    r"\s*,?\s*lp$",
    r"\s*,?\s*l\.?p\.?$",
]

# Minimum similarity score for fuzzy matching (0-100)
FUZZY_MATCH_THRESHOLD = 85


def deduplicate_companies(companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate a list of companies using fuzzy name matching.
    Merges data from multiple sources, preferring SEC data for official info.
    
    Args:
        companies: List of company dictionaries from various sources.
        
    Returns:
        Deduplicated list of companies with merged data.
    """
    print(f"\n{'='*60}")
    print("Deduplicating companies...")
    print(f"{'='*60}")
    print(f"  Input: {len(companies)} companies")
    
    if not companies:
        return []
    
    # Group companies by normalized name
    grouped = _group_similar_companies(companies)
    
    # Merge each group into a single company record
    deduplicated = []
    for group in grouped:
        merged = _merge_company_group(group)
        if merged:
            deduplicated.append(merged)
    
    print(f"  Output: {len(deduplicated)} unique companies")
    print(f"  Removed {len(companies) - len(deduplicated)} duplicates")
    
    return deduplicated


def _normalize_company_name(name: str) -> str:
    """
    Normalize a company name for comparison.
    - Lowercase
    - Remove common suffixes (Inc, LLC, Corp, etc.)
    - Remove extra whitespace
    - Remove special characters
    """
    if not name:
        return ""
    
    normalized = name.lower().strip()
    
    # Remove common suffixes
    for pattern in COMPANY_SUFFIXES:
        normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)
    
    # Remove special characters but keep spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def _group_similar_companies(companies: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """
    Group companies with similar names together using fuzzy matching.
    """
    if not companies:
        return []
    
    # Create normalized name lookup
    normalized_names = [
        (_normalize_company_name(c.get("company_name", "")), c) 
        for c in companies
    ]
    
    # Track which companies have been grouped
    used = set()
    groups = []
    
    for i, (norm_name_i, company_i) in enumerate(normalized_names):
        if i in used:
            continue
        
        # Start a new group
        group = [company_i]
        used.add(i)
        
        # Find similar companies
        for j, (norm_name_j, company_j) in enumerate(normalized_names):
            if j in used:
                continue
            
            # Check similarity
            similarity = _calculate_similarity(norm_name_i, norm_name_j)
            
            if similarity >= FUZZY_MATCH_THRESHOLD:
                group.append(company_j)
                used.add(j)
        
        groups.append(group)
    
    return groups


def _calculate_similarity(name1: str, name2: str) -> float:
    """
    Calculate similarity between two normalized company names.
    Uses a combination of fuzzy matching techniques.
    """
    if not name1 or not name2:
        return 0.0
    
    # Exact match
    if name1 == name2:
        return 100.0
    
    # Use token sort ratio for better matching of reordered words
    # e.g., "Open AI" vs "OpenAI" or "AI Open"
    ratio = fuzz.token_sort_ratio(name1, name2)
    
    return ratio


def _merge_company_group(group: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge a group of duplicate companies into a single record.
    Prioritizes SEC Form D data for official information.
    """
    if not group:
        return None
    
    if len(group) == 1:
        return group[0]
    
    # Sort by source priority (SEC first, then news sources)
    source_priority = {
        "SEC Form D": 0,
        "TechCrunch": 1,
        "VentureBeat": 2,
        "CB Insights": 3,
        "PitchBook": 4,
        "Founder Collective Portfolio": 5
    }
    
    sorted_group = sorted(
        group, 
        key=lambda x: source_priority.get(x.get("source", ""), 999)
    )
    
    # Start with the highest priority record
    merged = sorted_group[0].copy()
    
    # Collect all sources
    all_sources = list(set(c.get("source", "") for c in group if c.get("source")))
    merged["sources"] = all_sources
    
    # Merge data from other records (fill in missing fields)
    for company in sorted_group[1:]:
        # Fill in missing website
        if not merged.get("company_website") and company.get("company_website"):
            merged["company_website"] = company["company_website"]
        
        # Fill in missing description
        if not merged.get("description") and company.get("description"):
            merged["description"] = company["description"]
        
        # Merge investors lists
        existing_investors = set(merged.get("investors", []))
        new_investors = company.get("investors", [])
        if new_investors:
            for inv in new_investors:
                if inv and inv not in existing_investors:
                    merged.setdefault("investors", []).append(inv)
                    existing_investors.add(inv)
        
        # Fill in missing funding round from news (often more specific)
        if merged.get("funding_round") in ["Unknown", "Equity", None] and company.get("funding_round"):
            if company.get("funding_round") not in ["Unknown", None]:
                merged["funding_round"] = company["funding_round"]
        
        # Fill in missing industry
        if not merged.get("industry") and company.get("industry"):
            merged["industry"] = company["industry"]
        
        # Fill in missing location
        if not merged.get("location") and company.get("location"):
            merged["location"] = company["location"]
        
        # Fill in missing CEO name
        if not merged.get("ceo_name") and company.get("ceo_name"):
            merged["ceo_name"] = company["ceo_name"]
        
        # Prefer larger funding amount if SEC amount is missing
        if not merged.get("funding_amount") and company.get("funding_amount"):
            merged["funding_amount"] = company["funding_amount"]
    
    return merged


def get_dedup_stats(original: List[Dict[str, Any]], deduped: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Get statistics about the deduplication process.
    """
    original_count = len(original)
    deduped_count = len(deduped)
    
    # Count by source
    original_by_source = {}
    for c in original:
        source = c.get("source", "Unknown")
        original_by_source[source] = original_by_source.get(source, 0) + 1
    
    # Count merged sources
    merged_count = sum(1 for c in deduped if len(c.get("sources", [])) > 1)
    
    return {
        "original_count": original_count,
        "deduplicated_count": deduped_count,
        "duplicates_removed": original_count - deduped_count,
        "companies_with_merged_sources": merged_count,
        "original_by_source": original_by_source
    }


if __name__ == "__main__":
    # Test the module
    test_companies = [
        {"company_name": "OpenAI, Inc.", "source": "SEC Form D", "funding_amount": 1000000},
        {"company_name": "OpenAI", "source": "TechCrunch", "funding_amount": 1000000, "description": "AI company"},
        {"company_name": "Open AI LLC", "source": "VentureBeat", "investors": ["Microsoft"]},
        {"company_name": "Stripe, Inc.", "source": "SEC Form D"},
        {"company_name": "Stripe", "source": "CB Insights", "funding_round": "Series I"},
    ]
    
    deduped = deduplicate_companies(test_companies)
    print(f"\nDeduplication result:")
    for c in deduped:
        print(f"  - {c['company_name']} (sources: {c.get('sources', [c.get('source')])})")

