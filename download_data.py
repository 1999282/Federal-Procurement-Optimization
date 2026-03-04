"""
Project 2: Federal Procurement Cost Optimization
Dataset: US Federal Procurement Contracts from USASpending.gov API
Source: https://api.usaspending.gov/ (Official US Government Open Data)

Downloads ~100-200K contract records across multiple agencies and fiscal years.
"""

import requests
import csv
import os
import time
import json

# USASpending.gov API v2 base URL
BASE_URL = "https://api.usaspending.gov/api/v2"

# Output file
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "raw_federal_contracts.csv")

# Fiscal years to pull (gives us time dimension)
FISCAL_YEARS = [2021, 2022, 2023, 2024]

# Agencies to pull (gives us variety + region)
# Using agency codes for major departments
AGENCIES = [
    {"name": "Department of Defense", "code": "097"},
    {"name": "Department of Health and Human Services", "code": "075"},
    {"name": "Department of Homeland Security", "code": "070"},
    {"name": "Department of Veterans Affairs", "code": "036"},
    {"name": "Department of Energy", "code": "089"},
    {"name": "General Services Administration", "code": "047"},
    {"name": "Department of Transportation", "code": "069"},
    {"name": "Department of Agriculture", "code": "012"},
]

def download_contracts_spending_by_award():
    """
    Uses the /search/spending_by_award/ endpoint 
    to get individual contract records with full detail.
    """
    all_records = []
    
    for fy in FISCAL_YEARS:
        for agency in AGENCIES:
            print(f"\n📥 Downloading: {agency['name']} | FY{fy}...")
            
            page = 1
            max_pages = 20  # ~2000 records per agency per year (100 per page) -> ~64,000 total
            
            while page <= max_pages:
                payload = {
                    "filters": {
                        "time_period": [
                            {"start_date": f"{fy-1}-10-01", "end_date": f"{fy}-09-30"}
                        ],
                        "award_type_codes": ["A", "B", "C", "D"],  # All contract types
                        "agencies": [
                            {
                                "type": "awarding",
                                "tier": "toptier",
                                "name": agency["name"]
                            }
                        ]
                    },
                    "fields": [
                        "Award ID",
                        "Recipient Name",
                        "Start Date",
                        "End Date",
                        "Award Amount",
                        "Total Outlays",
                        "Description",
                        "def_codes",
                        "COVID-19 Obligations",
                        "COVID-19 Outlays",
                        "Infrastructure Obligations",
                        "Infrastructure Outlays",
                        "Awarding Agency",
                        "Awarding Sub Agency",
                        "Contract Award Type",
                        "recipient_id",
                        "prime_award_recipient_id",
                        "Place of Performance City Code",
                        "Place of Performance State Code",
                        "Place of Performance Zip5",
                        "Product or Service Code",
                        "NAICS Code",
                        "generated_internal_id",
                    ],
                    "page": page,
                    "limit": 100,
                    "sort": "Award Amount",
                    "order": "desc",
                    "subawards": False
                }
                
                try:
                    response = requests.post(
                        f"{BASE_URL}/search/spending_by_award/",
                        json=payload,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        results = data.get("results", [])
                        
                        if not results:
                            break
                        
                        for record in results:
                            all_records.append({
                                "award_id": record.get("Award ID", ""),
                                "recipient_name": record.get("Recipient Name", ""),
                                "start_date": record.get("Start Date", ""),
                                "end_date": record.get("End Date", ""),
                                "award_amount": record.get("Award Amount", ""),
                                "total_outlays": record.get("Total Outlays", ""),
                                "description": record.get("Description", ""),
                                "awarding_agency": record.get("Awarding Agency", ""),
                                "awarding_sub_agency": record.get("Awarding Sub Agency", ""),
                                "contract_type": record.get("Contract Award Type", ""),
                                "pop_city_code": record.get("Place of Performance City Code", ""),
                                "pop_state_code": record.get("Place of Performance State Code", ""),
                                "pop_zip5": record.get("Place of Performance Zip5", ""),
                                "product_service_code": record.get("Product or Service Code", ""),
                                "naics_code": record.get("NAICS Code", ""),
                                "covid_obligations": record.get("COVID-19 Obligations", ""),
                                "covid_outlays": record.get("COVID-19 Outlays", ""),
                                "infrastructure_obligations": record.get("Infrastructure Obligations", ""),
                                "infrastructure_outlays": record.get("Infrastructure Outlays", ""),
                                "fiscal_year": fy,
                                "agency_code": agency["code"],
                            })
                        
                        print(f"  Page {page}: {len(results)} records (Total: {len(all_records)})")
                        page += 1
                        time.sleep(0.3)  # Be respectful to the API
                        
                    elif response.status_code == 429:
                        print("  ⏳ Rate limited, waiting 5 seconds...")
                        time.sleep(5)
                        continue
                    else:
                        print(f"  ❌ Error {response.status_code}: {response.text[:200]}")
                        break
                        
                except requests.exceptions.Timeout:
                    print(f"  ⏳ Timeout on page {page}, retrying...")
                    time.sleep(2)
                    continue
                except Exception as e:
                    print(f"  ❌ Error: {e}")
                    break
    
    return all_records


def save_to_csv(records):
    """Save records to CSV file."""
    if not records:
        print("❌ No records to save!")
        return
    
    fieldnames = records[0].keys()
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\n✅ Saved {len(records)} records to: {OUTPUT_FILE}")
    print(f"📊 File size: {file_size_mb:.1f} MB")


def profile_data(records):
    """Quick profile of the downloaded data."""
    print("\n" + "="*60)
    print("📊 DATA PROFILE (SCAN Framework)")
    print("="*60)
    
    print(f"\n📐 SHAPE: {len(records)} rows × {len(records[0].keys())} columns")
    
    # Cardinality
    print(f"\n🔢 CARDINALITY:")
    for key in ['awarding_agency', 'contract_type', 'pop_state_code', 'fiscal_year']:
        unique_vals = set(r.get(key, '') for r in records if r.get(key))
        print(f"  {key}: {len(unique_vals)} unique values")
    
    # Nulls
    print(f"\n❓ NULLS:")
    for key in records[0].keys():
        null_count = sum(1 for r in records if not r.get(key) or r.get(key) == '' or r.get(key) is None)
        null_pct = (null_count / len(records)) * 100
        if null_pct > 0:
            print(f"  {key}: {null_count} ({null_pct:.1f}%)")
    
    # Anomalies (award amounts)
    amounts = []
    for r in records:
        try:
            amt = float(r.get('award_amount', 0) or 0)
            amounts.append(amt)
        except (ValueError, TypeError):
            pass
    
    if amounts:
        amounts.sort()
        print(f"\n💰 AWARD AMOUNTS:")
        print(f"  Min: ${min(amounts):,.2f}")
        print(f"  Max: ${max(amounts):,.2f}")
        print(f"  Median: ${amounts[len(amounts)//2]:,.2f}")
        print(f"  Total: ${sum(amounts):,.2f}")
        negative = sum(1 for a in amounts if a < 0)
        if negative > 0:
            print(f"  ⚠️ Negative amounts: {negative} (deobligations)")


if __name__ == "__main__":
    print("🏛️  US Federal Procurement Data Downloader")
    print("📌 Source: USASpending.gov (Official Government Open Data)")
    print("="*60)
    
    records = download_contracts_spending_by_award()
    
    if records:
        save_to_csv(records)
        profile_data(records)
    else:
        print("❌ No data downloaded. Check your internet connection.")
