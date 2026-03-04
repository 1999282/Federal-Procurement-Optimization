"""
Project 2: Federal Procurement Cost Optimization
Step 2: Data Cleaning (CLEAN Framework)

Applies Christine's CLEAN framework to the raw API data:
- C: Comprehensive coverage (handle missing outlays, standardize geography)
- L: Logical types (dates, floats)
- E: Error checking (negative amounts, extreme outliers)
- A: Anomalies & Missing Values (impute missing outlays using award amount proxy)
- N: New Columns (spend tier, active duration)
"""

import pandas as pd
import os
import numpy as np

# Setup paths
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(DIR_PATH, "raw_federal_contracts.csv")
OUTPUT_FILE = os.path.join(DIR_PATH, "cleaned_federal_contracts.csv")

def clean_procurement_data():
    print("Starting CLEAN Framework Pipeline...")
    
    # 1. Load Data
    print(f"  Loading raw data: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, dtype=str)
    print(f"  Initial shape: {df.shape}")
    
    # 2. Logical Types (Convert data types)
    print("  Converting logical types (dates and currency)...")
    
    # Dates
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    
    # Financials
    financial_cols = [
        'award_amount', 'total_outlays', 
        'covid_obligations', 'covid_outlays',
        'infrastructure_obligations', 'infrastructure_outlays'
    ]
    for col in financial_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 3. Handle Missing Values / Imputation
    print("  Handling missing values (Imputing Outlays)...")
    
    # For a cost optimization analysis, outlays (actual spend vs awarded spend)
    # is crucial. If outlays are missing, we assume 100% of award_amount was outlaid
    # for completed contracts, and pro-rate for active ones. For simplicity in this
    # dataset, we'll impute missing outlays as 90% of the award amount if missing,
    # as government contracts often underrun slightly.
    df['is_outlay_imputed'] = df['total_outlays'].isna()
    df.loc[df['total_outlays'].isna(), 'total_outlays'] = df['award_amount'] * 0.90
    
    # Fill categorical missing values
    df['pop_state_code'] = df['pop_state_code'].fillna('UNKNOWN')
    df['pop_zip5'] = df['pop_zip5'].fillna('00000')
    df['description'] = df['description'].fillna('No description provided')
    
    # Replace NaN in tag columns with 0
    tag_cols = ['covid_obligations', 'covid_outlays', 'infrastructure_obligations', 'infrastructure_outlays']
    for col in tag_cols:
        df[col] = df[col].fillna(0)
        
    # Drop irrelevant or 100% null columns (like pop_city_code which was 100% null in profiling)
    if 'pop_city_code' in df.columns and df['pop_city_code'].isna().all():
        df = df.drop(columns=['pop_city_code'])
    
    # 4. Error Checking (Deobligations)
    print("  Flagging anomalies and deobligations...")
    # Negative award amounts represent money returned to the government (deobligations)
    df['is_deobligation'] = df['award_amount'] < 0
    
    # Let's keep negative values because identifying returned money is a GREAT 
    # operational insight for cost optimization!
    
    # 5. New Columns (Feature Engineering)
    print("  Engineering new features for analysis...")
    
    # Contract duration in days
    df['contract_duration_days'] = (df['end_date'] - df['start_date']).dt.days
    # Handle negative durations (data entry errors) or NaN
    df.loc[df['contract_duration_days'] < 0, 'contract_duration_days'] = np.nan
    
    # Spend categories
    def categorize_spend(amount):
        if amount < 0: return 'Deobligation'
        if amount < 100000: return 'Micro/Small (<$100K)'
        if amount < 1000000: return 'Mid-size ($100K-$1M)'
        if amount < 10000000: return 'Large ($1M-$10M)'
        return 'Mega (>$10M)'
        
    df['spend_tier'] = df['award_amount'].apply(categorize_spend)
    
    # Unspent Balance (Award Amount - Outlays)
    # This is a critical metric for our strategic question!
    df['unspent_balance'] = df['award_amount'] - df['total_outlays']
    
    # 6. Final cleanup 
    # Drop rows where start_date is null (impossible to trend)
    df = df.dropna(subset=['start_date'])
    
    print("\n CLEANED DATA PROFILE")
    print(f"  Final shape: {df.shape}")
    print(f"  Total Award Amount: ${df['award_amount'].sum():,.2f}")
    print(f"  Total Imputed Outlays: {df['is_outlay_imputed'].sum()} records")
    print(f"  Total Deobligations: {df['is_deobligation'].sum()} records")
    
    # 7. Save
    print(f"\n Saving cleaned data to: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    print(" Cleaning complete!")

if __name__ == "__main__":
    clean_procurement_data()
