"""
Project 2: Federal Procurement Cost Optimization
Step 4: Exporting Datasets for Power BI Model

This script connects to the SQLite database and generates clean, 
dimensionally-modeled CSV files tailored specifically for our 
Premium Power BI Dashboard (Glassmorphism layout).
"""

import sqlite3
import pandas as pd
import os

DIR_PATH = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(DIR_PATH, "procurement_data.db")
OUTPUT_DIR = os.path.join(DIR_PATH, "powerbi_export")

def export_for_powerbi():
    print("=== Exporting Data for Power BI Model ===")
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    conn = sqlite3.connect(DB_FILE)
    
    # 1. Main Fact Table (Contracts)
    # We keep the granularity but only the necessary columns for performance
    print("Exporting Fact Table: fContracts...")
    query_fact = """
        SELECT 
            award_id,
            awarding_agency,
            start_date,
            end_date,
            contract_duration_days,
            spend_tier,
            pop_state_code as state,
            contract_type,
            award_amount,
            total_outlays,
            unspent_balance,
            is_deobligation,
            covid_obligations,
            infrastructure_obligations
        FROM contracts
    """
    df_fact = pd.read_sql_query(query_fact, conn)
    df_fact.to_csv(os.path.join(OUTPUT_DIR, "fContracts.csv"), index=False)
    
    # 2. Dimension Table (Agencies / Cost Centers)
    print("Exporting Dimension Table: dAgencies...")
    query_dim_agency = """
        SELECT DISTINCT 
            awarding_agency,
            awarding_sub_agency
        FROM contracts
    """
    df_agencies = pd.read_sql_query(query_dim_agency, conn)
    df_agencies.to_csv(os.path.join(OUTPUT_DIR, "dAgencies.csv"), index=False)
    
    # 3. Aggregated View for the Azure Maps Custom SVG visual
    # (Aggregating by state to ensure the map renders fast)
    print("Exporting Map Aggregate: vMapPerformance...")
    query_map = """
        SELECT 
            pop_state_code as state,
            COUNT(*) as total_contracts,
            SUM(award_amount) as total_award_amount,
            SUM(unspent_balance) as total_unspent_balance,
            SUM(CASE WHEN is_deobligation = 1 THEN 1 ELSE 0 END) as deobligation_count
        FROM contracts
        WHERE pop_state_code != 'UNKNOWN'
        GROUP BY pop_state_code
    """
    df_map = pd.read_sql_query(query_map, conn)
    df_map.to_csv(os.path.join(OUTPUT_DIR, "vMapPerformance.csv"), index=False)
    
    conn.close()
    print("\n All Power BI datasets exported successfully to:")
    print(f"   {OUTPUT_DIR}")
    print("\nNext Step: Open Power BI, connect to these CSV files, and apply the Premium Dashboard UI/UX.")

if __name__ == "__main__":
    export_for_powerbi()
