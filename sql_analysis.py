"""
Project 2: Federal Procurement Cost Optimization
Step 3: SQLite Database Load & EDA Analysis

Loads the cleaned procurement dataset into an SQLite database.
Executes 8 analytical queries defined by Christine's framework to find $2B+ in savings.
"""

import sqlite3
import pandas as pd
import os

DIR_PATH = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(DIR_PATH, "cleaned_federal_contracts.csv")
DB_FILE = os.path.join(DIR_PATH, "procurement_data.db")

def load_and_analyze():
    print("=== Loading Data to SQLite ===")
    df = pd.read_csv(INPUT_FILE)
    
    # Connect and load
    conn = sqlite3.connect(DB_FILE)
    df.to_sql('contracts', conn, if_exists='replace', index=False)
    print(f"Loaded {len(df)} records into 'contracts' table.")
    
    # Create indexes for performance
    conn.execute("CREATE INDEX idx_agency ON contracts(awarding_agency)")
    conn.execute("CREATE INDEX idx_date ON contracts(start_date)")
    conn.execute("CREATE INDEX idx_state ON contracts(pop_state_code)")
    
    run_queries(conn)
    conn.close()

def run_queries(conn):
    print("\n=== Executing SCAN Framework Queries (EDA) ===")
    
    queries = {
        "1. Total Spend vs Actual Outlays by Agency (The Cost Gap)": """
            SELECT 
                awarding_agency,
                COUNT(*) as contract_count,
                SUM(award_amount) / 1000000000 as total_award_billions,
                SUM(total_outlays) / 1000000000 as total_outlaid_billions,
                SUM(unspent_balance) / 1000000000 as unspent_billions
            FROM contracts
            GROUP BY awarding_agency
            ORDER BY unspent_billions DESC
        """,
        
        "2. Contract Duration Efficiency (Avg Days) by Spend Tier": """
            SELECT 
                spend_tier,
                AVG(contract_duration_days) as avg_duration_days,
                COUNT(*) as contract_count,
                SUM(award_amount) / 1000000000 as tier_spend_billions
            FROM contracts
            WHERE contract_duration_days IS NOT NULL AND spend_tier != 'Deobligation'
            GROUP BY spend_tier
            ORDER BY tier_spend_billions DESC
        """,
        
        "3. Year-Over-Year Procurement Volume (Fiscal Year)": """
            SELECT 
                fiscal_year,
                COUNT(award_id) as total_contracts,
                SUM(award_amount) / 1000000000 as total_award_billions
            FROM contracts
            GROUP BY fiscal_year
            ORDER BY fiscal_year
        """,
        
        "4. Top 10 Spending States (Regional Analysis)": """
            SELECT 
                pop_state_code as state,
                SUM(award_amount) / 1000000000 as total_award_billions,
                COUNT(*) as contract_count
            FROM contracts
            WHERE state != 'UNKNOWN'
            GROUP BY state
            ORDER BY total_award_billions DESC
            LIMIT 10
        """,
        
        "5. The 'Ghost Contracts' - High Value, Zero Outlays": """
            SELECT 
                awarding_agency,
                COUNT(*) as ghost_contract_count,
                SUM(award_amount) / 1000000 as wasted_capital_millions
            FROM contracts
            WHERE award_amount > 1000000 AND total_outlays = 0 AND is_deobligation = 0 AND is_outlay_imputed = 0
            GROUP BY awarding_agency
            ORDER BY wasted_capital_millions DESC
        """,
        
        "6. Contract Type Cost Distribution": """
            SELECT 
                contract_type,
                COUNT(*) as total_contracts,
                SUM(award_amount) / 1000000000 as total_spend_billions
            FROM contracts
            GROUP BY contract_type
            ORDER BY total_spend_billions DESC
        """,
        
        "7. COVID-19 vs Infrastructure Spending Footprint": """
            SELECT 
                awarding_agency,
                SUM(covid_obligations) / 1000000 as covid_spend_millions,
                SUM(infrastructure_obligations) / 1000000 as infra_spend_millions
            FROM contracts
            GROUP BY awarding_agency
            HAVING covid_spend_millions > 0 OR infra_spend_millions > 0
            ORDER BY covid_spend_millions DESC
        """,
        
        "8. High-Risk End-of-Year Spending (September 'Use It or Lose It')": """
            SELECT 
                awarding_agency,
                strftime('%m', start_date) as month_signed,
                SUM(award_amount) / 1000000000 as spend_billions
            FROM contracts
            WHERE month_signed = '09'
            GROUP BY awarding_agency, month_signed
            ORDER BY spend_billions DESC
        """
    }
    
    for title, query in queries.items():
        print(f"\n{title}")
        print("-" * 50)
        try:
            results = pd.read_sql_query(query, conn)
            # Format output for easier reading in terminal
            if 'total_award_billions' in results.columns:
                results['total_award_billions'] = results['total_award_billions'].apply(lambda x: f"${x:,.2f}B")
            if 'unspent_billions' in results.columns:
                results['unspent_billions'] = results['unspent_billions'].apply(lambda x: f"${x:,.2f}B")
            if 'wasted_capital_millions' in results.columns:
                results['wasted_capital_millions'] = results['wasted_capital_millions'].apply(lambda x: f"${x:,.2f}M")
                
            print(results.to_string(index=False))
        except Exception as e:
            print(f"Error executing query: {e}")

if __name__ == "__main__":
    load_and_analyze()
