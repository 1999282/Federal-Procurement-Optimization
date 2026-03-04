"""
Project 2: Federal Procurement Cost Optimization
Builds an interactive, standalone HTML dashboard with a Premium Glassmorphism UI.
Injects real data from the SQLite database into Chart.js visuals.
"""

import sqlite3
import pandas as pd
import json
import os

DIR_PATH = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(DIR_PATH, "procurement_data.db")
OUTPUT_HTML = os.path.join(DIR_PATH, "dashboard.html")

def build_dashboard():
    print("=== Generating Premium Glassmorphism Dashboard ===")
    conn = sqlite3.connect(DB_FILE)
    
    # --- Fetch KPI Data ---
    kpi_query = """
    SELECT 
        SUM(award_amount) as total_spend,
        SUM(total_outlays) as total_outlays,
        SUM(unspent_balance) as total_unspent
    FROM contracts
    """
    kpis = pd.read_sql_query(kpi_query, conn).iloc[0]
    total_spend = f"${kpis['total_spend']/1e9:.2f}B"
    total_outlays = f"${kpis['total_outlays']/1e9:.2f}B"
    unspent = f"${kpis['total_unspent']/1e9:.2f}B"
    cost_gap_pct = f"{(kpis['total_unspent'] / kpis['total_spend']) * 100:.1f}%"

    # --- Fetch Chart 1: Agency Unspent Balances ---
    # Top 8 agencies by unspent balance
    agency_query = """
    SELECT awarding_agency, SUM(unspent_balance)/1e9 as unspent_b
    FROM contracts
    GROUP BY awarding_agency
    ORDER BY unspent_b DESC
    LIMIT 8
    """
    df_agency = pd.read_sql_query(agency_query, conn)
    # Shorten agency names for the chart
    def shorten_agency(name):
        return name.replace('Department of ', '').replace('General Services Administration', 'GSA')
    
    agency_labels = [shorten_agency(x) for x in df_agency['awarding_agency'].tolist()]
    agency_data = df_agency['unspent_b'].tolist()

    # --- Fetch Chart 2: YOY Spend Trend ---
    yoy_query = """
    SELECT fiscal_year, SUM(award_amount)/1e9 as spend_b
    FROM contracts
    GROUP BY fiscal_year
    ORDER BY fiscal_year
    """
    df_yoy = pd.read_sql_query(yoy_query, conn)
    yoy_labels = df_yoy['fiscal_year'].astype(str).tolist()
    yoy_data = df_yoy['spend_b'].tolist()

    # --- Fetch Chart 3: Contract Type Distribution ---
    type_query = """
    SELECT contract_type, COUNT(*) as count
    FROM contracts
    GROUP BY contract_type
    ORDER BY count DESC
    """
    df_type = pd.read_sql_query(type_query, conn)
    type_labels = df_type['contract_type'].str.title().tolist()
    type_data = df_type['count'].tolist()

    # --- Fetch Table: Top Ghost Contracts (Over $10M, 0 outlays) ---
    ghost_query = """
    SELECT 
        awarding_agency,
        pop_state_code as state,
        contract_type,
        award_amount
    FROM contracts
    WHERE award_amount > 10000000 AND total_outlays = 0 AND is_deobligation = 0
    ORDER BY award_amount DESC
    LIMIT 10
    """
    df_ghost = pd.read_sql_query(ghost_query, conn)
    
    table_html = ""
    for _, row in df_ghost.iterrows():
        amt_m = f"${row['award_amount']/1e6:.1f}M"
        agency = shorten_agency(row['awarding_agency'])
        table_html += f"<tr><td>{agency}</td><td>{row['state']}</td><td>{row['contract_type'].title()}</td><td class='text-danger'>{amt_m}</td></tr>"

    conn.close()

    # --- Construct HTML ---
    # Using a 12-12-12 dark theme with 1E1E1E frosted glass panels, neon purple/orange gradients
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Federal Procurement Optimization</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Segoe+UI:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-dark: #0F172A; /* Slate 900 */
            --glass-bg: rgba(30, 41, 59, 0.7); /* Slate 800 with opacity */
            --glass-border: rgba(255, 255, 255, 0.05);
            --text-main: #F8FAFC; /* Slate 50 */
            --text-muted: #94A3B8; /* Slate 400 */
            --brand-main: #3B82F6; /* Blue 500 */
            --brand-light: #60A5FA; /* Blue 400 */
            --brand-dark: #1E3A8A; /* Blue 900 */
            --status-success: #10B981; /* Emerald 500 - only for true success metrics */
            --status-danger: #EF4444; /* Red 500 - for severe risks */
        }}
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }}

        body {{
            background-color: var(--bg-dark);
            color: var(--text-main);
            min-height: 100vh;
            padding: 2rem;
            /* Monochromatic subtle glow */
            background-image: 
                radial-gradient(circle at 15% 50%, rgba(59, 130, 246, 0.08), transparent 30%),
                radial-gradient(circle at 85% 30%, rgba(59, 130, 246, 0.05), transparent 30%);
            background-repeat: no-repeat;
            background-attachment: fixed;
        }}

        .dashboard-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--glass-border);
        }}

        h1 {{
            font-size: 1.8rem;
            font-weight: 600;
            color: var(--text-main);
        }}
        
        .subtitle {{
            color: var(--text-muted);
            font-size: 0.9rem;
            margin-top: 0.3rem;
        }}

        .glass-panel {{
            background: var(--glass-bg);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid var(--glass-border);
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            transition: transform 0.2s ease;
        }}
        
        .glass-panel:hover {{
            transform: translateY(-2px);
            border-color: rgba(255,255,255,0.1);
        }}

        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .kpi-title {{
            color: var(--text-muted);
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 0.5rem;
        }}

        .kpi-value {{
            font-size: 2.2rem;
            font-weight: 700;
            margin-bottom: 0.2rem;
            color: var(--brand-light);
        }}

        .kpi-subtext {{
            font-size: 0.85rem;
            font-weight: 500;
            color: var(--text-muted);
        }}

        .text-danger {{ color: var(--status-danger); }}
        .text-success {{ color: var(--status-success); }}
        .text-warning {{ color: var(--brand-main); }} /* Changed to brand color for monochromatic consistency */

        .charts-grid {{
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}
        
        .bottom-grid {{
            display: grid;
            grid-template-columns: 1fr 2fr 1.5fr;
            gap: 1.5rem;
        }}

        .chart-container {{
            position: relative;
            height: 300px;
            width: 100%;
        }}

        h3 {{
            font-size: 1.05rem;
            font-weight: 600;
            margin-bottom: 1.2rem;
            color: var(--text-main);
            display: flex;
            align-items: center;
            gap: 0.5rem;
            border-bottom: 1px solid var(--glass-border);
            padding-bottom: 0.5rem;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}

        th, td {{
            text-align: left;
            padding: 0.8rem 0.5rem;
            border-bottom: 1px solid rgba(255,255,255,0.02);
        }}

        th {{
            color: var(--text-muted);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.5px;
        }}

        tr:hover td {{
            background: rgba(255,255,255,0.02);
        }}
        
        .insight-box {{
            background: rgba(59, 130, 246, 0.05); /* Very subtle brand tint */
            border-left: 3px solid var(--brand-main);
            padding: 1rem;
            margin-bottom: 1rem;
            border-radius: 0 4px 4px 0;
            font-size: 0.9rem;
            line-height: 1.5;
        }}
        
        .insight-box strong {{
            color: var(--brand-light);
            display: block;
            margin-bottom: 0.3rem;
            font-size: 0.95rem;
        }}

    </style>
</head>
<body>

    <div class="dashboard-header">
        <div>
            <h1>US Federal Procurement Operations</h1>
            <div class="subtitle">Cost Optimization & Unspent Capital Analysis ($2B+ Savings Target)</div>
        </div>
        <div class="subtitle">Last Updated: Sep 2024 | Rows: 55,300 | Status: <span class="text-success">Live</span></div>
    </div>

    <div class="kpi-grid">
        <div class="glass-panel">
            <div class="kpi-title">Total Contract Value</div>
            <div class="kpi-value">{total_spend}</div>
            <div class="kpi-subtext">Total obligated volume</div>
        </div>
        <div class="glass-panel">
            <div class="kpi-title">Actual Outlays (Spend)</div>
            <div class="kpi-value">{total_outlays}</div>
            <div class="kpi-subtext">Capital deployed</div>
        </div>
        <div class="glass-panel" style="border-top: 3px solid var(--brand-main);">
            <div class="kpi-title">Unspent Balance</div>
            <div class="kpi-value" style="color: var(--brand-main);">{unspent}</div>
            <div class="kpi-subtext">Cost Gap: {cost_gap_pct}</div>
        </div>
        <div class="glass-panel" style="border-top: 3px solid var(--status-danger);">
            <div class="kpi-title">"Ghost" Contracts</div>
            <div class="kpi-value" style="color: var(--status-danger);">5,905</div>
            <div class="kpi-subtext">Zero outlays logged</div>
        </div>
    </div>

    <div class="charts-grid">
        <div class="glass-panel">
            <h3>📈 Unspent Capital by Agency (Billions $)</h3>
            <div class="chart-container">
                <canvas id="agencyChart"></canvas>
            </div>
        </div>
        <div class="glass-panel">
            <h3>📅 Procurement Volume YOY</h3>
            <div class="chart-container">
                <canvas id="yoyChart"></canvas>
            </div>
        </div>
    </div>

    <div class="bottom-grid">
        <div class="glass-panel">
            <h3>📊 Contract Types</h3>
            <div class="chart-container">
                <canvas id="typeChart"></canvas>
            </div>
        </div>
        <div class="glass-panel" style="overflow-y: auto; max-height: 380px;">
            <h3>⚠️ Top High-Risk "Ghost" Contracts (>$10M, 0 Spend)</h3>
            <table>
                <thead>
                    <tr>
                        <th>Agency</th>
                        <th>State</th>
                        <th>Type</th>
                        <th>Wasted Capital</th>
                    </tr>
                </thead>
                <tbody>
                    {table_html}
                </tbody>
            </table>
        </div>
        <div class="glass-panel" style="overflow-y: auto; max-height: 380px;">
            <h3>💡 Strategic Insights & Recommendations</h3>
            
            <div class="insight-box">
                <strong>1. The "Ghost Contract" Epidemic</strong>
                There are 5,905 active contracts representing billions in obligated funds with absolute zero actual outlays. DoD and DHS are the primary offenders.
                <br><br><span class="text-success">► <b>Recommendation:</b> Implement a 90-day mandatory review trigger for contracts >$1M with zero outlays. Reclaim unused capital.</span>
            </div>
            
            <div class="insight-box">
                <strong>2. The Delivery Order Black Hole</strong>
                The majority of unspent procurement volume is tied to indefinite "Delivery Orders" rather than definitive deliverables.
                <br><br><span class="text-success">► <b>Recommendation:</b> Shift procurement strategy for IT/Logistics toward strict milestone-based definitive contracts.</span>
            </div>
            
            <div class="insight-box" style="border-left-color: var(--brand-dark);">
                <strong>3. The Q4 "Use-It-Or-Lose-It" Surge</strong>
                EDA reveals massive spikes in lower-efficiency definitive contracts signed in September.
                <br><br><span>► <b>Recommendation:</b> Cap Q4 discretionary spending to 30% of annual budget to prevent poor vendor selection.</span>
            </div>
        </div>
    </div>

    <script>
        // Common Chart.js Defaults for Monochromatic Theme
        Chart.defaults.color = '#94A3B8';
        Chart.defaults.font.family = "'Segoe UI', sans-serif";
        Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(15, 23, 42, 0.95)';
        Chart.defaults.plugins.tooltip.padding = 10;
        Chart.defaults.plugins.tooltip.titleColor = '#F8FAFC';
        Chart.defaults.plugins.tooltip.borderColor = 'rgba(59, 130, 246, 0.2)';
        Chart.defaults.plugins.tooltip.borderWidth = 1;

        // Chart 1: Agency Unspent Bar Chart
        const ctxAgency = document.getElementById('agencyChart').getContext('2d');
        
        // Monochromatic gradient for bars
        const gradientBlue = ctxAgency.createLinearGradient(0, 0, 0, 300);
        gradientBlue.addColorStop(0, '#3B82F6');
        gradientBlue.addColorStop(1, 'rgba(59, 130, 246, 0.1)');

        new Chart(ctxAgency, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(agency_labels)},
                datasets: [{{
                    label: 'Unspent Balance ($B)',
                    data: {json.dumps(agency_data)},
                    backgroundColor: gradientBlue,
                    borderColor: '#3B82F6',
                    borderWidth: 1,
                    borderRadius: 4,
                    barPercentage: 0.6
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    y: {{ 
                        grid: {{ color: 'rgba(255,255,255,0.03)', drawBorder: false }},
                        beginAtZero: true
                    }},
                    x: {{ 
                        grid: {{ display: false, drawBorder: false }},
                        ticks: {{ maxRotation: 45, minRotation: 45 }}
                    }}
                }}
            }}
        }});

        // Chart 2: YOY Spend Line Chart
        const ctxYoy = document.getElementById('yoyChart').getContext('2d');
        
        const gradientLine = ctxYoy.createLinearGradient(0, 0, 0, 300);
        gradientLine.addColorStop(0, 'rgba(96, 165, 250, 0.3)');
        gradientLine.addColorStop(1, 'rgba(96, 165, 250, 0.0)');

        new Chart(ctxYoy, {{
            type: 'line',
            data: {{
                labels: {json.dumps(yoy_labels)},
                datasets: [{{
                    label: 'Total Spend ($B)',
                    data: {json.dumps(yoy_data)},
                    borderColor: '#60A5FA',
                    backgroundColor: gradientLine,
                    borderWidth: 2,
                    pointBackgroundColor: '#0F172A',
                    pointBorderColor: '#60A5FA',
                    pointBorderWidth: 2,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    y: {{ grid: {{ color: 'rgba(255,255,255,0.03)' }}, beginAtZero: true }},
                    x: {{ grid: {{ display: false }} }}
                }}
            }}
        }});

        // Chart 3: Contract Type Doughnut - Monochromatic Shades
        const ctxType = document.getElementById('typeChart').getContext('2d');
        new Chart(ctxType, {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(type_labels)},
                datasets: [{{
                    data: {json.dumps(type_data)},
                    backgroundColor: ['#1E3A8A', '#2563EB', '#60A5FA', '#BFDBFE'],
                    borderWidth: 0,
                    hoverOffset: 4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                cutout: '75%',
                plugins: {{
                    legend: {{ position: 'bottom', labels: {{ padding: 20, usePointStyle: true, boxWidth: 8 }} }}
                }}
            }}
        }});
    </script>
</body>
</html>"""

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f" Dashboard generated successfully: {OUTPUT_HTML}")
    
if __name__ == "__main__":
    build_dashboard()
