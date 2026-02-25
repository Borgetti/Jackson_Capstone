from flask import Flask, render_template_string, request
import pandas as pd
import plotly.express as px
import plotly.io as pio
import folium

app = Flask(__name__)

# =========================================================
# 1. LOAD DATA ONCE WHEN THE APP STARTS
# =========================================================
FILE_PATH = 'Client Files\Click Count and Outage Management Data.xlsx'

try:
    df = pd.read_excel(FILE_PATH)
    df = df.dropna(subset=['District'])
    df['# Out'] = pd.to_numeric(df['# Out'], errors='coerce').fillna(0)
    df['Customer Minutes'] = pd.to_numeric(df['Customer Minutes'], errors='coerce').fillna(0)
    
    # Convert 'Time Off' to actual datetime objects
    df['Time Off'] = pd.to_datetime(df['Time Off'], errors='coerce')
    df = df.dropna(subset=['Time Off']) 
    
    MIN_DATE = df['Time Off'].min().strftime('%Y-%m-%d')
    MAX_DATE = df['Time Off'].max().strftime('%Y-%m-%d')
except FileNotFoundError:
    print(f"Error: Could not find file at {FILE_PATH}. Please check the path.")
    df = pd.DataFrame()
    MIN_DATE = "2020-01-01"
    MAX_DATE = "2020-01-01"

# Map Districts to approximate center GPS coordinates
DISTRICT_COORDS = {
    'Gainesville': [34.2979, -83.8241],
    'Lawrenceville': [33.9562, -83.9880],
    'Jefferson': [34.1165, -83.5732],
    'Neese': [34.1678, -83.2505]
}

# =========================================================
# SHARED HTML COMPONENT: NAVIGATION BAR & CSS
# =========================================================
NAV_HTML = """
    <div class="nav-bar">
        <a href="/">Dashboard Graphs</a>
        <a href="/heatmap">Interactive Heatmap</a>
    </div>
"""

COMMON_CSS = """
    <style>
        body { font-family: Arial, sans-serif; background-color: #f3f4f6; margin: 0; padding: 20px; }
        h1 { text-align: center; color: #333; margin-top: 20px; }
        
        .nav-bar {
            background-color: #1f2937;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            text-align: center;
        }
        .nav-bar a {
            color: white; text-decoration: none; padding: 10px 20px; margin: 0 10px;
            font-weight: bold; border-radius: 4px; transition: background-color 0.3s;
        }
        .nav-bar a:hover { background-color: #3b82f6; }
        
        .filter-container {
            background-color: white; padding: 15px 20px; border-radius: 8px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1); margin-bottom: 20px;
            display: flex; align-items: center; gap: 15px; justify-content: center;
            max-width: 1200px; margin-left: auto; margin-right: auto;
        }
        .filter-container input[type="date"] { padding: 8px; border: 1px solid #ccc; border-radius: 4px; }
        .filter-container button {
            padding: 10px 15px; background-color: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;
        }
        .filter-container button:hover { background-color: #2563eb; }
    </style>
"""

# =========================================================
# ROUTE 1: THE MAIN DASHBOARD
# =========================================================
@app.route('/', methods=['GET', 'POST'])
def dashboard():
    start_date = MIN_DATE
    end_date = MAX_DATE
    
    if request.method == 'POST':
        start_date = request.form.get('start_date', MIN_DATE)
        end_date = request.form.get('end_date', MAX_DATE)

    mask = (df['Time Off'] >= start_date) & (df['Time Off'] <= end_date + " 23:59:59")
    filtered_df = df.loc[mask]

    if filtered_df.empty:
        return f"<h3>No outages found between {start_date} and {end_date}.</h3><a href='/'>Go Back</a>"

    district_out = filtered_df.groupby('District', as_index=False)['# Out'].sum().sort_values(by='# Out', ascending=False)
    fig1 = px.bar(district_out, x='District', y='# Out', title='Total Customers Affected by District', text_auto='.2s', color='# Out', color_continuous_scale='Reds')

    district_mins = filtered_df.groupby('District', as_index=False)['Customer Minutes'].sum().sort_values(by='Customer Minutes', ascending=False)
    fig2 = px.bar(district_mins, x='District', y='Customer Minutes', title='Total Customer Minutes by District', text_auto='.2s', color='Customer Minutes', color_continuous_scale='Oranges')

    cause_counts = filtered_df['Cause Desc'].value_counts().reset_index()
    cause_counts.columns = ['Cause Description', 'Number of Outages']
    fig3 = px.bar(cause_counts.head(10), x='Number of Outages', y='Cause Description', orientation='h', title='Top 10 Outage Causes', color='Number of Outages', color_continuous_scale='Blues')
    fig3.update_layout(yaxis={'categoryorder':'total ascending'})

    type_counts = filtered_df['Type'].value_counts().reset_index()
    type_counts.columns = ['Outage Type', 'Count']
    fig4 = px.pie(type_counts, names='Outage Type', values='Count', title='Distribution of Outage Types', hole=0.4)

    html1 = fig1.to_html(full_html=False, include_plotlyjs='cdn')
    html2 = fig2.to_html(full_html=False, include_plotlyjs=False)
    html3 = fig3.to_html(full_html=False, include_plotlyjs=False)
    html4 = fig4.to_html(full_html=False, include_plotlyjs=False)

    dashboard_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Engineering Capstone Dashboard</title>
        {COMMON_CSS}
        <style>
            .grid-container {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; max-width: 1200px; margin: 0 auto;}}
            .card {{ background-color: white; padding: 10px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
            @media (max-width: 1000px) {{ .grid-container {{ grid-template-columns: 1fr; }} }}
        </style>
    </head>
    <body>
        {NAV_HTML}
        
        <h1>Power Outage Dashboard</h1>
        
        <div class="filter-container">
            <form method="POST" action="/">
                <label for="start_date"><strong>Start Date:</strong></label>
                <input type="date" id="start_date" name="start_date" value="{start_date}" min="{MIN_DATE}" max="{MAX_DATE}">
                
                <label for="end_date" style="margin-left: 10px;"><strong>End Date:</strong></label>
                <input type="date" id="end_date" name="end_date" value="{end_date}" min="{MIN_DATE}" max="{MAX_DATE}">
                
                <button type="submit">Update Dashboard</button>
            </form>
        </div>

        <div class="grid-container">
            <div class="card">{html1}</div>
            <div class="card">{html2}</div>
            <div class="card">{html3}</div>
            <div class="card">{html4}</div>
        </div>
    </body>
    </html>
    """
    return render_template_string(dashboard_html)

# =========================================================
# ROUTE 2: THE MAP PAGE (Side-by-side Layout with 2 Lists)
# =========================================================
@app.route('/heatmap', methods=['GET', 'POST'])
def heatmap_page():
    
    start_date = MIN_DATE
    end_date = MAX_DATE
    
    if request.method == 'POST':
        start_date = request.form.get('start_date', MIN_DATE)
        end_date = request.form.get('end_date', MAX_DATE)

    mask = (df['Time Off'] >= start_date) & (df['Time Off'] <= end_date + " 23:59:59")
    filtered_df = df.loc[mask]
    
    # 1. District Stats (Customers Affected)
    district_stats = filtered_df.groupby('District')['# Out'].sum().to_dict()
    
    # 2. Type Stats (Number of Outage Incidents)
    type_stats = filtered_df['Type'].value_counts().to_dict()

    # --- Generate the Map ---
    m = folium.Map(location=[34.15, -83.75], zoom_start=9, width='100%', height='100%')
    
    for district, coords in DISTRICT_COORDS.items():
        customers_affected = district_stats.get(district, 0)
        
        popup_html = f"""
            <div style='font-family: Arial; min-width: 150px;'>
                <b>{district} District</b><br>
                Customers Affected: <b>{int(customers_affected):,}</b>
            </div>
        """
        
        folium.Marker(
            location=coords,
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color="blue", icon="info-sign"),
            tooltip=f"Click to see details for {district}"
        ).add_to(m)
        
    map_html = m._repr_html_()
    
    # --- Generate the First Sidebar List HTML (Districts) ---
    sorted_districts = sorted(district_stats.items(), key=lambda item: item[1], reverse=True)
    district_list_items = ""
    for district, count in sorted_districts:
        district_list_items += f"""
            <li class="list-item">
                <span class="item-name">{district}</span>
                <span class="item-count">{int(count):,} affected</span>
            </li>
        """
        
    # --- Generate the Second Sidebar List HTML (Outage Types) ---
    sorted_types = sorted(type_stats.items(), key=lambda item: item[1], reverse=True)
    type_list_items = ""
    for out_type, count in sorted_types:
        type_list_items += f"""
            <li class="list-item">
                <span class="item-name">{out_type}</span>
                <span class="item-count" style="color: #3b82f6;">{int(count):,} incidents</span>
            </li>
        """

    heatmap_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Outage Heatmap</title>
        {COMMON_CSS}
        <style>
            .content-wrapper {{
                display: flex;
                max-width: 1200px;
                margin: 20px auto;
                gap: 20px;
            }}
            
            /* Sidebar List Styling */
            .sidebar {{
                flex: 1; 
                background-color: white; 
                padding: 20px; 
                border-radius: 8px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                height: fit-content;
            }}
            .sidebar h2 {{ margin-top: 0; color: #1f2937; border-bottom: 2px solid #e5e7eb; padding-bottom: 10px; font-size: 1.2rem; }}
            .sidebar h2.second-heading {{ margin-top: 30px; }}
            .data-list {{ list-style-type: none; padding: 0; margin: 0; }}
            .list-item {{ 
                display: flex; 
                justify-content: space-between; 
                padding: 10px 0; 
                border-bottom: 1px solid #f3f4f6;
                font-size: 0.95rem;
            }}
            .item-name {{ font-weight: bold; color: #374151; }}
            .item-count {{ color: #ef4444; font-weight: bold; }}
            
            /* Map Styling */
            .map-container {{
                flex: 2; 
                background-color: white; 
                padding: 10px; 
                border-radius: 8px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1); 
                height: 600px; 
            }}
            .map-container iframe {{ border-radius: 8px; border: none; }}
            
            /* Make it stack vertically on smaller screens */
            @media (max-width: 800px) {{
                .content-wrapper {{ flex-direction: column; }}
                .map-container {{ height: 400px; }}
            }}
        </style>
    </head>
    <body>
        {NAV_HTML}
        
        <h1>District Map Overview</h1>
        
        <div class="filter-container">
            <form method="POST" action="/heatmap">
                <label for="start_date"><strong>Start Date:</strong></label>
                <input type="date" id="start_date" name="start_date" value="{start_date}" min="{MIN_DATE}" max="{MAX_DATE}">
                
                <label for="end_date" style="margin-left: 10px;"><strong>End Date:</strong></label>
                <input type="date" id="end_date" name="end_date" value="{end_date}" min="{MIN_DATE}" max="{MAX_DATE}">
                
                <button type="submit">Update Map</button>
            </form>
        </div>
        
        <div class="content-wrapper">
            <div class="sidebar">
                <h2>Customers Affected (By District)</h2>
                <ul class="data-list">
                    {district_list_items}
                </ul>
                
                <h2 class="second-heading">Outage Incidents (By Type)</h2>
                <ul class="data-list">
                    {type_list_items}
                </ul>
            </div>
            
            <div class="map-container">
                {map_html}
            </div>
        </div>
        
    </body>
    </html>
    """
    return render_template_string(heatmap_html)

if __name__ == "__main__":
    pio.renderers.default = 'browser'
    print("Starting Flask Server! Go to http://127.0.0.1:5000 in your browser.")
    app.run(debug=True, port=5000)