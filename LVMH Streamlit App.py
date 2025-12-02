import streamlit as st
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

# ================== CONFIG ==================
URL = "https://www.lvmh.com/api/search"
REGIONS_ALL = ["America", "Asia Pacific", "Europe", "Middle East / Africa"]
HITS_PER_PAGE = 50
SESSION_MAX_AGE = 30 * 60  # 30 minutes

# ================== GLOBAL SESSION (Caching) ==================
# Use st.session_state to hold the raw DataFrame across script reruns
if 'raw_df' not in st.session_state:
    st.session_state.raw_df = pd.DataFrame()
    
SESSION = None
SESSION_TIMESTAMP = None

@st.cache_data(show_spinner=False, ttl=3600)
def create_session():
    """Create or refresh the requests session with automatic cookies."""
    global SESSION, SESSION_TIMESTAMP
    now = time.time()
    if SESSION and (now - SESSION_TIMESTAMP) < SESSION_MAX_AGE:
        return SESSION  # reuse existing session

    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    session.headers.update({
        "accept": "*/*",
        "content-type": "application/json",
        "origin": "https://www.lvmh.com",
        "referer": "https://www.lvmh.com/en/join-us/our-job-offers",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    })

    # Refresh cookies by visiting the job offers page
    session.get("https://www.lvmh.com/en/join-us/our-job-offers", timeout=30)

    SESSION = session
    SESSION_TIMESTAMP = time.time()
    return session

# ================== FETCHING FUNCTIONS ==================
def fetch_jobs_page(session, regions, keyword=None, page=0):
    """Fetch a single page of jobs from the API."""
    facet_filters = [[f"geographicAreaFilter:{r}" for r in regions]]
    payload = {
        "queries": [
            {
                "indexName": "PRD-en-us-timestamp-desc",
                "params": {
                    "facetFilters": facet_filters,
                    "facets": ["businessGroupFilter", "cityFilter", "contractFilter", "countryRegionFilter"],
                    "filters": "category:job",
                    "highlightPostTag": "__/ais-highlight__",
                    "highlightPreTag": "__ais-highlight__",
                    "hitsPerPage": HITS_PER_PAGE,
                    "maxValuesPerFacet": 100,
                    "page": page,
                    "query": keyword if keyword else ""
                }
            }
        ]
    }
    resp = session.post(URL, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

def extract_jobs(data):
    """Extract jobs from the JSON response."""
    jobs = []
    for query_result in data.get("results", []):
        for hit in query_result.get("hits", []):
            jobs.append(hit)
    return jobs

@st.cache_data(show_spinner=True, ttl=3600)
def scrape_jobs(keyword, selected_regions):
    """Scrape all jobs for given regions and keyword."""
    session = create_session()
    all_jobs = []
    regions_to_use = selected_regions if selected_regions else REGIONS_ALL
    page = 0
    total_fetched = 0
    
    while True:
        data = fetch_jobs_page(session, regions_to_use, keyword, page)
        jobs = extract_jobs(data)
        if not jobs:
            break
        all_jobs.extend(jobs)
        total_fetched += len(jobs)
        page += 1
        time.sleep(0.5)  # polite delay
        
        if total_fetched > 5000:
            st.warning("Reached job limit to prevent excessive scraping.")
            break
            
    return pd.DataFrame(all_jobs)

# ================== FORMATTING AND FILTERING FUNCTIONS ==================

def format_output_dataframe(df):
    """
    Selects the required columns and renames them for display.
    """
    if df.empty:
        return pd.DataFrame()

    # MAPPING for final requested titles
    column_map = {
        'name': 'Name',
        'maison': 'Company',
        'contract': 'Type',
        'description': 'Description',
        'city': 'Location',
        'functionFilter': 'Industry',
        'fullTimePartTime': 'Level',
        'link': 'Apply URL'
    }

    # Select existing columns and rename
    existing_cols = [col for col in column_map.keys() if col in df.columns]
    df_formatted = df[existing_cols].rename(columns=column_map)
    
    # Clean up description text from highlight tags
    df_formatted['Description'] = df_formatted['Description'].str.replace('__ais-highlight__', '').str.replace('__/ais-highlight__', '')
    
    return df_formatted

def apply_filters(df, selected_companies, selected_types, selected_locations, selected_industries, selected_levels):
    """Apply filters to the job DataFrame."""
    filtered_df = df.copy()

    # Filter by Company
    if selected_companies:
        filtered_df = filtered_df[filtered_df['Company'].isin(selected_companies)]

    # Filter by Type (Contract)
    if selected_types:
        filtered_df = filtered_df[filtered_df['Type'].isin(selected_types)]

    # Filter by Location (City)
    if selected_locations:
        filtered_df = filtered_df[filtered_df['Location'].isin(selected_locations)]
        
    # Filter by Industry (Function)
    if selected_industries:
        filtered_df = filtered_df[filtered_df['Industry'].isin(selected_industries)]
        
    # Filter by Level (Full-Time/Part-Time)
    if selected_levels:
        filtered_df = filtered_df[filtered_df['Level'].isin(selected_levels)]

    return filtered_df

@st.cache_data
def convert_df_to_excel(df, filename="jobs.xlsx"):
    """Converts DataFrame to bytes for download."""
    # Create a temporary Excel file in memory
    import io
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='LVMH Jobs')
    writer.close()
    processed_data = output.getvalue()
    return processed_data

# ================== STREAMLIT UI ==================

st.title("LVMH Job Scraper 💼")

# --- 1. Fetch Job Data Section ---
st.header("1. Fetch Job Data")
keyword_input = st.text_input("Job Title / Keywords (leave blank for all)")
regions_input = st.multiselect("Select Regions to Search", REGIONS_ALL, default=REGIONS_ALL)

if st.button("Fetch Jobs"):
    with st.spinner("Scraping jobs... this may take a few minutes..."):
        try:
            # Scrape and save the RAW data to session state
            df_raw = scrape_jobs(keyword_input.strip() if keyword_input else None, regions_input)
            st.session_state.raw_df = df_raw
            
            if not df_raw.empty:
                st.success(f"Successfully fetched {len(df_raw)} jobs! Use the sidebar filters to refine the list.")
            else:
                st.warning("No jobs found with the current criteria.")
                
        except Exception as e:
            st.error(f"Error during scraping: {e}")

# --- 2. Filtering and Display Section ---
df_raw = st.session_state.raw_df

if not df_raw.empty:
    # Format the raw data once for filtering and display
    df_output = format_output_dataframe(df_raw)
    
    st.header("2. Filter Results")
    st.markdown(f"**Total Jobs Fetched:** **{len(df_output)}**")
    
    # --- Sidebar for Filtering ---
    st.sidebar.header("Job Filters 🔎")
    
    # Get all unique values for filters from the formatted data
    all_companies = df_output['Company'].dropna().unique().tolist()
    all_types = df_output['Type'].dropna().unique().tolist()
    all_locations = df_output['Location'].dropna().unique().tolist()
    all_industries = df_output['Industry'].dropna().unique().tolist()
    all_levels = df_output['Level'].dropna().unique().tolist()
    
    selected_companies = st.sidebar.multiselect("Company", options=all_companies, default=all_companies)
    selected_types = st.sidebar.multiselect("Type", options=all_types, default=all_types)
    selected_locations = st.sidebar.multiselect("Location", options=all_locations, default=all_locations)
    selected_industries = st.sidebar.multiselect("Industry", options=all_industries, default=all_industries)
    selected_levels = st.sidebar.multiselect("Level", options=all_levels, default=all_levels)
    
    # Apply filters
    filtered_df = apply_filters(
        df_output,
        selected_companies,
        selected_types,
        selected_locations,
        selected_industries,
        selected_levels
    )
    
    # Display results
    st.subheader(f"Showing {len(filtered_df)} Jobs")
    st.dataframe(filtered_df, use_container_width=True)
    
    # --- 3. Download Options ---
    st.header("3. Download Data")
    
    col1, col2 = st.columns(2)
    
    # Button 1: Download Full Data
    with col1:
        st.download_button(
            label="⬇️ Download ALL Fetched Data",
            data=convert_df_to_excel(df_output),
            file_name=f"lvmh_jobs_FULL_{len(df_output)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Downloads the entire dataset fetched, ignoring current filters."
        )

    # Button 2: Download Filtered Data
    with col2:
        if not filtered_df.empty:
            st.download_button(
                label=f"⬇️ Download Filtered Data ({len(filtered_df)} rows)",
                data=convert_df_to_excel(filtered_df),
                file_name=f"lvmh_jobs_FILTERED_{len(filtered_df)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Downloads only the jobs currently visible in the table."
            )
        else:
            st.button("Download Filtered Data (0 rows)", disabled=True)
