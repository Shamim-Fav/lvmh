import streamlit as st
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import io

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

# ================== CACHED SETUP AND SCRAPING FUNCTIONS ==================

@st.cache_data(show_spinner=False, ttl=3600)
def create_session():
    """Create or refresh the requests session with automatic cookies."""
    global SESSION, SESSION_TIMESTAMP
    now = time.time()
    if SESSION and (now - SESSION_TIMESTAMP) < SESSION_MAX_AGE:
        return SESSION

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

# Note: The scrape_jobs function is cached to prevent re-scraping
@st.cache_data(show_spinner=True, ttl=3600)
def scrape_jobs(keyword, selected_regions):
    """Scrape all jobs for given regions and keyword."""
    session = create_session()
    all_jobs = []
    regions_to_use = selected_regions if selected_regions else REGIONS_ALL
    page = 0
    total_fetched = 0
    
    # Helper functions nested inside scrape_jobs for caching stability
    def fetch_jobs_page(session, regions, keyword=None, page=0):
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
        jobs = []
        for query_result in data.get("results", []):
            for hit in query_result.get("hits", []):
                jobs.append(hit)
        return jobs

    while True:
        data = fetch_jobs_page(session, regions_to_use, keyword, page)
        jobs = extract_jobs(data)
        
        if not jobs:
            break
            
        all_jobs.extend(jobs)
        total_fetched += len(jobs)
        page += 1
        time.sleep(0.5)
        
        if total_fetched > 5000:
            st.warning("Reached job limit to prevent excessive scraping.")
            break
            
    return pd.DataFrame(all_jobs)


# ================== FORMATTING FUNCTIONS ==================

def format_output_dataframe(df):
    """
    Selects the required columns and renames them for display.
    """
    if df.empty:
        return pd.DataFrame()

    # Final requested column mapping
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
    
    # Clean up description text
    df_formatted['Description'] = df_formatted['Description'].astype(str).str.replace('__ais-highlight__', '').str.replace('__/ais-highlight__', '')
    
    return df_formatted

@st.cache_data
def convert_df_to_excel(df):
    """Converts DataFrame to bytes for Excel download."""
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='LVMH Jobs')
    writer.close()
    processed_data = output.getvalue()
    return processed_data

# ================== STREAMLIT UI ==================

st.title("LVMH Job Scraper 💼")

# --- 1. Fetch Job Data Section ---
st.header("1. Input and Fetch Data")
keyword_input = st.text_input("Job Title / Keywords (Input for Scrape)", key="keyword")
regions_input = st.multiselect("Select Regions to Search (Input for Scrape)", REGIONS_ALL, default=REGIONS_ALL, key="regions")

if st.button("Fetch Jobs"):
    with st.spinner("Scraping jobs... this may take a few minutes..."):
        try:
            # Scrape data using the keyword/region inputs
            df_raw = scrape_jobs(keyword_input.strip() if keyword_input else None, regions_input)
            st.session_state.raw_df = df_raw
            
            if not df_raw.empty:
                st.success(f"Successfully fetched {len(df_raw)} jobs!")
            else:
                st.warning("No jobs found with the current criteria.")
                
        except Exception as e:
            st.error(f"Error during scraping: {e}")

# Retrieve data for processing
df_raw = st.session_state.raw_df

if not df_raw.empty:
    # Format the raw data (new column names)
    df_output = format_output_dataframe(df_raw)
    
    # --- 2. Display Data ---
    st.header("2. Fetched Results")
    st.markdown(f"**Total Jobs Fetched:** **{len(df_output)}**")
    st.dataframe(df_output, use_container_width=True)
    
    # --- 3. Dual Download Options ---
    st.header("3. Download Data")
    
    col1, col2 = st.columns(2)
    
    # Button 1: Download Full Data (This is the entire result set)
    with col1:
        st.download_button(
            label="⬇️ Download Full Fetched Data",
            data=convert_df_to_excel(df_output),
            file_name=f"lvmh_jobs_FULL_fetched_{len(df_output)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Downloads all data scraped based on your initial input."
        )

    # Button 2: Download a 'Filtered' Data (Since there are no sidebar filters, this is essentially the same as Button 1 but labeled differently for user choice)
    with col2:
        st.download_button(
            label=f"⬇️ Download Filtered Data ({len(df_output)} rows)",
            data=convert_df_to_excel(df_output),
            file_name=f"lvmh_jobs_FILTERED_by_input_{len(df_output)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Downloads the data filtered by your initial Keyword and Region input."
        )
