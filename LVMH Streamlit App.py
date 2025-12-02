import streamlit as st
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import io

# ================== CONFIG ==================
URL = "https://www.lvmh.com/api/search"
HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://www.lvmh.com",
    "referer": "https://www.lvmh.com/en/join-us/our-job-offers",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}

REGIONS_ALL = ["America", "Asia Pacific", "Europe", "Middle East / Africa"]
HITS_PER_PAGE = 50

# ================== FUNCTIONS ==================

def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    session.get("https://www.lvmh.com/en/join-us/our-job-offers", timeout=30)
    return session

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

def scrape_jobs(keyword, selected_regions, progress_bar=None):
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
        if progress_bar:
            progress_bar.progress(min(total_fetched / 2500, 1.0))
        time.sleep(0.5)
    return pd.DataFrame(all_jobs)

# Encoding Fix Function
def fix_encoding(text):
    """Attempts to fix double-encoded UTF-8 strings like 'MahÃ©'."""
    if isinstance(text, str):
        try:
            return text.encode('latin1').decode('utf8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            return text
    return text

# Function to select and rename the desired columns and apply encoding fix
def create_filtered_df(df):
    """Selects the desired columns, renames them, and applies encoding correction."""
    if df.empty:
        return pd.DataFrame()

    # Apply Encoding Fix to known problematic columns
    for col in ['city', 'description', 'name']:
        if col in df.columns:
            df[col] = df[col].apply(fix_encoding)

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
    
    existing_cols = [col for col in column_map.keys() if col in df.columns]
    df_filtered = df[existing_cols].rename(columns=column_map)
    
    if 'Description' in df_filtered.columns:
        df_filtered['Description'] = df_filtered['Description'].astype(str).str.replace('__ais-highlight__', '').str.replace('__/ais-highlight__', '')
    
    return df_filtered

# *** CRITICAL FIX HERE ***
@st.cache_data
def convert_df_to_csv(df):
    """Converts DataFrame to CSV for download, using UTF-8-SIG for Excel compatibility."""
    # FIX: Use 'utf-8-sig' to include the BOM, resolving CSV display errors in Excel/other programs.
    return df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')


# ================== STREAMLIT UI ==================
st.title("LVMH Job Scraper with Dual Download")

# Inputs
keyword_input = st.text_input("Job Title / Keywords (leave blank for all)")
regions_input = st.multiselect("Select Regions", REGIONS_ALL, default=REGIONS_ALL)

if st.button("Fetch Jobs"):
    progress_bar = st.progress(0)
    with st.spinner("Scraping jobs... this may take a few minutes..."):
        try:
            df_raw = scrape_jobs(keyword_input.strip() if keyword_input else None, regions_input, progress_bar)
            progress_bar.empty()
            
            if not df_raw.empty:
                st.success(f"Found {len(df_raw)} jobs!")
                
                df_filtered = create_filtered_df(df_raw.copy()) 
                
                st.dataframe(df_filtered, use_container_width=True) 
                
                # --- Dual Download Buttons ---
                st.subheader("Download Options")
                col1, col2 = st.columns(2)
                
                # Download Button 1: Full Data (Original columns)
                with col1:
                    # Apply encoding fix to df_raw just for the download file
                    df_raw_fixed = df_raw.copy()
                    for col in ['city', 'description', 'name']:
                        if col in df_raw_fixed.columns:
                            df_raw_fixed[col] = df_raw_fixed[col].apply(fix_encoding)
                            
                    full_csv = convert_df_to_csv(df_raw_fixed)
                    st.download_button(
                        "Download Full Data (All Columns)", 
                        data=full_csv, 
                        file_name="lvmh_jobs_FULL.csv",
                        mime="text/csv"
                    )
                
                # Download Button 2: Filtered Columns Data (Requested columns/titles)
                with col2:
                    filtered_csv = convert_df_to_csv(df_filtered)
                    st.download_button(
                        "Download Filtered Columns (8 Columns)", 
                        data=filtered_csv, 
                        file_name="lvmh_jobs_FILTERED_COLUMNS.csv",
                        mime="text/csv"
                    )
                    
            else:
                st.warning("No jobs found.")
        except Exception as e:
            st.error(f"Error: {e}")
