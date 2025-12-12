import streamlit as st
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import numpy as np
import re 
from typing import List, Optional

# --- CRITICAL FIX: Ensure this is the first Streamlit command ---
st.set_page_config(
    page_title="💼 LVMH Job Scraper",
    layout="centered"
)
# ---------------------------------------------------------------

# ================== CONFIG ==================
URL = "https://www.lvmh.com/api/search"
REGIONS_ALL = ["America", "Asia Pacific", "Europe", "Middle East / Africa"]
HITS_PER_PAGE = 50
SESSION_MAX_AGE = 30 * 60 # 30 minutes

# ================== GLOBAL SESSION ==================
SESSION = None
SESSION_TIMESTAMP = None

def create_session() -> requests.Session:
    """Create or refresh the requests session with automatic cookies."""
    global SESSION, SESSION_TIMESTAMP
    now = time.time()
    if SESSION and (now - SESSION_TIMESTAMP) < SESSION_MAX_AGE:
        return SESSION  # reuse existing session

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
    try:
        session.get("https://www.lvmh.com/en/join-us/our-job-offers", timeout=15)
    except requests.RequestException as e:
        st.warning(f"Could not initialize session cookies: {e}")

    SESSION = session
    SESSION_TIMESTAMP = time.time()
    return session

# -------------------------------------------------------------------
## 🔍 Fetching Functions

def fetch_jobs_page(session: requests.Session, regions: List[str], keyword: Optional[str] = None, page: int = 0) -> dict:
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

def extract_jobs(data: dict) -> List[dict]:
    """Extract jobs from the JSON response."""
    jobs = []
    for query_result in data.get("results", []):
        for hit in query_result.get("hits", []):
            jobs.append(hit)
    return jobs

@st.cache_data(ttl=3600)  # Cache the scraped data for 1 hour
def scrape_jobs(keyword: Optional[str], selected_regions: List[str], _progress_bar=None) -> pd.DataFrame:
    """Scrape all jobs for given regions and keyword, with optional progress bar."""
    session = create_session()
    all_jobs = []
    regions_to_use = selected_regions if selected_regions else REGIONS_ALL
    page = 0
    total_fetched = 0
    
    while True:
        try:
            data = fetch_jobs_page(session, regions_to_use, keyword, page)
            jobs = extract_jobs(data)
            if not jobs:
                break
            all_jobs.extend(jobs)
            total_fetched += len(jobs)
            page += 1
            if _progress_bar:
                # Max 1.0 for Streamlit progress bar, assuming max ~2500 jobs
                _progress_bar.progress(min(total_fetched / 2500, 1.0)) 
            time.sleep(0.5)  # polite delay
        except requests.RequestException as e:
            st.error(f"Request failed on page {page}. Stopping scrape: {e}")
            break
    return pd.DataFrame(all_jobs)

# -------------------------------------------------------------------
## ✨ Data Processing Functions

def fix_encoding(text: str) -> str:
    """Attempts to fix double-encoded UTF-8 strings."""
    if isinstance(text, str):
        try:
            return text.encode('latin1').decode('utf8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            return text
    return text

def create_filtered_df(df: pd.DataFrame) -> pd.DataFrame:
    """Applies all cleaning, merging, slug creation, column renaming logic, and sets final column order."""
    if df.empty:
        return pd.DataFrame()

    # 1. Encoding Fix
    for col in ['city', 'description', 'name', 'profile', 'jobResponsabilities', 'salary']:
        if col in df.columns:
            df[col] = df[col].apply(fix_encoding)

    # 2. Salary Placeholder
    df['Salary Range'] = df['salary'] if 'salary' in df.columns else ''

    # 3. Merge Full Description
    if all(col in df.columns for col in ['profile', 'jobResponsabilities', 'description']):
        df['FullDescription'] = ''
        for col in ['profile', 'jobResponsabilities', 'description']:
            df[col] = df[col].astype(str).str.strip()
            df['FullDescription'] += "\n\n--- " + col.upper() + " ---\n" + df[col]
        df['description'] = df['FullDescription'].str.strip()

    # Prevent formula interpretation in Excel
    if 'description' in df.columns:
        df['description'] = np.where(df['description'].str.startswith(('=', '+', '-')),
                                     "'" + df['description'], df['description'])

    # 4. Slug Creation (FIXED ORDER: Company-Name-Location)
    if all(col in df.columns for col in ['name', 'maison', 'city']):
        cleaned_maison = df['maison'].astype(str).str.strip()
        cleaned_name = df['name'].astype(str).str.strip()
        cleaned_city = df['city'].astype(str).str.strip()
        
        df['Slug'] = (cleaned_maison.str.lower().str.replace(' ', '-') + '-' +
                      cleaned_name.str.lower().str.replace(' ', '-') + '-' +
                      cleaned_city.str.lower().str.replace(' ', '-'))
        
        # Robust cleaning: remove non-alphanumeric chars (except hyphen) and clean up multiple hyphens
        df['Slug'] = df['Slug'].str.replace(r'[^a-z0-9\-]+', '-', regex=True)
        df['Slug'] = df['Slug'].str.replace(r'[\-]+', '-', regex=True).str.strip('-')
    else:
        df['Slug'] = ''

    # 5. Column Mapping
    column_map = {
        'name': 'Name',
        'maison': 'Company',
        'contract': 'Type',
        'description': 'Description',
        'city': 'Location',
        'functionFilter': 'Industry',
        'fullTimePartTime': 'Level',
        'link': 'Apply URL',
        'Slug': 'Slug',
        'Salary Range': 'Salary Range'
    }
    # Select and rename columns that exist in the raw dataframe
    existing_cols = [col for col in column_map.keys() if col in df.columns]
    df_filtered = df[existing_cols].rename(columns=column_map)

    # 6. Add Blank Columns
    blank_columns = ['Collection ID', 'Locale ID', 'Item ID', 'Archived', 'Draft',
                     'Created On', 'Updated On', 'Published On', 'CMS ID', 'Access', 'Salary', 'Deadline']
    for col in blank_columns:
        df_filtered[col] = ''

    # 7. Clean Algolia highlights
    if 'Description' in df_filtered.columns:
        df_filtered['Description'] = df_filtered['Description'].astype(str).str.replace('__ais-highlight__', '').str.replace('__/ais-highlight__', '')

    # 8. Final Column Order
    final_order = ['Name', 'Slug', 'Collection ID', 'Locale ID', 'Item ID', 'Archived',
                   'Draft', 'Created On', 'Updated On', 'Published On', 'CMS ID',
                   'Company', 'Type', 'Description', 'Salary Range', 'Access',
                   'Location', 'Industry', 'Level', 'Salary', 'Deadline', 'Apply URL']
    
    # Reindex the dataframe to enforce the exact order
    df_filtered = df_filtered.reindex(columns=[col for col in final_order if col in df_filtered.columns])

    return df_filtered

@st.cache_data
def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    """Converts DataFrame to CSV string with BOM for Excel compatibility."""
    # Use utf-8-sig which automatically includes the BOM
    return df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')

# -------------------------------------------------------------------
## 💻 Streamlit UI

st.title("💼 LVMH Job Scraper") 

keyword_input = st.text_input("Job Title / Keywords (leave blank for all)")
regions_input = st.multiselect("Select Regions", REGIONS_ALL, default=REGIONS_ALL)

if st.button("Fetch Jobs"):
    progress_bar = st.progress(0)
    with st.spinner("Scraping jobs... this may take a few minutes..."):
        try:
            df_raw = scrape_jobs(keyword_input.strip() if keyword_input else None, regions_input, _progress_bar=progress_bar)
            progress_bar.empty()

            if not df_raw.empty:
                st.success(f"Found {len(df_raw)} jobs!")

                df_filtered = create_filtered_df(df_raw.copy())
                st.dataframe(df_filtered, use_container_width=True)

                # --- TWO SEPARATE DOWNLOAD BUTTONS (CSV with BOM) ---
                csv_raw = convert_df_to_csv(df_raw)
                csv_filtered = convert_df_to_csv(df_filtered)

                st.subheader("Download CSVs")
                st.download_button(
                    "Download Full Raw Data CSV",
                    data=csv_raw,
                    file_name="lvmh_jobs_FULL_RAW.csv",
                    mime="text/csv"
                )
                st.download_button(
                    "Download Filtered CSV",
                    data=csv_filtered,
                    file_name="lvmh_jobs_FILTERED_CLEAN.csv",
                    mime="text/csv"
                )

            else:
                st.warning("No jobs found. Try different search criteria.")

        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
