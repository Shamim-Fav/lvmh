import streamlit as st
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import io
import zipfile
import ast # For safely evaluating salary strings
import numpy as np # For conditional logic (np.where)

# Set the page icon (favicon) and title
st.set_page_config(
    page_title="LVMH Job Scraper",
    page_icon="💼" # Using the briefcase emoji
)

# ================== CONFIG ==================
URL = "https://www.lvmh.com/api/search"
REGIONS_ALL = ["America", "Asia Pacific", "Europe", "Middle East / Africa"]
HITS_PER_PAGE = 50
SESSION_MAX_AGE = 30 * 60 # 30 minutes

# ================== GLOBAL SESSION ==================
SESSION = None
SESSION_TIMESTAMP = None

def create_session():
    """Create or refresh the requests session with automatic cookies."""
    global SESSION, SESSION_TIMESTAMP
    now = time.time()
    if SESSION and (now - SESSION_TIMESTAMP) < SESSION_MAX_AGE:
        return SESSION # reuse existing session

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
# (fetch_jobs_page, extract_jobs, scrape_jobs functions remain the same as in your original script)

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

def scrape_jobs(keyword, selected_regions, progress_bar=None):
    """Scrape all jobs for given regions and keyword, with optional progress bar."""
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
        time.sleep(0.5) # polite delay
    return pd.DataFrame(all_jobs)


# ================== DATA PROCESSING FUNCTIONS ==================

def fix_encoding(text):
    """Attempts to fix double-encoded UTF-8 strings like 'MahÃ©'."""
    if isinstance(text, str):
        try:
            return text.encode('latin1').decode('utf8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            return text
    return text

def create_filtered_df(df):
    """
    Applies all cleaning, merging, slug creation, and column renaming logic.
    """
    if df.empty:
        return pd.DataFrame()

    # Apply Encoding Fix to known problematic columns
    for col in ['city', 'description', 'name', 'profile', 'jobResponsabilities', 'salary']:
        if col in df.columns:
            df[col] = df[col].apply(fix_encoding)
    
    # --- RAW SALARY MAPPING ---
    if 'salary' in df.columns:
        df['Salary Range'] = df['salary'] # Direct copy of the raw data
    else:
        df['Salary Range'] = '' 

    # --- FULL DESCRIPTION MERGE ---
    if 'profile' in df.columns and 'jobResponsabilities' in df.columns and 'description' in df.columns:
        df['FullDescription'] = '' 
        source_cols = ['profile', 'jobResponsabilities', 'description']
        
        for col in source_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                
                df['FullDescription'] = np.where(
                    (df[col] != ''),
                    df['FullDescription'] + "\n\n--- " + col.upper() + " ---\n" + df[col],
                    df['FullDescription']
                )

        df['description'] = df['FullDescription'].str.strip()
    
    # --- FORMULA/LIST ESCAPE FIX ---
    if 'description' in df.columns:
        df['description'] = np.where(
            df['description'].str.startswith(('=', '+', '-')),
            "'" + df['description'],
            df['description']
        )
    
    # --- SLUG CREATION ---
    if 'name' in df.columns and 'maison' in df.columns and 'city' in df.columns:
        cleaned_name = df['name'].astype(str).str.strip()
        cleaned_maison = df['maison'].astype(str).str.strip()
        cleaned_city = df['city'].astype(str).str.strip()
        
        def get_first_two_words(name_str):
            words = name_str.split(' ')
            return ' '.join(words[:2])

        first_two_name_parts = cleaned_name.apply(get_first_two_words)
        
        df['Slug'] = (first_two_name_parts.str.lower() + '-' + 
                      cleaned_maison.str.lower() + '-' + 
                      cleaned_city.str.lower())
        
        df['Slug'] = df['Slug'].str.replace(r'[^a-z0-9\-]+', '-', regex=True)
        df['Slug'] = df['Slug'].str.replace(r'[\-]+', '-', regex=True)
        df['Slug'] = df['Slug'].str.strip('-')
    else:
        df['Slug'] = '' 

    # MAPPING for final requested titles
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
        'Salary Range': 'Salary Range' # Includes raw salary data
    }
    
    # 1. Filter existing columns based on the map (raw 'salary' is excluded)
    existing_cols = [col for col in column_map.keys() if col in df.columns]
    df_filtered = df[existing_cols].rename(columns=column_map)
    
    # 2. Add the remaining BLANK columns
    blank_columns = [
        'Access', 'Salary', 'Deadline', 'Collection ID', 
        'Locale ID', 'Item ID', 'Archived', 'Draft', 'Created On', 
        'Updated On', 'Published On', 'CMS ID'
    ]
    
    for col_name in blank_columns:
        df_filtered[col_name] = '' 
    
    # Clean up highlight tags
    if 'Description' in df_filtered.columns:
        df_filtered['Description'] = df_filtered['Description'].astype(str).str.replace('__ais-highlight__', '').str.replace('__/ais-highlight__', '')
    
    return df_filtered

@st.cache_data
def convert_df_to_csv(df):
    """Converts DataFrame to CSV for download, using UTF-8-SIG for Excel compatibility."""
    # Using 'utf-8-sig' ensures Excel reads special characters correctly.
    return df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')

@st.cache_data
def create_zip_archive(df_raw, df_filtered):
    """
    Creates a single ZIP file containing both the full raw CSV and the filtered CSV.
    """
    # Create an in-memory byte stream for the ZIP file
    zip_io = io.BytesIO()

    with zipfile.ZipFile(zip_io, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        # File 1: Full Raw Data CSV
        # We need to make a copy of raw data just to apply the final encoding fix
        df_raw_fixed = df_raw.copy()
        for col in ['city', 'description', 'name']:
            if col in df_raw_fixed.columns:
                df_raw_fixed[col] = df_raw_fixed[col].apply(fix_encoding)
        
        full_csv = convert_df_to_csv(df_raw_fixed)
        zf.writestr('lvmh_jobs_FULL_RAW.csv', full_csv)

        # File 2: Filtered Data CSV
        filtered_csv = convert_df_to_csv(df_filtered)
        zf.writestr('lvmh_jobs_FILTERED_CLEAN.csv', filtered_csv)

    # Move to the beginning of the byte stream and return
    zip_io.seek(0)
    return zip_io.read()


# ================== STREAMLIT UI ==================

st.title("💼 LVMH Job Scraper")

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
                
                # --- Prepare DataFrames ---
                df_filtered = create_filtered_df(df_raw.copy()) 
                
                # Display the cleaned/filtered columns for user view
                st.dataframe(df_filtered, use_container_width=True) 
                
                # --- Single-Click ZIP Download ---
                st.subheader("Single-Click Download")
                
                # Create the ZIP file containing both CSVs
                zip_data = create_zip_archive(df_raw, df_filtered)

                st.download_button(
                    "Download All Data (ZIP)", 
                    data=zip_data, 
                    file_name="lvmh_jobs_data.zip",
                    mime="application/zip",
                    help="Downloads a single ZIP file containing both the FULL RAW CSV and the CLEAN FILTERED CSV."
                )
                    
            else:
                st.warning("No jobs found.")
        except Exception as e:
            st.error(f"An error occurred during scraping: {e}")







