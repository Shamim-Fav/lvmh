import streamlit as st
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import io
import numpy as np # <-- ADD THIS LINE

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
    """
    Combines description columns, handles encoding, creates the Slug, 
    parses the 'salary' column (handling string representations), and adds blank columns.
    """
    if df.empty:
        return pd.DataFrame()

    # Apply Encoding Fix to known problematic columns
    for col in ['city', 'description', 'name', 'profile', 'jobResponsabilities']:
        if col in df.columns:
            df[col] = df[col].apply(fix_encoding)
    
    # --- SALARY RANGE PARSING (FIXED LOGIC) ---
    if 'salary' in df.columns:
        def format_salary_range(salary_data):
            # 1. Safely convert string representation of dict to actual dict
            if isinstance(salary_data, str):
                try:
                    # Use ast.literal_eval to safely convert the string to a dictionary
                    salary_data = ast.literal_eval(salary_data)
                except (ValueError, SyntaxError):
                    # If conversion fails (e.g., if it's an empty cell or invalid string), treat as empty
                    salary_data = {}
            elif salary_data is None:
                salary_data = {}
            
            # Ensure the input is now treated as a dictionary
            if not isinstance(salary_data, dict):
                return ''
            
            min_val = salary_data.get('min')
            max_val = salary_data.get('max')
            currency = salary_data.get('currency')
            period = salary_data.get('period')
            
            # --- Format Min/Max ---
            if min_val == 'To be negotiated':
                return 'To be negotiated'
            
            # Function to format numbers with commas, handling None or non-numeric types
            def safe_format(val):
                if val is None:
                    return None
                try:
                    # Attempt to convert to float/int and format with commas
                    return f"{float(val):,.0f}".replace('.0', '')
                except (ValueError, TypeError):
                    # If it's a string like "Confidential", return as is
                    return str(val)

            min_str = safe_format(min_val)
            max_str = safe_format(max_val)

            # Construct the Range String
            if min_str and max_str:
                range_str = f"{min_str} - {max_str}"
            elif min_str:
                range_str = f"Min {min_str}"
            elif max_str:
                range_str = f"Max {max_str}"
            else:
                return ''
                
            # --- Combine with Currency and Period ---
            parts = [
                currency if currency else None,
                range_str,
                f"({period})" if period else None
            ]
            
            return ' '.join(p for p in parts if p)
        
        # Apply the formatting function to the 'salary' column
        df['Salary Range'] = df['salary'].apply(format_salary_range)
    else:
        df['Salary Range'] = ''


    # --- FULL DESCRIPTION MERGE (Unchanged) ---
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
    
    # --- FORMULA/LIST ESCAPE FIX (Unchanged) ---
    if 'description' in df.columns:
        df['description'] = np.where(
            df['description'].str.startswith(('=', '+', '-')),
            "'" + df['description'],
            df['description']
        )
    
    # --- SLUG CREATION (Unchanged) ---
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

    # MAPPING for final requested titles (Unchanged)
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
    
    # 1. Filter existing columns and rename
    existing_cols = [col for col in column_map.keys() if col in df.columns]
    df_filtered = df[existing_cols].rename(columns=column_map)
    
    # 2. Add the remaining BLANK columns (Unchanged)
    blank_columns = [
        'Access', 'Salary', 'Deadline', 'Collection ID', 
        'Locale ID', 'Item ID', 'Archived', 'Draft', 'Created On', 
        'Updated On', 'Published On', 'CMS ID'
    ]
    
    for col_name in blank_columns:
        df_filtered[col_name] = '' 
    
    # Clean up highlight tags (Unchanged)
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
