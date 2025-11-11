import streamlit as st
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

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
    # Get cookies automatically
    session.get("https://www.lvmh.com/en/join-us/our-job-offers")
    return session

def fetch_jobs_page(session, regions, keyword=None, page=0):
    # regions: list of strings like ["America", "Europe"]
    facet_filters = [[f"geographicAreaFilter:{r}" for r in regions]]  # correct API format
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
            # estimate fraction of 2500 jobs for visual progress
            progress_bar.progress(min(total_fetched / 2500, 1.0))
        time.sleep(0.5)
    return pd.DataFrame(all_jobs)  # keep original columns as-is

# ================== STREAMLIT UI ==================
st.title("LVMH Job Scraper")

# Inputs
keyword_input = st.text_input("Job Title / Keywords (leave blank for all)")
regions_input = st.multiselect("Select Regions", REGIONS_ALL, default=REGIONS_ALL)

if st.button("Fetch Jobs"):
    progress_bar = st.progress(0)
    with st.spinner("Scraping jobs... this may take a few minutes..."):
        try:
            df = scrape_jobs(keyword_input.strip() if keyword_input else None, regions_input, progress_bar)
            progress_bar.empty()  # remove progress bar when done
            if not df.empty:
                st.success(f"Found {len(df)} jobs!")
                st.dataframe(df)
                # Excel download
                df.to_excel("lvmh_jobs.xlsx", index=False)
                with open("lvmh_jobs.xlsx", "rb") as f:
                    st.download_button("Download Excel", data=f, file_name="lvmh_jobs.xlsx")
            else:
                st.warning("No jobs found.")
        except Exception as e:
            st.error(f"Error: {e}")
