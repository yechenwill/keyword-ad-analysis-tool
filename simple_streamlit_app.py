#!/usr/bin/env python3
"""
Simplified Keyword Ad Analysis Tool
A streamlined version that focuses on core functionality
"""

import streamlit as st
import json
import requests
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import io
import base64
import socket

# Page configuration
st.set_page_config(
    page_title="Keyword Ad Analysis Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .quick-start {
        background-color: #f3e5f5;
        border: 1px solid #9c27b0;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .vpn-info {
        background-color: #e3f2fd;
        border: 1px solid #2196f3;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .vpn-error {
        background-color: #ffebee;
        border: 1px solid #f44336;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .vpn-success {
        background-color: #e8f5e8;
        border: 1px solid #4caf50;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# API endpoint template
API_URL_TEMPLATE = (
    'http://prod-ssp-engine-private.ric1.admarketplace.net/isp'
    '?plid=cjqduwisj4&results-ta=100&qt={}&country-code={}'
    '&region-code=&form-factor={}&os-family=windows'
    '&v=2.0&out=json&diag=enabled&ctaid='
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json'
}

def check_vpn_connectivity():
    """Check if VPN connection is available by testing connectivity to the API server"""
    try:
        host = "prod-ssp-engine-private.ric1.admarketplace.net"
        port = 80
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            return True, "✅ VPN connection appears to be working"
        else:
            return False, f"❌ Cannot reach the API server (error code: {result}) - VPN connection may be required"
            
    except Exception as e:
        return False, f"❌ Connection test failed: {str(e)}"

def test_api_endpoint():
    """Test the actual API endpoint to verify it's accessible"""
    try:
        test_url = API_URL_TEMPLATE.format("test", "US", "desktop")
        session = requests.Session()
        response = session.get(test_url, headers=HEADERS, timeout=15)
        
        if response.status_code == 200:
            return True, "✅ API endpoint is accessible"
        else:
            return False, f"❌ API returned status code: {response.status_code}"
            
    except requests.exceptions.ConnectionError as e:
        return False, f"❌ Connection failed - Please check your VPN connection (Error: {str(e)})"
    except requests.exceptions.Timeout:
        return False, "❌ Request timeout - Please check your VPN connection"
    except Exception as e:
        return False, f"❌ API test failed: {str(e)}"

def create_session():
    """Create a requests session with connection pooling and retry logic"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def fetch_ads(keyword, country_code, form_factor, session):
    """Fetch ads for a single keyword"""
    url = API_URL_TEMPLATE.format(keyword, country_code, form_factor)
    try:
        response = session.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('text_ads', [])
    except requests.exceptions.ConnectionError:
        st.error(f'❌ Connection failed for "{keyword}" - Please ensure you are connected to the required VPN')
        return []
    except requests.exceptions.Timeout:
        st.error(f'⏰ Request timeout for "{keyword}" - Please check your VPN connection')
        return []
    except Exception as e:
        st.error(f'Failed for "{keyword}": {e}')
        return []

def process_keyword_batch(keyword_batch, country_code, form_factor, session, progress_bar, status_text):
    """Process a batch of keywords concurrently"""
    results = {}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_keyword = {
            executor.submit(fetch_ads, keyword, country_code, form_factor, session): keyword
            for keyword in keyword_batch
        }
        
        for i, future in enumerate(as_completed(future_to_keyword)):
            keyword = future_to_keyword[future]
            try:
                ads = future.result()
                results[keyword] = ads
                status_text.text(f"✅ Completed: {keyword} ({len(ads)} ads)")
            except Exception as e:
                st.error(f"Error processing {keyword}: {e}")
                results[keyword] = []
            
            # Update progress
            progress = (i + 1) / len(keyword_batch)
            progress_bar.progress(progress)
            time.sleep(0.1)  # Small delay to be respectful to API
    
    return results

def main():
    # Header
    st.markdown('<h1 class="main-header">🔍 Keyword Ad Analysis Tool</h1>', unsafe_allow_html=True)
    
    # Quick Start Guide
    st.markdown("""
    <div class="quick-start">
    <h4>🚀 Quick Start Guide</h4>
    <ol>
        <li><strong>Connect to VPN:</strong> Ensure you're connected to your company VPN</li>
        <li><strong>Test Connection:</strong> Run <code>python3 simple_vpn_check.py</code> in terminal</li>
        <li><strong>Upload JSON:</strong> Upload your keyword data file</li>
        <li><strong>Start Analysis:</strong> Click the analysis button</li>
    </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # VPN Connectivity Check
    st.subheader("🔒 VPN Connection Status")
    
    # Add manual refresh button
    col1, col2 = st.columns([3, 1])
    with col1:
        st.write("Click the button to manually check VPN status:")
    with col2:
        if st.button("🔄 Refresh VPN Status", type="secondary"):
            st.rerun()
    
    # Check VPN connectivity
    with st.spinner("Checking VPN connection..."):
        vpn_status, vpn_message = check_vpn_connectivity()
    
    if vpn_status:
        st.markdown(f"""
        <div class="vpn-success">
        <h4>🔒 VPN Status</h4>
        <p>{vpn_message}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Test API endpoint
        with st.spinner("Testing API endpoint..."):
            api_status, api_message = test_api_endpoint()
        
        if api_status:
            st.markdown(f"""
            <div class="vpn-success">
            <h4>🌐 API Status</h4>
            <p>{api_message}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="vpn-error">
            <h4>🌐 API Status</h4>
            <p>{api_message}</p>
            <p><strong>Please check your VPN connection and try again.</strong></p>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="vpn-error">
        <h4>🔒 VPN Status</h4>
        <p>{vpn_message}</p>
        <p><strong>Before using this tool, please ensure you are connected to your company VPN.</strong></p>
        <ol>
            <li>Connect to your company VPN</li>
            <li>Click "Refresh VPN Status" above</li>
            <li>Try the analysis again</li>
        </ol>
        </div>
        """, unsafe_allow_html=True)
        
        # Add troubleshooting section
        with st.expander("🔧 VPN Troubleshooting"):
            st.markdown("""
            **If you're connected to VPN but still see this error:**
            
            1. **Run the simple test script** in terminal:
               ```bash
               python3 simple_vpn_check.py
               ```
            
            2. **Check VPN split tunneling** - ensure all traffic goes through VPN
            
            3. **Try manual connectivity test**:
               ```bash
               ping prod-ssp-engine-private.ric1.admarketplace.net
               ```
            
            4. **Contact IT** if the test script works but app doesn't
            
            **Common issues:**
            - VPN client needs restart
            - Firewall blocking connection
            - DNS resolution issues
            - VPN session expired
            """)
    
    # VPN Information Section
    st.markdown("""
    <div class="vpn-info">
    <h4>🔒 VPN Connection Required</h4>
    <p><strong>Before using this tool, please ensure you are connected to your company VPN.</strong></p>
    <p>This tool accesses a private API server that requires VPN access. If you encounter connection errors, please:</p>
    <ol>
        <li>Connect to your company VPN</li>
        <li>Refresh this page</li>
        <li>Try the analysis again</li>
    </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuration")
    
    # Performance settings
    st.sidebar.subheader("Performance Settings")
    max_workers = st.sidebar.slider("Max Concurrent Requests", 5, 20, 10)
    request_delay = st.sidebar.slider("Request Delay (seconds)", 0.05, 0.5, 0.1, 0.05)
    timeout = st.sidebar.slider("Request Timeout (seconds)", 15, 60, 30)
    
    # API settings
    st.sidebar.subheader("API Settings")
    country_code = st.sidebar.selectbox("Country Code", ["FR", "UK", "US", "DE", "IT", "ES"])
    form_factor = st.sidebar.selectbox("Form Factor", ["desktop", "mobile", "tablet"])
    
    # File upload
    st.header("📁 Upload JSON File")
    uploaded_file = st.file_uploader(
        "Choose a JSON file with search terms",
        type=['json'],
        help="Upload a JSON file with the structure: {'country-code': 'FR', 'form-factor': 'desktop', 'search-terms': {...}}"
    )
    
    if uploaded_file is not None:
        try:
            # Load and validate JSON
            input_data = json.load(uploaded_file)
            
            # Handle both single object and array
            if isinstance(input_data, list):
                data_items = input_data
            else:
                data_items = [input_data]
            
            # Show file preview
            st.success(f"✅ File loaded successfully! Found {len(data_items)} data item(s)")
            
            # Display configuration summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Data Items", len(data_items))
            with col2:
                total_keywords = sum(
                    len([k for k in variations if len(k.strip()) >= 3])
                    for item in data_items
                    for variations in item.get('search-terms', {}).values()
                )
                st.metric("Total Keywords", total_keywords)
            with col3:
                estimated_time = total_keywords * request_delay / max_workers
                st.metric("Est. Time", f"{estimated_time:.1f}s")
            
            # Show data preview
            with st.expander("📋 Data Preview"):
                for i, item in enumerate(data_items):
                    st.write(f"**Data Item {i+1}:**")
                    st.json(item)
            
            # Process button - only enable if VPN is working
            if vpn_status and api_status:
                if st.button("🚀 Start Analysis", type="primary"):
                    # Initialize session
                    session = create_session()
                    
                    # Progress tracking
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    summary_rows = []
                    detailed_dict = {}
                    processed_keywords = 0
                    
                    # Count total keywords
                    total_keywords = sum(
                        len([k for k in variations if len(k.strip()) >= 3])
                        for item in data_items
                        for variations in item.get('search-terms', {}).values()
                    )
                    
                    # Process each data item
                    for item_index, input_data in enumerate(data_items):
                        st.subheader(f"📋 Processing Data Item {item_index + 1}/{len(data_items)}")
                        
                        item_country = input_data.get('country-code', country_code)
                        item_form_factor = input_data.get('form-factor', form_factor)
                        search_terms = input_data.get('search-terms', {})
                        
                        if not search_terms:
                            st.warning("No search terms found in this item, skipping...")
                            continue
                        
                        # Process each search term
                        for main_term, keyword_variations in search_terms.items():
                            st.write(f"🔍 **{main_term}** ({len(keyword_variations)} variations)")
                            
                            # Filter valid keywords
                            valid_keywords = [k.strip() for k in keyword_variations if len(k.strip()) >= 3]
                            
                            if not valid_keywords:
                                st.warning("No valid keywords found, skipping...")
                                continue
                            
                            # Process in batches
                            batch_size = 20
                            for i in range(0, len(valid_keywords), batch_size):
                                batch = valid_keywords[i:i + batch_size]
                                st.write(f"📦 Processing batch {i//batch_size + 1}/{(len(valid_keywords) + batch_size - 1)//batch_size}")
                                
                                # Process batch
                                batch_results = process_keyword_batch(
                                    batch, item_country, item_form_factor, session, 
                                    progress_bar, status_text
                                )
                                
                                # Process results
                                for keyword, ads in batch_results.items():
                                    advertiser_names = []
                                    details = []
                                    
                                    for ad in ads:
                                        name = ad.get('adv_name', '').strip()
                                        score = ad.get('keywordMatchingResult', {}).get('relevanceScore', '')
                                        if name:
                                            advertiser_names.append(name)
                                            details.append({
                                                "advertiser_name": name,
                                                "relevance_score": score
                                            })
                                    
                                    summary_rows.append({
                                        "data_item": item_index + 1,
                                        "main_term": main_term,
                                        "qt": keyword,
                                        "advertisers": ",".join(advertiser_names),
                                        "ad_count": len(ads)
                                    })
                                    
                                    detailed_dict[keyword] = {
                                        "data_item": item_index + 1,
                                        "main_term": main_term,
                                        "details": details
                                    }
                                    
                                    processed_keywords += 1
                                    
                                    # Update overall progress
                                    overall_progress = processed_keywords / total_keywords
                                    progress_bar.progress(overall_progress)
                                    status_text.text(f"📊 Progress: {processed_keywords}/{total_keywords} ({overall_progress*100:.1f}%)")
                    
                    session.close()
                    
                    # Results section
                    st.success("✅ Analysis completed!")
                    
                    # Create DataFrame
                    df = pd.DataFrame(summary_rows)
                    
                    # Display results
                    st.header("📊 Results")
                    
                    # Metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Keywords", len(df))
                    with col2:
                        st.metric("Keywords with Ads", len(df[df['ad_count'] > 0]))
                    with col3:
                        st.metric("Total Ads Found", df['ad_count'].sum())
                    with col4:
                        st.metric("Unique Advertisers", len(set(
                            advertiser for advertisers in df['advertisers'] 
                            if advertisers for advertiser in advertisers.split(',')
                        )))
                    
                    # Results table
                    st.subheader("📋 Results Table")
                    st.dataframe(df, use_container_width=True)
                    
                    # Charts
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Advertiser distribution
                        if not df.empty:
                            advertiser_counts = {}
                            for advertisers in df['advertisers']:
                                if advertisers:
                                    for advertiser in advertisers.split(','):
                                        advertiser = advertiser.strip()
                                        advertiser_counts[advertiser] = advertiser_counts.get(advertiser, 0) + 1
                            
                            if advertiser_counts:
                                advertiser_df = pd.DataFrame(list(advertiser_counts.items()), 
                                                           columns=['Advertiser', 'Count'])
                                advertiser_df = advertiser_df.sort_values('Count', ascending=False).head(10)
                                
                                fig = px.bar(advertiser_df, x='Advertiser', y='Count', 
                                            title="Top 10 Advertisers")
                                st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        # Ads per keyword distribution
                        if not df.empty:
                            fig = px.histogram(df, x='ad_count', nbins=20, 
                                             title="Distribution of Ads per Keyword")
                            st.plotly_chart(fig, use_container_width=True)
                    
                    # Export options
                    st.header("💾 Export Results")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        # CSV export
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📄 Download CSV",
                            data=csv,
                            file_name="keyword_analysis_results.csv",
                            mime="text/csv"
                        )
                    
                    with col2:
                        # JSON export
                        json_str = json.dumps(detailed_dict, ensure_ascii=False, indent=2)
                        st.download_button(
                            label="📄 Download JSON",
                            data=json_str,
                            file_name="keyword_analysis_detailed.json",
                            mime="application/json"
                        )
                    
                    with col3:
                        # Excel export
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df.to_excel(writer, sheet_name='Summary', index=False)
                            
                            # Create detailed sheet
                            detailed_rows = []
                            for keyword, data in detailed_dict.items():
                                for detail in data['details']:
                                    detailed_rows.append({
                                        'keyword': keyword,
                                        'main_term': data['main_term'],
                                        'data_item': data['data_item'],
                                        'advertiser': detail['advertiser_name'],
                                        'relevance_score': detail['relevance_score']
                                    })
                            
                            if detailed_rows:
                                detailed_df = pd.DataFrame(detailed_rows)
                                detailed_df.to_excel(writer, sheet_name='Detailed', index=False)
                        
                        excel_data = output.getvalue()
                        st.download_button(
                            label="📄 Download Excel",
                            data=excel_data,
                            file_name="keyword_analysis_results.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
            else:
                st.warning("⚠️ Please ensure VPN connection is working before starting analysis")
                
        except json.JSONDecodeError as e:
            st.error(f"❌ Invalid JSON file: {e}")
        except Exception as e:
            st.error(f"❌ Error processing file: {e}")
    
    # Instructions
    with st.sidebar:
        st.header("📖 Instructions")
        st.markdown("""
        1. **Connect to VPN** - Ensure you're connected to your company VPN
        2. **Test Connection** - Run `python3 simple_vpn_check.py` in terminal
        3. **Upload JSON file** with search terms
        4. **Configure settings** in sidebar
        5. **Click Start Analysis** to begin
        6. **View results** and download exports
        
        **JSON Format:**
        ```json
        {
          "country-code": "FR",
          "form-factor": "desktop", 
          "search-terms": {
            "main term": ["keyword1", "keyword2"]
          }
        }
        ```
        
        **VPN Requirements:**
        - Must be connected to company VPN
        - Contact IT if you need VPN credentials
        - Refresh page after connecting to VPN
        """)

if __name__ == "__main__":
    main() 