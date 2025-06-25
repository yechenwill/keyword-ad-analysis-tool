# 🔍 Keyword Ad Analysis Tool

A powerful web application for analyzing keyword advertising data using the Qwant API. Built with Streamlit for easy deployment and sharing. This tool requires VPN access to connect to private company networks.

## 🌐 Live Demo

**Try the app now**: [https://keyword-ad-analysis-tool-yechenwill.streamlit.app](https://keyword-ad-analysis-tool-yechenwill.streamlit.app)

## 📁 Repository

**GitHub**: [https://github.com/yechenwill/keyword-ad-analysis-tool](https://github.com/yechenwill/keyword-ad-analysis-tool)

## ✨ Features

- **📁 File Upload**: Drag & drop JSON files with search terms
- **⚙️ Configurable Settings**: Adjust performance, country, and form factor
- **🚀 Concurrent Processing**: Fast analysis with multiple concurrent requests
- **📊 Real-time Progress**: Live progress tracking and status updates
- **📈 Interactive Charts**: Visualize advertiser distribution and keyword performance
- **💾 Multiple Export Formats**: CSV, JSON, and Excel downloads
- **🌐 Web Interface**: No installation required for end users

## 🎯 Use Cases

- **SEO Analysis**: Understand which advertisers are targeting specific keywords
- **Competitive Research**: Analyze competitor advertising strategies
- **Keyword Research**: Discover keyword variations and their advertising potential
- **Market Analysis**: Track advertising trends across different markets

## 🔒 VPN Requirements

**IMPORTANT**: This tool requires a VPN connection to access the private API server. Before using the tool, ensure you are connected to your company VPN.

### VPN Connection Steps

1. **Connect to Company VPN**
   - Use your company's VPN client
   - Enter your VPN credentials if required
   - Ensure the connection is established

2. **Verify VPN Connection**
   - Run the VPN test script: `python test_vpn_connection.py`
   - Check that all tests pass before using the main app

3. **Troubleshooting VPN Issues**
   - Contact your IT department for VPN credentials
   - Ensure your VPN client is up to date
   - Try disconnecting and reconnecting to VPN
   - Check if your VPN allows access to internal APIs

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Company VPN access
- VPN credentials (if required)

### Installation

1. **Clone or download the repository**
   ```bash
   git clone <repository-url>
   cd keyword-ad-analysis-tool
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Test VPN connection**
   ```bash
   python test_vpn_connection.py
   ```

4. **Run the application**
   ```bash
   streamlit run streamlit_app.py
   ```

## 📋 Usage

### 1. VPN Connection Check

The app automatically checks your VPN connection status when it loads. You'll see:
- ✅ **Green status**: VPN is working correctly
- ❌ **Red status**: VPN connection issues detected

### 2. Prepare Your Data

Create a JSON file with your search terms in this format:

```json
{
  "country-code": "FR",
  "form-factor": "desktop",
  "search-terms": {
    "laptops": [
      "gaming laptop",
      "business laptop",
      "student laptop",
      "cheap laptop"
    ],
    "phones": [
      "smartphone",
      "mobile phone",
      "iphone",
      "android phone"
    ]
  }
}
```

### 3. Run Analysis

1. Upload your JSON file
2. Configure settings in the sidebar
3. Click "Start Analysis"
4. View results and download exports

## 🔧 Configuration

### Performance Settings

- **Max Concurrent Requests**: Number of simultaneous API calls (5-20)
- **Request Delay**: Delay between requests to avoid rate limiting (0.05-0.5s)
- **Request Timeout**: Maximum time to wait for API responses (15-60s)

### API Settings

- **Country Code**: Target country for analysis (FR, UK, US, DE, IT, ES)
- **Form Factor**: Device type (desktop, mobile, tablet)

## 📊 Output

The tool provides:

- **Summary Table**: Overview of keywords and advertisers
- **Charts**: Visualizations of advertiser distribution and ad counts
- **Export Options**: CSV, JSON, and Excel downloads
- **Detailed Results**: Comprehensive data for further analysis

## 🛠️ Troubleshooting

### VPN Connection Issues

**Problem**: "Cannot reach the API server" error
**Solution**:
1. Ensure you're connected to company VPN
2. Run `python test_vpn_connection.py` to diagnose
3. Contact IT if VPN credentials are needed
4. Try refreshing the page after connecting to VPN

**Problem**: "Connection failed" errors during analysis
**Solution**:
1. Check VPN connection is still active
2. Reduce concurrent requests in settings
3. Increase request timeout
4. Try again with fewer keywords

### API Issues

**Problem**: "API returned status code: 403/401"
**Solution**:
1. Verify VPN connection
2. Check if API access is granted to your account
3. Contact system administrator

**Problem**: "Request timeout" errors
**Solution**:
1. Check VPN connection stability
2. Increase timeout setting
3. Reduce concurrent requests
4. Try during off-peak hours

### Performance Issues

**Problem**: Analysis is very slow
**Solution**:
1. Increase concurrent requests (if VPN allows)
2. Reduce request delay
3. Process fewer keywords at once
4. Check VPN connection speed

## 📁 File Structure

```
keyword-ad-analysis-tool/
├── streamlit_app.py          # Main application
├── test_vpn_connection.py    # VPN connectivity test
├── requirements.txt          # Python dependencies
├── README.md                # This file
└── LICENSE                  # License information
```

## 🔍 VPN Test Script

The `test_vpn_connection.py` script performs three tests:

1. **Basic Connectivity**: Tests if the API server is reachable
2. **API Endpoint**: Tests if the API responds correctly
3. **Sample Query**: Tests if the API returns expected data

Run it before using the main app:
```bash
python test_vpn_connection.py
```

## 📞 Support

If you encounter issues:

1. **VPN Issues**: Contact your IT department
2. **API Issues**: Contact the system administrator
3. **Tool Issues**: Check this README and troubleshoot

## 🔐 Security Notes

- This tool accesses internal APIs - keep VPN credentials secure
- Don't share VPN credentials or API access
- Follow company security policies
- Log out of VPN when not in use

## 📄 License

[Add your license information here]

## 🆘 Support

- **Issues**: Create an issue on GitHub
- **Questions**: Use GitHub Discussions
- **Email**: [Your email]

## 🙏 Acknowledgments

- Built with [Streamlit](https://streamlit.io/)
- Data visualization with [Plotly](https://plotly.com/)
- HTTP requests with [Requests](https://requests.readthedocs.io/)

---

**Made with ❤️ for the digital marketing community** # keyword-ad-analysis-tool
