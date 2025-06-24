# 🔍 Keyword Ad Analysis Tool

A powerful web application for analyzing keyword advertising data using the Qwant API. Built with Streamlit for easy deployment and sharing.

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

## 🚀 Quick Start

### Option 1: Use the Web App (Recommended)
1. Visit the deployed app: [Your Streamlit Cloud URL]
2. Upload your JSON file
3. Configure settings
4. Start analysis
5. Download results

### Option 2: Run Locally

```bash
# Clone the repository
git clone https://github.com/yourusername/keyword-ad-analysis-tool.git
cd keyword-ad-analysis-tool

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run streamlit_app.py
```

## 📁 Input Format

Your JSON file should follow this structure:

```json
{
  "country-code": "FR",
  "form-factor": "desktop",
  "search-terms": {
    "main search term": [
      "keyword variation 1",
      "keyword variation 2",
      "keyword variation 3"
    ],
    "another search term": [
      "more variations"
    ]
  }
}
```

### Example:
```json
{
  "country-code": "FR",
  "form-factor": "desktop",
  "search-terms": {
    "chaussures tendance pour 2025": [
      "chaussures t",
      "chaussures te",
      "chaussures ten",
      "chaussures tendance",
      "chaussures tendance pour 2025"
    ]
  }
}
```

## ⚙️ Configuration

### Performance Settings
- **Max Concurrent Requests**: 5-20 (default: 10)
- **Request Delay**: 0.05-0.5 seconds (default: 0.1)
- **Request Timeout**: 15-60 seconds (default: 30)

### API Settings
- **Country Code**: FR, UK, US, DE, IT, ES
- **Form Factor**: desktop, mobile, tablet

## 📊 Output

### Summary CSV
- `data_item`: Which configuration item
- `main_term`: Original search term
- `qt`: Keyword variation
- `advertisers`: Comma-separated advertiser names
- `ad_count`: Number of ads found

### Detailed JSON
- Complete advertiser information
- Relevance scores
- Structured data for further analysis

### Excel Export
- **Summary Sheet**: Overview of all results
- **Detailed Sheet**: Individual advertiser data with scores

## 🛠️ Technical Details

- **Backend**: Python with Streamlit
- **HTTP Client**: Requests with connection pooling
- **Concurrency**: ThreadPoolExecutor for parallel processing
- **Visualization**: Plotly for interactive charts
- **Data Processing**: Pandas for data manipulation

## 🔧 Development

### Project Structure
```
keyword-ad-analysis-tool/
├── streamlit_app.py      # Main application
├── requirements.txt      # Python dependencies
├── README.md            # This file
├── input_search_terms.json  # Example input file
└── .gitignore           # Git ignore file
```

### Adding Features
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally
5. Submit a pull request

## 🌐 Deployment

### Streamlit Cloud (Recommended)
1. Push code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repository
4. Deploy with one click

### Other Platforms
- **Heroku**: Use the Procfile and requirements.txt
- **Railway**: Direct GitHub integration
- **DigitalOcean**: App Platform deployment

## 🤝 Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

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
