# Bitcoin Wallet Tracker

A Streamlit application that scrapes, stores, and visualizes data from the top Bitcoin wallets. The app tracks historical wallet data and identifies patterns in wallet balances.

## Features

- 🔍 Scrapes wallet data from BitInfoCharts
- 📊 Displays duplicate balance patterns
- 📈 Shows historical wallet data
- 🔄 Daily automatic data collection
- 📱 Responsive web interface

## Requirements

- Python 3.11+
- Required packages (installed automatically):
  - streamlit
  - pandas
  - beautifulsoup4
  - requests
  - plotly
  - apscheduler

## Running the Application

1. Install dependencies:
```bash
# Install required packages
pip install streamlit pandas beautifulsoup4 requests plotly apscheduler
```

2. Run the application:
```bash
streamlit run app.py
```

The application will be available at `http://localhost:5000`

## Configuration

- **Pages to Scan**: Adjust the number of pages to scan (1-50) using the sidebar slider
- **Data Collection**: Data is automatically collected daily at midnight
- **Manual Refresh**: Use the "Refresh Data" button in the sidebar to trigger immediate data collection

## Data Views

1. **Duplicate Balance Analysis**
   - Shows wallets with identical BTC balances
   - Groups similar wallets together

2. **Historical Tracking**
   - Track balance changes over time
   - View last in/out transaction dates

## Export Options

- Export complete wallet data to CSV
- Export duplicate wallet groups to CSV

## Project Structure

```
├── src/
│   ├── scraper.py      # Web scraping functionality
│   ├── database.py     # SQLite database operations
│   └── scheduler.py    # Automated data collection
├── app.py              # Main Streamlit application
└── .streamlit/
    └── config.toml     # Streamlit configuration
```
