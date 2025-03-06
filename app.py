import streamlit as st
import pandas as pd
import plotly.express as px
from src.scraper import BTCWalletScraper
from src.database import Database
from src.scheduler import DataCollectionScheduler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize components
db = Database()
scraper = BTCWalletScraper()
scheduler = DataCollectionScheduler()

def format_balance(balance):
    """Format BTC balance with proper notation"""
    if balance >= 1_000_000:
        return f"{balance/1_000_000:.2f}M BTC"
    elif balance >= 1_000:
        return f"{balance/1_000:.2f}K BTC"
    else:
        return f"{balance:.2f} BTC"

def main():
    st.set_page_config(
        page_title="Bitcoin Wallet Tracker",
        page_icon="💰",
        layout="wide"
    )

    st.title("Bitcoin Wallet Tracker 💰")

    # Sidebar configuration
    st.sidebar.header("Configuration")
    pages_to_scan = st.sidebar.slider(
        "Number of pages to scan",
        min_value=1,
        max_value=50,
        value=20
    )

    # Manual refresh button
    if st.sidebar.button("Refresh Data"):
        with st.spinner("Fetching latest wallet data..."):
            try:
                wallets = scraper.scrape_wallets(pages_to_scan)
                db.store_wallets(wallets)
                st.success("Data updated successfully!")
            except Exception as e:
                st.error(f"Error updating data: {str(e)}")

    # Main content
    try:
        df = db.get_latest_wallets()
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Wallets", len(df))
        with col2:
            total_btc = df['balance'].sum()
            st.metric("Total BTC", format_balance(total_btc))
        with col3:
            avg_btc = df['balance'].mean()
            st.metric("Average BTC per Wallet", format_balance(avg_btc))

        # Search and filter
        search_term = st.text_input("Search wallet address")
        if search_term:
            df = df[df['address'].str.contains(search_term, case=False)]

        # Display data table
        st.subheader("Wallet Data")
        st.dataframe(
            df.style.format({
                'balance': lambda x: format_balance(x)
            })
        )

        # Historical data visualization
        st.subheader("Historical Data")
        selected_wallet = st.selectbox(
            "Select wallet for historical view",
            df['address'].tolist()
        )

        if selected_wallet:
            historical_data = db.get_historical_data(selected_wallet)
            if not historical_data.empty:
                fig = px.line(
                    historical_data,
                    x='timestamp',
                    y='balance',
                    title=f"Balance History for {selected_wallet}"
                )
                st.plotly_chart(fig)

        # Export functionality
        st.subheader("Export Data")
        if st.button("Export to CSV"):
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="btc_wallets.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

if __name__ == "__main__":
    main()
    # Start the scheduler
    scheduler.start(pages_to_scan=20)
