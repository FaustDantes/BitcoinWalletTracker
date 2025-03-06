import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

    # Tab selection
    tab1, tab2, tab3 = st.tabs(["Wallet Analysis", "Transaction Statistics", "Historical Data"])

    try:
        df = db.get_latest_wallets()

        with tab1:
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

            # Duplicate Balance Wallets
            st.header("Wallets with Duplicate Balances")
            duplicate_wallets = db.get_duplicate_balance_wallets()

            if not duplicate_wallets.empty:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Duplicate Groups", 
                             len(duplicate_wallets['balance'].unique()))
                with col2:
                    st.metric("Total Wallets with Duplicates", 
                             len(duplicate_wallets))

                st.dataframe(
                    duplicate_wallets.style.format({
                        'balance': lambda x: format_balance(x)
                    })
                )
            else:
                st.info("No wallets with duplicate balances found")

            # Balance Groups
            st.header("Balance Groups")
            balance_groups = db.get_balance_groups()

            if not balance_groups.empty:
                balance_groups['group_label'] = balance_groups['group_balance'].apply(format_balance)
                balance_groups['last_activity'] = balance_groups.apply(
                    lambda x: f"In: {x['last_in_dates']}\nOut: {x['last_out_dates']}", 
                    axis=1
                )

                st.dataframe(
                    balance_groups[[
                        'group_label', 
                        'wallet_count', 
                        'last_activity'
                    ]].rename(columns={
                        'group_label': 'Balance Group',
                        'wallet_count': 'Number of Wallets',
                        'last_activity': 'Last Activity Dates'
                    })
                )
            else:
                st.info("No balance groups found")

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

        with tab2:
            st.header("Transaction Statistics")

            # Get market signal
            signal_data = db.get_market_signal()

            # Display trading signal
            signal_col1, signal_col2 = st.columns(2)
            with signal_col1:
                signal_color = {
                    "BUY": "green",
                    "SELL": "red",
                    "NEUTRAL": "orange",
                    "ERROR": "gray"
                }[signal_data["signal"]]

                st.markdown(f"""
                    <h2 style='text-align: center; color: {signal_color};'>
                        {signal_data["signal"]}
                    </h2>
                    <p style='text-align: center;'>
                        Confidence: {signal_data["confidence"]:.1%}
                    </p>
                    """, 
                    unsafe_allow_html=True
                )

            with signal_col2:
                st.markdown(f"""
                    ### Signal Analysis
                    {signal_data["reason"]}

                    **Recent Metrics:**
                    - Transaction Trend: {signal_data["metrics"]["tx_trend"]:.2f}
                    - Volume Trend: {signal_data["metrics"]["volume_trend"]:.2f}
                    """)

            # Transaction statistics chart
            st.subheader("Daily Transaction Activity")
            stats_df = db.get_daily_transaction_stats()

            if not stats_df.empty:
                # Create figure with secondary y-axis
                fig = go.Figure()

                # Add traces
                fig.add_trace(
                    go.Bar(
                        x=stats_df['activity_date'],
                        y=stats_df['net_transactions'],
                        name="Net Transactions",
                        marker_color='lightblue'
                    )
                )

                fig.add_trace(
                    go.Scatter(
                        x=stats_df['activity_date'],
                        y=stats_df['net_volume'],
                        name="Net Volume (BTC)",
                        yaxis="y2",
                        line=dict(color='darkblue')
                    )
                )

                # Update layout
                fig.update_layout(
                    title="Daily Transaction Activity and Volume",
                    xaxis_title="Date",
                    yaxis_title="Number of Transactions",
                    yaxis2=dict(
                        title="Net Volume (BTC)",
                        overlaying="y",
                        side="right"
                    ),
                    hovermode='x unified'
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No transaction statistics available")

        with tab3:
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
            if st.button("Export Duplicate Wallets to CSV"):
                csv = duplicate_wallets.to_csv(index=False)
                st.download_button(
                    label="Download Duplicate Wallets CSV",
                    data=csv,
                    file_name="btc_duplicate_wallets.csv",
                    mime="text/csv"
                )

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        logger.error(f"Application error: {str(e)}")

if __name__ == "__main__":
    main()
    scheduler.start(20)  # Start with default 20 pages