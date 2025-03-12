import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.scraper import BTCWalletScraper
from src.database import Database
from src.scheduler import DataCollectionScheduler
import logging
import re
from src.logger_config import setup_logger

# Configure logging
logger = setup_logger()

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

def extract_btc_address(text):
    """Extract clean Bitcoin address from text"""
    clean_text = re.sub(r'<[^>]+>', '', text).strip()
    btc_pattern = r'[13][a-km-zA-HJ-NP-Z1-9]{25,34}'
    match = re.search(btc_pattern, clean_text)
    return match.group(0) if match else clean_text

def main():
    # Custom CSS for grayscale theme with green accents
    st.markdown("""
        <style>
        .stApp {
            background-color: #1E1E1E;
            color: #E0E0E0;
        }
        .stMetric {
            background: #2D2D2D;
            border-radius: 4px;
            padding: 12px;
            border: 1px solid #404040;
        }
        .stDataFrame {
            background: #2D2D2D;
            border-radius: 4px;
        }
        .stSelectbox, .stTextInput, .stSlider {
            background: #2D2D2D;
            border-radius: 4px;
            border: 1px solid #404040;
        }
        h1, h2, h3 {
            color: #00FF00;
        }
        .stButton button {
            background-color: #2D2D2D;
            color: #00FF00;
            border: 1px solid #00FF00;
        }
        .stButton button:hover {
            background-color: #00FF00;
            color: #1E1E1E;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            background-color: #2D2D2D;
            padding: 8px;
            border-radius: 4px;
        }
        .stTabs [data-baseweb="tab"] {
            background-color: #404040;
            color: #E0E0E0;
            border-radius: 4px;
        }
        .stTabs [aria-selected="true"] {
            background-color: #00FF00 !important;
            color: #1E1E1E !important;
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("Bitcoin Whale Tracker")
    st.markdown("### Monitor Large Bitcoin Wallets")

    # Sidebar configuration
    with st.sidebar:
        st.markdown("""
            <div style='background: #2D2D2D; padding: 20px; border-radius: 4px; border: 1px solid #404040;'>
            <h3 style='color: #00FF00; margin-top: 0;'>Settings</h3>
            </div>
        """, unsafe_allow_html=True)

        pages_to_scan = st.slider(
            "Pages to scan",
            min_value=1,
            max_value=50,
            value=20
        )

        if st.button("Refresh Data", use_container_width=True):
            with st.spinner("Fetching latest data..."):
                try:
                    wallets = scraper.scrape_wallets(pages_to_scan)
                    db.store_wallets(wallets)
                    st.success("Data updated successfully")
                except Exception as e:
                    st.error(f"Error updating data: {str(e)}")
                    logger.error(f"Data refresh error: {str(e)}")

    # Tab selection
    tab1, tab2, tab3, tab4 = st.tabs([
        "Wallet Analysis", 
        "Transaction Stats", 
        "Historical Data",
        "System Logs"
    ])

    try:
        df = db.get_latest_wallets()
        df['clean_address'] = df['address'].apply(extract_btc_address)

        with tab1:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Wallets", f"{len(df):,}")
            with col2:
                total_btc = df['balance'].sum()
                st.metric("Total BTC", format_balance(total_btc))
            with col3:
                avg_btc = df['balance'].mean()
                st.metric("Average BTC", format_balance(avg_btc))

            st.subheader("Wallet Groups Analysis")
            wallet_groups = db.get_wallet_groups()

            if not wallet_groups.empty:
                # Summary metrics
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Groups", 
                             len(wallet_groups))
                with col2:
                    st.metric("Total Grouped Wallets", 
                             wallet_groups['wallet_count'].sum())

                # Display groups
                st.dataframe(
                    wallet_groups.style.format({
                        'balance_value': lambda x: format_balance(x),
                        'total_group_balance': lambda x: format_balance(x),
                        'first_wallet_seen': lambda x: x.split('.')[0],
                        'last_wallet_seen': lambda x: x.split('.')[0],
                        'last_updated': lambda x: x.split('.')[0]
                    })
                )

                # Group details
                st.subheader("Group Details")
                selected_balance = st.selectbox(
                    "Select balance group",
                    wallet_groups['balance_value'].tolist(),
                    format_func=format_balance
                )

                if selected_balance:
                    group_wallets = db.get_wallets_in_group(selected_balance)
                    st.dataframe(
                        group_wallets.style.format({
                            'balance': lambda x: format_balance(x),
                            'first_seen': lambda x: x.split('.')[0],
                            'last_updated': lambda x: x.split('.')[0]
                        })
                    )
            else:
                st.info("No wallet groups found")

            # Main wallet list
            st.subheader("All Wallets")
            all_wallets = db.get_all_wallets()

            # Search functionality
            search_term = st.text_input("Search address")
            if search_term:
                all_wallets = all_wallets[
                    all_wallets['address'].str.contains(search_term, case=False)
                ]

            st.dataframe(
                all_wallets.style.format({
                    'balance': lambda x: format_balance(x),
                    'first_seen': lambda x: x.split('.')[0],
                    'last_updated': lambda x: x.split('.')[0]
                })
            )

        with tab2:
            st.subheader("Transaction Statistics")
            signal_data = db.get_market_signal()

            signal_col1, signal_col2 = st.columns(2)
            with signal_col1:
                signal_color = {
                    "BUY": "#00FF00",
                    "SELL": "#FF0000",
                    "NEUTRAL": "#808080",
                    "ERROR": "#404040"
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
                    ### Analysis
                    {signal_data["reason"]}

                    **Metrics:**
                    - Transaction Trend: {signal_data["metrics"]["tx_trend"]:.2f}
                    - Volume Trend: {signal_data["metrics"]["volume_trend"]:.2f}
                    """)

            stats_df = db.get_daily_transaction_stats()
            if not stats_df.empty:
                fig = go.Figure()
                fig.add_trace(
                    go.Bar(
                        x=stats_df['activity_date'],
                        y=stats_df['net_transactions'],
                        name="Net Transactions",
                        marker_color='#404040'
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=stats_df['activity_date'],
                        y=stats_df['net_volume'],
                        name="Net Volume (BTC)",
                        yaxis="y2",
                        line=dict(color='#00FF00')
                    )
                )
                fig.update_layout(
                    plot_bgcolor='#2D2D2D',
                    paper_bgcolor='#2D2D2D',
                    font=dict(color='#E0E0E0'),
                    title="Daily Activity and Volume",
                    xaxis_title="Date",
                    yaxis_title="Transactions",
                    yaxis2=dict(
                        title="Volume (BTC)",
                        overlaying="y",
                        side="right"
                    ),
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)

        with tab3:
            st.subheader("Historical Analysis")
            selected_wallet = st.selectbox(
                "Select wallet",
                df['clean_address'].tolist()
            )

            if selected_wallet:
                historical_data = db.get_historical_data(selected_wallet)
                if not historical_data.empty:
                    fig = px.line(
                        historical_data,
                        x='timestamp',
                        y='balance',
                        title=f"Balance History: {selected_wallet}"
                    )
                    fig.update_layout(
                        plot_bgcolor='#2D2D2D',
                        paper_bgcolor='#2D2D2D',
                        font=dict(color='#E0E0E0')
                    )
                    st.plotly_chart(fig)

            st.subheader("Export Options")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Export All Data"):
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "Download CSV",
                        data=csv,
                        file_name="btc_wallets.csv",
                        mime="text/csv"
                    )
            with col2:
                if st.button("Export Duplicates"):
                    csv = duplicate_wallets.to_csv(index=False)
                    st.download_button(
                        "Download Duplicates",
                        data=csv,
                        file_name="btc_duplicate_wallets.csv",
                        mime="text/csv"
                    )

        with tab4:
            st.subheader("System Logs")
            log_level = st.selectbox(
                "Log Level",
                ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                index=1
            )

            logging.getLogger().setLevel(log_level)

            # Get logs from database
            scan_stats = db.get_scan_stats()
            if scan_stats:
                st.markdown("### Latest Scan Details")
                st.json(scan_stats)

            # Show recent application logs
            st.markdown("### Application Logs")
            try:
                with open("app.log", "r") as log_file:
                    logs = log_file.readlines()
                    filtered_logs = [
                        log for log in logs 
                        if log.split()[0] in [log_level, "CRITICAL"]
                    ]
                    for log in filtered_logs[-50:]:  # Show last 50 logs
                        severity = log.split()[0]
                        color = {
                            "DEBUG": "#808080",
                            "INFO": "#E0E0E0",
                            "WARNING": "#FFA500",
                            "ERROR": "#FF0000",
                            "CRITICAL": "#FF0000"
                        }.get(severity, "#E0E0E0")
                        st.markdown(f"<pre style='color: {color}'>{log}</pre>", 
                                  unsafe_allow_html=True)
            except FileNotFoundError:
                st.info("No log file found.")


    except Exception as e:
        st.error(f"Error: {str(e)}")
        logger.error(f"Application error: {str(e)}")

if __name__ == "__main__":
    main()
    scheduler.start(20)  # Start with default 20 pages