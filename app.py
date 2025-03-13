import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.scraper import BTCWalletScraper
from src.database import Database
from src.scheduler import DataCollectionScheduler
import logging
from src.logger_config import setup_logger
import re

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
    # Custom CSS for grayscale theme
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
        .stSelectbox, .stTextInput {
            background: #2D2D2D;
            border-radius: 4px;
            border: 1px solid #404040;
        }
        h1, h2, h3 {
            color: #E0E0E0;
        }
        .stButton button {
            background-color: #2D2D2D;
            color: #E0E0E0;
            border: 1px solid #404040;
        }
        .stButton button:hover {
            background-color: #404040;
            color: #FFFFFF;
        }
        pre {
            background: #2D2D2D;
            padding: 10px;
            border-radius: 4px;
            border: 1px solid #404040;
            white-space: pre-wrap;
            word-wrap: break-word;
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
            background-color: #808080 !important;
            color: #FFFFFF !important;
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("Bitcoin Wallet Tracker")
    st.markdown("### Monitor Large Bitcoin Wallets")

    # Sidebar configuration
    with st.sidebar:
        st.markdown("""
            <div style='background: #2D2D2D; padding: 20px; border-radius: 4px; border: 1px solid #404040;'>
            <h3>Settings</h3>
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
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Wallet Analysis", 
        "Duplicate Balances", 
        "Groups Summary",
        "History",
        "Application Logs"
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

            # Search and filter
            search_term = st.text_input("Search address")
            if search_term:
                df = df[df['clean_address'].str.contains(search_term, case=False)]

            # Display data table
            st.subheader("Wallet Data")
            st.dataframe(
                df[['clean_address', 'balance', 'first_in', 'last_in', 'last_out']].style.format({
                    'balance': lambda x: format_balance(x)
                })
            )

        with tab2:
            st.subheader("Wallets with Duplicate Balances")
            duplicate_wallets = db.get_duplicate_balance_wallets()

            if not duplicate_wallets.empty:
                duplicate_wallets['clean_address'] = duplicate_wallets['address'].apply(extract_btc_address)
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Duplicate Groups", 
                             len(duplicate_wallets['balance'].unique()))
                with col2:
                    st.metric("Total Wallets with Duplicates", 
                             len(duplicate_wallets))

                # Group the wallets by balance
                balances = duplicate_wallets['balance'].unique()
                
                for balance in balances:
                    group_wallets = duplicate_wallets[duplicate_wallets['balance'] == balance]
                    st.markdown(f"### Group: {format_balance(balance)} - {len(group_wallets)} wallets")
                    st.dataframe(
                        group_wallets[['clean_address', 'balance', 'first_in', 'last_in', 'last_out']].style.format({
                            'balance': lambda x: format_balance(x)
                        })
                    )
                    st.markdown("---")
            else:
                st.info("No wallets with duplicate balances found")

        with tab3:
            st.subheader("Balance Groups Summary")
            duplicate_wallets = db.get_duplicate_balance_wallets()
            
            if not duplicate_wallets.empty:
                # Create summary dataframe with balance groups
                balance_groups = []
                for balance in duplicate_wallets['balance'].unique():
                    group_wallets = duplicate_wallets[duplicate_wallets['balance'] == balance]
                    balance_groups.append({
                        'Group Balance': format_balance(balance),
                        'Raw Balance': balance,
                        'Number of Wallets': len(group_wallets)
                    })
                
                # Convert to dataframe and sort by number of wallets (descending)
                groups_df = pd.DataFrame(balance_groups)
                groups_df = groups_df.sort_values(by=['Number of Wallets', 'Raw Balance'], ascending=[False, False])
                
                # Display the summary table
                st.dataframe(
                    groups_df[['Group Balance', 'Number of Wallets']].reset_index(drop=True),
                    use_container_width=True
                )
                
                # Add some metrics
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Groups", len(groups_df))
                with col2:
                    st.metric("Total Wallets in Groups", groups_df['Number of Wallets'].sum())
            else:
                st.info("No balance groups found")

        with tab4:
            st.subheader("Historical Data")

            # Get wallet history
            history_df = db.get_wallet_history(limit=1000)
            if not history_df.empty:
                history_df['clean_address'] = history_df['address'].apply(extract_btc_address)

                # Filter by address if specified
                selected_address = st.selectbox(
                    "Filter by address",
                    ["All Addresses"] + history_df['clean_address'].unique().tolist()
                )

                if selected_address != "All Addresses":
                    history_df = history_df[history_df['clean_address'] == selected_address]

                st.dataframe(
                    history_df[['scan_timestamp', 'clean_address', 'balance', 'first_in', 'last_in', 'last_out']].style.format({
                        'balance': lambda x: format_balance(x),
                        'scan_timestamp': lambda x: x.split('.')[0]
                    })
                )
            else:
                st.info("No historical data available")

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

        with tab4:
            st.subheader("Application Logs")

            # Log level filter
            log_level = st.selectbox(
                "Log Level Filter",
                ["ALL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                index=0
            )

            # Number of logs to display
            num_logs = st.slider("Number of logs to display", 10, 200, 50)

            # Show recent application logs
            st.markdown("### Recent Logs")
            try:
                with open("app.log", "r") as log_file:
                    logs = log_file.readlines()

                    # Filter logs based on selected level
                    if log_level != "ALL":
                        filtered_logs = [
                            log for log in logs 
                            if f"[{log_level}]" in log
                        ]
                    else:
                        filtered_logs = logs

                    # Display logs with appropriate styling
                    for log in filtered_logs[-num_logs:]:
                        severity = log.split("]")[0].split("[")[-1] if "[" in log else "INFO"
                        color = {
                            "DEBUG": "#808080",
                            "INFO": "#E0E0E0",
                            "WARNING": "#FFA500",
                            "ERROR": "#FF0000",
                            "CRITICAL": "#FF0000"
                        }.get(severity, "#E0E0E0")

                        # Format timestamp if present
                        parts = log.split(" ", 1)
                        if len(parts) > 1:
                            timestamp, message = parts
                            formatted_log = f"<span style='color: #808080'>{timestamp}</span> {message}"
                        else:
                            formatted_log = log

                        st.markdown(
                            f"<pre style='color: {color}'>{formatted_log}</pre>", 
                            unsafe_allow_html=True
                        )
            except FileNotFoundError:
                st.info("No log file found. The application will create one as events occur.")

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        logger.error(f"Application error: {str(e)}")

if __name__ == "__main__":
    main()
    scheduler.start(20)  # Start with default 20 pages