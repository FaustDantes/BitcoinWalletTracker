import streamlit as st
import pandas as pd
from src.scheduler import DataCollectionScheduler
import logging
from src.logger_config import setup_logger
from src.data_manager import (
    format_balance, refresh_data, get_latest_wallet_data,
    get_duplicate_balances, get_wallet_history, get_balance_groups
)

# Configure logging
logger = setup_logger()

# Initialize scheduler
scheduler = DataCollectionScheduler()

def main():
    # Load external CSS
    with open("static/styles.css", "r") as css_file:
        st.markdown(f"<style>{css_file.read()}</style>", unsafe_allow_html=True)

    st.title("Bitcoin Wallet Tracker")
    st.markdown("### Monitor Large Bitcoin Wallets")

    # Sidebar configuration
    with st.sidebar:
        st.markdown("""
            <div class="settings-panel">
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
                success, message = refresh_data(pages_to_scan)
                if success:
                    st.success(message)
                else:
                    st.error(message)

    # Tab selection
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Wallet Analysis", 
        "Duplicate Balances", 
        "Groups Summary",
        "History",
        "Application Logs"
    ])

    try:
        df = get_latest_wallet_data()

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
            duplicate_wallets = get_duplicate_balances()

            if not duplicate_wallets.empty:
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
            duplicate_wallets = get_duplicate_balances()

            if not duplicate_wallets.empty:
                # Get balance groups summary
                groups_df = get_balance_groups(duplicate_wallets)

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
            history_df = get_wallet_history(limit=1000)
            if not history_df.empty:
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

        with tab5:
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
                        css_class = f"log-{severity.lower()}"

                        # Format timestamp if present
                        parts = log.split(" ", 1)
                        if len(parts) > 1:
                            timestamp, message = parts
                            formatted_log = f"<span class='log-timestamp'>{timestamp}</span> {message}"
                        else:
                            formatted_log = log

                        st.markdown(
                            f"<pre class='{css_class}'>{formatted_log}</pre>", 
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