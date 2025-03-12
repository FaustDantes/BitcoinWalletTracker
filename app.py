import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.scraper import BTCWalletScraper
from src.database import Database
from src.scheduler import DataCollectionScheduler
import logging
import re

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

def extract_btc_address(text):
    """Extract clean Bitcoin address from text"""
    # Remove HTML and extra spaces
    clean_text = re.sub(r'<[^>]+>', '', text).strip()
    # Bitcoin addresses are typically 26-35 characters long
    btc_pattern = r'[13][a-km-zA-HJ-NP-Z1-9]{25,34}'
    match = re.search(btc_pattern, clean_text)
    return match.group(0) if match else clean_text

def main():
    # Custom CSS
    st.markdown("""
        <style>
        .stApp {
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e2e2e2;
        }
        .stMetric {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
            padding: 15px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .stDataFrame {
            background: rgba(255, 255, 255, 0.05);
            border-radius: 10px;
            padding: 10px;
        }
        .stSelectbox, .stTextInput {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 5px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        h1, h2, h3 {
            color: #ffd700;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            background-color: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
            padding: 10px;
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 5px;
            color: #ffffff;
            background-color: rgba(255, 255, 255, 0.1);
        }
        .stTabs [aria-selected="true"] {
            background-color: #ffd700 !important;
            color: #1a1a2e !important;
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("🌟 Bitcoin Whale Tracker")
    st.markdown("### Monitoring the Biggest Bitcoin Wallets")

    # Sidebar configuration with custom styling
    with st.sidebar:
        st.markdown("""
            <div style='background: rgba(255, 215, 0, 0.1); padding: 20px; border-radius: 10px; border: 1px solid #ffd700;'>
            <h3 style='color: #ffd700; margin-top: 0;'>Configuration</h3>
            </div>
        """, unsafe_allow_html=True)

        pages_to_scan = st.slider(
            "Number of pages to scan",
            min_value=1,
            max_value=50,
            value=20
        )

        if st.button("🔄 Refresh Data", use_container_width=True):
            with st.spinner("Fetching latest wallet data..."):
                try:
                    wallets = scraper.scrape_wallets(pages_to_scan)
                    db.store_wallets(wallets)
                    st.success("✅ Data updated successfully!")
                except Exception as e:
                    st.error(f"❌ Error updating data: {str(e)}")

    # Tab selection
    tab1, tab2, tab3 = st.tabs(["📊 Wallet Analysis", "📈 Transaction Stats", "📜 Historical Data"])

    try:
        df = db.get_latest_wallets()
        # Clean Bitcoin addresses
        df['clean_address'] = df['address'].apply(extract_btc_address)

        with tab1:
            # Summary metrics in styled containers
            st.markdown("<div style='padding: 20px 0;'>", unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("🔢 Total Wallets", f"{len(df):,}")
            with col2:
                total_btc = df['balance'].sum()
                st.metric("💰 Total BTC", format_balance(total_btc))
            with col3:
                avg_btc = df['balance'].mean()
                st.metric("📊 Average BTC", format_balance(avg_btc))
            st.markdown("</div>", unsafe_allow_html=True)

            # Duplicate Balance Analysis
            st.markdown("""
                <div style='background: rgba(255, 255, 255, 0.05); padding: 20px; border-radius: 10px; margin: 20px 0;'>
                <h2 style='color: #ffd700; margin-top: 0;'>🔍 Duplicate Balance Analysis</h2>
                </div>
            """, unsafe_allow_html=True)

            duplicate_wallets = db.get_duplicate_balance_wallets()
            if not duplicate_wallets.empty:
                duplicate_wallets['clean_address'] = duplicate_wallets['address'].apply(extract_btc_address)
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("👥 Duplicate Groups", len(duplicate_wallets['balance'].unique()))
                with col2:
                    st.metric("🔄 Total Duplicates", len(duplicate_wallets))

                # Display cleaned duplicate wallets
                st.dataframe(
                    duplicate_wallets[['clean_address', 'balance', 'first_in', 'last_in', 'last_out']].style.format({
                        'balance': lambda x: format_balance(x)
                    })
                )
            else:
                st.info("📝 No wallets with duplicate balances found")

            # Balance Groups
            st.markdown("""
                <div style='background: rgba(255, 255, 255, 0.05); padding: 20px; border-radius: 10px; margin: 20px 0;'>
                <h2 style='color: #ffd700; margin-top: 0;'>👥 Balance Groups</h2>
                </div>
            """, unsafe_allow_html=True)

            balance_groups = db.get_balance_groups()
            if not balance_groups.empty:
                balance_groups['group_label'] = balance_groups['group_balance'].apply(format_balance)
                st.dataframe(balance_groups)
            else:
                st.info("📝 No balance groups found")

            # Search and filter
            search_term = st.text_input("Search wallet address")
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
                df['clean_address'].tolist()
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