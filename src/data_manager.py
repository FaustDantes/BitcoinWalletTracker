
import pandas as pd
import re
from src.scraper import BTCWalletScraper
from src.database import Database
import logging
from src.logger_config import setup_logger

# Configure logging
logger = setup_logger()

# Initialize components
db = Database()
scraper = BTCWalletScraper()

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

def refresh_data(pages_to_scan):
    """Fetch latest data from the source"""
    try:
        wallets = scraper.scrape_wallets(pages_to_scan)
        db.store_wallets(wallets)
        logger.info(f"Data updated successfully - {len(wallets)} wallets fetched")
        return True, "Data updated successfully"
    except Exception as e:
        error_msg = f"Error updating data: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def get_latest_wallet_data():
    """Get the latest wallet data with clean addresses"""
    df = db.get_latest_wallets()
    df['clean_address'] = df['address'].apply(extract_btc_address)
    return df

def get_duplicate_balances():
    """Get wallets with duplicate balances"""
    duplicate_wallets = db.get_duplicate_balance_wallets()
    if not duplicate_wallets.empty:
        duplicate_wallets['clean_address'] = duplicate_wallets['address'].apply(extract_btc_address)
    return duplicate_wallets

def get_wallet_history(limit=1000):
    """Get wallet history data"""
    history_df = db.get_wallet_history(limit=limit)
    if not history_df.empty:
        history_df['clean_address'] = history_df['address'].apply(extract_btc_address)
    return history_df

def get_balance_groups(duplicate_wallets):
    """Create summary dataframe with balance groups"""
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
    if not groups_df.empty:
        groups_df = groups_df.sort_values(by=['Number of Wallets', 'Raw Balance'], ascending=[False, False])
    
    return groups_df
