import sqlite3
import pandas as pd
from typing import List, Dict, Tuple
import logging
import numpy as np
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "btc_wallets.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize database and create tables if they don't exist"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS wallets (
                        address TEXT,
                        balance REAL,
                        first_in TEXT,
                        last_in TEXT,
                        last_out TEXT,
                        timestamp TEXT,
                        PRIMARY KEY (address, timestamp)
                    )
                """)
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {str(e)}")
            raise

    def store_wallets(self, wallets: List[Dict]):
        """Store wallet data in the database"""
        try:
            df = pd.DataFrame(wallets)
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql('wallets', conn, if_exists='append', index=False)
        except Exception as e:
            logger.error(f"Error storing wallets: {str(e)}")
            raise

    def get_duplicate_balance_wallets(self) -> pd.DataFrame:
        """Get wallets where the balance appears more than once"""
        query = """
        WITH duplicate_balances AS (
            SELECT balance
            FROM wallets w
            INNER JOIN (
                SELECT address, MAX(timestamp) as max_timestamp
                FROM wallets
                GROUP BY address
            ) latest
            ON w.address = latest.address AND w.timestamp = latest.max_timestamp
            GROUP BY balance
            HAVING COUNT(*) > 1
        )
        SELECT w.*
        FROM wallets w
        INNER JOIN (
            SELECT address, MAX(timestamp) as max_timestamp
            FROM wallets
            GROUP BY address
        ) latest
        ON w.address = latest.address AND w.timestamp = latest.max_timestamp
        WHERE w.balance IN (SELECT balance FROM duplicate_balances)
        ORDER BY w.balance DESC
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(query, conn)
        except sqlite3.Error as e:
            logger.error(f"Error fetching duplicate balance wallets: {str(e)}")
            raise

    def get_balance_groups(self) -> pd.DataFrame:
        """Get grouped wallet data by balance"""
        query = """
        WITH latest_wallet_data AS (
            SELECT w.*
            FROM wallets w
            INNER JOIN (
                SELECT address, MAX(timestamp) as max_timestamp
                FROM wallets
                GROUP BY address
            ) latest
            ON w.address = latest.address AND w.timestamp = latest.max_timestamp
        )
        SELECT 
            balance as group_balance,
            COUNT(*) as wallet_count,
            GROUP_CONCAT(last_in) as last_in_dates,
            GROUP_CONCAT(last_out) as last_out_dates
        FROM latest_wallet_data
        GROUP BY balance
        HAVING COUNT(*) > 1
        ORDER BY balance DESC
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(query, conn)
        except sqlite3.Error as e:
            logger.error(f"Error fetching balance groups: {str(e)}")
            raise

    def get_latest_wallets(self) -> pd.DataFrame:
        """Get the most recent wallet data"""
        query = """
        SELECT w.*
        FROM wallets w
        INNER JOIN (
            SELECT address, MAX(timestamp) as max_timestamp
            FROM wallets
            GROUP BY address
        ) latest
        ON w.address = latest.address AND w.timestamp = latest.max_timestamp
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(query, conn)
        except sqlite3.Error as e:
            logger.error(f"Error fetching latest wallets: {str(e)}")
            raise

    def get_historical_data(self, address: str) -> pd.DataFrame:
        """Get historical data for a specific wallet"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT * FROM wallets WHERE address = ? ORDER BY timestamp"
                return pd.read_sql_query(query, conn, params=(address,))
        except sqlite3.Error as e:
            logger.error(f"Error fetching historical data: {str(e)}")
            raise

    def get_daily_transaction_stats(self) -> pd.DataFrame:
        """Get daily transaction statistics"""
        query = """
        WITH daily_activity AS (
            SELECT 
                date(last_in) as activity_date,
                COUNT(*) as incoming_txs,
                SUM(balance) as total_volume
            FROM wallets
            WHERE last_in != ''
            GROUP BY date(last_in)
            UNION ALL
            SELECT 
                date(last_out) as activity_date,
                COUNT(*) * -1 as incoming_txs,
                SUM(balance) * -1 as total_volume
            FROM wallets
            WHERE last_out != '' AND last_out != 'Never'
            GROUP BY date(last_out)
        )
        SELECT 
            activity_date,
            SUM(incoming_txs) as net_transactions,
            SUM(total_volume) as net_volume
        FROM daily_activity
        GROUP BY activity_date
        ORDER BY activity_date DESC
        LIMIT 30
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(query, conn)
        except sqlite3.Error as e:
            logger.error(f"Error fetching daily transaction stats: {str(e)}")
            raise

    def get_market_signal(self) -> Dict:
        """Generate buy/sell signal based on recent wallet activity"""
        try:
            stats_df = self.get_daily_transaction_stats()
            if stats_df.empty:
                return {"signal": "NEUTRAL", "confidence": 0.0, "reason": "Insufficient data"}

            # Calculate 7-day moving averages
            stats_df['net_tx_ma7'] = stats_df['net_transactions'].rolling(7).mean()
            stats_df['net_volume_ma7'] = stats_df['net_volume'].rolling(7).mean()

            # Get latest trends
            recent_tx_trend = stats_df['net_tx_ma7'].iloc[0] if len(stats_df) > 0 else 0
            recent_volume_trend = stats_df['net_volume_ma7'].iloc[0] if len(stats_df) > 0 else 0

            # Generate signal
            if recent_tx_trend > 0 and recent_volume_trend > 0:
                signal = "BUY"
                confidence = min(abs(recent_tx_trend / 10), 1.0)
                reason = "Positive transaction and volume trends"
            elif recent_tx_trend < 0 and recent_volume_trend < 0:
                signal = "SELL"
                confidence = min(abs(recent_tx_trend / 10), 1.0)
                reason = "Negative transaction and volume trends"
            else:
                signal = "NEUTRAL"
                confidence = 0.5
                reason = "Mixed signals in transaction and volume trends"

            return {
                "signal": signal,
                "confidence": confidence,
                "reason": reason,
                "metrics": {
                    "tx_trend": float(recent_tx_trend),
                    "volume_trend": float(recent_volume_trend)
                }
            }
        except Exception as e:
            logger.error(f"Error generating market signal: {str(e)}")
            return {"signal": "ERROR", "confidence": 0.0, "reason": str(e)}