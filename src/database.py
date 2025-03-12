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
                # Main wallets table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS wallets (
                        address TEXT PRIMARY KEY,
                        balance REAL,
                        first_in TEXT,
                        last_in TEXT,
                        last_out TEXT,
                        first_seen TEXT,
                        last_updated TEXT,
                        total_transactions INTEGER DEFAULT 0,
                        is_active BOOLEAN DEFAULT 1
                    )
                """)

                # Wallet groups table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_groups (
                        group_id TEXT PRIMARY KEY,
                        balance_value REAL,
                        wallet_count INTEGER,
                        first_wallet_seen TEXT,
                        last_wallet_seen TEXT,
                        last_updated TEXT,
                        common_pattern TEXT,
                        total_group_balance REAL
                    )
                """)

                # Track scanning history
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS scans (
                        scan_id TEXT PRIMARY KEY,
                        timestamp TEXT,
                        pages_scanned INTEGER,
                        total_wallets INTEGER,
                        total_balance REAL
                    )
                """)
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {str(e)}")
            raise

    def store_wallets(self, wallets: List[Dict]):
        """Store wallet data and update groups"""
        try:
            # Generate a unique scan ID
            scan_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Prepare wallet data
            df = pd.DataFrame(wallets)

            with sqlite3.connect(self.db_path) as conn:
                # Update existing wallets or insert new ones
                for _, wallet in df.iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO wallets 
                        (address, balance, first_in, last_in, last_out, 
                         first_seen, last_updated, total_transactions)
                        VALUES (?, ?, ?, ?, ?, 
                                COALESCE((SELECT first_seen FROM wallets WHERE address = ?), ?),
                                ?, 
                                COALESCE((SELECT total_transactions FROM wallets WHERE address = ?), 0) + 1)
                    """, (
                        wallet['address'], wallet['balance'], 
                        wallet['first_in'], wallet['last_in'], wallet['last_out'],
                        wallet['address'], current_time,
                        current_time,
                        wallet['address']
                    ))

                # Update wallet groups
                self._update_wallet_groups(conn)

                # Store scan metadata
                scan_data = {
                    'scan_id': scan_id,
                    'timestamp': current_time,
                    'pages_scanned': len(wallets) // 100 + 1,
                    'total_wallets': len(wallets),
                    'total_balance': df['balance'].sum() if 'balance' in df.columns else 0
                }
                pd.DataFrame([scan_data]).to_sql('scans', conn, if_exists='append', index=False)

            logger.info(f"Successfully stored {len(wallets)} wallets with scan ID {scan_id}")
            return scan_id

        except Exception as e:
            logger.error(f"Error storing wallets: {str(e)}")
            raise

    def _update_wallet_groups(self, conn):
        """Update wallet groups based on current wallet data"""
        try:
            # Find groups of wallets with same balance
            groups = pd.read_sql_query("""
                SELECT 
                    balance as balance_value,
                    COUNT(*) as wallet_count,
                    MIN(first_seen) as first_wallet_seen,
                    MAX(last_updated) as last_wallet_seen,
                    SUM(balance) as total_group_balance
                FROM wallets 
                GROUP BY balance
                HAVING COUNT(*) > 1
            """, conn)

            # Update groups table
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for _, group in groups.iterrows():
                group_id = f"BAL_{group['balance_value']}"
                conn.execute("""
                    INSERT OR REPLACE INTO wallet_groups 
                    (group_id, balance_value, wallet_count, 
                     first_wallet_seen, last_wallet_seen, 
                     last_updated, total_group_balance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    group_id, group['balance_value'], 
                    group['wallet_count'], group['first_wallet_seen'],
                    group['last_wallet_seen'], current_time,
                    group['total_group_balance']
                ))

        except Exception as e:
            logger.error(f"Error updating wallet groups: {str(e)}")
            raise

    def get_wallet_groups(self) -> pd.DataFrame:
        """Get all wallet groups"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query("""
                    SELECT * FROM wallet_groups 
                    ORDER BY wallet_count DESC, balance_value DESC
                """, conn)
        except sqlite3.Error as e:
            logger.error(f"Error fetching wallet groups: {str(e)}")
            raise

    def get_wallets_in_group(self, balance_value: float) -> pd.DataFrame:
        """Get all wallets in a specific balance group"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query("""
                    SELECT * FROM wallets 
                    WHERE balance = ? 
                    ORDER BY last_updated DESC
                """, conn, params=(balance_value,))
        except sqlite3.Error as e:
            logger.error(f"Error fetching wallets in group: {str(e)}")
            raise

    def get_all_wallets(self) -> pd.DataFrame:
        """Get all wallets"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query("""
                    SELECT * FROM wallets 
                    ORDER BY balance DESC
                """, conn)
        except sqlite3.Error as e:
            logger.error(f"Error fetching all wallets: {str(e)}")
            raise

    def get_scan_stats(self, scan_id: str = None) -> Dict:
        """Get statistics for a specific scan or the latest scan"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if not scan_id:
                    result = conn.execute("""
                        SELECT scan_id FROM scans 
                        ORDER BY timestamp DESC LIMIT 1
                    """).fetchone()
                    scan_id = result[0] if result else None

                if not scan_id:
                    return None

                query = "SELECT * FROM scans WHERE scan_id = ?"
                result = pd.read_sql_query(query, conn, params=(scan_id,))

                if result.empty:
                    return None

                return result.iloc[0].to_dict()
        except sqlite3.Error as e:
            logger.error(f"Error fetching scan stats: {str(e)}")
            raise

    def get_latest_scan_id(self) -> str:
        """Get the most recent scan ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                result = conn.execute("""
                    SELECT scan_id 
                    FROM scans 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """).fetchone()
                return result[0] if result else None
        except sqlite3.Error as e:
            logger.error(f"Error fetching latest scan ID: {str(e)}")
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