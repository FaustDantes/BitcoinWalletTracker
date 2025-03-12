from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from src.scraper import BTCWalletScraper
from src.database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCollectionScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scraper = BTCWalletScraper()
        self.db = Database()
        self._pages = 20  # Default value

    def collect_data(self):
        """Collect and store wallet data"""
        try:
            logger.info(f"Starting data collection for {self._pages} pages")
            wallets = self.scraper.scrape_wallets(self._pages)

            # Store data and get scan ID
            scan_id = self.db.store_wallets(wallets)

            # Get and log scan statistics
            stats = self.db.get_scan_stats(scan_id)
            logger.info(f"Scan completed - ID: {scan_id}")
            logger.info(f"Collected {stats['total_wallets']} wallets with total balance of {stats['total_balance']} BTC")

        except Exception as e:
            logger.error(f"Data collection failed: {str(e)}")

    def start(self, pages: int = 20):
        """Start the scheduler with daily data collection"""
        self._pages = pages
        self.scheduler.add_job(
            func=self.collect_data,
            trigger=CronTrigger(hour=0),  # Run at midnight
            id='daily_collection',
            name='Daily wallet data collection',
            replace_existing=True
        )
        self.scheduler.start()
        logger.info(f"Scheduler started with {pages} pages configuration")

    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")