import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BTCWalletScraper:
    def __init__(self):
        self.base_url = "https://bitinfocharts.com/top-100-richest-bitcoin-addresses-{}.html"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def _get_page(self, page: int) -> str:
        """Fetch a single page with proper error handling and rate limiting"""
        url = self.base_url.format('' if page == 1 else str(page))
        try:
            time.sleep(2)  # Rate limiting
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            raise

    def _parse_page(self, html: str) -> List[Dict]:
        """Parse HTML content and extract wallet information"""
        soup = BeautifulSoup(html, 'html.parser')
        wallets = []
        
        try:
            table = soup.find('table', {'id': 'tblOne'})
            if not table:
                return []

            rows = table.find_all('tr')[1:]  # Skip header row
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 6:
                    wallet = {
                        'address': cols[1].text.strip(),
                        'balance': float(cols[2].text.strip().split()[0].replace(',', '')),
                        'first_in': cols[4].text.strip(),
                        'last_in': cols[5].text.strip(),
                        'last_out': cols[6].text.strip() if len(cols) > 6 else 'Never',
                        'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    wallets.append(wallet)
        except Exception as e:
            logger.error(f"Error parsing HTML: {str(e)}")
            raise

        return wallets

    def scrape_wallets(self, pages: int = 20) -> List[Dict]:
        """Scrape wallet data from multiple pages"""
        all_wallets = []
        
        for page in range(1, pages + 1):
            try:
                logger.info(f"Scraping page {page}")
                html = self._get_page(page)
                wallets = self._parse_page(html)
                all_wallets.extend(wallets)
            except Exception as e:
                logger.error(f"Failed to scrape page {page}: {str(e)}")
                continue

        return all_wallets
