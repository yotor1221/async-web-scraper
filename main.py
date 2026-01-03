import asyncio
import csv
import logging
import re
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, BrowserContext

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class BookScraper:
    def __init__(self, base_url: str, output_file: str = 'books_data.csv', max_concurrent: int = 5):
        self.base_url = base_url
        self.output_file = output_file
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.fieldnames = ['title', 'price', 'availability', 'url']
        
        # Initialize the file with headers
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()

    async def get_total_pages(self, page: Page) -> int:
        """Extracts total page count from the pagination text (e.g., 'Page 1 of 50')."""
        try:
            await page.goto(self.base_url)
            await page.wait_for_selector('.pager', timeout=5000)
            text = await page.inner_text('.current') # Format: "Page 1 of 50"
            match = re.search(r'Page \d+ of (\d+)', text)
            if match:
                return int(match.group(1))
        except Exception as e:
            logger.warning(f"Could not automatically detect total pages: {e}. Defaulting to 1.")
        return 50 # Default fallback if detection fails, though usually it works on toscrape

    async def scrape_page(self, context: BrowserContext, page_num: int):
        """Scrapes a single page safely using a semaphore to limit concurrency."""
        async with self.semaphore:
            page = await context.new_page()
            url = f"http://books.toscrape.com/catalogue/page-{page_num}.html"
            
            try:
                # Retry logic
                for attempt in range(3):
                    try:
                        logger.info(f"Scraping Page {page_num} (Attempt {attempt+1})...")
                        response = await page.goto(url, timeout=30000)
                        if response.status == 404:
                            logger.warning(f"Page {page_num} not found (404).")
                            return []
                        
                        await page.wait_for_selector('.product_pod', timeout=5000)
                        
                        books = await page.evaluate('''() => {
                            const results = [];
                            document.querySelectorAll('.product_pod').forEach(book => {
                                const titleEl = book.querySelector('h3 a');
                                const priceEl = book.querySelector('.price_color');
                                const availEl = book.querySelector('.instock.availability');
                                results.push({
                                    title: titleEl ? titleEl.getAttribute('title') : null,
                                    price: priceEl ? priceEl.innerText : null,
                                    availability: availEl ? availEl.innerText.trim() : null,
                                    url: titleEl ? titleEl.href : null
                                });
                            });
                            return results;
                        }''')
                        
                        logger.info(f"Page {page_num}: Found {len(books)} books.")
                        self.save_data(books)
                        return books
                        
                    except Exception as e:
                        logger.error(f"Error scraping page {page_num}, attempt {attempt+1}: {e}")
                        await asyncio.sleep(2) # Backoff
                
                logger.error(f"Failed to scrape page {page_num} after all attempts.")
                return []
                
            finally:
                await page.close()
    def save_data(self, books: List[Dict]):
        """Appends a batch of books to the CSV file immediately."""
        if not books:
            return
            
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerows(books)
        except Exception as e:
            logger.error(f"Failed to save data: {e}")

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            
            # Get total pages first
            page_one = await context.new_page()
            total_pages = await self.get_total_pages(page_one)
            await page_one.close()
            
            logger.info(f"Starting scrape for {total_pages} pages with {self.max_concurrent} concurrent workers.")
            
            # Create tasks for all pages
            tasks = [self.scrape_page(context, i) for i in range(1, total_pages + 1)]
            
            # Run them
            await asyncio.gather(*tasks)
            
            await browser.close()
            logger.info("Scraping completed.")

if __name__ == "__main__":
    scraper = BookScraper(base_url="http://books.toscrape.com/")
    asyncio.run(scraper.run())