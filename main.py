"""
Main entry point for the Instalily Case Study application.
Runs the complete pipeline: scraping, ETL, and insights generation.
"""

import os
import sys
import asyncio
import logging
import argparse
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("main")

# Import settings
from config.settings import (
    SCRAPER_DEFAULT_ZIP,
    SCRAPER_DEFAULT_DISTANCE,
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    INSIGHTS_DATA_PATH,
    DB_PATH
)

# Import modules
from scraper.scraper import GAFScraper
from etl.processor import ContractorDataProcessor
from db.db_manager import DBManager

async def run_scraper(zip_code: str, distance: int, headless: bool = True) -> List[Dict[str, Any]]:
    """
    Run the web scraper to extract contractor data.
    
    Args:
        zip_code: ZIP code to search
        distance: Search radius in miles
        headless: Whether to run browser in headless mode
        
    Returns:
        List of contractor data dictionaries
    """
    logger.info(f"Starting scraper for ZIP code {zip_code} with {distance} mile radius")
    
    scraper = GAFScraper(
        zip_code=zip_code,
        distance=distance,
        headless=headless
    )
    
    contractors = await scraper.scrape()
    
    logger.info(f"Scraping complete: {len(contractors)} contractors found")
    return contractors

def run_etl() -> List[Dict[str, Any]]:
    """
    Run the ETL process to clean and transform the raw data.
    
    Returns:
        List of processed contractor data dictionaries
    """
    logger.info("Starting ETL process")
    
    processor = ContractorDataProcessor(
        input_path=RAW_DATA_PATH,
        output_path=PROCESSED_DATA_PATH
    )
    
    processed_data = processor.process()
    
    logger.info(f"ETL process complete: {len(processed_data)} records processed")
    return processed_data

def run_db_import() -> int:
    """
    Import processed data into the database.
    
    Returns:
        Number of records imported
    """
    logger.info("Starting database import")
    
    # Create DB manager
    db_manager = DBManager(db_path=DB_PATH)
    
    try:
        # Connect to database
        db_manager.connect()
        
        # Initialize schema if needed
        db_manager.initialize_db()
        
        # Import data
        count = db_manager.import_contractors_from_json(PROCESSED_DATA_PATH)
        
        logger.info(f"Database import complete: {count} records imported")
        return count
    
    finally:
        # Close database connection
        db_manager.close()

async def main():
    """Main function to run the complete pipeline."""
    parser = argparse.ArgumentParser(description="Instalily Case Study Pipeline")
    parser.add_argument("--zip-code", type=str, default=SCRAPER_DEFAULT_ZIP, help="ZIP code to search")
    parser.add_argument("--distance", type=int, default=SCRAPER_DEFAULT_DISTANCE, help="Search radius in miles")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--skip-scraper", action="store_true", help="Skip the scraping step")
    parser.add_argument("--skip-etl", action="store_true", help="Skip the ETL step")
    parser.add_argument("--skip-db", action="store_true", help="Skip the database import step")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Configure logging level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)
    
    print("=== Instalily Case Study Pipeline ===")
    
    # Run scraper if not skipped
    if not args.skip_scraper:
        print(f"\n[1/3] Running web scraper for ZIP code {args.zip_code}...")
        contractors = await run_scraper(args.zip_code, args.distance, args.headless)
        print(f"      Scraped {len(contractors)} contractors")
    else:
        print("\n[1/3] Skipping web scraper")
    
    # Run ETL if not skipped
    if not args.skip_etl:
        print("\n[2/3] Running ETL process...")
        processed_data = run_etl()
        print(f"      Processed {len(processed_data)} contractor records")
        
        # Print some statistics
        high_value_count = sum(1 for c in processed_data if c.get("high_value_prospect", False))
        print(f"      Identified {high_value_count} high-value prospects")
    else:
        print("\n[2/3] Skipping ETL process")
    
    # Run database import if not skipped
    if not args.skip_db:
        print("\n[3/3] Importing data into database...")
        count = run_db_import()
        print(f"      Imported {count} records into database")
    else:
        print("\n[3/3] Skipping database import")
    
    print("\nPipeline execution complete!")
    print(f"Raw data saved to: {RAW_DATA_PATH}")
    print(f"Processed data saved to: {PROCESSED_DATA_PATH}")
    print(f"Database saved to: {DB_PATH}")
    
    # Suggest next steps
    print("\nNext steps:")
    print("1. Generate AI insights with: python -m insights.insight_generator")
    print("2. Run the API server with: python -m backend.api")
    print("3. View database statistics with: python -m db.db_manager --stats")

if __name__ == "__main__":
    asyncio.run(main())