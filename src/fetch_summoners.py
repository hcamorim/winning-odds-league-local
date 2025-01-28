from api.riot_client import RiotClient
from database.db_manager import DatabaseManager
from utils.logging_config import setup_logging
import logging

def main():
    try:
        # Initialize clients
        riot_client = RiotClient()
        db_manager = DatabaseManager()

        # Fetch summoners
        logging.info("Fetching summoners from Riot API...")
        summoners = riot_client.fetch_top_summoners()

        if not summoners:
            logging.warning("No summoners fetched from the API.")
            return

        # Update database
        logging.info("Updating database...")
        stats = db_manager.update_summoners(summoners)
        
        logging.info("\nUpdate Statistics:")
        logging.info(f"Total summoners before: {stats['before']}")
        logging.info(f"Total summoners after: {stats['after']}")
        logging.info(f"New summoners added: {stats['inserted']}")
        logging.info(f"Existing summoners updated: {stats['updated']}")
        logging.info(f"Outdated summoners removed: {stats['deleted']}")

    except Exception as e:
        logging.error(f"Error in main process: {e}")

if __name__ == "__main__":
    setup_logging("fetch_summoners")
    main() 