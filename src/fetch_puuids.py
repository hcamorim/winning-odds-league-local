import time
from typing import List, Dict
import logging
from database.db_manager import DatabaseManager
from api.riot_client import RiotClient
from utils.logging_config import setup_logging

logging.basicConfig(level=logging.INFO)

class PUUIDFetcher:
    def __init__(self):
        self.db = DatabaseManager()
        self.riot_client = RiotClient()
        self.batch_size = 100  # Maximum calls per 2 minutes
        self.rate_limit_window = 120  # 2 minutes in seconds

    def get_summoners_without_puuid(self) -> List[Dict]:
        """Fetch summoners that don't have a PUUID yet."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT summonerID, region
                FROM Summoners
                WHERE puuid IS NULL
            """)
            return [{"summonerID": row[0], "region": row[1]} for row in cursor.fetchall()]
        finally:
            conn.close()

    def update_puuid_batch(self, summoners: List[Dict]) -> None:
        """Update PUUIDs for a batch of summoners."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            for summoner in summoners:
                try:
                    response = self.riot_client.get_summoner_by_id(
                        summoner["summonerID"], 
                        summoner["region"]
                    )
                    if response and "puuid" in response:
                        cursor.execute("""
                            UPDATE Summoners
                            SET puuid = ?
                            WHERE summonerID = ? AND region = ?
                        """, (response["puuid"], summoner["summonerID"], summoner["region"]))
                        logging.info(f"Updated PUUID for summoner {summoner['summonerID']}")
                except Exception as e:
                    logging.error(f"Error updating PUUID for summoner {summoner['summonerID']}: {str(e)}")
            
            conn.commit()
        finally:
            conn.close()

    def process_summoners(self, num_batches: int = None) -> None:
        """Process summoners in batches, with option to limit number of batches."""
        summoners = self.get_summoners_without_puuid()
        total_summoners = len(summoners)
        total_possible_batches = (total_summoners + self.batch_size - 1) // self.batch_size
        
        # Calculate estimated time
        estimated_time_per_batch = self.rate_limit_window  # 2 minutes per batch
        total_estimated_time = total_possible_batches * estimated_time_per_batch

        logging.info(f"Found {total_summoners} summoners without PUUID")
        logging.info(f"This will require {total_possible_batches} batches total")
        logging.info(f"Estimated time for all batches: {total_estimated_time/60:.1f} minutes")

        if num_batches is None:
            # Ask user how many batches to process
            while True:
                try:
                    user_input = input(f"\nHow many batches would you like to process? (1-{total_possible_batches}, or 0 for all): ")
                    num_batches = int(user_input)
                    if num_batches == 0:
                        num_batches = total_possible_batches
                    if 0 <= num_batches <= total_possible_batches:
                        break
                    print(f"Please enter a number between 1 and {total_possible_batches} (or 0 for all)")
                except ValueError:
                    print("Please enter a valid number")

        batches_to_process = min(num_batches, total_possible_batches)
        estimated_time = batches_to_process * estimated_time_per_batch
        logging.info(f"\nProcessing {batches_to_process} batches")
        logging.info(f"Estimated time: {estimated_time/60:.1f} minutes")

        for i in range(0, min(batches_to_process * self.batch_size, total_summoners), self.batch_size):
            batch = summoners[i:i + self.batch_size]
            logging.info(f"\nProcessing batch {i//self.batch_size + 1} of {batches_to_process}")
            
            start_time = time.time()
            self.update_puuid_batch(batch)
            
            # Calculate time to wait before next batch
            elapsed_time = time.time() - start_time
            wait_time = max(0, self.rate_limit_window - elapsed_time)
            
            if wait_time > 0 and i + self.batch_size < total_summoners and i//self.batch_size + 1 < batches_to_process:
                logging.info(f"Rate limit window - waiting {wait_time:.2f} seconds before next batch")
                time.sleep(wait_time)

        remaining = total_summoners - (batches_to_process * self.batch_size)
        if remaining > 0:
            logging.info(f"\nCompleted requested batches. {remaining} summoners remaining to be processed in the future.")

def main():
    fetcher = PUUIDFetcher()
    fetcher.process_summoners()

if __name__ == "__main__":
    setup_logging("fetch_puuids")
    main() 