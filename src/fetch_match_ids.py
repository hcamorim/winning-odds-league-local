import time
from typing import List, Dict
import logging
from datetime import datetime
from database.db_manager import DatabaseManager
from api.riot_client import RiotClient
from utils.logging_config import setup_logging

logging.basicConfig(level=logging.INFO)

class MatchIDFetcher:
    def __init__(self):
        self.db = DatabaseManager()
        self.riot_client = RiotClient()
        self.batch_size = 100  # Maximum calls per 2 minutes
        self.rate_limit_window = 120  # 2 minutes in seconds

    def get_summoners_for_match_fetch(self) -> List[Dict]:
        """Fetch summoners that we need matches for."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    s.puuid, 
                    s.region, 
                    COALESCE(
                        MAX(m.created_at),  -- If they have matches, use latest match timestamp
                        s.created_at        -- If no matches, use when they were added to database
                    ) as start_time
                FROM Summoners s
                LEFT JOIN MatchIDs m ON s.puuid = m.summoner_puuid
                WHERE s.puuid IS NOT NULL
                GROUP BY s.puuid, s.region, s.created_at
            """)
            return [
                {
                    "puuid": row[0], 
                    "region": row[1],
                    "created_at": datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
                } 
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()

    def update_match_ids_batch(self, summoners: List[Dict]) -> None:
        """Fetch and store match IDs for a batch of summoners."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            for summoner in summoners:
                try:
                    # Get match IDs for this summoner
                    match_ids = self.riot_client.get_matches_by_puuid(
                        summoner["puuid"],
                        summoner["region"],
                        start_time=int(summoner["created_at"].timestamp())
                    )
                    
                    # Insert match IDs (unique constraint will handle duplicates)
                    for match_id in match_ids:
                        cursor.execute("""
                            INSERT OR IGNORE INTO MatchIDs (
                                match_id, summoner_puuid, region
                            ) VALUES (?, ?, ?)
                        """, (match_id, summoner["puuid"], summoner["region"]))
                    
                    logging.info(f"Processed {len(match_ids)} matches for summoner {summoner['puuid']}")

                except Exception as e:
                    logging.error(f"Error processing matches for summoner {summoner['puuid']}: {str(e)}")
            
            conn.commit()
        finally:
            conn.close()

    def process_summoners(self, num_batches: int = None) -> None:
        """Process summoners in batches, with option to limit number of batches."""
        summoners = self.get_summoners_for_match_fetch()
        total_summoners = len(summoners)
        total_possible_batches = (total_summoners + self.batch_size - 1) // self.batch_size
        
        # Calculate estimated time
        estimated_time_per_batch = self.rate_limit_window
        total_estimated_time = total_possible_batches * estimated_time_per_batch

        logging.info(f"Found {total_summoners} summoners needing match IDs")
        logging.info(f"This will require {total_possible_batches} batches total")
        logging.info(f"Estimated time for all batches: {total_estimated_time/60:.1f} minutes")

        if num_batches is None:
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
            self.update_match_ids_batch(batch)
            
            elapsed_time = time.time() - start_time
            wait_time = max(0, self.rate_limit_window - elapsed_time)
            
            if wait_time > 0 and i + self.batch_size < total_summoners and i//self.batch_size + 1 < batches_to_process:
                logging.info(f"Rate limit window - waiting {wait_time:.2f} seconds before next batch")
                time.sleep(wait_time)

def main():
    fetcher = MatchIDFetcher()
    fetcher.process_summoners()

if __name__ == "__main__":
    setup_logging("fetch_match_ids")
    main() 