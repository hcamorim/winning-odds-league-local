import time
from typing import List, Dict
import logging
from datetime import datetime
from database.db_manager import DatabaseManager
from api.riot_client import RiotClient
from utils.logging_config import setup_logging

logging.basicConfig(level=logging.INFO)

class MatchMetadataFetcher:
    def __init__(self):
        self.db = DatabaseManager()
        self.riot_client = RiotClient()
        self.batch_size = 100  # Maximum calls per 2 minutes
        self.rate_limit_window = 120  # 2 minutes in seconds

    def get_matches_needing_metadata(self) -> List[Dict]:
        """Get matches that don't have metadata yet."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT m.match_id, m.region
                FROM MatchIDs m
                LEFT JOIN MatchMetadata mm ON m.match_id = mm.match_id
                WHERE mm.match_id IS NULL
            """)
            return [{"match_id": row[0], "region": row[1]} for row in cursor.fetchall()]
        finally:
            conn.close()

    def update_match_metadata_batch(self, matches: List[Dict]) -> None:
        """Fetch and store metadata for a batch of matches."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            for match in matches:
                try:
                    # Get match metadata
                    match_data = self.riot_client.get_match_metadata(
                        match["match_id"],
                        match["region"]
                    )
                    
                    if match_data and match_data.get("info"):
                        info = match_data["info"]
                        cursor.execute("""
                            INSERT OR IGNORE INTO MatchMetadata (
                                match_id,
                                game_duration,
                                game_version,
                                queue_id,
                                winner_team_id,
                                early_surrender,
                                game_start_timestamp
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            match["match_id"],
                            info.get("gameDuration"),
                            info.get("gameVersion"),
                            info.get("queueId"),
                            100 if info.get("teams")[0].get("win") else 200,
                            any(team.get("earlyRendered", False) for team in info.get("teams", [])),
                            info.get("gameStartTimestamp")
                        ))
                        
                        logging.info(f"Processed metadata for match {match['match_id']}")

                except Exception as e:
                    logging.error(f"Error processing metadata for match {match['match_id']}: {str(e)}")
            
            conn.commit()
        finally:
            conn.close()

    def process_matches(self, num_batches: int = None) -> None:
        """Process matches in batches, with option to limit number of batches."""
        matches = self.get_matches_needing_metadata()
        total_matches = len(matches)
        total_possible_batches = (total_matches + self.batch_size - 1) // self.batch_size
        
        logging.info(f"Found {total_matches} matches needing metadata")
        logging.info(f"This will require {total_possible_batches} batches total")
        
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
        
        for i in range(0, min(batches_to_process * self.batch_size, total_matches), self.batch_size):
            batch = matches[i:i + self.batch_size]
            logging.info(f"\nProcessing batch {i//self.batch_size + 1} of {batches_to_process}")
            
            start_time = time.time()
            self.update_match_metadata_batch(batch)
            
            elapsed_time = time.time() - start_time
            wait_time = max(0, self.rate_limit_window - elapsed_time)
            
            if wait_time > 0 and i + self.batch_size < total_matches:
                logging.info(f"Rate limit window - waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)

def main():
    fetcher = MatchMetadataFetcher()
    fetcher.process_matches()

if __name__ == "__main__":
    setup_logging("fetch_match_metadata")
    main() 