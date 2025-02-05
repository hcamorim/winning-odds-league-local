import sqlite3
import logging
import shutil
from datetime import datetime
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path="riot_data.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database and create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # Check if Summoners table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='Summoners'
            """)
            exists = cursor.fetchone()
            
            if not exists:
                # Create new table with timestamps
                cursor.execute("""
                    CREATE TABLE Summoners (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        summonerID TEXT NOT NULL,
                        rank TEXT NOT NULL,
                        region TEXT NOT NULL,
                        puuid TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(summonerID, region)
                    )
                """)
            else:
                # Check if timestamp columns exist, add them if they don't
                cursor.execute("PRAGMA table_info(Summoners)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'created_at' not in columns:
                    cursor.execute("ALTER TABLE Summoners ADD COLUMN created_at TIMESTAMP")
                    cursor.execute("UPDATE Summoners SET created_at = CURRENT_TIMESTAMP")
                if 'updated_at' not in columns:
                    cursor.execute("ALTER TABLE Summoners ADD COLUMN updated_at TIMESTAMP")
                    cursor.execute("UPDATE Summoners SET updated_at = CURRENT_TIMESTAMP")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS MatchIDs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id TEXT NOT NULL,
                    summoner_puuid TEXT NOT NULL,
                    region TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(match_id),
                    FOREIGN KEY(summoner_puuid) REFERENCES Summoners(puuid)
                )
            """)
            
            # Add MatchMetadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS MatchMetadata (
                    match_id TEXT PRIMARY KEY,
                    game_duration INTEGER,
                    game_version TEXT,
                    queue_id INTEGER,
                    winner_team_id INTEGER,
                    early_surrender BOOLEAN,
                    game_start_timestamp INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(match_id) REFERENCES MatchIDs(match_id)
                )
            """)
            
            conn.commit()
        finally:
            conn.close()

    def create_backup(self):
        """Create a backup of the database with timestamp."""
        if not Path(self.db_path).exists():
            logging.warning("No database file exists yet to backup.")
            return None
        
        # Create backups directory if it doesn't exist
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        
        # Create backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"riot_data_backup_{timestamp}.db"
        
        # Copy database file
        shutil.copy2(self.db_path, backup_path)
        logging.info(f"Created database backup: {backup_path}")
        return backup_path

    def get_connection(self):
        """Get a database connection."""
        return sqlite3.connect(self.db_path)

    def update_summoners(self, summoners):
        """Update the summoners table with new data."""
        # Create backup before updating
        self.create_backup()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            # Create temporary table
            cursor.execute("DROP TABLE IF EXISTS temp_summoners")
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_summoners (
                    summonerID TEXT NOT NULL,
                    rank TEXT NOT NULL,
                    region TEXT NOT NULL
                )
            """)

            # Insert new data into temporary table
            cursor.executemany(
                "INSERT INTO temp_summoners (summonerID, rank, region) VALUES (?, ?, ?)",
                [(s["summonerID"], s["rank"], s["region"]) for s in summoners]
            )

            # Get counts before update
            cursor.execute("SELECT COUNT(*) FROM Summoners")
            count_before = cursor.fetchone()[0]

            # Update existing records (only if rank changed)
            cursor.execute("""
                UPDATE Summoners
                SET 
                    rank = (
                        SELECT temp.rank 
                        FROM temp_summoners temp 
                        WHERE temp.summonerID = Summoners.summonerID 
                        AND temp.region = Summoners.region
                    ),
                    updated_at = CURRENT_TIMESTAMP
                WHERE EXISTS (
                    SELECT 1 
                    FROM temp_summoners temp 
                    WHERE temp.summonerID = Summoners.summonerID 
                    AND temp.region = Summoners.region
                    AND temp.rank != Summoners.rank
                )
            """)
            updated_count = cursor.rowcount

            # Insert new records
            cursor.execute("""
                INSERT INTO Summoners (summonerID, rank, region, created_at, updated_at)
                SELECT 
                    summonerID, 
                    rank, 
                    region,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                FROM temp_summoners
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM Summoners 
                    WHERE Summoners.summonerID = temp_summoners.summonerID 
                    AND Summoners.region = temp_summoners.region
                )
            """)
            inserted_count = cursor.rowcount

            # Delete outdated records
            cursor.execute("""
                DELETE FROM Summoners
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM temp_summoners
                    WHERE temp_summoners.summonerID = Summoners.summonerID
                    AND temp_summoners.region = Summoners.region
                )
            """)
            deleted_count = cursor.rowcount

            # Get count after update
            cursor.execute("SELECT COUNT(*) FROM Summoners")
            count_after = cursor.fetchone()[0]

            conn.commit()
            
            return {
                'before': count_before,
                'after': count_after,
                'inserted': inserted_count,
                'updated': updated_count,
                'deleted': deleted_count
            }

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close() 