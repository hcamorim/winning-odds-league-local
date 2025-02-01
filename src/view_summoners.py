from database.db_manager import DatabaseManager
import sqlite3

def view_summoners(limit=10):
    db = DatabaseManager()
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT summonerID, rank, region, puuid, created_at, updated_at
        FROM Summoners
        ORDER BY RANDOM()
        LIMIT {limit}
    """)
    
    rows = cursor.fetchall()
    print(f"\n=== Random Summoners (Showing {limit} entries) ===")
    for row in rows:
        print(f"SummonerID: {row[0]}, Rank: {row[1]}, Region: {row[2]}, PUUID: {row[3]}, Created At: {row[4]}, Updated At: {row[5]}")

    conn.close()

if __name__ == "__main__":
    view_summoners() 