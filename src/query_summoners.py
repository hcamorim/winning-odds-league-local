from database.db_manager import DatabaseManager
import sqlite3

def query_summoner_stats():
    db = DatabaseManager()
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()

    print("\n=== Summoners by Rank ===")
    cursor.execute("""
        SELECT rank, COUNT(*) as count
        FROM Summoners
        GROUP BY rank
        ORDER BY count DESC
    """)
    for rank, count in cursor.fetchall():
        print(f"{rank}: {count}")

    print("\n=== Summoners by Region ===")
    cursor.execute("""
        SELECT region, COUNT(*) as count
        FROM Summoners
        GROUP BY region
        ORDER BY count DESC
    """)
    for region, count in cursor.fetchall():
        print(f"{region}: {count}")

    print("\n=== Summoners by Region and Rank ===")
    cursor.execute("""
        SELECT region, rank, COUNT(*) as count
        FROM Summoners
        GROUP BY region, rank
        ORDER BY region, rank
    """)
    current_region = None
    for region, rank, count in cursor.fetchall():
        if region != current_region:
            print(f"\n{region}:")
            current_region = region
        print(f"  {rank}: {count}")

    # Show one random example at the end
    print("\n=== Example Summoner ===")
    cursor.execute("""
        SELECT summonerID, region, rank
        FROM Summoners
        ORDER BY RANDOM()
        LIMIT 1
    """)
    example = cursor.fetchone()
    print(f"Random summoner: {example}")

    conn.close()

if __name__ == "__main__":
    query_summoner_stats() 