import sqlite3
import pandas as pd

def analyze_game_durations(db_path="riot_data.db"):
    # 1) Connect to the database
    conn = sqlite3.connect(db_path)
    try:
        # 2) Fetch all game durations from MatchMetadata
        query = "SELECT game_duration FROM MatchMetadata"
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    # 3) Convert durations to minutes
    df['duration_mins'] = df['game_duration'] / 60.0

    # Basic stats
    avg_duration = df['duration_mins'].mean()
    median_duration = df['duration_mins'].median()
    q1 = df['duration_mins'].quantile(0.25)
    q3 = df['duration_mins'].quantile(0.75)

    # Define IQR-based outliers
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    outliers = df[(df['duration_mins'] < lower_bound) | (df['duration_mins'] > upper_bound)]

    # Percentages below certain time thresholds
    total = len(df)
    pct_below_20 = (df[df['duration_mins'] < 20].shape[0] / total) * 100
    pct_below_25 = (df[df['duration_mins'] < 25].shape[0] / total) * 100
    pct_below_30 = (df[df['duration_mins'] < 30].shape[0] / total) * 100

    print(f"Total matches: {total}")
    print(f"Average duration: {avg_duration:.2f} min")
    print(f"Median duration: {median_duration:.2f} min")
    print(f"25th quartile: {q1:.2f} min  |  75th quartile: {q3:.2f} min")

    print(f"\nEstimated outliers using IQR (below {lower_bound:.2f} or above {upper_bound:.2f}): {len(outliers)}")
    print("A few outlier examples:")
    print(outliers.head())  # Show a few outlier rows

    print(f"\nPct below 20 min: {pct_below_20:.2f}%")
    print(f"Pct below 25 min: {pct_below_25:.2f}%")
    print(f"Pct below 30 min: {pct_below_30:.2f}%")

if __name__ == "__main__":
    analyze_game_durations()