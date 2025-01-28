# Riot Prediction Local

A Python project for collecting and analyzing League of Legends player data.

## Description

This project fetches and stores data about high-ranked League of Legends players (Challenger and Grandmaster) from multiple regions using the Riot Games API. It includes features for:
- Automatic database backups
- Rate limit handling
- Batch processing for PUUID updates
- Logging system

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with your Riot Games API key:
```text
RIOT_API_KEY=your_api_key_here
```

## Project Structure

- `src/api/`: Riot Games API integration
- `src/database/`: Database management
- `src/utils/`: Utility functions and logging
- `logs/`: Application logs (automatically created)
- `backups/`: Database backups (automatically created)

## Scripts

- `fetch_summoners.py`: Fetch top-ranked summoners from Riot API
- `fetch_puuids.py`: Update PUUIDs for existing summoners (rate-limited, supports batch processing)
- `query_summoners.py`: View database statistics and summoner information

## Usage

1. Fetch top summoners:
```bash
python src/fetch_summoners.py
```

2. Update PUUIDs (with batch control):
```bash
python src/fetch_puuids.py
```

3. Query database:
```bash
python src/query_summoners.py
```

## Rate Limits

The Riot Games API has rate limits that this project handles automatically:
- 100 requests per 2 minutes for PUUID updates
- Automatic waiting between batches
- Progress tracking for long-running operations

## Data Storage

- Uses SQLite database for local storage
- Automatic backups before updates
- Timestamps for creation and updates
- Logs stored in dated files