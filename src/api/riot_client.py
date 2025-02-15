import os
import time
import requests
from dotenv import load_dotenv
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)

load_dotenv()

class RiotClient:
    def __init__(self):
        self.api_key = os.getenv('RIOT_API_KEY')
        if not self.api_key:
            raise ValueError("RIOT_API_KEY not found in environment variables")
        
        self.headers = {"X-Riot-Token": self.api_key}
        self.regions = ["euw1", "eun1", "kr", "na1"]
        self.ranks = [
            ("challenger", "Challenger"),
            ("grandmaster", "Grandmaster"),
        ]

    def fetch_top_summoners(self):
        """Fetch top-ranked summoners from all regions."""
        summoners = []
        base_url = "https://{region}.api.riotgames.com/lol/league/v4/{rank}leagues/by-queue/RANKED_SOLO_5x5"

        for region in self.regions:
            for api_rank, rank_label in self.ranks:
                url = base_url.format(region=region, rank=api_rank)
                
                try:
                    response = requests.get(url, headers=self.headers)

                    if response.status_code == 429:  # Rate limit exceeded
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
                        time.sleep(retry_after)
                        continue

                    if response.status_code != 200:
                        logging.error(f"Failed to fetch {rank_label} summoners for {region}: {response.text}")
                        continue

                    league_data = response.json()
                    if "entries" not in league_data:
                        logging.error(f"Unexpected API response format: {league_data}")
                        continue

                    for entry in league_data["entries"]:
                        summoners.append({
                            "summonerID": entry["summonerId"],
                            "rank": rank_label,
                            "region": region
                        })

                except Exception as e:
                    logging.error(f"Error fetching data for {region} {rank_label}: {str(e)}")
                    continue

        return summoners 

    def get_summoner_by_id(self, summoner_id: str, region: str) -> dict:
        """Fetch summoner data by summoner ID."""
        url = f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 429:  # Rate limit exceeded
            retry_after = int(response.headers.get("Retry-After", 60))
            logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return self.get_summoner_by_id(summoner_id, region)  # Retry the request
        
        response.raise_for_status()
        return response.json() 

    def get_matches_by_puuid(self, puuid: str, region: str, start_time: int = None) -> List[str]:
        """Fetch match IDs for a summoner."""
        region_routing = self._get_region_routing(region)
        url = f"https://{region_routing}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        
        params = {
            "startTime": start_time,
            "queue": 420,  # Ranked Solo/Duo games only
            "count": 100   # Maximum allowed
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 429:  # Rate limit exceeded
            retry_after = int(response.headers.get("Retry-After", 60))
            logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return self.get_matches_by_puuid(puuid, region, start_time)
        
        response.raise_for_status()
        return response.json()

    def _get_region_routing(self, region: str) -> str:
        """Convert platform routing to region routing."""
        routing_map = {
            'euw1': 'europe',
            'eun1': 'europe',
            'kr': 'asia',
            'na1': 'americas'
        }
        return routing_map.get(region, 'europe') 

    def get_match_metadata(self, match_id: str, region: str) -> Dict:
        """Fetch basic match data."""
        region_routing = self._get_region_routing(region)
        url = f"https://{region_routing}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        
        response = requests.get(url, headers=self.headers)
        if response.status_code == 429:  # Rate limit exceeded
            retry_after = int(response.headers.get("Retry-After", 60))
            logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return self.get_match_metadata(match_id, region)
        
        response.raise_for_status()
        return response.json()