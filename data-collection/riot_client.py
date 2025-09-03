import os
import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
from dotenv import load_dotenv


class RiotClient:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("RIOT_API_KEY")
        self.base_url = "https://kr.api.riotgames.com"
        self.queue = "RANKED_SOLO_5x5"
        self.headers = {
            'X-Riot-Token': self.api_key
        }
        self.kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
    
    def get_challenger_league(self) -> Optional[Dict]:
        """챌린저 리그 정보 조회"""

        url = f"{self.base_url}/lol/league/v4/challengerleagues/by-queue/{self.queue}"

        try:
            response = requests.get(url, headers=self.headers)

            if response.status_code == 200:
                return response.json()
            else:
                print(f"API 호출 실패 : {response.status_code}")

        except Exception as e:
            print(f"에러 : {e}")
            return None

    def extract_challenger_data(self, raw_data: Dict) -> List[Dict]:
        """챌린저 데이터 변환"""

        if not raw_data or "entries" not in raw_data:
            return []
        
        processed_data = []
        current_time = self.kst_now

        for entry in raw_data["entries"]:
            processed_entry = {
                  'puuid': entry['puuid'],
                  'league_points': entry['leaguePoints'],
                  'wins': entry['wins'],
                  'losses': entry['losses'],
                  'is_veteran': entry['veteran'],
                  'is_hot_streak': entry['hotStreak'],
                  'collected_at': current_time
              }
            processed_data.append(processed_entry)

        return processed_data

if __name__ == "__main__":
    client = RiotClient()

    print("챌린저 리그 데이터 수집 중")

    raw_data = client.get_challenger_league()

    if raw_data:
        processed = client.extract_challenger_data(raw_data)

        print(f"총 {len(processed)}명의 데이터 수집 완료")

        for i,player in enumerate(processed[:5]):
            print(f"{i+1}. LP: {player['league_points']}, 승률 : {player['wins']}/{player['losses']}")

