import os
import requests
import json
from riot_client import RiotClient


def test_match_data():
    client = RiotClient()

    print("챌린저 데이터 가져오기")
    challenger_data = client.get_challenger_league()

    if challenger_data and challenger_data["entries"]:


        puuid = challenger_data["entries"][0]["puuid"]
        print(f"테스트용 puuid : {puuid[:20]}")

        print("최근 매치 ID 조회")
        match_ids_url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?count=5"

        try:
            response = requests.get(match_ids_url, headers=client.headers)
            
            if response.status_code == 200:
                match_ids = response.json()
                
                print(f"매치 ID {len(match_ids)}개 조회 성공")

                for i,match_id in enumerate(match_ids):
                    print(f" {i+1}. {match_id}")

                if match_ids:
                    first_match_id = match_ids[0]
                    match_detail_url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{first_match_id}"

                    detail_response = requests.get(match_detail_url, headers=client.headers)

                    if detail_response.status_code == 200:
                        match_data = detail_response.json()

                        info = match_data["info"]
                        
                        if info["participants"]:
                            participants = info["participants"]


                        # JSON 구조 분석을 위한 상세 출력
                        print(f"\n📊 매치 데이터 상세 구조 분석:")

                        print("\n=== 1. 최상위 구조 ===")
                        print(f"match_data 키: {list(match_data.keys())}")
                        print(f"match_data: {match_data}")

                        print("\n=== 2. metadata 구조 ===")
                        metadata = match_data["metadata"]
                        print(f"metadata 키: {list(metadata.keys())}")
                        # print(f"metadata: {metadata}")
                        print(f"dataVersion: {metadata.get('dataVersion')}")
                        print(f"matchId: {metadata.get('matchId')}")
                        print(f"participants (PUUIDs): {metadata.get('participants')[:2]}...")  # 처음 2개만

                        print("\n=== 3. info 기본 정보 ===")
                        print(f"info 키: {list(info.keys())}")
                        # print(f"info: {info}")
                        print(f"게임 모드: {info.get('gameMode')}")
                        print(f"게임 시간: {info.get('gameDuration')}초")
                        print(f"게임 생성 시간: {info.get('gameCreation')}")
                        print(f"큐 타입: {info.get('queueId')}")
                        print(f"참가자 수: {len(info.get('participants', []))}")

                        print("\n=== 4. 첫 번째 참가자 상세 ===")
                        if participants:
                            first_participant = participants[0]
                            print(f"first_participant 키: {list(first_participant.keys())}")
                            # print(f"first_participant: {first_participant}")
                            print(f"챔피언 ID: {first_participant.get('championId')}")
                            print(f"챔피언 이름: {first_participant.get('championName')}")
                            print(f"소환사명: {first_participant.get('summonerName')}")
                            print(f"KDA: {first_participant.get('kills')}/{first_participant.get('deaths')}/{first_participant.get('assists')}")
                            print(f"승리: {first_participant.get('win')}")
                            print(f"포지션: {first_participant.get('teamPosition')}")
                            print(f"레벨: {first_participant.get('champLevel')}")
                            print(f"CS: {first_participant.get('totalMinionsKilled')}")

                        print("\n=== 5. 팀 정보 ===")
                        teams = info.get('teams', [])
                        for i, team in enumerate(teams):
                            print(f"팀 {team.get('teamId')}: 승리={team.get('win')}, 킬수={team.get('kills')}")
                    else:
                        print(f"매치 상세 조회 실패 : {detail_response.status_code}")
            else:
                print(f"매치 ID 조회 실패: {response.status_code}")
                print(f"에러 메시지: {response.text}")
        except Exception as e:
            print(f"에러 발생 : {e}")
    else:
        print("챌린저 데이터 조회 실패")




if __name__ == "__main__":
    test_match_data()