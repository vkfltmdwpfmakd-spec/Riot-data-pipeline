import os
import requests
from dotenv import load_dotenv
import logging

load_dotenv()

def test_riot_api():

    api_key = os.getenv("RIOT_API_KEY")

    if not api_key:
        print("RIOT_API_KEY가 설정 되지 않았습니다. .env 파일을 확인해 주세요.")
        return
    
    print(f"API 키 길이: {len(api_key) if api_key else 0}")
    print(f"API 키 첫 10자리: {api_key[:10] if api_key else 'None'}")

    queue = "RANKED_SOLO_5x5"  
    url = f"https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{queue}"

    headers = {
        "X-Riot-Token" : api_key
    }

    print(f"{queue} 정보를 조회합니다.")

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            print("API 호출 성공")
            print(data)
        else:
            print("API 호출 실패")
            print(f"에러메세지 : {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"네트워크 에러 : {e}")
    except Exception as e:
        print(f"에러 : {e}")

if __name__ == "__main__":
    test_riot_api()