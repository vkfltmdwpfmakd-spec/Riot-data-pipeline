import os
import requests
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
from dotenv import load_dotenv

# 상위 디렉토리의 모듈들 import
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

try:
    from config import Config
    from rate_limiter import AdaptiveRateLimit
except ImportError as e:
    print(f"Import error: {e}")
    print("config.py와 rate_limiter.py가 상위 디렉토리에 있는지 확인하세요.")
    # 기본값으로 폴백
    class Config:
        RIOT_BASE_URL = "https://kr.api.riotgames.com"
        RIOT_MATCH_URL = "https://asia.api.riotgames.com"
        DEFAULT_QUEUE = "RANKED_SOLO_5x5"
        DEFAULT_MATCH_COUNT = 20
        API_RATE_LIMIT_DELAY = 0.5
        RETRY_DELAY = 2.0
        MAX_RETRIES = 3
        PLAYER_BATCH_DELAY = 1.0
        riot_api_key = os.getenv("RIOT_API_KEY")
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        dataset_id = "riot_analytics"
        is_production = os.getenv("ENV") == "production"
        matches_per_player = 20 if is_production else 5
        challenger_count = 300 if is_production else 50
    
    # 단순한 레이트 리미터 폴백
    class AdaptiveRateLimit:
        def __init__(self, initial_delay=0.5, max_delay=10.0, min_delay=0.1):
            self.delay = initial_delay
        def wait_if_needed(self):
            import time
            time.sleep(self.delay)
        def record_response(self, status_code, response_time=None):
            if status_code == 429:
                self.delay = min(self.delay * 1.5, 10.0)
        def get_stats(self):
            return {'total_requests': 0, 'rate_limited_requests': 0, 'rate_limit_percentage': 0, 'total_wait_time': 0, 'avg_wait_time_per_request': 0}

# 로거 설정
logger = logging.getLogger(__name__)

class RiotClient:
    def __init__(self, config: Optional[Config] = None):
        load_dotenv()
        self.config = config or Config()
        self.api_key = self.config.riot_api_key
        
        if not self.api_key:
            raise ValueError("RIOT_API_KEY가 설정되지 않았습니다. 환경변수를 확인해주세요.")
        
        self.base_url = self.config.RIOT_BASE_URL
        self.match_url = self.config.RIOT_MATCH_URL
        self.queue = self.config.DEFAULT_QUEUE
        
        self.headers = {
            'X-Riot-Token': self.api_key
        }
        self.kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
        
        # 적응형 레이트 리미터 설정
        self.rate_limiter = AdaptiveRateLimit(
            initial_delay=self.config.API_RATE_LIMIT_DELAY,
            max_delay=10.0,
            min_delay=0.1
        )
    
    def get_challenger_league(self) -> Optional[Dict]:
        """챌린저 리그 정보 조회"""
        url = f"{self.base_url}/lol/league/v4/challengerleagues/by-queue/{self.queue}"
        
        # 레이트 리밋 대기
        self.rate_limiter.wait_if_needed()
        
        try:
            start_time = time.time()
            response = requests.get(url, headers=self.headers, timeout=30)
            response_time = time.time() - start_time
            
            # 레이트 리미터에 응답 기록
            self.rate_limiter.record_response(response.status_code, response_time)
            
            if response.status_code == 200:
                logger.info(f"챌린저 리그 데이터 조회 성공 (응답시간: {response_time:.2f}s)")
                return response.json()
            elif response.status_code == 429:
                logger.warning(f"레이트 리밋 발생, 자동 조정됨")
                return self.get_challenger_league()  # 재귀 호출로 재시도
            else:
                logger.error(f"API 호출 실패: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.Timeout:
            logger.error(f"API 호출 타임아웃: {url}")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"네트워크 연결 오류: {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"요청 에러: {e}")
            return None
        except Exception as e:
            logger.error(f"예상치 못한 에러: {e}")
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
    
    def get_match_ids_by_puuid(self, puuid: str, count: int = None) -> List[str]:
        """puuid 기반으로 최근 매치 조회"""
        if count is None:
            count = self.config.DEFAULT_MATCH_COUNT
            
        url = f"{self.match_url}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {"count": count}
        
        # 레이트 리밋 대기
        self.rate_limiter.wait_if_needed()

        try:
            start_time = time.time()
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response_time = time.time() - start_time
            
            # 레이트 리미터에 응답 기록
            self.rate_limiter.record_response(response.status_code, response_time)
            
            if response.status_code == 200:
                match_ids = response.json()
                logger.debug(f"매치 ID 조회 성공: {len(match_ids)}개 (PUUID: {puuid[:10]}...)")
                return match_ids
            elif response.status_code == 429:
                logger.warning("매치 ID 조회 레이트 리밋, 자동 재시도")
                return self.get_match_ids_by_puuid(puuid, count)
            elif response.status_code == 404:
                logger.warning(f"플레이어 매치 기록 없음: {puuid[:10]}...")
                return []
            else:
                logger.error(f"매치 ID 조회 실패: {response.status_code} - {response.text}")
                return []
        
        except requests.exceptions.Timeout:
            logger.error(f"매치 ID 조회 타임아웃: {puuid[:10]}...")
            return []
        except requests.exceptions.ConnectionError:
            logger.error(f"매치 ID 조회 연결 오류: {puuid[:10]}...")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"매치 ID 조회 요청 에러: {e}")
            return []
        except Exception as e:
            logger.error(f"매치 ID 조회 예상치 못한 에러: {e}")
            return []
        
    def get_match_details(self, match_id: str) -> Optional[Dict]:
        """매치 상세정보 조회"""
        url = f"{self.match_url}/lol/match/v5/matches/{match_id}"
        
        # 레이트 리밋 대기
        self.rate_limiter.wait_if_needed()

        try:
            start_time = time.time()
            response = requests.get(url, headers=self.headers, timeout=30)
            response_time = time.time() - start_time
            
            # 레이트 리미터에 응답 기록
            self.rate_limiter.record_response(response.status_code, response_time)
            
            if response.status_code == 200:
                logger.debug(f"매치 상세정보 조회 성공: {match_id}")
                return response.json()
            elif response.status_code == 429:
                logger.warning(f"매치 상세정보 조회 레이트 리밋, 자동 재시도: {match_id}")
                return self.get_match_details(match_id)
            elif response.status_code == 404:
                logger.warning(f"매치를 찾을 수 없음: {match_id}")
                return None
            else:
                logger.error(f"매치 상세 조회 실패: {response.status_code} - {match_id}")
                return None
            
        except requests.exceptions.Timeout:
            logger.error(f"매치 상세정보 조회 타임아웃: {match_id}")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"매치 상세정보 조회 연결 오류: {match_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"매치 상세정보 조회 요청 에러: {e}")
            return None
        except Exception as e:
            logger.error(f"매치 상세정보 조회 예상치 못한 에러: {e}")
            return None
        
    def extract_match_data(self, match_data: Dict) -> Dict:
        """매치 데이터 변환"""

        if not match_data:
            return {}
        
        metadata = match_data.get("metadata", {})
        info = match_data.get("info", {})

        # 기본 매치 정보
        match_record = {
            'match_id': metadata.get('matchId'),  # 매치 고유 ID (REQUIRED)
            'data_version': metadata.get('dataVersion', '1.0'),  # API 데이터 버전 (REQUIRED)
            'game_creation': datetime.fromtimestamp(info.get('gameCreation', 0) / 1000, tz=ZoneInfo("Asia/Seoul")),  # 게임 생성 시간 KST (REQUIRED)
            'game_duration': info.get('gameDuration', 0),  # 게임 지속 시간(초) (REQUIRED)
            'game_mode': info.get('gameMode', 'CLASSIC'),  # 게임 모드 (CLASSIC, ARAM, CHERRY 등) (REQUIRED)
            'game_type': info.get('gameType', 'MATCHED_GAME'),  # 게임 타입 (MATCHED_GAME 등) (REQUIRED)
            'game_version': info.get('gameVersion', '14.18'),  # 게임 클라이언트 버전 (REQUIRED)
            'queue_id': info.get('queueId', 420),  # 큐 ID (420=랭크, 1700=아레나 등) (REQUIRED)
            'map_id': info.get('mapId', 11),  # 맵 ID (11=소환사의 협곡) (REQUIRED)
            'platform_id': info.get('platformId', 'KR'),  # 플랫폼 ID (KR, NA1 등) (REQUIRED)
            'game_end_timestamp': datetime.fromtimestamp(info.get('gameEndTimestamp', 0) / 1000, tz=ZoneInfo("Asia/Seoul")) if info.get('gameEndTimestamp') else None,  # 게임 종료 시간 KST (NULLABLE)
            'participants_count': len(info.get('participants', [])),  # 실제 참가자 수 (REQUIRED)
            'teams_data': info.get('teams', [])  # 팀별 상세 정보 리스트 (REPEATED)
        }

        return match_record
    
    def extract_participants_data(self, match_data: Dict) -> List[Dict]:
        """매치 참가자 데이터 변환 (BigQuery 스키마와 완전 일치)"""

        if not match_data:
            return []
        
        metadata = match_data.get("metadata", {})
        info = match_data.get("info", {})
        match_id = metadata.get("matchId")
        game_creation_kst = datetime.fromtimestamp(info.get('gameCreation', 0) / 1000, tz=ZoneInfo("Asia/Seoul"))  # KST 변환

        participants_data = []

        for participant in info.get("participants", []):
            participant_record = {
                # 관계 키들
                'match_id': match_id,  # 매치 ID (REQUIRED)
                'participant_id': participant.get('participantId', 0),  # 참가자 번호 (REQUIRED)
                'puuid': participant.get('puuid'),  # 플레이어 고유 ID (REQUIRED)
                
                # 플레이어 기본 정보
                'summoner_name': participant.get('summonerName'),  # 소환사명 (NULLABLE)
                'riot_id_game_name': participant.get('riotIdGameName'),  # 라이엇 게임명 (NULLABLE)
                'riot_id_tagline': participant.get('riotIdTagline'),  # 라이엇 태그 (NULLABLE)
                'summoner_level': participant.get('summonerLevel'),  # 소환사 레벨 (NULLABLE)
                
                # 챔피언 정보
                'champion_id': participant.get('championId', 0),  # 챔피언 ID (REQUIRED)
                'champion_name': participant.get('championName', 'Unknown'),  # 챔피언 이름 (REQUIRED)
                'champion_level': participant.get('champLevel', 1),  # 챔피언 레벨 (REQUIRED)
                
                # 게임 결과
                'win': participant.get('win', False),  # 승리 여부 (REQUIRED)
                'team_id': participant.get('teamId', 100),  # 팀 ID (100=블루, 200=레드) (REQUIRED)
                'team_position': participant.get('teamPosition'),  # 팀 내 포지션 (NULLABLE)
                'individual_position': participant.get('individualPosition'),  # 개별 포지션 (NULLABLE)
                
                # 핵심 통계 (KDA)
                'kills': participant.get('kills', 0),  # 킬 수 (REQUIRED)
                'deaths': participant.get('deaths', 0),  # 데스 수 (REQUIRED)
                'assists': participant.get('assists', 0),  # 어시스트 수 (REQUIRED)
                
                # 게임 플레이 통계
                'total_minions_killed': participant.get('totalMinionsKilled', 0),  # CS (미니언 킬) (REQUIRED)
                'neutral_minions_killed': participant.get('neutralMinionsKilled', 0),  # 정글 몬스터 킬 (REQUIRED)
                'gold_earned': participant.get('goldEarned', 0),  # 획득 골드 (REQUIRED)
                'total_damage_dealt_to_champions': participant.get('totalDamageDealtToChampions', 0),  # 챔피언 딜량 (REQUIRED)
                'vision_score': participant.get('visionScore', 0),  # 시야 점수 (REQUIRED)
                
                # 아이템 정보 (6개 슬롯 + 장신구)
                'item0': participant.get('item0', 0),  # 아이템 슬롯 0 (NULLABLE)
                'item1': participant.get('item1', 0),  # 아이템 슬롯 1 (NULLABLE)
                'item2': participant.get('item2', 0),  # 아이템 슬롯 2 (NULLABLE)
                'item3': participant.get('item3', 0),  # 아이템 슬롯 3 (NULLABLE)
                'item4': participant.get('item4', 0),  # 아이템 슬롯 4 (NULLABLE)
                'item5': participant.get('item5', 0),  # 아이템 슬롯 5 (NULLABLE)
                'item6': participant.get('item6', 0),  # 아이템 슬롯 6 (장신구) (NULLABLE)
                
                # 스펠 정보
                'summoner1_id': participant.get('summoner1Id'),  # 소환사 주문 1 (NULLABLE)
                'summoner2_id': participant.get('summoner2Id'),  # 소환사 주문 2 (NULLABLE)
                
                # 특수 모드 (아레나 등)
                'placement': participant.get('placement'),  # 순위 (아레나 모드용) (NULLABLE)
                'subteam_placement': participant.get('subteamPlacement'),  # 서브팀 순위 (NULLABLE)
                
                # 상세 통계 및 메타데이터
                'detailed_stats': participant,  # 전체 상세 통계 (JSON) (NULLABLE)
                'game_creation': game_creation_kst,  # 게임 생성 시간 KST (파티셔닝용) (REQUIRED)
                'collected_at': datetime.now(ZoneInfo("Asia/Seoul"))  # 데이터 수집 시간 KST (REQUIRED)
            }

            participants_data.append(participant_record)

        return participants_data
    
    def collect_matches_for_challengers(self, challenger_data: List[Dict], matches_per_player: int = 5) -> tuple[List[Dict], List[Dict]]:
        """챌린저 유저들 매치 데이터 수집"""

        all_matches = []
        all_participants = []
        processed_match_ids = set()

        print(f"총 {len(challenger_data)}명의 챌린저 유저 매치 수집 시작")

        for i, player in enumerate(challenger_data):
            puuid = player['puuid']
            print(f"{i+1}/{len(challenger_data)} - PUUID : {puuid[:20]}")

            # 유저별 최근 매치 ID 조회
            match_ids = self.get_match_ids_by_puuid(puuid, matches_per_player)

            for match_id in match_ids:
                # 이미 처리한 매치 스킵
                if match_id in processed_match_ids:
                    continue
                
                # 매치 상세 정보 조회
                match_details = self.get_match_details(match_id)
                if not match_details:
                    continue

                # 매치 기본정보 추출
                match_record = self.extract_match_data(match_details)
                if match_record:
                    all_matches.append(match_record)

                
                # 매치 상세정보 추출
                participants = self.extract_participants_data(match_details)
                all_participants.extend(participants)

                processed_match_ids.add(match_id)
            
            # 플레이어별 처리 후 딜레이 (설정값 사용)
            if i < len(challenger_data) - 1:  # 마지막 플레이어가 아닌 경우만
                time.sleep(self.config.PLAYER_BATCH_DELAY)

        # 레이트 리미터 통계 출력
        stats = self.rate_limiter.get_stats()
        logger.info(f"매치 수집 완료: {len(all_matches)}개 매치, {len(all_participants)}명 참가자")
        logger.info(f"API 호출 통계: {stats['total_requests']}회 요청, "
                   f"{stats['rate_limited_requests']}회 레이트 리밋 "
                   f"({stats['rate_limit_percentage']:.1f}%)")
        logger.info(f"총 대기시간: {stats['total_wait_time']:.1f}초, "
                   f"평균 요청당 대기: {stats['avg_wait_time_per_request']:.2f}초")
        
        return all_matches, all_participants
    
    def get_rate_limit_stats(self) -> Dict:
        """레이트 리미터 통계 반환"""
        return self.rate_limiter.get_stats()


                    





if __name__ == "__main__":
    client = RiotClient()

    print("챌린저 리그 데이터 수집 중")

    challenger_raw = client.get_challenger_league()

    if challenger_raw:
        challenger_data = client.extract_challenger_data(challenger_raw)

        print(f"총 {len(challenger_data)}명의 데이터 수집 완료")

        for i,player in enumerate(challenger_data[:5]):
            print(f"{i+1}. LP: {player['league_points']}, 승률 : {player['wins']}/{player['losses']}")
    else:
        print("챌린저 데이터가 없습니다.")

    print("매치 데이터 수집 중")
    top_players = challenger_data[:10]

    matches, participants = client.collect_matches_for_challengers(top_players, matches_per_player=3)
    print(f"- 매치: {len(matches)}개")
    print(f"- 참가자 기록: {len(participants)}개")

    # 샘플데이터 출력
    if matches:
          print(f"\n첫 번째 매치 예시:")
          print(f"매치 ID: {matches[0]['match_id']}")
          print(f"게임 모드: {matches[0]['game_mode']}")
          print(f"게임 시간: {matches[0]['game_duration']}초")

    if participants:
        print(f"\n첫 번째 참가자 예시:")
        p = participants[0]
        print(f"챔피언: {p['champion_name']}")
        print(f"KDA: {p['kills']}/{p['deaths']}/{p['assists']}")
        print(f"승리: {p['win']}")




