import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from match_schema import MatchDataSchema

class BigQueryClient:
    def __init__(self):
        load_dotenv()
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_id = "riot_analytics"
        self.table_id = "challengers"
        self.schema_manager = MatchDataSchema(self)

    def create_dataset_if_not_exists(self):
        """데이터셋 없으면 생성"""

        dataset_ref = self.client.dataset(self.dataset_id)

        try:
            self.client.get_dataset(dataset_ref)
            print(f"데이터셋 --{self.dataset_id}-- 이미 존재")
            return True
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset)
            print(f"데이터셋 --{self.dataset_id}-- 생성 완료")
            return True
        
    def create_challengers_table_if_not_exists(self):
        """challengers 테이블 없으면 생성"""

        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)

        try:
            self.client.get_table(table_ref)
            print(f"테이블 --{self.table_id}-- 이미 존재")
            return True
        except NotFound:
            # 테이블 스키마 정의
            schema = [
                bigquery.SchemaField("puuid" , "STRING" , mode="REQUIRED"),
                bigquery.SchemaField("league_points", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("wins", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("losses", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("is_veteran", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("is_hot_streak", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED")
            ]

            table = bigquery.Table(table_ref, schema=schema)

            # 파티셔닝 (날짜별 분할)
            table.time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.DAY,
                field = "collected_at"
            )

            table = self.client.create_table(table)

            print(f"테이블 --{self.table_id}-- 생성 완료")
            return True
    
    def insert_challenger_data(self, data: List[Dict]):
        """챌린저 데이터 bigquery에 삽입"""

        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)

        # bigquery 형식으로 변환
        rows_to_insert = []
        for row in data:
            formatted_row = {
                "puuid": row["puuid"],
                "league_points": row["league_points"],
                "wins": row["wins"],
                "losses": row["losses"],
                "is_veteran": row["is_veteran"],
                "is_hot_streak": row["is_hot_streak"],
                "collected_at": row["collected_at"].isoformat()
            }
            rows_to_insert.append(formatted_row)

        errors = self.client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            print(f"데이터 삽입 중 에러 발생 : {errors}")
            return False
        else:
            print(f"{len(rows_to_insert)}개의 데이터 삽입 완료")
            return True
        
            
    def create_match_tables_if_not_exists(self):
        """매치 관련 테이블 생성"""
        return self.schema_manager.create_all_tables()
    

    def insert_match_data(self, matches_data: List[Dict]) -> bool:
        """매치 기본 데이터 bigquery에 삽입"""

        if not matches_data:
            print("저장할 매치 데이터가 없습니다.")
            return True
        
        table_ref = self.client.dataset(self.dataset_id).table("matches")

        rows_to_insert = []
        for match in matches_data:
            formatted_row = {
                "match_id": match["match_id"],  # 매치 고유 ID (REQUIRED)
                "data_version": match.get("data_version", "1.0"),  # API 데이터 버전 (REQUIRED)
                "game_creation": match["game_creation"].strftime('%Y-%m-%d %H:%M:%S'),  # 게임 생성 시간 KST (REQUIRED)
                "game_duration": match["game_duration"],  # 게임 지속 시간(초) (REQUIRED)
                "game_mode": match["game_mode"],  # 게임 모드 (CLASSIC, ARAM, CHERRY 등) (REQUIRED)
                "game_type": match.get("game_type", "MATCHED_GAME"),  # 게임 타입 (REQUIRED)
                "game_version": match.get("game_version", "14.18.1"),  # 게임 클라이언트 버전 (REQUIRED)
                "queue_id": match["queue_id"],  # 큐 ID (420=랭크, 1700=아레나 등) (REQUIRED)
                "map_id": match.get("map_id", 11),  # 맵 ID (11=소환사의 협곡) (REQUIRED)
                "platform_id": match.get("platform_id", "KR"),  # 플랫폼 ID (KR, NA1 등) (REQUIRED)
                "game_end_timestamp": match["game_end_timestamp"].strftime('%Y-%m-%d %H:%M:%S') if match.get("game_end_timestamp") else None,  # 게임 종료 시간 KST (NULLABLE)
                "participants_count": match["participants_count"],  # 실제 참가자 수 (REQUIRED)
                "teams_data": json.dumps(match["teams_data"], ensure_ascii=False),  # 팀별 상세 정보를 JSON 문자열로 변환 (REPEATED)
                "collected_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat()  # 데이터 수집 시간 KST (REQUIRED)
            }
            rows_to_insert.append(formatted_row)

        errors = self.client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            print(f"매치 데이터 저장 실패 : {errors}")
            return False
        else:
            print(f"{len(rows_to_insert)}개 매치 데이터 저장 완료")
            return True
        
    def insert_participants_data(self, participants_data: List[Dict]) -> bool:
        """매치 상세 정보 bigquery에 삽입"""

        if not participants_data:
            print("저장할 매치 상세 데이터가 없습니다.")
            return True
        
        table_ref = self.client.dataset(self.dataset_id).table("match_participants")

        rows_to_insert = []
        for participant in participants_data:
            formatted_row = {
                # 관계 키들
                "match_id": participant["match_id"],  # 매치 ID (REQUIRED)
                "participant_id": participant["participant_id"],  # 참가자 번호 (REQUIRED)
                "puuid": participant["puuid"],  # 플레이어 고유 ID (REQUIRED)

                # 플레이어 기본 정보
                "summoner_name": participant["summoner_name"],  # 소환사명 (NULLABLE)
                "riot_id_game_name": participant["riot_id_game_name"],  # 라이엇 게임명 (NULLABLE)
                "riot_id_tagline": participant["riot_id_tagline"],  # 라이엇 태그 (NULLABLE)
                "summoner_level": participant["summoner_level"],  # 소환사 레벨 (NULLABLE)

                # 챔피언 정보
                "champion_id": participant["champion_id"],  # 챔피언 ID (REQUIRED)
                "champion_name": participant["champion_name"],  # 챔피언 이름 (REQUIRED)
                "champion_level": participant["champion_level"],  # 챔피언 레벨 (REQUIRED)

                # 게임 결과
                "win": participant["win"],  # 승리 여부 (REQUIRED)
                "team_id": participant["team_id"],  # 팀 ID (REQUIRED)
                "team_position": participant["team_position"],  # 팀 내 포지션 (NULLABLE)
                "individual_position": participant["individual_position"],  # 개별 포지션 (NULLABLE)

                # 핵심 통계 (KDA)
                "kills": participant["kills"],  # 킬 수 (REQUIRED)
                "deaths": participant["deaths"],  # 데스 수 (REQUIRED)
                "assists": participant["assists"],  # 어시스트 수 (REQUIRED)

                # 게임 플레이 통계
                "total_minions_killed": participant["total_minions_killed"],  # CS (REQUIRED)
                "neutral_minions_killed": participant["neutral_minions_killed"],  # 정글 몬스터 킬 (REQUIRED)
                "gold_earned": participant["gold_earned"],  # 획득 골드 (REQUIRED)
                "total_damage_dealt_to_champions": participant["total_damage_dealt_to_champions"],  # 챔피언 딜량 (REQUIRED)
                "vision_score": participant["vision_score"],  # 시야 점수 (REQUIRED)

                # 아이템 정보
                "item0": participant["item0"],  # 아이템 슬롯 0 (NULLABLE)
                "item1": participant["item1"],  # 아이템 슬롯 1 (NULLABLE)
                "item2": participant["item2"],  # 아이템 슬롯 2 (NULLABLE)
                "item3": participant["item3"],  # 아이템 슬롯 3 (NULLABLE)
                "item4": participant["item4"],  # 아이템 슬롯 4 (NULLABLE)
                "item5": participant["item5"],  # 아이템 슬롯 5 (NULLABLE)
                "item6": participant["item6"],  # 아이템 슬롯 6 (NULLABLE)

                # 스펠 정보
                "summoner1_id": participant["summoner1_id"],  # 소환사 주문 1 (NULLABLE)
                "summoner2_id": participant["summoner2_id"],  # 소환사 주문 2 (NULLABLE)

                # 특수 모드
                "placement": participant["placement"],  # 순위 (아레나 모드용) (NULLABLE)
                "subteam_placement": participant["subteam_placement"],  # 서브팀 순위 (NULLABLE)

                # 상세 통계 및 메타데이터
                "detailed_stats": json.dumps(participant["detailed_stats"], ensure_ascii=False),  # 전체 상세 통계를 JSON 문자열로 변환 (NULLABLE)
                "game_creation": participant["game_creation"].strftime('%Y-%m-%d %H:%M:%S'),  # 게임 생성 시간 KST (REQUIRED)
                "collected_at": participant["collected_at"].strftime('%Y-%m-%d %H:%M:%S')  # 데이터 수집 시간 KST (REQUIRED)
            }
            rows_to_insert.append(formatted_row)
        
        errors = self.client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            print(f"매치 상세 데이터 저장 실패 : {errors}")
            return False
        else:
            print(f"{len(rows_to_insert)}개 매치 상세 데이터 저장 완료")
            return True


    def test_connection(self):
        """연결 테스트"""

        try:
            query = f"SELECT COUNT(*) as total FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`"
            result = self.client.query(query).result()

            for row in result:
                print(f"BigQuery 연결 성공! 현재 데이터 수 : {row.total}개")
                return True
        
        except Exception as e:
            print(f"BigQuery 연결 실패 : {e}")
            return False
        
    def test_match_data_connection(self):
        """매치 데이터 연결 테스트"""

        try:
            # 매치 테이블
            query = f"SELECT COUNT(*) as total FROM `{self.project_id}.{self.dataset_id}.matches`"
            result = self.client.query(query).result()

            for row in result:
                print(f"매치 테이블 연결 성공! 현재 매치 수 : {row.total}개")        

            # 매치 상세 테이블
            query = f"SELECT COUNT(*) as total FROM `{self.project_id}.{self.dataset_id}.match_participants`"
            result = self.client.query(query).result()

            for row in result:
                print(f"매치 상세 테이블 연결 성공! 현재 매치 상세 수 : {row.total}개")  
            
            return True
        except Exception as e:
            print(f"매치 테이블 연결 실패 : {e}")
            return False
            

if __name__ == "__main__":
    bq_client = BigQueryClient()

    print("1. 데이터셋 생성")
    bq_client.create_dataset_if_not_exists()

    print("2. 챌린저 테이블 생성")
    bq_client.create_challengers_table_if_not_exists()

    print("3. 매치 테이블들 생성")
    bq_client.create_match_tables_if_not_exists()

    print("4. 연결 테스트")
    bq_client.test_connection()
    bq_client.test_match_data_connection()

