from google.cloud import bigquery
from typing import List
import structlog
from google.cloud.exceptions import NotFound

logger = structlog.get_logger()

class MatchDataSchema:
    def __init__(self, bigquery_client):
        self.client = bigquery_client.client
        self.project_id = bigquery_client.project_id
        self.dataset_id = bigquery_client.dataset_id

    def create_matches_table(self) -> bool:
        """매치 기본 정보 테이블 생성"""

        table_id = "matches"
        table_ref = self.client.dataset(self.dataset_id).table(table_id)

        try:
            self.client.get_table(table_ref)
            print(f"테이블 --{table_id}-- 이미 존재")
            return True
        except NotFound:
            schema = [
                # 기본키 및 메타데이터
                  bigquery.SchemaField("match_id", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("data_version", "STRING", mode="REQUIRED"),

                  # 게임 기본 정보
                  bigquery.SchemaField("game_creation", "TIMESTAMP", mode="REQUIRED"),
                  bigquery.SchemaField("game_duration", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("game_mode", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("game_type", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("game_version", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("queue_id", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("map_id", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("platform_id", "STRING", mode="REQUIRED"),

                  # 게임 결과
                  bigquery.SchemaField("game_end_timestamp", "TIMESTAMP", mode="NULLABLE"),
                  bigquery.SchemaField("participants_count", "INTEGER", mode="REQUIRED"),

                  # 팀 정보 (JSON으로 저장)
                  bigquery.SchemaField("teams_data", "JSON", mode="REPEATED"),

                  # 수집 메타데이터
                  bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED")
            ]

            table = bigquery.Table(table_ref, schema=schema)

            # 날짜별 파티셔닝
            table.time_partitioning = bigquery.TimePartitioning(field="game_creation")

            # 클러스터링
            table.clustering_fields = ["queue_id", "game_mode"]

            table = self.client.create_table(table)
            print(f"{table_id} 생성 완료")
            return True

    def create_match_participants_table(self) -> bool:
        """매치 참가자 상세 정보 테이블 생성""" 

        table_id = "match_participants"
        table_ref = self.client.dataset(self.dataset_id).table(table_id)

        try:
            self.client.get_table(table_ref)
            print(f"테이블 --{table_id}-- 이미 존재")
            return True
        except NotFound:
            schema = [
                # 관계 키들
                  bigquery.SchemaField("match_id", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("participant_id", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("puuid", "STRING", mode="REQUIRED"),

                  # 플레이어 기본 정보
                  bigquery.SchemaField("summoner_name", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("riot_id_game_name", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("riot_id_tagline", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("summoner_level", "INTEGER", mode="NULLABLE"),

                  # 챔피언 정보
                  bigquery.SchemaField("champion_id", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("champion_name", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("champion_level", "INTEGER", mode="REQUIRED"),

                  # 게임 결과
                  bigquery.SchemaField("win", "BOOLEAN", mode="REQUIRED"),
                  bigquery.SchemaField("team_id", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("team_position", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("individual_position", "STRING", mode="NULLABLE"),

                  # 핵심 통계 (KDA)
                  bigquery.SchemaField("kills", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("deaths", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("assists", "INTEGER", mode="REQUIRED"),

                  # 게임 플레이 통계
                  bigquery.SchemaField("total_minions_killed", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("neutral_minions_killed", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("gold_earned", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("total_damage_dealt_to_champions", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("vision_score", "INTEGER", mode="REQUIRED"),

                  # 아이템 정보
                  bigquery.SchemaField("item0", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("item1", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("item2", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("item3", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("item4", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("item5", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("item6", "INTEGER", mode="NULLABLE"),

                  # 스펠 정보
                  bigquery.SchemaField("summoner1_id", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("summoner2_id", "INTEGER", mode="NULLABLE"),

                  # 특수 모드 (아레나 등)
                  bigquery.SchemaField("placement", "INTEGER", mode="NULLABLE"),
                  bigquery.SchemaField("subteam_placement", "INTEGER", mode="NULLABLE"),

                  # 상세 통계 (JSON으로 모든 추가 데이터)
                  bigquery.SchemaField("detailed_stats", "JSON", mode="NULLABLE"),

                  # 메타데이터
                  bigquery.SchemaField("game_creation", "TIMESTAMP", mode="REQUIRED"),  # 파티셔닝용
                  bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED")
            ]

            table = self.client.create_table(table_ref, schema=schema)

            # 날짜별 파티셔닝
            table.time_partitioning = bigquery.TimePartitioning(field="game_creation")

            # 클러스터링
            table.clustering_fields = ["champion_id", "team_position", "win"]

            table = self.client.create_table(table)
            print(f"{table_id} 생성 완료")
            return True
    
    def create_all_tables(self) -> bool:
        """모든 매치 관련 테이블 생성"""

        print("매치데이터 관련 테이블 생성 ")

        matches_ok = self.create_matches_table()
        participants_ok = self.create_match_participants_table()

        if matches_ok and participants_ok:
            print("매치데이터 관련 테이블 생성 완료")
            return True
        else:
            return False
        

if __name__ == "__main__":
    from bigquery_client import BigQueryClient

    bq_client = BigQueryClient()
    schema_manager = MatchDataSchema(bq_client)
    schema_manager.create_all_tables()