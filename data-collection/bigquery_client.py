import os
from datetime import datetime
from typing import List, Dict
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

class BigQueryClient:
    def __init__(self):
        load_dotenv()
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_id = "riot_analytics"
        self.table_id = "challengers"

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
        """데이터 bigquery에 삽입"""

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
        

if __name__ == "__main__":
    bq_client = BigQueryClient()

    print("1. 데이터셋 생성")
    bq_client.create_dataset_if_not_exists()

    print("2. 테이블 생성")
    bq_client.create_challengers_table_if_not_exists()

    print("1. 연결 테스트")
    bq_client.test_connection()

