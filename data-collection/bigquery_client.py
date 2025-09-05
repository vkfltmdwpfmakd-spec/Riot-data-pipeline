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
        """챌린저 데이터 bigquery에 삽입 MERGE 쿼리로 UPSERT (중복 방지)"""

        if not data:
            print("저장할 챌린저 데이터가 없습니다.")
            return True

        # STRUCT 배열용 데이터 준비
        struct_rows = []
        for row in data:
            struct_row = f"STRUCT('{row['puuid']}' AS puuid, {row['league_points']} AS league_points, {row['wins']} AS wins, {row['losses']} AS losses, {row['is_veteran']} AS is_veteran, {row['is_hot_streak']} AS is_hot_streak, TIMESTAMP('{row['collected_at'].isoformat()}') AS collected_at)"
            struct_rows.append(struct_row)
        
        # MERGE 쿼리 (STRUCT 배열 사용)
        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_id}.{self.table_id}` AS target
        USING (
            SELECT * FROM UNNEST([{', '.join(struct_rows)}])
        ) AS source
        ON target.puuid = source.puuid

        WHEN MATCHED THEN
            UPDATE SET
            league_points = source.league_points,
            wins = source.wins,
            losses = source.losses,
            is_veteran = source.is_veteran,
            is_hot_streak = source.is_hot_streak,
            collected_at = source.collected_at

        WHEN NOT MATCHED THEN
            INSERT (puuid, league_points, wins, losses, is_veteran, is_hot_streak, collected_at)
            VALUES (source.puuid, source.league_points, source.wins, source.losses, source.is_veteran, source.is_hot_streak, source.collected_at)
        """

        try:
            query_job = self.client.query(merge_query)
            result = query_job.result()

            print(f"MERGE 완료 - 처리된 행: {query_job.num_dml_affected_rows}개")
            return True
            
        except Exception as e:
            print(f"MERGE 실패: {e}")
            return False

            
    def create_match_tables_if_not_exists(self):
        """매치 관련 테이블 생성"""
        return self.schema_manager.create_all_tables()
    

    def insert_match_data(self, matches_data: List[Dict]) -> bool:
        """매치 기본 데이터 bigquery에 삽입 MERGE 쿼리로 UPSERT (중복 방지)"""

        if not matches_data:
            print("저장할 매치 데이터가 없습니다.")
            return True

        # STRUCT 배열용 데이터 준비
        struct_rows = []
        for match in matches_data:
            game_end_ts = f"TIMESTAMP('{match['game_end_timestamp'].strftime('%Y-%m-%d %H:%M:%S')}')" if match.get("game_end_timestamp") else "NULL"
            teams_data_json = json.dumps(match["teams_data"], ensure_ascii=False).replace("'", "\\'")
            
            struct_row = f"""STRUCT(
                '{match["match_id"]}' AS match_id,
                '{match.get("data_version", "1.0")}' AS data_version,
                TIMESTAMP('{match["game_creation"].strftime('%Y-%m-%d %H:%M:%S')}') AS game_creation,
                {match["game_duration"]} AS game_duration,
                '{match["game_mode"]}' AS game_mode,
                '{match.get("game_type", "MATCHED_GAME")}' AS game_type,
                '{match.get("game_version", "14.18.1")}' AS game_version,
                {match["queue_id"]} AS queue_id,
                {match.get("map_id", 11)} AS map_id,
                '{match.get("platform_id", "KR")}' AS platform_id,
                {game_end_ts} AS game_end_timestamp,
                {match["participants_count"]} AS participants_count,
                PARSE_JSON('{teams_data_json}') AS teams_data,
                TIMESTAMP('{datetime.now(ZoneInfo("Asia/Seoul")).isoformat()}') AS collected_at
            )"""
            struct_rows.append(struct_row)
        
        # MERGE 쿼리 (STRUCT 배열 사용)
        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_id}.matches` AS target
        USING (
            SELECT * FROM UNNEST([{', '.join(struct_rows)}])
        ) AS source
        ON target.match_id = source.match_id

        WHEN MATCHED THEN
            UPDATE SET
            data_version = source.data_version,
            game_creation = source.game_creation,
            game_duration = source.game_duration,
            game_mode = source.game_mode,
            game_type = source.game_type,
            game_version = source.game_version,
            queue_id = source.queue_id,
            map_id = source.map_id,
            platform_id = source.platform_id,
            game_end_timestamp = source.game_end_timestamp,
            participants_count = source.participants_count,
            teams_data = source.teams_data,
            collected_at = source.collected_at

        WHEN NOT MATCHED THEN
            INSERT (match_id, data_version, game_creation, game_duration, game_mode, game_type, game_version, queue_id, map_id, platform_id, game_end_timestamp, participants_count, teams_data, collected_at)
            VALUES (source.match_id, source.data_version, source.game_creation, source.game_duration, source.game_mode, source.game_type, source.game_version, source.queue_id, source.map_id, source.platform_id, source.game_end_timestamp, source.participants_count, source.teams_data, source.collected_at)
        """

        try:
            query_job = self.client.query(merge_query)
            result = query_job.result()

            print(f"MERGE 완료 - 처리된 행: {query_job.num_dml_affected_rows}개")
            return True
            
        except Exception as e:
            print(f"MERGE 실패: {e}")
            return False


        
    def insert_participants_data(self, participants_data: List[Dict]) -> bool:
        """매치 상세 정보 bigquery에 삽입 MERGE 쿼리로 UPSERT (중복 방지)"""

        if not participants_data:
            print("저장할 매치 상세 데이터가 없습니다.")
            return True

        # STRUCT 배열용 데이터 준비
        struct_rows = []
        for participant in participants_data:
            # NULL 처리 함수
            def safe_str(value):
                return f"'{value}'" if value is not None else "NULL"
            
            def safe_int(value):
                return str(value) if value is not None else "NULL"
            
            # JSON 문자열 이스케이프 처리
            detailed_stats_json = json.dumps(participant["detailed_stats"], ensure_ascii=False).replace("'", "\\'")
            
            struct_row = f"""STRUCT(
                '{participant["match_id"]}' AS match_id,
                {participant["participant_id"]} AS participant_id,
                '{participant["puuid"]}' AS puuid,
                {safe_str(participant["summoner_name"])} AS summoner_name,
                {safe_str(participant["riot_id_game_name"])} AS riot_id_game_name,
                {safe_str(participant["riot_id_tagline"])} AS riot_id_tagline,
                {safe_int(participant["summoner_level"])} AS summoner_level,
                {participant["champion_id"]} AS champion_id,
                '{participant["champion_name"]}' AS champion_name,
                {participant["champion_level"]} AS champion_level,
                {participant["win"]} AS win,
                {participant["team_id"]} AS team_id,
                {safe_str(participant["team_position"])} AS team_position,
                {safe_str(participant["individual_position"])} AS individual_position,
                {participant["kills"]} AS kills,
                {participant["deaths"]} AS deaths,
                {participant["assists"]} AS assists,
                {participant["total_minions_killed"]} AS total_minions_killed,
                {participant["neutral_minions_killed"]} AS neutral_minions_killed,
                {participant["gold_earned"]} AS gold_earned,
                {participant["total_damage_dealt_to_champions"]} AS total_damage_dealt_to_champions,
                {participant["vision_score"]} AS vision_score,
                {safe_int(participant["item0"])} AS item0,
                {safe_int(participant["item1"])} AS item1,
                {safe_int(participant["item2"])} AS item2,
                {safe_int(participant["item3"])} AS item3,
                {safe_int(participant["item4"])} AS item4,
                {safe_int(participant["item5"])} AS item5,
                {safe_int(participant["item6"])} AS item6,
                {safe_int(participant["summoner1_id"])} AS summoner1_id,
                {safe_int(participant["summoner2_id"])} AS summoner2_id,
                {safe_int(participant["placement"])} AS placement,
                {safe_int(participant["subteam_placement"])} AS subteam_placement,
                PARSE_JSON('{detailed_stats_json}') AS detailed_stats,
                TIMESTAMP('{participant["game_creation"].strftime('%Y-%m-%d %H:%M:%S')}') AS game_creation,
                TIMESTAMP('{participant["collected_at"].strftime('%Y-%m-%d %H:%M:%S')}') AS collected_at
            )"""
            struct_rows.append(struct_row)
        
        # MERGE 쿼리 (STRUCT 배열 사용)
        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_id}.match_participants` AS target
        USING (
            SELECT * FROM UNNEST([{', '.join(struct_rows)}])
        ) AS source
        ON target.match_id = source.match_id AND target.puuid = source.puuid

        WHEN MATCHED THEN
            UPDATE SET
            participant_id = source.participant_id,
            summoner_name = source.summoner_name,
            riot_id_game_name = source.riot_id_game_name,
            riot_id_tagline = source.riot_id_tagline,
            summoner_level = source.summoner_level,
            champion_id = source.champion_id,
            champion_name = source.champion_name,
            champion_level = source.champion_level,
            win = source.win,
            team_id = source.team_id,
            team_position = source.team_position,
            individual_position = source.individual_position,
            kills = source.kills,
            deaths = source.deaths,
            assists = source.assists,
            total_minions_killed = source.total_minions_killed,
            neutral_minions_killed = source.neutral_minions_killed,
            gold_earned = source.gold_earned,
            total_damage_dealt_to_champions = source.total_damage_dealt_to_champions,
            vision_score = source.vision_score,
            item0 = source.item0,
            item1 = source.item1,
            item2 = source.item2,
            item3 = source.item3,
            item4 = source.item4,
            item5 = source.item5,
            item6 = source.item6,
            summoner1_id = source.summoner1_id,
            summoner2_id = source.summoner2_id,
            placement = source.placement,
            subteam_placement = source.subteam_placement,
            detailed_stats = source.detailed_stats,
            game_creation = source.game_creation,
            collected_at = source.collected_at

        WHEN NOT MATCHED THEN
            INSERT (match_id, participant_id, puuid, summoner_name, riot_id_game_name, riot_id_tagline, summoner_level, champion_id, champion_name, champion_level, win, team_id, team_position, individual_position, kills, deaths, assists, total_minions_killed, neutral_minions_killed, gold_earned, total_damage_dealt_to_champions, vision_score, item0, item1, item2, item3, item4, item5, item6, summoner1_id, summoner2_id, placement, subteam_placement, detailed_stats, game_creation, collected_at)
            VALUES (source.match_id, source.participant_id, source.puuid, source.summoner_name, source.riot_id_game_name, source.riot_id_tagline, source.summoner_level, source.champion_id, source.champion_name, source.champion_level, source.win, source.team_id, source.team_position, source.individual_position, source.kills, source.deaths, source.assists, source.total_minions_killed, source.neutral_minions_killed, source.gold_earned, source.total_damage_dealt_to_champions, source.vision_score, source.item0, source.item1, source.item2, source.item3, source.item4, source.item5, source.item6, source.summoner1_id, source.summoner2_id, source.placement, source.subteam_placement, source.detailed_stats, source.game_creation, source.collected_at)
        """

        try:
            query_job = self.client.query(merge_query)
            result = query_job.result()

            print(f"MERGE 완료 - 처리된 행: {query_job.num_dml_affected_rows}개")
            return True
            
        except Exception as e:
            print(f"MERGE 실패: {e}")
            return False


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

