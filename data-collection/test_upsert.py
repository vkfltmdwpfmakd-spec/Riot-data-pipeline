import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# 경로추가
sys.path.append("/app")

from bigquery_client import BigQueryClient

def test_challenger_upsert():
    """챌린저 데이터 UPSERT 테스트"""

    print("챌린저 데이터 UPSERT 테스트 시작")

    bq = BigQueryClient()

    # 테스트 데이터 (같은 puuid로 2번 삽입)
    test_data_1 = [{
        "puuid": "test_player_1",
        "league_points": 1000,
        "wins": 100,
        "losses": 50,
        "is_veteran": True,
        "is_hot_streak": False,
        "collected_at": datetime.now(ZoneInfo("Asia/Seoul"))
    }]

    test_data_2 = [{
        "puuid": "test_player_1",  # 같은 puuid
        "league_points": 1100,     # 업데이트된 LP
        "wins": 105,               # 업데이트된 승수
        "losses": 51,              # 업데이트된 패수
        "is_veteran": True,
        "is_hot_streak": True,     # 상태 변화
        "collected_at": datetime.now(ZoneInfo("Asia/Seoul"))
    }]

    print("1차 챌린저 삽입")
    result1 = bq.insert_challenger_data(test_data_1)
    print(f"1차 챌린저 삽입 결과 : {result1}")

    print("2차 챌린저 삽입")
    result2 = bq.insert_challenger_data(test_data_2)
    print(f"2차 챌린저 삽입 결과 : {result2}")

    print("챌린저 최종 데이터 확인")
    query = f"""
    SELECT puuid, league_points, wins, losses, is_veteran, is_hot_streak, collected_at
    FROM `{bq.project_id}.{bq.dataset_id}.{bq.table_id}`
    WHERE puuid = 'test_player_1'
    """

    try:
        result = bq.client.query(query).result()
        for row in result:
            print(f"puuid : {row.puuid}")
            print(f"league_points : {row.league_points}")
            print(f"wins : {row.wins}") 
            print(f"losses : {row.losses}")
            print(f"is_veteran : {row.is_veteran}")
            print(f"is_hot_streak : {row.is_hot_streak}")
            print(f"collected_at : {row.collected_at}")
    except Exception as e:
        print(f"쿼리 실행 실패 : {e}")


def test_match_upsert():
      """매치 데이터 UPSERT 테스트"""
      print("매치 데이터 UPSERT 테스트 시작")

      bq = BigQueryClient()

      # 테스트 매치 데이터 (같은 match_id로 2번 삽입)
      test_match_1 = [{
          "match_id": "TEST_MATCH_001",
          "data_version": "1.0",
          "game_creation": datetime.now(ZoneInfo("Asia/Seoul")),
          "game_duration": 1800,  # 30분
          "game_mode": "CLASSIC",
          "game_type": "MATCHED_GAME",
          "game_version": "14.18.1",
          "queue_id": 420,
          "map_id": 11,
          "platform_id": "KR",
          "game_end_timestamp": datetime.now(ZoneInfo("Asia/Seoul")),
          "participants_count": 10,
          "teams_data": [{"teamId": 100, "win": True}, {"teamId": 200, "win": False}]
      }]

      test_match_2 = [{
          "match_id": "TEST_MATCH_001",  # 같은 match_id
          "data_version": "1.1",         # 업데이트된 버전
          "game_creation": datetime.now(ZoneInfo("Asia/Seoul")),
          "game_duration": 1850,         # 업데이트된 시간
          "game_mode": "CLASSIC",
          "game_type": "MATCHED_GAME",
          "game_version": "14.18.2",     # 업데이트된 버전
          "queue_id": 420,
          "map_id": 11,
          "platform_id": "KR",
          "game_end_timestamp": datetime.now(ZoneInfo("Asia/Seoul")),
          "participants_count": 10,
          "teams_data": [{"teamId": 100, "win": True}, {"teamId": 200, "win": False}]
      }]

      # 첫 번째 삽입
      print("1차 매치 삽입")
      result1 = bq.insert_match_data(test_match_1)
      print(f"결과: {result1}")

      # 두 번째 삽입 (UPSERT 테스트)
      print("2차 매치 삽입")
      result2 = bq.insert_match_data(test_match_2)
      print(f"결과: {result2}")

      # 데이터 확인
      print("매치 데이터 확인")
      query = f"""
      SELECT match_id, data_version, game_duration, game_version
      FROM `{bq.project_id}.{bq.dataset_id}.matches`
      WHERE match_id = 'TEST_MATCH_001'
      """

      try:
          result = bq.client.query(query).result()
          for row in result:
              print(f"match_id: {row.match_id}")
              print(f"data_version: {row.data_version}")
              print(f"game_creation: {row.game_creation}")
              print(f"game_duration: {row.game_duration}")
              print(f"game_mode: {row.game_mode}")
              print(f"game_type: {row.game_type}")
              print(f"game_version: {row.game_version}")
              print(f"queue_id: {row.queue_id}")
              print(f"map_id: {row.map_id}")
              print(f"platform_id: {row.platform_id}")
              print(f"game_end_timestamp: {row.game_end_timestamp}")
              print(f"participants_count: {row.participants_count}")
              print(f"teams_data: {row.teams_data}")
      except Exception as e:
          print(f"매치 쿼리 실패: {e}")

def test_participants_upsert():
    """매치 상세 데이터 UPSERT 테스트"""
    print("\n매치 상세 UPSERT 테스트 시작")

    bq = BigQueryClient()

    # 테스트 매치 상세 데이터 (같은 match_id + puuid로 2번 삽입)
    test_participants_1 = [{
        "match_id": "TEST_MATCH_002",
        "participant_id": 1,
        "puuid": "test_player_participant_1",
        "summoner_name": "TestPlayer1",
        "riot_id_game_name": "TestPlayer",
        "riot_id_tagline": "KR1",
        "summoner_level": 100,
        "champion_id": 1,
        "champion_name": "Annie",
        "champion_level": 18,
        "win": True,
        "team_id": 100,
        "team_position": "MIDDLE",
        "individual_position": "MIDDLE",
        "kills": 10,
        "deaths": 5,
        "assists": 8,
        "total_minions_killed": 150,
        "neutral_minions_killed": 20,
        "gold_earned": 15000,
        "total_damage_dealt_to_champions": 25000,
        "vision_score": 30,
        "item0": 3089, "item1": 3020, "item2": 3135,
        "item3": 3165, "item4": 3157, "item5": 3116, "item6": 3364,
        "summoner1_id": 4, "summoner2_id": 14,
        "placement": None,
        "subteam_placement": None,
        "detailed_stats": {"totalDamageDealt": 30000, "magicDamageDealt": 25000},
        "game_creation": datetime.now(ZoneInfo("Asia/Seoul")),
        "collected_at": datetime.now(ZoneInfo("Asia/Seoul"))
    }]

    test_participants_2 = [{
        "match_id": "TEST_MATCH_002",      # 같은 match_id
        "participant_id": 1,
        "puuid": "test_player_participant_1",  # 같은 puuid
        "summoner_name": "TestPlayer1Updated",  # 업데이트된 이름
        "riot_id_game_name": "TestPlayer",
        "riot_id_tagline": "KR1",
        "summoner_level": 101,                  # 업데이트된 레벨
        "champion_id": 1,
        "champion_name": "Annie",
        "champion_level": 18,
        "win": True,
        "team_id": 100,
        "team_position": "MIDDLE",
        "individual_position": "MIDDLE",
        "kills": 12,                           # 업데이트된 킬수
        "deaths": 4,                           # 업데이트된 데스
        "assists": 10,                         # 업데이트된 어시스트
        "total_minions_killed": 155,
        "neutral_minions_killed": 22,
        "gold_earned": 15500,
        "total_damage_dealt_to_champions": 27000,
        "vision_score": 35,
        "item0": 3089, "item1": 3020, "item2": 3135,
        "item3": 3165, "item4": 3157, "item5": 3116, "item6": 3364,
        "summoner1_id": 4, "summoner2_id": 14,
        "placement": None,
        "subteam_placement": None,
        "detailed_stats": {"totalDamageDealt": 32000, "magicDamageDealt": 27000},
        "game_creation": datetime.now(ZoneInfo("Asia/Seoul")),
        "collected_at": datetime.now(ZoneInfo("Asia/Seoul"))
    }]

    # 첫 번째 삽입
    print("1차 매치 상세 삽입")
    result1 = bq.insert_participants_data(test_participants_1)
    print(f"결과: {result1}")

    # 두 번째 삽입 (UPSERT 테스트)
    print("2차 매치 상세 삽입")
    result2 = bq.insert_participants_data(test_participants_2)
    print(f"결과: {result2}")

    # 데이터 확인
    print("매치 상세 확인")
    query = f"""
    SELECT match_id, puuid, summoner_name, summoner_level, kills, deaths, assists
    FROM `{bq.project_id}.{bq.dataset_id}.match_participants`
    WHERE match_id = 'TEST_MATCH_002' AND puuid = 'test_player_participant_1'
    """

    try:
        result = bq.client.query(query).result()
        for row in result:
            print(f"match_id: {row.match_id}")
            print(f"participant_id: {row.participant_id}")
            print(f"puuid: {row.puuid}")
            print(f"summoner_name: {row.summoner_name}")
            print(f"riot_id_game_name: {row.riot_id_game_name}")
            print(f"riot_id_tagline: {row.riot_id_tagline}")
            print(f"summoner_level: {row.summoner_level}")
            print(f"champion_id: {row.champion_id}")
            print(f"champion_name: {row.champion_name}")
            print(f"champion_level: {row.champion_level}")
            print(f"win: {row.win}")
            print(f"team_id: {row.team_id}")
            print(f"team_position: {row.team_position}")
            print(f"individual_position: {row.individual_position}")
            print(f"kills: {row.kills}")
            print(f"deaths: {row.deaths}")
            print(f"assists: {row.assists}")
            print(f"total_minions_killed: {row.total_minions_killed}")
            print(f"neutral_minions_killed: {row.neutral_minions_killed}")
            print(f"gold_earned: {row.gold_earned}")
            print(f"total_damage_dealt_to_champions: {row.total_damage_dealt_to_champions}")
            print(f"vision_score: {row.vision_score}")
            print(f"item0: {row.item0}")
            print(f"item1: {row.item1}")
            print(f"item2: {row.item2}")
            print(f"item3: {row.item3}")
            print(f"item4: {row.item4}")
            print(f"item5: {row.item5}")
            print(f"item6: {row.item6}")
            print(f"summoner1_id: {row.summoner1_id}")
            print(f"summoner2_id: {row.summoner2_id}")
            print(f"placement: {row.placement}")
            print(f"subteam_placement: {row.subteam_placement}")
            print(f"detailed_stats: {row.detailed_stats}")
            print(f"game_creation: {row.game_creation}")
            print(f"collected_at: {row.collected_at}")
    except Exception as e:
        print(f"매치 상세 쿼리 실패: {e}")


def test_duplicate_prevention():
    """전체 테이블 중복 방지 확인"""
    print("전체 테이블 중복 방지 테스트")

    bq = BigQueryClient()

    print("중복 확인 쿼리 실행")

    # 1. 챌린저 중복 확인
    challenger_query = f"""
    SELECT
        'challengers' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT puuid) as unique_puuids,
        (COUNT(*) - COUNT(DISTINCT puuid)) as duplicates
    FROM `{bq.project_id}.{bq.dataset_id}.challengers`
    WHERE puuid LIKE 'test_%'
    """

    # 2. 매치 중복 확인
    match_query = f"""
    SELECT
        'matches' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT match_id) as unique_match_ids,
        (COUNT(*) - COUNT(DISTINCT match_id)) as duplicates
    FROM `{bq.project_id}.{bq.dataset_id}.matches`
    WHERE match_id LIKE 'TEST_%'
    """

    # 3. 매치 참가자 중복 확인
    participants_query = f"""
    SELECT
        'match_participants' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT CONCAT(match_id, '_', puuid)) as unique_combinations,
        (COUNT(*) - COUNT(DISTINCT CONCAT(match_id, '_', puuid))) as duplicates
    FROM `{bq.project_id}.{bq.dataset_id}.match_participants`
    WHERE match_id LIKE 'TEST_%'
    """

    queries = [challenger_query, match_query, participants_query]

    for query in queries:
        try:
            result = bq.client.query(query).result()
            for row in result:
                print(f"{row.table_name}:")
                print(f"전체 레코드: {row.total_records}")
                print(f"고유 키: {row.unique_match_ids if hasattr(row, 'unique_match_ids') else row.unique_puuids if hasattr(row, 'unique_puuids') else row.unique_combinations}")
                print(f"중복: {row.duplicates}")
                if row.duplicates == 0:
                    print(" 중복 없음")
                else:
                    print("중복 발견")
                print()
        except Exception as e:
            print(f"쿼리 실패: {e}")

def cleanup_test_data():
    """테스트 데이터 정리"""
    print("테스트 데이터 정리")

    bq = BigQueryClient()

    cleanup_queries = [
        f"DELETE FROM `{bq.project_id}.{bq.dataset_id}.challengers` WHERE puuid LIKE 'test_%'",
        f"DELETE FROM `{bq.project_id}.{bq.dataset_id}.matches` WHERE match_id LIKE 'TEST_%'",
        f"DELETE FROM `{bq.project_id}.{bq.dataset_id}.match_participants` WHERE match_id LIKE 'TEST_%'"
    ]

    for i, query in enumerate(cleanup_queries, 1):
        try:
            result = bq.client.query(query).result()
            table_name = ["challengers", "matches", "match_participants"][i-1]
            print(f"{table_name} 테스트 데이터 정리 완료")
        except Exception as e:
            print(f"{table_name} 정리 실패: {e}")

if __name__ == "__main__":
    print("전체 UPSERT 기능 통합 테스트 시작")

    try:
        # 모든 UPSERT 테스트 실행
        test_challenger_upsert()
        test_match_upsert()
        test_participants_upsert()
        test_duplicate_prevention()

        # 정리할지 물어보기
        response = input("테스트 데이터를 정리하시겠습니까? (y/N): ")
        if response.lower() == 'y':
            cleanup_test_data()

        print("모든 UPSERT 테스트 완료!")

    except KeyboardInterrupt:
        print("테스트 중단됨")
    except Exception as e:
        print(f"전체 테스트 실패: {e}")

    