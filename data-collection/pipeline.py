from riot_client import RiotClient
from bigquery_client import BigQueryClient

def run_data_pipeline():
    print("Riot 데이터 파이프라인 시작")

    # 클라이언트 초기화
    riot_client = RiotClient()
    print("Riot API 클라이언트 초기화")

    bq_client = BigQueryClient()
    print("BigQuery 클라이언트 초기화")

    dataset_check = bq_client.create_dataset_if_not_exists()
    table_check = bq_client.create_challengers_table_if_not_exists()
    match_tables_check = bq_client.create_match_tables_if_not_exists()

    if not dataset_check or not table_check or not match_tables_check:
        print("BigQuery 설정 실패")
        return False
    
    # 챌린저 데이터 수집
    print("챌린저 데이터 수집")
    raw_data = riot_client.get_challenger_league()

    if not raw_data:
        print("챌린저 데이터 수집 실패")
        return False
    
    # 챌린저 데이터 변환
    challenger_data = riot_client.extract_challenger_data(raw_data)
    print(f"{len(challenger_data)}명의 챌린저 데이터 변환 완료")

    # 챌린저 데이터 삽입
    print("챌린저 데이터 저장 중")

    challenger_success = bq_client.insert_challenger_data(challenger_data)

    if not challenger_success:
        print("데이터 저장 실패")
        return False
    
    # 매치 데이터 수집
    print("매치 데이터 수집")
    top_player = challenger_data[:10] # 상뤼 10명만 (테스트)
    
    matches, participants = riot_client.collect_matches_for_challengers(top_player, matches_per_player=5)

    if not matches or not participants:
        print("매치 데이터 수집 실패")
        return False
    
    print(f"수집 된 매치데이터 : {len(matches)}개 매치 , {len(participants)}명 참가자")

    # 매치 데이터 저장
    print("매치 데이터 저장 중")
    match_success = bq_client.insert_match_data(matches)
    participant_success = bq_client.insert_participants_data(participants)

    if not match_success or not participant_success:
        print("메치 데이터 저장 실패")
        return False
    
    # 최종 확인
    print("데이터 수집 및 저장완료")
    bq_client.test_connection()
    bq_client.test_match_data_connection()

    print(f" 최종 결과:")
    print(f"   - 챌린저: {len(challenger_data)}명")
    print(f"   - 매치: {len(matches)}개")
    print(f"   - 참가자: {len(participants)}명")

    return True
    
if __name__ == "__main__":
    success = run_data_pipeline()
    if not success:
        print("파이프라인 실행 실패")
        exit(1)