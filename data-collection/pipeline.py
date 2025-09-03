from riot_client import RiotClient
from bigquery_client import BigQueryClient

def run_data_pipeline():
    print("Riot 데이터 파이프라인 시작")

    # 클라이언트 초기화
    riot_client = RiotClient()
    print("Riot API 클라이언트 초기화")

    bq_client = BigQueryClient()
    print("BigQuery 클라이언트 초기화")

    # 데이터 수집
    print("챌린저 데이터 수집")
    raw_data = riot_client.get_challenger_league()

    if not raw_data:
        print("챌린저 데이터 수집 실패")
        return False
    
    # 데이터 변환
    processed_data = riot_client.extract_challenger_data(raw_data)
    print(f"{len(processed_data)}명의 챌린저 데이터 변환 완료")

    # 데이터 삽입
    print("BigQuery에 데이터 저장 중")

    dataset_check = bq_client.create_dataset_if_not_exists()
    table_check = bq_client.create_challengers_table_if_not_exists()

    if not dataset_check or not table_check:
        print("BigQuery 설정 실패")
        return False

    success = bq_client.insert_challenger_data(processed_data)

    if success:
        print("저장된 데이터 확인 중")
        bq_client.test_connection()
        return True
    else:
        print("데이터 저장 실패")
        return False
    
if __name__ == "__main__":
    run_data_pipeline()