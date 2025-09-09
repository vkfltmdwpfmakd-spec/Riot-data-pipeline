from riot_client import RiotClient
from bigquery_client import BigQueryClient
import sys
import os
import time
from datetime import datetime

# 상위 디렉토리의 모듈들 접근
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

try:
    from config import Config
    from logger_config import get_logger, configure_logging
    from monitoring import PipelineMonitoring
    logger = get_logger(__name__)
except ImportError as e:
    print(f"Import error: {e}")
    print("상위 디렉토리 모듈들을 찾을 수 없습니다. 기본값으로 실행합니다.")
    # 기본 로깅으로 폴백
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 기본 Config 클래스
    class Config:
        riot_api_key = os.getenv("RIOT_API_KEY")
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        dataset_id = "riot_analytics"
        is_production = os.getenv("ENV") == "production"
        matches_per_player = 20 if is_production else 5
        challenger_count = 300 if is_production else 50
    
    # 기본 모니터링 클래스
    class PipelineMonitoring:
        def log_pipeline_start(self): 
            logger.info("파이프라인 시작")
        def log_pipeline_failure(self, msg, stage): 
            logger.error(f"파이프라인 실패 [{stage}]: {msg}")
        def log_pipeline_success(self, stats, duration): 
            logger.info(f"파이프라인 성공: {stats}, 시간: {duration}초")
        def log_api_performance(self, stats): 
            logger.info(f"API 성능: {stats}")
    
    def configure_logging(level):
        logging.basicConfig(level=getattr(logging, level))
        
    # 로거에 추가 메서드 추가
    def add_log_methods(logger):
        def data_pipeline_log(stage, success=True, count=None, **kwargs):
            msg = f"Stage: {stage}, Success: {success}"
            if count: msg += f", Count: {count}"
            logger.info(msg)
        
        def performance_log(operation, duration, items_processed=None, **kwargs):
            msg = f"Performance - {operation}: {duration:.2f}s"
            if items_processed: msg += f", Items: {items_processed}"
            logger.info(msg)
        
        logger.data_pipeline_log = data_pipeline_log
        logger.performance_log = performance_log
        return logger
    
    logger = add_log_methods(logger)

def run_data_pipeline():
    """메인 데이터 파이프라인 실행"""
    start_time = time.time()
    
    try:
        # Config 설정 로드
        config = Config()
        
        # 로깅 설정 (환경에 따라)
        if config.is_production:
            configure_logging("INFO")
        else:
            configure_logging("DEBUG")
            
        # 모니터링 초기화
        monitoring = PipelineMonitoring(config)
        monitoring.log_pipeline_start()
        
        logger.info("Riot 데이터 파이프라인 시작", 
                   environment="Production" if config.is_production else "Development",
                   challenger_count=config.challenger_count,
                   matches_per_player=config.matches_per_player)

        # 클라이언트 초기화
        riot_client = RiotClient(config)
        bq_client = BigQueryClient(config)

        # BigQuery 설정 확인
        logger.data_pipeline_log(stage="bigquery_setup", success=True)
        
        dataset_check = bq_client.create_dataset_if_not_exists()
        table_check = bq_client.create_challengers_table_if_not_exists()
        match_tables_check = bq_client.create_match_tables_if_not_exists()

        if not all([dataset_check, table_check, match_tables_check]):
            error_msg = "BigQuery 테이블 설정 실패"
            monitoring.log_pipeline_failure(error_msg, "bigquery_setup")
            return False
    
        # 챌린저 데이터 수집
        logger.data_pipeline_log(stage="challenger_collection", success=True)
        raw_data = riot_client.get_challenger_league()

        if not raw_data:
            error_msg = "챌린저 데이터 수집 실패"
            monitoring.log_pipeline_failure(error_msg, "challenger_collection")
            return False
        
        # 챌린저 데이터 변환
        challenger_data = riot_client.extract_challenger_data(raw_data)
        logger.info("챌린저 데이터 변환 완료", 
                   challenger_count=len(challenger_data))

        # 챌린저 데이터 삽입
        logger.data_pipeline_log(stage="challenger_storage", 
                               count=len(challenger_data), 
                               success=True)
        
        challenger_success = bq_client.insert_challenger_data(challenger_data)

        if not challenger_success:
            error_msg = "챌린저 데이터 저장 실패"
            monitoring.log_pipeline_failure(error_msg, "challenger_storage")
            return False
    
        # 매치 데이터 수집 (Config 적용)
        logger.data_pipeline_log(stage="match_collection", success=True)
        top_players = challenger_data[:config.challenger_count]
        
        logger.info("매치 데이터 수집 시작",
                   target_players=len(top_players),
                   matches_per_player=config.matches_per_player)
        
        match_start_time = time.time()
        matches, participants = riot_client.collect_matches_for_challengers(
            top_players, 
            matches_per_player=config.matches_per_player
        )
        match_duration = time.time() - match_start_time

        if not matches or not participants:
            error_msg = "매치 데이터 수집 실패"
            monitoring.log_pipeline_failure(error_msg, "match_collection")
            return False
        
        logger.performance_log(
            operation="match_data_collection",
            duration=match_duration,
            items_processed=len(matches),
            matches_collected=len(matches),
            participants_collected=len(participants)
        )

        # 매치 데이터 저장
        logger.data_pipeline_log(stage="match_storage", 
                               count=len(matches), 
                               success=True)
        
        match_success = bq_client.insert_match_data(matches)
        participant_success = bq_client.insert_participants_data(participants)

        if not match_success or not participant_success:
            error_msg = "매치 데이터 저장 실패"
            monitoring.log_pipeline_failure(error_msg, "match_storage")
            return False
        
        # API 성능 통계 로깅
        rate_limit_stats = riot_client.get_rate_limit_stats()
        monitoring.log_api_performance(rate_limit_stats)
        
        # 최종 확인
        bq_client.test_connection()
        bq_client.test_match_data_connection()

        # 파이프라인 완료
        total_duration = time.time() - start_time
        final_stats = {
            'challengers': len(challenger_data),
            'matches': len(matches),
            'participants': len(participants),
            'api_requests': rate_limit_stats.get('total_requests', 0),
            'rate_limited_requests': rate_limit_stats.get('rate_limited_requests', 0)
        }
        
        monitoring.log_pipeline_success(final_stats, total_duration)
        
        logger.info("데이터 파이프라인 완료", 
                   total_duration_seconds=total_duration,
                   **final_stats)

        return True
        
    except Exception as e:
        error_duration = time.time() - start_time
        logger.error("파이프라인 실행 중 예상치 못한 오류",
                    error=str(e),
                    duration_before_error=error_duration)
        
        if 'monitoring' in locals():
            monitoring.log_pipeline_failure(str(e), "unexpected_error")
            
        return False
    
if __name__ == "__main__":
    success = run_data_pipeline()
    if not success:
        print("파이프라인 실행 실패")
        exit(1)