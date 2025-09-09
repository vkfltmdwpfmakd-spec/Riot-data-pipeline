#!/usr/bin/env python3
"""
개선된 코드의 기본 기능들을 테스트하는 스크립트
"""

import os
import sys
import traceback

# .env 파일 로딩 시도
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("dotenv로 .env 파일 로딩 성공")
except ImportError:
    print("dotenv가 없어서 환경변수 직접 로딩을 시도합니다")
    # 수동으로 .env 파일 파싱
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
        print("수동으로 .env 파일 로딩 성공")
    else:
        print("⚠️ .env 파일을 찾을 수 없습니다")

def test_basic_imports():
    """기본 모듈 import 테스트"""
    print("=== 기본 Import 테스트 ===")
    
    try:
        import config
        print("[OK] config.py import 성공")
        
        # Config 인스턴스 생성 테스트
        cfg = config.Config()
        print(f"[OK] Config 인스턴스 생성 성공")
        print(f"   - API 키 설정됨: {'Yes' if cfg.riot_api_key else 'No'}")
        print(f"   - 프로젝트 ID 설정됨: {'Yes' if cfg.project_id else 'No'}")
        print(f"   - 환경: {'Production' if cfg.is_production else 'Development'}")
        
    except Exception as e:
        print(f"[ERROR] config.py import 실패: {e}")
        traceback.print_exc()
    
    try:
        import rate_limiter
        print("[OK] rate_limiter.py import 성공")
        
        # RateLimiter 테스트
        limiter = rate_limiter.AdaptiveRateLimit()
        print("[OK] AdaptiveRateLimit 인스턴스 생성 성공")
        
    except Exception as e:
        print(f"[ERROR] rate_limiter.py import 실패: {e}")
        traceback.print_exc()
    
    try:
        import logger_config
        print("[OK] logger_config.py import 성공")
        
        # Logger 테스트
        logger = logger_config.get_logger("test")
        logger.info("로거 테스트 메시지", test_param="success")
        print("[OK] StructuredLogger 테스트 성공")
        
    except Exception as e:
        print(f"[ERROR] logger_config.py import 실패: {e}")
        traceback.print_exc()

def test_data_collection_modules():
    """data-collection 디렉토리 모듈들 테스트"""
    print("\n=== Data Collection 모듈 테스트 ===")
    
    # data-collection 디렉토리를 sys.path에 추가
    data_collection_dir = os.path.join(os.path.dirname(__file__), 'data-collection')
    sys.path.insert(0, data_collection_dir)
    
    try:
        import riot_client
        print("[OK] riot_client.py import 성공")
        
        # RiotClient 인스턴스 생성 테스트 (API 키 없어도 테스트)
        if os.getenv("RIOT_API_KEY"):
            client = riot_client.RiotClient()
            print("[OK] RiotClient 인스턴스 생성 성공")
        else:
            print("[WARN] RIOT_API_KEY 없음, RiotClient 인스턴스 생성 건너뛰기")
        
    except Exception as e:
        print(f"[ERROR] riot_client.py import 실패: {e}")
        traceback.print_exc()
    
    try:
        import bigquery_client
        print("[OK] bigquery_client.py import 성공")
        
        # BigQueryClient 테스트는 GCP 인증이 필요하므로 건너뛰기
        print("[WARN] BigQueryClient 인스턴스 생성은 GCP 인증이 필요하여 건너뛰기")
        
    except Exception as e:
        print(f"[ERROR] bigquery_client.py import 실패: {e}")
        traceback.print_exc()
    
    try:
        import pipeline
        print("[OK] pipeline.py import 성공")
        
    except Exception as e:
        print(f"[ERROR] pipeline.py import 실패: {e}")
        traceback.print_exc()

def test_rate_limiter():
    """레이트 리미터 기능 테스트"""
    print("\n=== Rate Limiter 기능 테스트 ===")
    
    try:
        from rate_limiter import AdaptiveRateLimit
        
        limiter = AdaptiveRateLimit(initial_delay=0.1)
        
        # 성공 응답 시뮬레이션
        limiter.record_response(200, 0.5)
        print("[OK] 성공 응답 기록")
        
        # 레이트 리밋 응답 시뮬레이션
        limiter.record_response(429, 1.0)
        print("[OK] 레이트 리밋 응답 기록")
        
        # 통계 확인
        stats = limiter.get_stats()
        print(f"[OK] 통계 조회: {stats}")
        
    except Exception as e:
        print(f"[ERROR] Rate Limiter 테스트 실패: {e}")
        traceback.print_exc()

def test_monitoring():
    """모니터링 모듈 테스트"""
    print("\n=== Monitoring 모듈 테스트 ===")
    
    try:
        import monitoring
        
        # 기본 Config 없이 테스트
        monitor = monitoring.PipelineMonitoring()
        monitor.log_pipeline_start()
        
        test_stats = {
            'challengers': 100,
            'matches': 50,
            'participants': 500
        }
        monitor.log_pipeline_success(test_stats, 30.5)
        
        print("[OK] 모니터링 기본 기능 테스트 성공")
        
    except Exception as e:
        print(f"[ERROR] Monitoring 테스트 실패: {e}")
        traceback.print_exc()

def test_environment():
    """환경 변수 확인"""
    print("\n=== 환경 변수 확인 ===")
    
    required_vars = ["RIOT_API_KEY", "GOOGLE_CLOUD_PROJECT"]
    optional_vars = ["ENV", "LOG_LEVEL"]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"[OK] {var}: 설정됨")
        else:
            print(f"[ERROR] {var}: 설정되지 않음 (필수)")
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"[OK] {var}: {value}")
        else:
            print(f"[WARN] {var}: 설정되지 않음 (선택사항)")

def main():
    print("Riot Data Pipeline - 수정된 코드 테스트 시작\n")
    
    test_environment()
    test_basic_imports()
    test_rate_limiter()
    test_monitoring()
    test_data_collection_modules()
    
    print("\n테스트 완료!")
    print("\n다음 단계:")
    print("1. 필요한 환경 변수를 설정하세요")
    print("2. 'pip install -r data-collection/requirements.txt' 실행")
    print("3. 'python data-collection/pipeline.py'로 파이프라인 실행")

if __name__ == "__main__":
    main()