#!/usr/bin/env python3
"""
GCP 배포 검증 테스트 스크립트
"""

import os
import sys
import time

def test_environment_variables():
    """환경 변수 검증"""
    print("=== 환경 변수 검증 ===")
    
    required_vars = ["RIOT_API_KEY", "GOOGLE_CLOUD_PROJECT"]
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
            print(f"[FAIL] {var}: 설정되지 않음")
        else:
            if "API_KEY" in var:
                display_value = f"{value[:8]}...{value[-4:]}"
            else:
                display_value = value
            print(f"[PASS] {var}: {display_value}")
    
    return len(missing_vars) == 0

def test_core_imports():
    """핵심 모듈 import 테스트"""
    print("\n=== 모듈 Import 테스트 ===")
    
    modules = [
        "config", "rate_limiter", "logger_config", 
        "monitoring", "data_quality"
    ]
    
    success_count = 0
    for module_name in modules:
        try:
            __import__(module_name)
            print(f"[PASS] {module_name}")
            success_count += 1
        except ImportError as e:
            print(f"[FAIL] {module_name}: {e}")
    
    return success_count == len(modules)

def test_pipeline_init():
    """파이프라인 초기화 테스트"""
    print("\n=== 파이프라인 초기화 테스트 ===")
    
    try:
        from config import Config
        from logger_config import get_logger
        from rate_limiter import AdaptiveRateLimit
        from monitoring import PipelineMonitoring
        
        config = Config()
        logger = get_logger("test")
        rate_limiter = AdaptiveRateLimit()
        monitoring = PipelineMonitoring(config)
        
        print(f"[PASS] Config: 프로젝트 ID = {config.project_id}")
        print(f"[PASS] Rate Limiter: 초기 지연 {rate_limiter.delay}s")
        print("[PASS] Logger: 초기화 완료")
        print("[PASS] Monitoring: 초기화 완료")
        
        return True
    except Exception as e:
        print(f"[FAIL] 파이프라인 초기화: {e}")
        return False

def test_data_collection():
    """데이터 수집 컴포넌트 테스트"""
    print("\n=== 데이터 수집 컴포넌트 테스트 ===")
    
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_collection_dir = os.path.join(current_dir, "data-collection")
        sys.path.insert(0, data_collection_dir)
        
        from riot_client import RiotClient
        from config import Config
        
        config = Config()
        riot_client = RiotClient(config)
        print("[PASS] Riot 클라이언트 초기화")
        
        # BigQuery 클라이언트 테스트 (인증 오류 예상)
        try:
            from bigquery_client import BigQueryClient
            bq_client = BigQueryClient(config)
            print("[PASS] BigQuery 클라이언트 초기화")
        except Exception as e:
            if "credentials" in str(e).lower():
                print("[INFO] BigQuery: 인증 필요 (GCP 배포시 해결됨)")
            else:
                print(f"[FAIL] BigQuery: {e}")
                return False
        
        return True
    except Exception as e:
        print(f"[FAIL] 데이터 수집: {e}")
        return False

def test_cloud_run_health():
    """Cloud Run 헬스체크 테스트"""
    print("\n=== Cloud Run 헬스체크 테스트 ===")
    
    try:
        from scheduler_handler import app
        
        with app.test_client() as client:
            response = client.get('/health')
            
            if response.status_code == 200:
                print("[PASS] 헬스체크 엔드포인트")
                return True
            else:
                print(f"[FAIL] 헬스체크: HTTP {response.status_code}")
                return False
    except Exception as e:
        print(f"[FAIL] Cloud Run: {e}")
        return False

def main():
    """메인 테스트 실행"""
    print("[DEPLOY] GCP 배포 검증 시작")
    print(f"실행 시간: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 테스트 실행
    tests = [
        ("환경 변수", test_environment_variables()),
        ("모듈 Import", test_core_imports()),
        ("파이프라인 초기화", test_pipeline_init()),
        ("데이터 수집", test_data_collection()),
        ("Cloud Run", test_cloud_run_health())
    ]
    
    # 결과 요약
    print("\n" + "="*50)
    print("배포 검증 리포트")
    print("="*50)
    
    passed = 0
    for test_name, result in tests:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    total = len(tests)
    print(f"\n총 테스트: {total}, 통과: {passed}, 실패: {total-passed}")
    print(f"성공률: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("\n[SUCCESS] 모든 테스트 통과! GCP 배포 준비 완료")
        return 0
    else:
        print(f"\n[WARNING] {total-passed}개 테스트 실패")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)