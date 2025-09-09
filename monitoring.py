import logging
import os
from typing import Dict, Any, Optional
from google.cloud import monitoring_v3
from datetime import datetime, timezone, timedelta
from config import Config
from logger_config import get_logger

# 로거 설정
logger = get_logger(__name__)

class PipelineMonitoring:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        
        try:
            self.client = monitoring_v3.MetricServiceClient()
            self.project_name = f"projects/{self.config.project_id}"
            logger.info("모니터링 클라이언트 초기화 성공", 
                       project_id=self.config.project_id)
        except Exception as e:
            logger.error("모니터링 클라이언트 초기화 실패", error=str(e))
            self.client = None
            
        self.kst = timezone(timedelta(hours=9))

    def record_metric(self, metric_name: str, value: float, labels: Dict[str, str] = None):
        """클라우드 모니터링 메트릭 전송"""
        try:
            if not self.client:
                logger.warning("모니터링 클라이언트가 없어서 메트릭을 로컬로만 기록합니다",
                             metric_name=metric_name, value=value)
                return
                
            # 로컬 로그로 메트릭 기록
            logger.performance_log(
                operation="metric_record",
                duration=0,
                metric_name=metric_name,
                value=value,
                labels=labels or {}
            )
            
            # TODO: 실제 Google Cloud Monitoring API 호출 구현
            # series = monitoring_v3.TimeSeries()
            # ... 메트릭 전송 로직
            
        except Exception as e:
            logger.error("메트릭 기록 실패", 
                        metric_name=metric_name, 
                        error=str(e))

    def send_alert(self, message: str, severity: str = "INFO", 
                  extra_data: Dict[str, Any] = None):
        """구조화된 알림 전송"""
        try:
            alert_data = {
                "alert_message": message,
                "severity": severity,
                "timestamp_kst": datetime.now(self.kst).isoformat(),
                **(extra_data or {})
            }
            
            # 심각도별 로깅
            if severity == "ERROR":
                logger.error("파이프라인 알림", **alert_data)
            elif severity == "WARNING":
                logger.warning("파이프라인 알림", **alert_data)
            else:
                logger.info("파이프라인 알림", **alert_data)
                
            # TODO: 실제 알림 전송 구현 (이메일, Slack, SMS 등)
            
        except Exception as e:
            logger.error("알림 전송 실패", error=str(e))
    
    def log_pipeline_start(self):
        """파이프라인 시작 로그"""
        logger.data_pipeline_log(
            stage="start",
            success=True,
            message="Riot 데이터 파이프라인 시작"
        )
    
    def log_pipeline_success(self, stats: Dict[str, Any], duration: float = None):
        """파이프라인 성공 로그"""
        logger.data_pipeline_log(
            stage="complete",
            success=True,
            duration=duration,
            **stats
        )
        
        message = f"파이프라인 완료 - 챌린저: {stats.get('challengers', 0)}명, 매치: {stats.get('matches', 0)}개"
        if duration:
            message += f", 실행시간: {duration:.1f}초"
            
        self.send_alert(message, "INFO", stats)
    
    def log_pipeline_failure(self, error_message: str, stage: str = "unknown", 
                            error_details: Dict[str, Any] = None):
        """파이프라인 실패 로그"""
        logger.data_pipeline_log(
            stage=stage,
            success=False,
            error_message=error_message,
            **(error_details or {})
        )
        
        self.send_alert(
            f"파이프라인 실패 ({stage}): {error_message}", 
            "ERROR", 
            error_details
        )
        
    def log_api_performance(self, rate_limit_stats: Dict[str, Any]):
        """API 성능 메트릭 로그"""
        logger.performance_log(
            operation="api_batch_processing",
            duration=rate_limit_stats.get('total_wait_time', 0),
            items_processed=rate_limit_stats.get('total_requests', 0),
            **rate_limit_stats
        )
        
        # 레이트 리밋 비율이 높으면 경고
        rate_limit_percentage = rate_limit_stats.get('rate_limit_percentage', 0)
        if rate_limit_percentage > 20:  # 20% 이상 레이트 리밋 발생 시
            self.send_alert(
                f"높은 레이트 리밋 발생률: {rate_limit_percentage:.1f}%",
                "WARNING",
                rate_limit_stats
            )
