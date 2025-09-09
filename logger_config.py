import logging
import sys
from typing import Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo

# structlog 사용 가능한지 확인
try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False
    print("structlog가 설치되지 않았습니다. 기본 로깅을 사용합니다.")

def configure_logging(log_level: str = "INFO") -> None:
    """
    구조화된 로깅 설정
    structlog 사용 가능하면 JSON, 없으면 기본 로깅 사용
    """
    
    if STRUCTLOG_AVAILABLE:
        # 기본 로깅 설정
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=getattr(logging, log_level.upper())
        )
        
        # structlog 설정
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                add_timestamp,
                add_service_info,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(ensure_ascii=False)
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
    else:
        # 기본 로깅 설정 (structlog 없을 때)
        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            stream=sys.stdout,
            level=getattr(logging, log_level.upper()),
            datefmt='%Y-%m-%d %H:%M:%S'
        )

def add_timestamp(logger, method_name, event_dict):
    """KST 타임스탬프 추가"""
    event_dict["timestamp"] = datetime.now(ZoneInfo("Asia/Seoul")).isoformat()
    return event_dict

def add_service_info(logger, method_name, event_dict):
    """서비스 정보 추가"""
    event_dict["service"] = "riot-data-pipeline"
    event_dict["version"] = "1.0.0"
    return event_dict

class StructuredLogger:
    """
    구조화된 로거 래퍼 클래스
    structlog 사용 가능하면 구조화된 로그, 없으면 기본 로깅 사용
    """
    
    def __init__(self, name: str):
        if STRUCTLOG_AVAILABLE:
            self.logger = structlog.get_logger(name)
            self.use_structured = True
        else:
            self.logger = logging.getLogger(name)
            self.use_structured = False
        
    def info(self, message: str, **kwargs):
        """정보 로그"""
        if self.use_structured:
            self.logger.info(message, **kwargs)
        else:
            extra_msg = " | ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            full_msg = f"{message} | {extra_msg}" if extra_msg else message
            self.logger.info(full_msg)
        
    def warning(self, message: str, **kwargs):
        """경고 로그"""
        if self.use_structured:
            self.logger.warning(message, **kwargs)
        else:
            extra_msg = " | ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            full_msg = f"{message} | {extra_msg}" if extra_msg else message
            self.logger.warning(full_msg)
        
    def error(self, message: str, **kwargs):
        """에러 로그"""
        if self.use_structured:
            self.logger.error(message, **kwargs)
        else:
            extra_msg = " | ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            full_msg = f"{message} | {extra_msg}" if extra_msg else message
            self.logger.error(full_msg)
        
    def debug(self, message: str, **kwargs):
        """디버그 로그"""
        if self.use_structured:
            self.logger.debug(message, **kwargs)
        else:
            extra_msg = " | ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            full_msg = f"{message} | {extra_msg}" if extra_msg else message
            self.logger.debug(full_msg)
        
    def api_call_log(self, endpoint: str, method: str = "GET", 
                    status_code: int = None, response_time: float = None,
                    **kwargs):
        """API 호출 전용 로그"""
        log_data = {
            "event_type": "api_call",
            "endpoint": endpoint,
            "method": method,
            **kwargs
        }
        
        if status_code:
            log_data["status_code"] = status_code
        if response_time:
            log_data["response_time_seconds"] = round(response_time, 3)
            
        if status_code and status_code >= 400:
            self.logger.error("API call failed", **log_data)
        else:
            self.logger.info("API call completed", **log_data)
            
    def data_pipeline_log(self, stage: str, count: int = None, 
                         duration: float = None, success: bool = True,
                         **kwargs):
        """데이터 파이프라인 단계별 로그"""
        log_data = {
            "event_type": "pipeline_stage",
            "stage": stage,
            "success": success,
            **kwargs
        }
        
        if count is not None:
            log_data["count"] = count
        if duration is not None:
            log_data["duration_seconds"] = round(duration, 3)
            
        if success:
            self.logger.info(f"Pipeline stage completed: {stage}", **log_data)
        else:
            self.logger.error(f"Pipeline stage failed: {stage}", **log_data)
            
    def performance_log(self, operation: str, duration: float, 
                       items_processed: int = None, **kwargs):
        """성능 측정 로그"""
        log_data = {
            "event_type": "performance",
            "operation": operation,
            "duration_seconds": round(duration, 3),
            **kwargs
        }
        
        if items_processed:
            log_data["items_processed"] = items_processed
            log_data["items_per_second"] = round(items_processed / duration, 2)
            
        self.logger.info(f"Performance metric: {operation}", **log_data)
        
    def security_log(self, event: str, severity: str = "INFO", **kwargs):
        """보안 관련 로그"""
        log_data = {
            "event_type": "security",
            "security_event": event,
            "severity": severity,
            **kwargs
        }
        
        if severity == "ERROR":
            self.logger.error(f"Security event: {event}", **log_data)
        elif severity == "WARNING":
            self.logger.warning(f"Security event: {event}", **log_data)
        else:
            self.logger.info(f"Security event: {event}", **log_data)

def get_logger(name: str) -> StructuredLogger:
    """구조화된 로거 인스턴스 반환"""
    return StructuredLogger(name)

# 로깅 설정 함수들
def setup_production_logging():
    """프로덕션 환경 로깅 설정"""
    configure_logging("INFO")

def setup_development_logging():
    """개발 환경 로깅 설정"""
    configure_logging("DEBUG")

def setup_testing_logging():
    """테스트 환경 로깅 설정"""
    configure_logging("WARNING")