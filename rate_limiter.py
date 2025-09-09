import time
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AdaptiveRateLimit:
    """
    적응형 레이트 리밋 관리자
    API 응답에 따라 동적으로 딜레이를 조정합니다.
    """
    
    def __init__(self, initial_delay: float = 0.5, max_delay: float = 10.0, min_delay: float = 0.1):
        self.delay = initial_delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        self.last_request_time = 0
        
        # 통계 추적
        self.total_requests = 0
        self.rate_limited_requests = 0
        self.total_wait_time = 0
        
    def wait_if_needed(self) -> float:
        """필요시 대기하고 실제 대기 시간을 반환"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.delay:
            wait_time = self.delay - time_since_last
            logger.debug(f"Rate limit wait: {wait_time:.2f}s")
            time.sleep(wait_time)
            self.total_wait_time += wait_time
            return wait_time
        
        return 0
        
    def record_response(self, status_code: int, response_time: Optional[float] = None):
        """API 응답을 기록하고 딜레이를 조정"""
        self.total_requests += 1
        self.last_request_time = time.time()
        
        if status_code == 429:  # Rate limited
            self._handle_rate_limit()
        elif status_code == 200:  # Success
            self._handle_success(response_time)
        elif status_code >= 500:  # Server error
            self._handle_server_error()
        else:
            # Other errors don't affect rate limiting
            pass
            
    def _handle_rate_limit(self):
        """레이트 리밋 발생 시 처리"""
        self.rate_limited_requests += 1
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        
        # 지수적 백오프 (최대값 제한)
        self.delay = min(self.delay * 1.5, self.max_delay)
        logger.warning(f"Rate limited! Increased delay to {self.delay:.2f}s")
        
    def _handle_success(self, response_time: Optional[float] = None):
        """성공 응답 시 처리"""
        self.consecutive_successes += 1
        self.consecutive_failures = 0
        
        # 연속 성공 시 딜레이 감소
        if self.consecutive_successes >= 5:
            self.delay = max(self.delay * 0.9, self.min_delay)
            logger.debug(f"Decreased delay to {self.delay:.2f}s after {self.consecutive_successes} successes")
            
    def _handle_server_error(self):
        """서버 에러 시 처리"""
        self.consecutive_failures += 1
        if self.consecutive_failures >= 3:
            self.delay = min(self.delay * 1.2, self.max_delay)
            logger.warning(f"Multiple server errors, increased delay to {self.delay:.2f}s")
            
    def get_stats(self) -> Dict:
        """레이트 리밋 통계 반환"""
        rate_limit_percentage = (self.rate_limited_requests / self.total_requests * 100) if self.total_requests > 0 else 0
        avg_wait_time = self.total_wait_time / self.total_requests if self.total_requests > 0 else 0
        
        return {
            'total_requests': self.total_requests,
            'rate_limited_requests': self.rate_limited_requests,
            'rate_limit_percentage': rate_limit_percentage,
            'current_delay': self.delay,
            'total_wait_time': self.total_wait_time,
            'avg_wait_time_per_request': avg_wait_time
        }
        
    def reset_stats(self):
        """통계 초기화"""
        self.total_requests = 0
        self.rate_limited_requests = 0
        self.total_wait_time = 0
        logger.info("Rate limiter statistics reset")

class RateLimitManager:
    """
    여러 엔드포인트에 대한 레이트 리밋 관리
    """
    
    def __init__(self):
        self.limiters: Dict[str, AdaptiveRateLimit] = {}
        
    def get_limiter(self, endpoint: str, **kwargs) -> AdaptiveRateLimit:
        """엔드포인트별 레이트 리미터 반환"""
        if endpoint not in self.limiters:
            self.limiters[endpoint] = AdaptiveRateLimit(**kwargs)
            
        return self.limiters[endpoint]
        
    def get_global_stats(self) -> Dict:
        """모든 엔드포인트의 통계 반환"""
        stats = {}
        for endpoint, limiter in self.limiters.items():
            stats[endpoint] = limiter.get_stats()
            
        return stats