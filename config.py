import os
from dataclasses import dataclass

@dataclass
class Config:
    # API 설정
    riot_api_key: str = os.getenv("RIOT_API_KEY")
    project_id: str = os.getenv("GOOGLE_CLOUD_PROJECT")
    dataset_id: str = "riot_analytics"
    
    # 환경 설정
    is_production: bool = os.getenv("ENV") == "production"
    matches_per_player: int = 20 if is_production else 5
    challenger_count: int = 300 if is_production else 50
    
    # API 상수들
    RIOT_BASE_URL: str = "https://kr.api.riotgames.com"
    RIOT_MATCH_URL: str = "https://asia.api.riotgames.com"
    DEFAULT_QUEUE: str = "RANKED_SOLO_5x5"
    
    # 성능 관련 상수
    DEFAULT_MATCH_COUNT: int = 20
    API_RATE_LIMIT_DELAY: float = 0.5
    RETRY_DELAY: float = 2.0
    MAX_RETRIES: int = 3
    PLAYER_BATCH_DELAY: float = 1.0
    
    # BigQuery 설정
    DATASET_LOCATION: str = "US"
    
    # 로깅 설정
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # 데이터 품질 설정
    MIN_CHALLENGER_COUNT: int = 250
    DATA_FRESHNESS_HOURS: int = 24