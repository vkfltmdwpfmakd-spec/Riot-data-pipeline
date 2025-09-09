-- 1. 챌린저 랭킹 대시보드 뷰
CREATE OR REPLACE VIEW `riot-data-pipeline.riot_analytics.challenger_dashboard` AS
SELECT
  puuid,
  league_points,
  wins,
  losses,
  ROUND(wins / (wins + losses) * 100, 2) AS win_rate,
  is_veteran,
  is_hot_streak,
  collected_at,
  DATE(collected_at) AS collection_date,
  EXTRACT(HOUR FROM collected_at) AS collection_hour,
  RANK() OVER (PARTITION BY DATE(collected_at) ORDER BY league_points DESC) AS daily_rank,
  RANK() OVER (ORDER BY league_points DESC) AS current_rank
FROM `riot-data-pipeline.riot_analytics.challengers`
WHERE date(collected_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- 2. 매치 분석 대시보드 뷰
CREATE OR REPLACE VIEW `riot-data-pipeline.riot_analytics.match_analysis` AS
SELECT
  m.match_id,
  m.game_creation,
  m.game_duration,
  ROUND(m.game_duration / 60.0, 1) AS game_duration_minutes,
  m.game_mode,
  m.game_type,
  m.queue_id,
  m.map_id,
  m.platform_id,
  m.participants_count,
  m.collected_at,
  DATE(m.game_creation) AS game_date,
  EXTRACT(HOUR FROM m.game_creation) AS game_hour,
  EXTRACT(DAYOFWEEK FROM m.game_creation) AS day_of_week,
  CASE
    WHEN m.game_duration < 900 THEN '15분 미만'
    WHEN m.game_duration < 1800 THEN '15-30분'
    WHEN m.game_duration < 2700 THEN '30-45분'
    ELSE '45분 이상'
  END AS game_duration_category
FROM `riot-data-pipeline.riot_analytics.matches` m
WHERE DATE(m.game_creation) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- 3. 플레이어 성과 분석 뷰
CREATE OR REPLACE VIEW `riot-data-pipeline.riot_analytics.player_performance` AS
SELECT
  mp.puuid,
  mp.summoner_name,
  mp.riot_id_game_name,
  mp.champion_name,
  mp.champion_id,
  mp.win,
  mp.kills,
  mp.deaths,
  mp.assists,
  ROUND((mp.kills + mp.assists) / GREATEST(mp.deaths, 1), 2) AS kda_ratio,
  mp.total_damage_dealt_to_champions,
  mp.gold_earned,
  mp.total_minions_killed,
  mp.vision_score,
  mp.team_position,
  mp.individual_position,
  m.game_creation,
  m.game_duration,
  m.game_mode,
  m.queue_id,
  DATE(m.game_creation) AS game_date,
  ROUND(mp.total_damage_dealt_to_champions / (m.game_duration / 60.0), 0) AS damage_per_minute,
  ROUND(mp.gold_earned / (m.game_duration / 60.0), 0) AS gold_per_minute,
  ROUND(mp.total_minions_killed / (m.game_duration / 60.0), 1) AS cs_per_minute
FROM `riot-data-pipeline.riot_analytics.match_participants` mp
JOIN `riot-data-pipeline.riot_analytics.matches` m ON mp.match_id = m.match_id
WHERE DATE(m.game_creation) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

-- 4. 챔피언 통계 뷰
CREATE OR REPLACE VIEW `riot-data-pipeline.riot_analytics.champion_stats` AS
SELECT
  champion_name,
  champion_id,
  COUNT(*) AS games_played,
  SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
  ROUND(SUM(CASE WHEN win THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS win_rate,
  ROUND(AVG(kills), 2) AS avg_kills,
  ROUND(AVG(deaths), 2) AS avg_deaths,
  ROUND(AVG(assists), 2) AS avg_assists,
  ROUND(AVG((kills + assists) / GREATEST(deaths, 1)), 2) AS avg_kda,
  ROUND(AVG(total_damage_dealt_to_champions), 0) AS avg_damage,
  ROUND(AVG(gold_earned), 0) AS avg_gold,
  ROUND(AVG(total_minions_killed), 1) AS avg_cs,
  RANK() OVER (ORDER BY COUNT(*) DESC) AS popularity_rank,
  RANK() OVER (ORDER BY
    CASE WHEN COUNT(*) >= 10 THEN SUM(CASE WHEN win THEN 1 ELSE 0 END) / COUNT(*) ELSE 0 END DESC
  ) AS winrate_rank
FROM `riot-data-pipeline.riot_analytics.match_participants` mp
JOIN `riot-data-pipeline.riot_analytics.matches` m ON mp.match_id = m.match_id
WHERE DATE(m.game_creation) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY champion_name, champion_id
HAVING COUNT(*) >= 3;

-- 5. 포지션별 통계 뷰
CREATE OR REPLACE VIEW `riot-data-pipeline.riot_analytics.position_stats` AS
SELECT
  team_position,
  individual_position,
  COUNT(*) AS games_count,
  ROUND(AVG(CASE WHEN win THEN 1.0 ELSE 0.0 END) * 100, 2) AS avg_win_rate,
  ROUND(AVG(kills), 2) AS avg_kills,
  ROUND(AVG(deaths), 2) AS avg_deaths,
  ROUND(AVG(assists), 2) AS avg_assists,
  ROUND(AVG((kills + assists) / GREATEST(deaths, 1)), 2) AS avg_kda,
  ROUND(AVG(total_damage_dealt_to_champions), 0) AS avg_damage,
  ROUND(AVG(gold_earned), 0) AS avg_gold,
  ROUND(AVG(total_minions_killed), 1) AS avg_cs,
  ROUND(AVG(vision_score), 1) AS avg_vision_score
FROM `riot-data-pipeline.riot_analytics.match_participants` mp
JOIN `riot-data-pipeline.riot_analytics.matches` m ON mp.match_id = m.match_id
WHERE DATE(m.game_creation) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND team_position IS NOT NULL
GROUP BY team_position, individual_position;

-- 6. 시간대별 게임 활동 뷰
CREATE OR REPLACE VIEW `riot-data-pipeline.riot_analytics.time_analysis` AS
SELECT
  DATE(game_creation) AS game_date,
  EXTRACT(HOUR FROM game_creation) AS game_hour,
  EXTRACT(DAYOFWEEK FROM game_creation) AS day_of_week,
  CASE EXTRACT(DAYOFWEEK FROM game_creation)
    WHEN 1 THEN '일요일'
    WHEN 2 THEN '월요일'
    WHEN 3 THEN '화요일'
    WHEN 4 THEN '수요일'
    WHEN 5 THEN '목요일'
    WHEN 6 THEN '금요일'
    WHEN 7 THEN '토요일'
  END AS day_name,
  COUNT(*) AS games_count,
  ROUND(AVG(game_duration / 60.0), 1) AS avg_duration_minutes,
  game_mode,
  queue_id
FROM `riot-data-pipeline.riot_analytics.matches`
WHERE DATE(game_creation) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY
  game_date,    
  game_hour,    
  day_of_week,  
  day_name,     
  game_mode,
  queue_id;

-- 7. 종합 KPI 대시보드 뷰
CREATE OR REPLACE VIEW `riot-data-pipeline.riot_analytics.kpi_summary` AS
SELECT
  COUNT(DISTINCT c.puuid) AS total_challengers,
  COUNT(DISTINCT m.match_id) AS total_matches,
  COUNT(*) AS total_participants,
  MAX(m.game_creation) AS latest_match_time,
  MIN(m.game_creation) AS earliest_match_time,
  ROUND(AVG(m.game_duration / 60.0), 1) AS avg_game_duration_minutes,
  ROUND(AVG(mp.kills), 2) AS avg_kills_per_game,
  ROUND(AVG(mp.deaths), 2) AS avg_deaths_per_game,
  ROUND(AVG(mp.assists), 2) AS avg_assists_per_game,
  ROUND(AVG((mp.kills + mp.assists) / GREATEST(mp.deaths, 1)), 2) AS avg_kda,
  MAX(c.league_points) AS highest_lp,
  MIN(c.league_points) AS lowest_lp,
  ROUND(AVG(c.league_points), 0) AS avg_lp,
  MAX(c.collected_at) AS last_data_collection,
  COUNT(DISTINCT DATE(c.collected_at)) AS collection_days
FROM `riot-data-pipeline.riot_analytics.challengers` c
FULL OUTER JOIN `riot-data-pipeline.riot_analytics.matches` m
  ON DATE(c.collected_at) = DATE(m.game_creation)
LEFT JOIN `riot-data-pipeline.riot_analytics.match_participants` mp
  ON m.match_id = mp.match_id
WHERE DATE(c.collected_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  OR DATE(m.game_creation) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);