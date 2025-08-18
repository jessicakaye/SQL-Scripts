/* ----------------------------------------------------------------------
Public-safe rollup with pseudonymized source table/columns
- Adds param: finish_pct_threshold (default 75), used for “percent-finished” logic
- Keeps param: min_threshold_mins (default 2), used for “time-gated engagement”

SOURCE (public alias):
  `{{PROJECT_ID}}.{{DATASET}}.content_episode_user_engagement`

Objective:
  Produces rolling, per-episode and per-season completion metrics, survivorship, and episodic completion rates for Paramount+ originals.
  Handles both “intended” (metadata) and “actual available” (based on episode air date and current window) logic, robust to incomplete drops and data leakage.

Major CTEs:

cte1_user_episode_viewing:
  - For each user/episode, flags if user watched ≥ min_threshold_mins (default 2) or ≥finish_pct_threshold % (default 75%).
  - Prepares episode-level engagement, filters out phantom/future episodes.

cte2_user_episode_arrays:
  - Aggregates each user's episode-level viewing (structs) into an array for each show/season/week.
  - Enables threshold cohort analysis.

cte3_actual_available_episodes:
  - Calculates the number of “truly available” episodes for each series/season/week, based on whether the scheduled air date is on or before the cohort window end.
  - Caps available episodes at the intended drop size (batch or total) for edge-case data quality.
  - Outputs both “raw” available episode count and “capped” available_episodes.

cte4_series_completion_thresholds:
  - Generates one row per show/season/interval for every rolling threshold.
  - Allows analysis like "completed x of N episodes", for both batch and standard drops.

cte5:
  - For each user/show/season/threshold/week:
      - Flags if user watched any ep ≥2min (survivorship denom)
      - Flags if user watched at least x distinct eps ≥2min (survivorship num)
      - Flags if user watched all eps 1..x ≥2min (classic completion)
      - Flags if user watched 2min+ or 75%+ of a specific episode (episodic completion)
      - Calculates “N-1 completer” metrics for both intended (metadata) and actual available episode count.

cte6_aggregated_completion_counts:
  - Sums all flags per group (series/season/week/threshold/episode).
  - Outputs user counts for each metric: survivorship, all-ep, n-1, n, 75%, episodic, and both intended and available-episode-based completions.

Final SELECT:
  - Calculates all final rates (episodic_completion_rate, survivorship_x_of_any, intended and available completion rates, etc).
  - Orders output for time-based, funnel-style, or executive analytics.

NOTES:
  - Robust to future-episode data leakage and incomplete drops.
  - Can compare “intended” vs “actual possible” completions for any show, cohort, or drop.
*/
---------------------------------------------------------------------- */

-- ---------- Parameters ----------
DECLARE run_date DATE DEFAULT CURRENT_DATE();
DECLARE lookback_days INT64 DEFAULT 105;

-- Minimum minutes to count as “engaged” on an episode (cohort denominators)
DECLARE min_threshold_mins INT64 DEFAULT 2;

-- Percent-finished threshold (replaces hard-coded 75%); used wherever we previously used the 75% flag
DECLARE finish_pct_threshold INT64 DEFAULT 75;

-- Optional platform filter example
DECLARE platform_filter STRING DEFAULT 'PRIMARY_PLATFORM_GROUP';

-- Titles that should NOT be capped to episodes_initial_drop for binge (examples)
DECLARE excluded_titles_binge_cap ARRAY<STRING> DEFAULT [
  'SHOW_A S1', 'SHOW_B S2'
];

-- Titles that should force episode_air_dt as the availability date (examples)
DECLARE override_titles_use_air_dt ARRAY<STRING> DEFAULT [
  -- 'SERIES_X S1'
];

DECLARE x_days_ago DATE;
SET x_days_ago = DATE_SUB(run_date, INTERVAL lookback_days DAY);

-- ---------- Write pattern (materialize into a neutral table name) ----------
DELETE `{{PROJECT_ID}}.{{TEMP_DATASET}}.agg_content_engagement_rollup`
WHERE premiere_date >= x_days_ago;

INSERT INTO `{{PROJECT_ID}}.{{TEMP_DATASET}}.agg_content_engagement_rollup`
WITH
-- Select a single display title per (series/season/episode)
cte0_episode_titles AS (
  SELECT DISTINCT
    season_display_title,
    series_display_title,
    season_number,
    episode_number,
    episode_display_title,
    COUNT(DISTINCT user_id) AS users_cnt,
    ROW_NUMBER() OVER (
      PARTITION BY season_display_title, series_display_title, season_number, episode_number
      ORDER BY COUNT(DISTINCT user_id) DESC
    ) AS episode_title_rank,
    DENSE_RANK() OVER (
      PARTITION BY season_display_title, series_display_title, season_number
      ORDER BY SAFE_CAST(episode_number AS INT64)
    ) AS episode_seq_rank
  FROM `{{PROJECT_ID}}.{{DATASET}}.content_episode_user_engagement`
  WHERE platform_group = platform_filter
    AND series_premiere_dt >= x_days_ago
  GROUP BY season_display_title, series_display_title, season_number, episode_number, episode_display_title
),

-- Per-user, per-episode engagement flags
cte1_user_episode_engagement AS (
  SELECT
    season_display_title,
    series_premiere_dt,
    cohort_window_end_dt,
    episodes_intended_total,
    episodes_initial_drop,
    release_model,
    series_display_title,
    season_number,
    episode_number,
    episode_air_dt,
    episode_available_dt,
    CASE
      WHEN season_display_title IN UNNEST(override_titles_use_air_dt) THEN episode_air_dt
      WHEN release_model <> 'Weekly' THEN series_premiere_dt
      WHEN (episode_available_dt IS NULL OR episode_available_dt < series_premiere_dt) AND episode_air_dt IS NOT NULL THEN episode_air_dt
      WHEN (episode_air_dt IS NULL OR (episode_air_dt < series_premiere_dt AND episode_available_dt IS NULL))
        THEN DATE_ADD(series_premiere_dt, INTERVAL (7 * (CAST(episode_number AS INT64) - CAST(episodes_initial_drop AS INT64))) DAY)
      ELSE episode_available_dt
    END AS normalized_episode_dt,
    days_since_premiere,
    cohort_week,
    user_id,
    plan_tier,
    platform_group,

    -- raw watch measures
    episode_watch_minutes,
    -- OPTIONAL runtime column: if not present in your data, keep it NULL and the % logic will fall back to the baked 75% flag
    episode_runtime_minutes,

    -- Time-gated “engaged” flag (parameterized minutes)
    CASE WHEN episode_watch_minutes >= min_threshold_mins THEN 1 ELSE 0 END AS ep_threshold_met,

    -- Percent-finished flag (parameterized percent):
    -- If runtime is available, compute % directly; otherwise, fall back to the precomputed 75% boolean when the threshold is 75.
    CASE
      WHEN episode_runtime_minutes IS NOT NULL AND episode_runtime_minutes > 0 THEN
        CASE WHEN SAFE_DIVIDE(episode_watch_minutes, episode_runtime_minutes) >= (finish_pct_threshold / 100.0) THEN 1 ELSE 0 END
      ELSE
        CASE WHEN finish_pct_threshold = 75 AND episode_watch_75pct_flag = 1 THEN 1 ELSE 0 END
    END AS ep_pct_threshold_met
  FROM `{{PROJECT_ID}}.{{DATASET}}.content_episode_user_engagement`
  WHERE series_premiere_dt >= x_days_ago
),

-- Aggregate each user's per-episode flags into an array
cte2_user_episode_arrays AS (
  SELECT
    season_display_title,
    series_premiere_dt,
    series_display_title,
    season_number,
    days_since_premiere,
    cohort_week,
    cohort_window_end_dt,
    user_id,
    plan_tier,
    platform_group,
    ARRAY_AGG(STRUCT(episode_number, ep_threshold_met, ep_pct_threshold_met)) AS episode_behavior_array
  FROM cte1_user_episode_engagement
  GROUP BY
    season_display_title, series_premiere_dt, series_display_title, season_number,
    days_since_premiere, cohort_week, cohort_window_end_dt, user_id, plan_tier, platform_group
),

-- Actually-available episodes by cohort window
cte3_available_episodes AS (
  SELECT
    season_display_title,
    series_display_title,
    season_number,
    days_since_premiere,
    cohort_week,
    MIN(SAFE_CAST(episode_number AS INT64)) AS batch_first_episode_nbr,
    MAX(SAFE_CAST(episode_number AS INT64)) AS batch_final_episode_nbr,
    MAX(SAFE_CAST(episode_number AS INT64)) - 1 AS batch_penultimate_episode_nbr, --will need to adjust for seasons where not all episodes are available in order
    COUNT(DISTINCT CASE WHEN cohort_window_end_dt >= normalized_episode_dt THEN episode_number END) AS available_episodes_raw,
    CASE
      WHEN release_model = 'Binge'
        AND SAFE_CAST(episodes_initial_drop AS INT64) <> SAFE_CAST(episodes_intended_total AS INT64)
        AND season_display_title NOT IN UNNEST(excluded_titles_binge_cap)
      THEN SAFE_CAST(episodes_initial_drop AS INT64)
      ELSE SAFE_CAST(episodes_intended_total AS INT64)
    END AS episodes_total_adjusted,
    LEAST(
      COUNT(DISTINCT CASE WHEN cohort_window_end_dt >= normalized_episode_dt THEN episode_number END),
      MAX(
        CASE
          WHEN release_model = 'Binge'
            AND SAFE_CAST(episodes_initial_drop AS INT64) <> SAFE_CAST(episodes_intended_total AS INT64)
            AND season_display_title NOT IN UNNEST(excluded_titles_binge_cap)
          THEN SAFE_CAST(episodes_initial_drop AS INT64)
          ELSE SAFE_CAST(episodes_intended_total AS INT64)
        END
      )
    ) AS episodes_total_available
  FROM cte1_user_episode_engagement
  GROUP BY
    season_display_title, series_display_title, release_model, episodes_initial_drop, episodes_intended_total,
    season_number, days_since_premiere, cohort_week
),

-- Generate rolling completion thresholds 1..N (or movie case)
cte4_thresholds AS (
  SELECT DISTINCT
    ep.season_display_title,
    ep.series_display_title,
    ep.season_number,
    ep.genre,
    ep.release_model,
    ep.is_original,
    ep.episodes_initial_drop,
    ep.episodes_intended_total,
    ep.days_since_premiere,
    ep.cohort_week,
    av.episodes_total_available,
    av.available_episodes_raw,
    av.episodes_total_adjusted,
    av.batch_first_episode_nbr,
    av.batch_final_episode_nbr,
    av.batch_penultimate_episode_nbr,
    x AS completion_episode_threshold
  FROM `{{PROJECT_ID}}.{{DATASET}}.content_episode_user_engagement` ep
  LEFT JOIN cte3_available_episodes av
    USING (season_display_title, series_display_title, season_number, days_since_premiere)
  , UNNEST(GENERATE_ARRAY(1, episodes_total_adjusted)) AS x
  WHERE CAST(episodes_intended_total AS INT64) > 1
    AND series_premiere_dt >= x_days_ago

  UNION ALL

  -- Movie logic (episodes_intended_total = 0)
  SELECT DISTINCT
    season_display_title,
    series_display_title,
    season_number,
    genre,
    release_model,
    is_original,
    episodes_initial_drop,
    episodes_intended_total,
    days_since_premiere,
    cohort_week,
    0 AS episodes_total_available,
    0 AS available_episodes_raw,
    0 AS episodes_total_adjusted,
    1 AS batch_first_episode_nbr,
    0 AS batch_final_episode_nbr,
    0 AS batch_penultimate_episode_nbr,
    1 AS completion_episode_threshold
  FROM `{{PROJECT_ID}}.{{DATASET}}.content_episode_user_engagement`
  WHERE (CAST(episodes_intended_total AS INT64) = 0 OR TRIM(episodes_intended_total) = '')
    AND series_premiere_dt >= x_days_ago
),

-- Per-user flags at each threshold
cte5_user_flags AS (
  SELECT
    th.season_display_title,
    ua.series_premiere_dt,
    th.series_display_title,
    th.season_number,
    th.genre,
    th.release_model,
    th.is_original,
    th.episodes_initial_drop,
    th.episodes_intended_total,
    th.episodes_total_available,
    th.available_episodes_raw,
    th.episodes_total_adjusted,
    th.completion_episode_threshold,
    t0.episode_number AS actual_episode_nbr,
    t0.episode_display_title,
    th.batch_first_episode_nbr,
    th.batch_final_episode_nbr,
    th.batch_penultimate_episode_nbr,
    ua.days_since_premiere,
    ua.cohort_week,
    ua.user_id,
    ua.platform_group,
    ua.plan_tier,
    ua.cohort_window_end_dt,

    -- episodic completion (percent-threshold)
    MAX(IF(CAST(e.episode_number AS INT64) = CAST(t0.episode_number AS INT64) AND e.ep_pct_threshold_met = 1, 1, 0)) AS watched_pct_threshold_of_this_ep,

    -- denominators
    IF(COUNTIF(e.ep_threshold_met = 1) >= 1, 1, 0) AS watched_any_one_ep_time_threshold,
    IF(COUNTIF(e.ep_threshold_met = 1) >= th.completion_episode_threshold, 1, 0) AS watched_at_least_x_eps_time_threshold,
    IF(
      COUNTIF(CAST(e.episode_number AS INT64) <= th.batch_first_episode_nbr + (th.completion_episode_threshold - 1) AND e.ep_threshold_met = 1)
      = th.completion_episode_threshold, 1, 0
    ) AS watched_all_eps_1_to_x_time_threshold,

    -- numerators (time-threshold)
    IF(COUNTIF(e.ep_threshold_met = 1) >= th.episodes_total_adjusted - 1, 1, 0) AS watched_n_minus_1_eps_time_threshold,
    IF(COUNTIF(e.ep_threshold_met = 1) >= th.episodes_total_available - 1, 1, 0) AS watched_n_minus_1_avail_eps_time_threshold,
    IF(COUNTIF(e.ep_threshold_met = 1) = th.episodes_total_adjusted, 1, 0)      AS watched_n_eps_time_threshold,
    IF(COUNTIF(e.ep_threshold_met = 1) = th.episodes_total_available, 1, 0)     AS watched_n_avail_eps_time_threshold,

    -- percent-threshold on penultimate/final
    MAX(IF(CAST(e.episode_number AS INT64) = th.batch_first_episode_nbr + (th.episodes_total_adjusted - 2) AND e.ep_pct_threshold_met = 1, 1, 0)) AS watched_pct_threshold_penultimate,
    MAX(IF(CAST(e.episode_number AS INT64) = CAST(th.batch_penultimate_episode_nbr AS INT64) AND e.ep_pct_threshold_met = 1, 1, 0))                AS watched_pct_threshold_avail_penultimate,
    MAX(IF(CAST(e.episode_number AS INT64) = th.batch_first_episode_nbr + (th.episodes_total_adjusted - 1) AND e.ep_pct_threshold_met = 1, 1, 0)) AS watched_pct_threshold_final,
    MAX(IF(CAST(e.episode_number AS INT64) = CAST(th.batch_final_episode_nbr AS INT64) AND e.ep_pct_threshold_met = 1, 1, 0))                        AS watched_pct_threshold_avail_final

  FROM cte4_thresholds th
  JOIN cte2_user_episode_arrays ua
    ON th.season_display_title = ua.season_display_title
   AND th.series_display_title = ua.series_display_title
   AND COALESCE(th.season_number, "0") = COALESCE(ua.season_number, "0")
   AND th.days_since_premiere = ua.days_since_premiere
  LEFT JOIN cte0_episode_titles t0
    ON th.season_display_title = t0.season_display_title
   AND th.series_display_title = t0.series_display_title
   AND COALESCE(th.season_number, '0') = COALESCE(t0.season_number, '0')
   AND CAST(th.completion_episode_threshold AS INT64) = CAST(t0.episode_seq_rank AS INT64)
   AND t0.episode_title_rank = 1
  LEFT JOIN UNNEST(ua.episode_behavior_array) e
  GROUP BY
    th.season_display_title, ua.series_premiere_dt, th.series_display_title, th.season_number, th.genre, th.release_model, th.is_original,
    th.episodes_initial_drop, th.episodes_intended_total, th.episodes_total_available, th.available_episodes_raw, th.episodes_total_adjusted,
    th.completion_episode_threshold, t0.episode_number, t0.episode_display_title,
    th.batch_first_episode_nbr, th.batch_final_episode_nbr, th.batch_penultimate_episode_nbr,
    ua.days_since_premiere, ua.cohort_week, ua.cohort_window_end_dt, ua.user_id, ua.platform_group, ua.plan_tier
),

-- Aggregate counts
cte6_aggregated_counts AS (
  SELECT
    f.season_display_title,
    f.series_premiere_dt,
    f.series_display_title,
    f.season_number,
    f.genre,
    f.release_model,
    f.is_original,
    f.episodes_initial_drop,
    f.episodes_intended_total,
    f.episodes_total_adjusted,
    f.episodes_total_available,
    f.available_episodes_raw,
    f.days_since_premiere,
    f.cohort_week,
    f.platform_group,
    f.plan_tier,
    f.completion_episode_threshold,
    f.actual_episode_nbr,
    f.batch_first_episode_nbr,
    f.batch_final_episode_nbr,
    f.batch_penultimate_episode_nbr,
    f.episode_display_title,
    MAX(f.cohort_window_end_dt) AS last_day_dt,

    -- episodic “percent threshold” completion counts
    COUNTIF(watched_pct_threshold_of_this_ep = 1)      AS users_ep_pct_threshold_cnt,

    -- time-threshold (min) survivorship/denominators
    COUNTIF(watched_any_one_ep_time_threshold = 1)     AS users_any_ep_time_threshold_cnt,
    COUNTIF(watched_at_least_x_eps_time_threshold = 1) AS users_ge_threshold_eps_time_cnt,

    -- time-threshold all-episodes-1..x
    COUNTIF(watched_all_eps_1_to_x_time_threshold = 1) AS users_all_eps_1_to_n_time_cnt,

    -- time-threshold numerators joined to denoms
    COUNTIF(watched_at_least_x_eps_time_threshold = 1 AND watched_n_minus_1_eps_time_threshold = 1) AS users_nminus1_of_ge_n_eps_time_cnt,
    COUNTIF(watched_at_least_x_eps_time_threshold = 1 AND watched_n_eps_time_threshold = 1)         AS users_n_of_ge_n_eps_time_cnt,
    COUNTIF(watched_all_eps_1_to_x_time_threshold = 1 AND watched_n_minus_1_eps_time_threshold = 1) AS users_nminus1_of_all_1_to_n_time_cnt,
    COUNTIF(watched_all_eps_1_to_x_time_threshold = 1 AND watched_n_eps_time_threshold = 1)         AS users_n_of_all_1_to_n_time_cnt,

    -- percent-threshold penultimate/final combos
    COUNTIF(watched_pct_threshold_penultimate = 1 AND watched_pct_threshold_of_this_ep = 1) AS users_pct_threshold_penultimate_of_pct_x_cnt,
    COUNTIF(watched_pct_threshold_final = 1 AND watched_pct_threshold_of_this_ep = 1)       AS users_pct_threshold_final_of_pct_x_cnt,

    -- available-episodes variants (time-threshold)
    COUNTIF(watched_at_least_x_eps_time_threshold = 1 AND watched_n_minus_1_avail_eps_time_threshold = 1) AS users_nminus1_avail_of_ge_n_eps_time_cnt,
    COUNTIF(watched_at_least_x_eps_time_threshold = 1 AND watched_n_avail_eps_time_threshold = 1)         AS users_n_avail_of_ge_n_eps_time_cnt,
    COUNTIF(watched_all_eps_1_to_x_time_threshold = 1 AND watched_n_minus_1_avail_eps_time_threshold = 1) AS users_nminus1_avail_of_all_1_to_n_time_cnt,
    COUNTIF(watched_all_eps_1_to_x_time_threshold = 1 AND watched_n_avail_eps_time_threshold = 1)         AS users_n_avail_of_all_1_to_n_time_cnt,

    -- percent-threshold penultimate/final (available)
    COUNTIF(watched_pct_threshold_avail_penultimate = 1 AND watched_pct_threshold_of_this_ep = 1) AS users_pct_threshold_avail_penultimate_of_pct_x_cnt,
    COUNTIF(watched_pct_threshold_avail_final = 1 AND watched_pct_threshold_of_this_ep = 1)       AS users_pct_threshold_avail_final_of_pct_x_cnt

  FROM cte5_user_flags f
  GROUP BY
    season_display_title, series_premiere_dt, series_display_title, season_number, genre, release_model, is_original,
    episodes_initial_drop, episodes_intended_total, episodes_total_adjusted, episodes_total_available, available_episodes_raw,
    days_since_premiere, cohort_week, platform_group, plan_tier, completion_episode_threshold, actual_episode_nbr,
    batch_first_episode_nbr, batch_final_episode_nbr, batch_penultimate_episode_nbr, episode_display_title
)

-- ---------- Final output (neutral, non-redundant names) ----------
SELECT
  -- dimensions
  season_display_title              AS season_label,
  series_premiere_dt                AS premiere_date,
  series_display_title              AS series_name,
  COALESCE(season_number, '0')      AS season_number,
  genre                              AS genre,
  release_model                      AS release_model,
  is_original                        AS is_original_flag,
  episodes_initial_drop              AS episodes_released_at_launch,
  episodes_intended_total            AS episodes_total_intended,
  episodes_total_adjusted            AS episodes_total_adjusted,
  episodes_total_available           AS episodes_total_available,
  available_episodes_raw             AS episodes_available_raw,
  days_since_premiere                AS cohort_days_since_premiere,
  cohort_week                        AS cohort_week_number,
  platform_group                     AS platform_group,
  plan_tier                          AS plan_tier,
  completion_episode_threshold       AS threshold_episode_n,
  actual_episode_nbr                 AS episode_number,
  batch_first_episode_nbr            AS batch_first_episode_number,
  batch_final_episode_nbr            AS batch_final_episode_number,
  batch_penultimate_episode_nbr      AS batch_penultimate_episode_number,
  episode_display_title              AS episode_title,
  last_day_dt                        AS cohort_window_end_date,

  -- metrics (generic, 100%-agnostic)
  users_ep_pct_threshold_cnt                       AS cnt_users_episode_pct_threshold,
  users_any_ep_time_threshold_cnt                  AS cnt_users_any_ep_time_threshold,
  users_ge_threshold_eps_time_cnt                  AS cnt_users_ge_threshold_eps_time,
  users_all_eps_1_to_n_time_cnt                    AS cnt_users_all_eps_1_to_n_time,
  users_nminus1_of_ge_n_eps_time_cnt               AS cnt_users_nminus1_of_ge_n_eps_time,
  users_n_of_ge_n_eps_time_cnt                     AS cnt_users_n_of_ge_n_eps_time,
  users_nminus1_of_all_1_to_n_time_cnt             AS cnt_users_nminus1_of_all_1_to_n_time,
  users_n_of_all_1_to_n_time_cnt                   AS cnt_users_n_of_all_1_to_n_time,
  users_pct_threshold_penultimate_of_pct_x_cnt     AS cnt_users_pct_threshold_penultimate_of_pct_x,
  users_pct_threshold_final_of_pct_x_cnt           AS cnt_users_pct_threshold_final_of_pct_x,
  users_nminus1_avail_of_ge_n_eps_time_cnt         AS cnt_users_nminus1_avail_of_ge_n_eps_time,
  users_n_avail_of_ge_n_eps_time_cnt               AS cnt_users_n_avail_of_ge_n_eps_time,
  users_nminus1_avail_of_all_1_to_n_time_cnt       AS cnt_users_nminus1_avail_of_all_1_to_n_time,
  users_n_avail_of_all_1_to_n_time_cnt             AS cnt_users_n_avail_of_all_1_to_n_time,
  users_pct_threshold_avail_penultimate_of_pct_x_cnt AS cnt_users_pct_threshold_avail_penultimate_of_pct_x,
  users_pct_threshold_avail_final_of_pct_x_cnt       AS cnt_users_pct_threshold_avail_final_of_pct_x

  --include section here if want to calculate rates in final table

FROM cte6_aggregated_counts;
