INSERT INTO adbeyer.actors_history_scd --creates scd table as a btach
WITH lagged
     AS (SELECT *,
                Lag(is_active, 1)
                  over(
                    PARTITION BY actor_id
                    ORDER BY current_year) is_active_lag,
                Lag(quality_class, 1)
                  over(
                    PARTITION BY actor_id
                    ORDER BY current_year) AS quality_class_lag
         FROM   adbeyer.actors
         WHERE  current_year <= 1919), --date is arbitrary
     summed_lag
     AS (SELECT *,
                SUM(CASE
                      WHEN is_active_lag = is_active
                           AND quality_class_lag = quality_class THEN 0
                      ELSE 1
                    END)
                  over (
                    PARTITION BY actor_id
                    ORDER BY current_year) AS INDICATOR
         FROM   lagged),
     aggregated
     AS (SELECT actor_id,
                actor,
                quality_class,
                is_active,
                INDICATOR,
                Min(current_year) AS start_date,
                Max(current_year) AS end_date,
                1919              AS current_year
         FROM   summed_lag
         GROUP  BY 1,
                   2,
                   3,
                   4,
                   5)
SELECT actor_id,
       actor,
       quality_class,
       is_active,
       start_date,
       end_date,
       current_year
FROM   aggregated 
