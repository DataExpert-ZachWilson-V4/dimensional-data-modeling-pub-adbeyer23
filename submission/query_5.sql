INSERT INTO adbeyer.actors_history_scd --incrementally updates an scd table
WITH --combining the previous year scd with the current year's data
  last_year_scd AS (
    SELECT
      *
    FROM
      adbeyer.actors_history_scd
    WHERE
      current_year = 1919
  ),
  current_year AS (
    SELECT
      *
    FROM
      adbeyer.actors
    WHERE
      current_year = 1920
  ),
  combined AS ( --combine last year and current year
    SELECT
      COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
      COALESCE(ly.start_date, cy.current_year) AS start_year,
      COALESCE(ly.start_date, cy.current_year) AS end_year,
      CASE --seeing if active changed
        WHEN COALESCE(ly.is_active, FALSE) <> cy.is_active THEN 1
        WHEN COALESCE(ly.is_active, FALSE) = cy.is_active THEN 0
      END AS active_change,
      -- active for this and last year 
      COALESCE(ly.is_active, FALSE) AS is_active_last_year,
      cy.is_active AS is_active_this_year,
      CASE --seeing if quality class changed
        WHEN COALESCE(ly.quality_class, '') <> cy.quality_class THEN 1
        WHEN COALESCE(ly.quality_class, '') = cy.quality_class THEN 0
      END AS qc_change,
      -- quality class values for this and last year
      COALESCE(ly.quality_class, '') AS qc_last_year,
      cy.quality_class AS qc_this_year,
      1920 AS current_year  -- current year
    FROM
      last_year_scd AS ly
      FULL OUTER JOIN current_year AS cy ON ly.actor_id = cy.actor_id
      AND ly.end_date + 1 = cy.current_year
  ),
  results AS (
    SELECT
      actor_id,
      current_year,
      CASE
      -- no changes
        WHEN active_change = 0
        AND qc_change = 0 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              qc_last_year,
              start_year,
              end_year + 1
            ) AS ROW(
              is_active boolean,
              qc VARCHAR,
              start_year integer,
              end_year integer
            )
          )
        ]
        -- something changed
        WHEN active_change = 1
        OR qc_change = 1 THEN ARRAY[
          CAST(
            ROW(
              is_active_last_year,
              qc_last_year,
              start_year,
              end_year
            ) AS ROW(
              is_active boolean,
              qc VARCHAR,
              start_year integer,
              end_year integer
            )
          ),
          CAST(
            ROW(
              is_active_this_year,
              qc_this_year,
              current_year,
              current_year
            ) AS ROW(
              is_active boolean,
              qc VARCHAR,
              start_year integer,
              end_year integer
            )
          )
        ] --when there are new records
        WHEN active_change IS NULL
        OR qc_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_last_year, is_active_this_year),
              COALESCE(qc_last_year, qc_this_year),
              start_year,
              end_year
            ) AS ROW(
              is_active boolean,
              qc VARCHAR,
              start_year integer,
              end_year integer
            )
          )
        ]
      END AS change_array
    FROM
      combined
  )
SELECT  --unpacking the array to produce the results
  results.actor_id,
  arr.qc,
  arr.is_active,
  arr.start_year,
  arr.end_year,
  results.current_year
FROM
  results
  CROSS JOIN UNNEST (results.change_array) AS arr
