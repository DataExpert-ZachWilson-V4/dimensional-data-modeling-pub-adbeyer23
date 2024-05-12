CREATE or REPLACE TABLE adbeyer.actors_history_scd (
  actor_id VARCHAR,
  actor_name VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER --adding current year to be able to quickly filter new data
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']

)
