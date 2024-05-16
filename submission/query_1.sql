CREATE or REPLACE table adbeyer.actors  (
	"actor"  VARCHAR,
	"actor_id" VARCHAR,
	"films" ARRAY(
	Row(year INT, film VARCHAR, votes INT, rating DOUBLE, film_id VARCHAR) --creating an array for cumulative table design
	),
	"quality_class" VARCHAR, --- either star,good, average, bad
	"is_active" BOOLEAN,
	"current_year" INT
	)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['current_year']

)
