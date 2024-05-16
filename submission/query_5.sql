INSERT INTO adbeyer.actors_history_scd --incrementally updates an scd table
WITH current
     AS (SELECT actor,
                actor_id,
                quality_class,
                is_active
         FROM   adbeyer.actors
         WHERE  current_year = 1918),
     previous
     AS (SELECT actor_name,
                actor_id,
                quality_class,
                is_active,
                start_date
         FROM   adbeyer.actors_history_scd
         WHERE  end_date = 1917), --year is always 1 year before the year in current
     same
     AS (SELECT c.actor,
                c.actor_id,
                c.quality_class,
                c.is_active,
                p.start_date,
                1917 AS end_date,
                1917 AS current_year
         FROM   current c
                join previous p
                  ON c.actor_id = p.actor_id
                     AND c.is_active = p.is_active
                     AND c.quality_class = p.quality_class
                     AND c.actor = p.actor_name),
     new
     AS (SELECT c.actor,
                c.actor_id,
                c.quality_class,
                c.is_active,
                1917 AS start_date,
                1917 AS end_date,
                1917 AS current_year
         FROM   CURRENT c
                left join previous p
                       ON p.actor_id = c.actor_id
         WHERE  p.actor_name IS NULL),
     different
     AS (SELECT c.actor,
                c.actor_id,
                c.quality_class,
                c.is_active,
                1917 AS start_date,
                1917 AS end_date,
                1917 AS current_year
         FROM   current c
                join previous p
                  ON p.actor_id = c.actor_id
         WHERE  ( p.actor_name != c.actor )
                 OR ( p.is_active != c.is_active )
                 OR ( p.quality_class != c.quality_class )),
     historical
     AS (SELECT actor_name,
                actor_id,
                quality_class,
                is_active,
                start_date,
                end_date,
                current_year
         FROM   adbeyer.actors_history_scd
         WHERE  current_year = 1917
                AND end_date < 1917)
SELECT * --combining the historical records (before 1917 above), records in which there were a change, records that are new, and records that are the same as the previous year
FROM   different
UNION
SELECT *
FROM   new
UNION
SELECT *
FROM   same
UNION
SELECT *
FROM   historical 
