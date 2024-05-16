INSERT INTO adbeyer.actors WITH previous_year AS ---cummulative table composition query
            (
                   SELECT *
                   FROM   adbeyer.actors
                   WHERE  current_year = 1918 --arbritrary value to show how it would work 
            )
            ,
            current_year AS
            (
                     SELECT   actor,
                              actor_id,
                              array_agg(row(year, film, votes, rating , film_id )) AS films, --an array as an actor could be in multiple films in the same year
                              avg(rating)                                    AS avg_rating,
                              max(year)                                      AS year
                     FROM     bootcamp.actor_films
                     WHERE    year = 1919 --always going to be the year after the year in previous_year
                     GROUP BY actor,
                              actor_id
            )SELECT          COALESCE(py.actor, cy.actor)       AS actor,
                   COALESCE(py.actor_id, cy.actor_id) AS actor_id,
                   CASE
                                   WHEN cy.films IS NULL THEN py.films
                                   WHEN py.films IS NULL THEN cy.films
                                   ELSE cy.films
                                                                   || py.films
                   END AS films , (
                   CASE
                                   WHEN cy.avg_rating IS NOT NULL THEN (
                                                   CASE
                                                                   WHEN avg_rating > 8 THEN 'star'
                                                                   WHEN avg_rating > 7
                                                                   AND             avg_rating <= 8 THEN 'good'
                                                                   WHEN avg_rating > 6
                                                                   AND             avg_rating <=7 THEN 'average'
                                                                   ELSE 'bad'
                                                   END)
                                   ELSE py.quality_class
                   END )                                  AS quality_class,
                   cy.films IS not NULL                   AS is_active,
                   COALESCE(cy.year, py.current_year + 1) AS current_year
   FROM            previous_year py
   FULL OUTER JOIN current_year cy
   ON              py.actor_id = cy.actor_id
