--- Lomen dotazy IMDb

--- 1. SQL dotaz pre: Distribúcia Priemerného Hodnotenia Filmov
SELECT avg_rating, COUNT(*) AS movie_count
FROM fact_ratings
GROUP BY avg_rating
ORDER BY avg_rating;

--- 2. SQL dotaz pre: Top 10 Najproduktívnejších Režisérov
SELECT dd.name AS director_name, COUNT(*) AS movie_count
FROM fact_ratings fr
JOIN dim_director dd ON fr.dim_director_id = dd.dim_director_id
GROUP BY dd.name
ORDER BY movie_count DESC
LIMIT 10;


--- 3. SQL dotaz pre: Najpopulárnejšie Filmové Žánre Podľa Počtu Filmov
SELECT dg.genre, COUNT(*) AS genre_count
FROM fact_ratings fr
JOIN dim_genre dg ON fr.dim_genre_id = dg.dim_genre_id
GROUP BY dg.genre
ORDER BY genre_count DESC;

--- 4. SQL dotaz pre: Vývoj Počtu Filmov v Čase
SELECT dm.year, COUNT(*) AS movie_count
FROM dim_movie dm
GROUP BY dm.year
ORDER BY dm.year;


--- 5. SQL dotaz pre: Priemerné Hodnotenie Filmov podľa Produkčných Spoločností
SELECT dm.production_company, AVG(fr.avg_rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_movie dm ON fr.dim_movie_id = dm.dim_movie_id
WHERE dm.production_company IS NOT NULL
GROUP BY dm.production_company
ORDER BY avg_rating DESC
LIMIT 10;
