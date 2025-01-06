-- Vytvorenie databázy
CREATE DATABASE TERMITE_IMDb_DB;

-- Vytvorenie schémy pre staging tabuľky
CREATE SCHEMA TERMITE_IMDb_DB.staging;

USE SCHEMA TERMITE_IMDb_DB.staging;

CREATE OR REPLACE STAGE TERMITE_stage;

-- Vytvorenie tabuľky names (staging)
CREATE TABLE names_staging (
    id VARCHAR(20) PRIMARY KEY, -- Zvýšenie dĺžky pre ID
    name VARCHAR(100),
    height INT,
    date_of_birth DATE,
    known_for_movies VARCHAR(100)
);

-- Vytvorenie tabuľky movie (staging)
CREATE TABLE movie_staging (
    id VARCHAR(20) PRIMARY KEY, -- Zvýšenie dĺžky pre ID
    title VARCHAR(200),
    year INT,
    date_published DATE,
    duration INT,
    country VARCHAR(100),
    worldwide_gross_income VARCHAR(30),
    languages VARCHAR(200),
    production_company VARCHAR(200)
);

-- Vytvorenie tabuľky ratings (staging)
CREATE TABLE ratings_staging (
    movie_id VARCHAR(20), -- Zvýšenie dĺžky pre ID
    avg_rating DECIMAL(3, 1),
    total_votes INT,
    median_rating INT,
    PRIMARY KEY (movie_id)
);

-- Vytvorenie tabuľky genre (staging)
CREATE TABLE genre_staging (
    movie_id VARCHAR(20), -- Zvýšenie dĺžky pre ID
    genre VARCHAR(20),
    PRIMARY KEY (movie_id, genre)
);

-- Vytvorenie tabuľky director_mapping (staging)
CREATE TABLE director_mapping_staging (
    movie_id VARCHAR(20), -- Zvýšenie dĺžky pre ID
    name_id VARCHAR(20), -- Zvýšenie dĺžky pre ID
    PRIMARY KEY (movie_id, name_id)
);

-- Vytvorenie tabuľky role_mapping (staging)
CREATE TABLE role_mapping_staging (
    movie_id VARCHAR(20), -- Zvýšenie dĺžky pre ID
    name_id VARCHAR(20), -- Zvýšenie dĺžky pre ID
    category VARCHAR(50),
    PRIMARY KEY (movie_id, name_id, category)
);

-- COPY príkazy pre načítanie dát
COPY INTO names_staging
FROM @TERMITE_stage/names.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL')  -- Treat 'NULL' as actual NULL value
)
ON_ERROR = 'CONTINUE';


COPY INTO movie_staging
FROM @TERMITE_stage/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @TERMITE_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genre_staging
FROM @TERMITE_stage/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO director_mapping_staging
FROM @TERMITE_stage/director_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO role_mapping_staging
FROM @TERMITE_stage/role_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

--- ELT - (T)ransform

-- dim_movie
CREATE OR REPLACE TABLE dim_movie AS
SELECT 
    m.id AS movie_id,
    ROW_NUMBER() OVER (ORDER BY m.title) AS dim_movie_id,
    m.title,
    m.year,
    m.duration,
    m.country,
    m.worldwide_gross_income,
    m.production_company,
    m.languages
FROM movie_staging m;

-- dim_names
CREATE OR REPLACE TABLE dim_names AS
SELECT 
    n.id AS dim_actor_id,
    n.name,
    CASE 
        WHEN n.height < 150 THEN 'Short'
        WHEN n.height BETWEEN 150 AND 180 THEN 'Average'
        ELSE 'Tall'
    END AS height,
    n.date_of_birth,
    n.known_for_movies
FROM names_staging n;


-- dim_genre
CREATE OR REPLACE TABLE dim_genre AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY g.genre) AS dim_genre_id,
    g.genre
FROM genre_staging g
GROUP BY g.genre;

-- dim_director
CREATE OR REPLACE TABLE dim_director AS
SELECT 
    n.id AS dim_director_id,
    n.name,
    n.date_of_birth,
    n.known_for_movies
FROM names_staging n
JOIN director_mapping_staging d ON n.id = d.name_id
GROUP BY n.id, n.name, n.date_of_birth, n.known_for_movies;


-- fact_ratings
CREATE OR REPLACE TABLE fact_ratings AS
SELECT 
    r.avg_rating,
    r.total_votes,
    r.median_rating,
    dm.dim_movie_id,
    dd.dim_director_id,
    dg.dim_genre_id,
    da.dim_actor_id
FROM ratings_staging r
JOIN dim_movie dm ON r.movie_id = dm.movie_id
LEFT JOIN director_mapping_staging d ON r.movie_id = d.movie_id
LEFT JOIN dim_director dd ON d.name_id = dd.dim_director_id
JOIN genre_staging g ON r.movie_id = g.movie_id
JOIN dim_genre dg ON g.genre = dg.genre
LEFT JOIN role_mapping_staging rm ON r.movie_id = rm.movie_id
LEFT JOIN dim_names da ON rm.name_id = da.dim_actor_id;



-- Drop staging tables
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;
