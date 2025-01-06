# **ETL proces datasetu IMDb**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **IMDB datasetu**. Projekt sa zameriava na preskúmanie správania divákov a ich preferencií vo filmových a seriálových hodnoteniach, ako aj na analýzu demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrík, ako sú hodnotenia filmov, obľúbenosť žánrov a výkonnosť režisérov či hercov.

---
## **1. Úvod a popis zdrojových dát**
Cieľom tohto semestrálneho projektu je analyzovať rozsiahle dátové súbory z databázy IMDb (Internet Movie Database). IMDb je jednou z najväčších online databáz filmov, televíznych seriálov a hercov na svete. Analýza týchto dát nám umožní získať cenné poznatky o trendoch vo filmovom priemysle, identifikovať najúspešnejšie filmy a hercov, preskúmať vzťahy medzi rôznymi atribútmi filmov (žánre, režiséri, herci) a predpovedať potenciálny úspech nových filmov.

Zdrojové dáta pochádzajú z GitHubu dostupného [tu](https://github.com/AntaraChat/SQL---IMDb-Movie-Analysis/blob/main/EXECUTIVE%20SUMMARY.pdf). Dataset obsahuje päť hlavných tabuliek:
- `movies`
- `ratings`
- `genre`
- `director_mapping`
- `role_mapping`
- `names`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/OjoLomen/databazy/blob/main/IMDB_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDb</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_movie`**: Obsahuje podrobné informácie o filmoch (názov, rok vydania, trvanie, rozpočet).
- **`dim_name`**: Obsahuje informácie o jednotlivcoch (herci, režiséri, scenáristi) vrátane ich mien, dátumov narodenia a kariérnych úspechov.
- **`dim_director`**: Zahrňuje informácie o režiséroch, ich identifikátoroch a relevantných atribútoch.
- **`dim_genre`**: Obsahuje kategorizáciu filmov podľa žánrov (napr. akčný, dráma, komédia).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/OjoLomen/databazy/blob/main/dim_model.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre IMDb</em>
</p>

---
## **3 ETL proces v Snowflake**
ETL proces zahŕňal tri kľúčové fázy: extrakciu (Extract), transformáciu (Transform) a nahrávanie (Load). V prostredí Snowflake bol tento proces realizovaný s cieľom spracovať zdrojové dáta zo staging vrstvy a pripraviť ich do viacdimenzionálneho dátového modelu vhodného na analytické spracovanie a vizualizáciu.

---

### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `TERMITE_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE TERMITE_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO names_staging
FROM @TERMITE_stage/names.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```

Tu som použil parameter `ON_ERROR = 'CONTINUE'`, v prípade nekonzistentných záznamov, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a pripravené na analytické účely. Hlavným cieľom bolo vytvoriť dimenzionálne tabuľky a faktovú tabuľku, ktoré umožnia efektívnu analýzu a rýchle získavanie odpovedí na kľúčové otázky.

#### **Dimenzia `dim_movie`**
Dimenzia `dim_movie` obsahuje informácie o filmoch, ako názov, rok vydania a produkčnú spoločnosť. Táto dimenzia bola navrhnutá ako SCD Typ 0 (Static), pretože údaje o filmoch sa v čase nemenia. Filmy sa pridávajú ako nové záznamy a staré údaje sa nemenia, čo šetrí miesto a zjednodušuje správu dát.
```sql
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
```
Primárny kľúč: `dim_movie_id`
SCD Typ: `0` (nemenná tabuľka, statické údaje)

#### **Dimenzia `dim_names`**
Dimenzia dim_names sa týka hercov a obsahuje informácie ako meno, výšku a známe filmy. Pre túto dimenziu sme použili SCD Typ 2 (Historical), pretože chceme sledovať historické zmeny hercov, napríklad ak sa mení ich známy film alebo ak sa zmení osobná charakteristika. Tento prístup zabezpečuje, že všetky zmeny sú zachované.

- Short (do 150 cm)
- Average (150 – 180 cm)
- Tall (nad 180 cm)

```sql
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
```
Primárny kľúč: `dim_actor_id`
SCD Typ: `1` (hodnoty sa môžu aktualizovať bez uchovávania histórie)

#### **Dimenzia `dim_genre`**
Dimenzia dim_genre obsahuje informácie o žánroch filmov, ako je akčný, romantický alebo komédia. Táto dimenzia je statická, takže bola navrhnutá ako SCD Typ 0 (Static). Žánre sa zvyčajne nemenia, takže nové žánre sa jednoducho pridávajú ako nové záznamy, čím sa zjednodušuje správa.

```sql
CREATE TABLE dim_genre AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY g.genre) AS dim_genre_id,
    g.genre
FROM genre_staging g
GROUP BY g.genre;
```
Primárny kľúč: `dim_genre_id`
SCD Typ: `0` (statické údaje bez historických zmien)

#### **Dimenzia `dim_director`**
Dimenzia dim_director obsahuje informácie o režiséroch filmov. Táto dimenzia bola navrhnutá ako SCD Typ 1 (Overwriting), pretože informácie o režiséroch sa môžu meniť, ale uchovávame len najaktuálnejšie údaje. Ak sa zmení informácia, jednoducho sa nahradí, čo uľahčuje správu aktuálnych údajov.

```sql
CREATE OR REPLACE TABLE dim_director AS
SELECT 
    n.id AS dim_director_id,
    n.name,
    n.date_of_birth,
    n.known_for_movies
FROM names_staging n
JOIN director_mapping_staging d ON n.id = d.name_id
GROUP BY n.id, n.name, n.date_of_birth, n.known_for_movies;
```
Primárny kľúč: `dim_director_id`
SCD Typ: `1` (hodnoty sa môžu aktualizovať bez uchovávania histórie)

#### **Faktová tabuľka `fact_ratings`**
Faktová tabuľka fact_ratings spája dimenzie s faktami ako priemerné hodnotenie a počet hlasov. Je navrhnutá tak, aby poskytovala analytický základ pre sledovanie výkonu filmov a hodnotenia. Táto tabuľka je optimálne navrhnutá na vykonávanie analytických dotazov a vizualizácií.

Týmto spôsobom sme zabezpečili, že model je flexibilný a zároveň efektívny pri vykonávaní analýz.

```sql
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
```
---
### **3.3 Load (Načítanie dát)**
Po vytvorení dimenzií a faktovej tabuľky boli dáta nahrané do finálnej štruktúry. Staging tabuľky boli následne odstránené, aby sa optimalizovalo využitie úložiska.

```sql
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;
```
ETL proces v Snowflake spracoval pôvodné dáta z formátu .csv do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu dát, čím sa vytvoril model vhodný pre analýzu a vizualizácie, ktoré poskytujú prehľad o hodnoteniach a trendoch filmov.

---

## **4. Vizualizácia dát**
Dashboard obsahuje 5 vizualizácií, ktoré poskytujú prehľad o kľúčových trendoch a metrikách v oblasti filmov, hercov, režisérov, žánrov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky, ktoré umožňujú lepšie pochopiť správanie používateľov a ich preferencie. Všetky vizualizácie sú navrhnuté tak, aby poskytovali detailný pohľad na rôzne aspekty filmového priemyslu, pričom zameriavajú pozornosť na najdôležitejšie faktory, ktoré ovplyvňujú výber filmov a hodnotenie používateľmi.

<p align="center">
  <img src="https://github.com/OjoLomen/databazy/blob/main/IMDB_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDb</em>
</p>

---

Graf 1: Distribúcia Priemerného Hodnotenia Filmov
Tento graf zobrazuje, ako sa filmy rozdeľujú podľa ich priemerného hodnotenia. Pomocou tejto vizualizácie môžeme získať prehľad o kvalite filmov a ich hodnotení medzi používateľmi. Zobrazuje počet filmov, ktoré sa nachádzajú v rôznych hodnotiacich intervaloch, čo nám umožňuje analyzovať, či väčšina filmov patrí do vyšších alebo nižších hodnotiacich kategórií. Táto vizualizácia poskytuje dôležité informácie pre analýzu preferencií a hodnotenia filmov.

```sql
SELECT avg_rating, COUNT(*) AS movie_count
FROM fact_ratings
GROUP BY avg_rating
ORDER BY avg_rating;
```

---

Graf 2: Top 10 Najproduktívnejších Režisérov
Táto vizualizácia zobrazuje 10 režisérov s najväčším počtom filmov v našej databáze. Pomáha nám identifikovať, ktorí režiséri sú najaktívnejší v produkcii filmov a ktorí z nich sa podieľali na najväčšom počte projektov. Tieto informácie môžu byť užitočné pri analýze kariérnych dráh režisérov alebo pri hodnotení ich vplyvu na filmový priemysel.

```sql
SELECT dd.name AS director_name, COUNT(*) AS movie_count
FROM fact_ratings fr
JOIN dim_director dd ON fr.dim_director_id = dd.dim_director_id
GROUP BY dd.name
ORDER BY movie_count DESC
LIMIT 10;
```

---

Graf 3: Najpopulárnejšie Filmové Žánre Podľa Počtu Filmov
Tento graf zobrazuje najpopulárnejšie filmové žánre na základe počtu filmov v každom žánri. Táto vizualizácia pomáha identifikovať dominujúce žánre, ktoré sú najviac zastúpené v databáze. Môžeme sledovať, ako sa vyvíjajú trendy v oblasti filmovej produkcie a aké žánre sú najviac vyhľadávané. Analýza týchto dát môže byť užitočná pri predpovedaní populárnych žánrov v budúcnosti, ako aj pri vytváraní marketingových kampaní zameraných na konkrétne skupiny divákov.

```sql
SELECT dg.genre, COUNT(*) AS genre_count
FROM fact_ratings fr
JOIN dim_genre dg ON fr.dim_genre_id = dg.dim_genre_id
GROUP BY dg.genre
ORDER BY genre_count DESC;

```

---

Graf 4: Vývoj Počtu Filmov v Čase
Táto vizualizácia ukazuje, ako sa počet filmov menil v priebehu rokov. Pomáha analyzovať vývoj filmovej produkcie v rôznych obdobiach a odhaliť trendy v intenzite produkcie filmov. Môže sa ukázať, že niektoré roky zaznamenali výrazný nárast v počte filmov, zatiaľ čo iné obdobia boli menej aktívne. Tento typ analýzy poskytuje hodnotné informácie o dynamike filmového priemyslu a jeho vývoji v časovom horizonte.

```sql
SELECT dm.year, COUNT(*) AS movie_count
FROM dim_movie dm
GROUP BY dm.year
ORDER BY dm.year;
```

---

Graf 5. Priemerné Hodnotenie Filmov podľa Produkčných Spoločností
Tento graf zobrazuje priemerné hodnotenie filmov podľa produkčných spoločností. Umožňuje identifikovať produkčné spoločnosti, ktoré vytvárajú filmy s najvyšším priemerným hodnotením, a naopak, spoločnosti, ktorých filmy nedosahujú vysoké hodnotenia. Tieto informácie môžu byť užitočné pre hodnotenie kvality filmovej produkcie rôznych spoločností, a to nielen z hľadiska hodnotenia používateľov, ale aj z pohľadu budúcej spolupráce alebo investícií.

```sql
SELECT dm.production_company, AVG(fr.avg_rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_movie dm ON fr.dim_movie_id = dm.dim_movie_id
WHERE dm.production_company IS NOT NULL
GROUP BY dm.production_company
ORDER BY avg_rating DESC
LIMIT 10;
```

---

Autor: Andrej Lomen
