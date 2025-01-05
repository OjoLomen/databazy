# **ETL proces datasetu IMDb**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **IMDB datasetu**. Projekt sa zameriava na preskúmanie správania divákov a ich preferencií vo filmových a seriálových hodnoteniach, ako aj na analýzu demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrík, ako sú hodnotenia filmov, obľúbenosť žánrov a výkonnosť režisérov či hercov.

---
## **1. Úvod a popis zdrojových dát**
Cieľom tohto semestrálneho projektu je analyzovať rozsiahle dátové súbory z databázy IMDb (Internet Movie Database). IMDb je jednou z najväčších online databáz filmov, televíznych seriálov a hercov na svete. Analýza týchto dát nám umožní získať cenné poznatky o trendoch vo filmovom priemysle, identifikovať najúspešnejšie filmy a hercov, preskúmať vzťahy medzi rôznymi atribútmi filmov (žánre, režiséri, herci) a predpovedať potenciálny úspech nových filmov.

Zdrojové dáta pochádzajú z Kaggle datasetu dostupného [tu](https://www.kaggle.com/datasets/saurabhbagchi/books-dataset). Dataset obsahuje päť hlavných tabuliek:
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
