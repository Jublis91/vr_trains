# VR Trains + dbt + DuckDB

Tämä repo tuo yhteen DuckDB:n, dbt-duckdb-adapterin sekä Digitraffic Rail API:n (VR:n julkinen rajapinta) esimerkkipolun. Mukana on:

- venv/requirements-ohjeet
- dbt-projekti valmiilla mallipoluilla (bronze/silver/gold)
- ingest-skripti Digitrafficin VR-train datan lataamiseen
- notebook-esimerkki DuckDB + Polars -kokeiluun
- muistiinpanot row vs columnar -varastoinnista, OLAP-kyselyistä ja indekseistä

## 1) Virtuaaliympäristö ja riippuvuudet

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Asennettavat paketit: `dbt-duckdb`, `duckdb`, `pyarrow`, `polars`, `requests`, `jupyter`.

## 2) Projektikansion rakenne

```
.
├── data/
├── dbt/
│   └── train_dbt/
│       ├── dbt_project.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── sources/
│       │   ├── staging/
│       │   ├── bronze/
│       │   ├── silver/
│       │   └── gold/
│       └── seeds/
├── src/
│   ├── ingest/
│   └── playground/
└── requirements.txt
```

## 3) DBT setup ja init

Käytä mukana tulevaa `dbt/train_dbt/dbt_project.yml` -projektia ja profiilia.

```bash
export DBT_PROFILES_DIR=./dbt
cd dbt/train_dbt
```

Testaa yhteys:

```bash
dbt debug
```

## 4) Aja dbt build

```bash
dbt build
```

Jos joku malli epäonnistuu, tarkista erityisesti `models/bronze/br_live_trains.sql` (tahallinen esimerkkirivi on merkitty kommentein). Korjaa se ja aja `dbt build` uudelleen.

## 5) dbt docs

```bash
dbt docs --help

dbt docs generate

dbt docs serve
```

## 6) Selaa DuckDB:tä

DuckDB-tietokanta syntyy oletuksena tiedostoon `data/vr_trains.duckdb`. Voit avata sen DuckDB CLI:llä tai Pythonilla.

## 7) Notebook-kokeilu (duckdb + polars)

Avaa `src/playground/jotain.ipynb` ja kokeile:
- `duckdb.connect()`
- `conn.execute()`

Notebookissa on valmiit esimerkkisolut.

## 8) CSV → taulu → Parquet

`data/sample_train_events.csv` on esimerkkiaineisto. Aja Python-skripti:

```bash
python src/ingest/csv_to_parquet.py
```

Tämä luo taulun DuckDB:hen ja kirjoittaa Parquetin polkuun `data/parquet/train_events.parquet`.

## 9) JSON-skeemat ja datatyypit

Katso `docs/notes.md` JSON-skeeman määrittelyn sekä NDJSON/Parquet/DuckDB -tyyppien pikaohjeisiin.

## 10) Ingestion (Digitraffic Rail API)

Aja ingest-skripti:

```bash
python src/ingest/fetch_digitraffic.py --station=HKI --days=1
python src/ingest/bronze_merge.py
```

`fetch_digitraffic.py` hakee junadataa Digitrafficista ja tallentaa JSON/NDJSON -tiedostot `data/staging/`-hakemistoon. `bronze_merge.py` vie ne DuckDB:n bronze-kerrokseen.

## 11) Mallit: bronze → silver → gold

Mallit on mallinnettu poluissa:
- `models/bronze` (raw/bronze)
- `models/silver` (intermediate/star)
- `models/gold` (presentation/aggregations)

## 12) Keskusteluaiheet

`docs/notes.md` sisältää tiiviin muistilistan:
- row vs columnar storage
- OLAP-kyselyt ja indeksit
- NDJSON/JSON Lines
- Parquet- ja DuckDB-datatyyppien linkit
