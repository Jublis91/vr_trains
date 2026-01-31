# Muistiinpanot

## Row vs columnar storage
- **Row store** on hyvä OLTP-tyyppiseen käyttöön, koska rivin kaikki kentät ovat vierekkäin.
- **Columnar store** loistaa OLAP-kyselyissä (aggregaatit, scans), koska luetaan vain tarvittavat sarakkeet.

## OLAP-kyselyt ja indeksit
- OLAP-kyselyissä tehdään usein laajoja skannauksia ja aggregaatteja.
- Columnar + vektorointi vähentää I/O:ta.
- Indeksit auttavat selektiivisiin suodatuksiin, mutta isossa analytiikassa tärkeämpää on partitiointi/segmentointi ja columnar-compression.

## JSON-skeemat
- DuckDB:
  - `read_json(columns={"col1": "INTEGER", "col2": "VARCHAR"})`
- Polars:
  - `pl.read_json(file_path, schema={"col1": pl.Int64, "col2": pl.Utf8})`

## NDJSON / JSON Lines
- Jokainen rivi on yksi JSON-objekti.
- Helppo streamata ja appendata.

## Parquet: types
- https://parquet.apache.org/documentation/latest/

## DuckDB: data types
- https://duckdb.org/docs/sql/data_types/overview.html
