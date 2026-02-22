# Sensor Integrity Pipeline (PySpark)

A small, **locally runnable** PySpark pipeline for synthetic generated regulated-style sensor data workflow:
- strict schemas
- timestamp normalization
- **time-based consent enforcement**
- validation (nulls, ranges, duplicates)
- curated dataset build (join sensor + device readings)

The project is inspired by an earlier cloud-based ETL implementation:
https://github.com/mila-vasiutynska/glue-spark-data-lakes

In this version, the focus is on:
- realistic data modeling
- consent enforcement
- validation strategy

clean, testable local execution
---

## What to look at first (review path)

1. `submission.md` — engineering decisions + challenges
2. `src/pipeline.py` — end-to-end orchestration
3. `src/validator.py` — validation + consent enforcement
4. `src/transformer.py` — parsing + curated dataset join
5. `src/schemas.py` — strict input schemas

---

## Project structure
.
├── data/
│ └── sample/
│ ├── customers.json
│ ├── accelerometer.json
│ └── step_trainer.json
├── src/
│ ├── schemas.py
│ ├── loader.py
│ ├── transformer.py
│ ├── validator.py
│ └── pipeline.py
├── tests/
│ └── test_validator.py
├── generate_realistic_data.py
├── submission.md
└── README.md

---

## Data model (synthetic but realistic)

### customers.json (JSON Lines)
Includes time-based consent fields:
- `shareWithResearchAsOfDate` (epoch seconds) is treated as “consent timestamp”.
- Events earlier than consent are excluded from curated data.

Key fields:
- `customerName`, `email`, `phone`, `birthDay`
- `serialNumber`
- `registrationDate`, `lastUpdateDate` (epoch seconds)
- `shareWithResearchAsOfDate`, `shareWithPublicAsOfDate`, `shareWithFriendsAsOfDate` (epoch seconds / nullable)

### accelerometer.json (JSON Lines)
- `serialNumber`, `timestamp` (epoch seconds), `x`, `y`, `z`

### step_trainer.json (JSON Lines)
- `sensorReadingTime` (ISO string), `serialNumber`, `distanceFromObject` (int)

---

## Requirements

- Python 3.10+
- Java 11+ (required for Spark)

Install Python deps:
```bash
pip install pyspark==3.5.1 pytest
```
---

## Run local
1. Generate synthetic data
2. Run the Spark pipeline
3. Run tests

```bash
python3 ./data/sample/generate_data.py
python3 -m src.pipeline
pytest -q
```

## Expected output
Example run:
=== Validation Summary (Accelerometer) ===
Total rows:   301
Dropped rows: 58
- null_axis_rows: 4
- out_of_range_rows: 2
- duplicate_rows: 1
- no_consent_rows: 0
- pre_consent_rows: 51

=== Headline Metrics ===
- customers: 25
- accelerometer_raw: 301
- accelerometer_validated: 243
- curated: 243

## Inspect output

```bash
python3 src/inspect_parquet.py