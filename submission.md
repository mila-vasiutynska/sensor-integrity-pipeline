# Project Submission – Sensor Integrity Pipeline

## Project Overview

This project is a local PySpark pipeline that processes customer, phone accelerometer, and step trainer device data to produce a clean dataset for analytics or future machine learning.
The project is inspired by an earlier cloud-based ETL implementation:
https://github.com/mila-vasiutynska/glue-spark-data-lakes

The pipeline:

- Reads raw JSON files using strict schemas
- Normalizes timestamps
- Enforces time-based research consent
- Validates sensor integrity (nulls, range checks, duplicates)
- Maps phone data to device data via customer relationships
- Produces curated Parquet output

I intentionally introduced realistic issues (nulls, outliers, duplicates, consent timing) to ensure the system handles imperfect data.

## Assumption About Device Placement

In this project, I assumed the step trainer device is worn on the lower leg or ankle and measures distance to the ground.
Because of that, the distance does not go to zero. It changes around a baseline value as the person walks.
In a real medical device, the exact placement of the sensor would affect how the signal looks and how features are engineered. I kept the model simple, but this is something I would consider carefully in a real system.


## Technical Challenge 1: Time-Based Consent

Consent is not just a boolean — it has a timestamp.
Sensor events recorded before consent must not be included in research data.

The challenge was aligning consent timestamps with high-frequency phone sensor events and filtering correctly.

Accelerometer events are linked to customers via email, and consent timestamps are stored at the customer level. I normalized timestamps to Spark types and kept only rows where:

`event_ts >= consent_ts`

This ensures historical correctness and mirrors how a regulated system should behave.

---

## Technical Challenge 2: Multi-Device Data Modeling

Accelerometer data originates from the mobile phone, while step trainer readings come from a separate physical device.

These datasets do not share a direct key.

The challenge was designing a realistic join path:

1. Link accelerometer events to customers via email
2. Map customers to their step trainer device (`serialNumber`)
3. Join with device readings using `serialNumber` and aligned timestamps

This reflects how real-world systems handle multi-device data integration.

---

## Technical Challenge 3: Validation Strategy

Raw sensor data is noisy. I needed to decide whether to repair invalid data or remove it.

I chose a fail-fast validation approach:

- Drop null axis values
- Remove out-of-range readings
- Remove duplicate events (`user` + `event_ts`)
- Enforce consent filtering

The pipeline prints validation metrics so data loss is transparent.

---

## Reflection

In a regulated medical environment, I would extend this with:

- Schema versioning
- Data lineage tracking
- Monitoring and alerting
- Access control and audit logs (who accessed or changed data)
- Data encryption and masking for sensitive information
- Clear data quality rules and automatic checks
- Ability to reprocess data safely if something fails
- Documentation for audits and compliance

This would make the pipeline secure, easy to track, reliable, and aligned with medical regulations.
