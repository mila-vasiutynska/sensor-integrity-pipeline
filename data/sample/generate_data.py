import json
import random
import string
from datetime import datetime, timedelta
import time
import os

random.seed(42)

NUM_CUSTOMERS = 25
READINGS_PER_CUSTOMER = 12

# TUNING knobs (to reduce drops)
CONSENT_RATE = 0.92          # 92% of customers consent
CONSENT_EARLY_PCT = 0.85     # consent happens in first 85% of cases very early
NULL_RATE = 0.01             # 1% nulls
OUTLIER_RATE = 0.01          # 1% outliers
DUPLICATE_ROWS = 1           # keep only 1 duplicate

base_time = int(time.time())

def random_phone():
    return f"+61{random.randint(400000000, 499999999)}"

def random_serial():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))

customers = []
accelerometer = []
step_trainer = []

for i in range(NUM_CUSTOMERS):
    serial = random_serial()
    email = f"user{i}@example.com"

    registration = base_time - random.randint(100000, 200000)
    last_update = registration + random.randint(1000, 50000)

    # Consent for most customers
    consent_given = random.random() < CONSENT_RATE

    # Event window (hourly readings spanning ~12 hours)
    event_start = registration - 2 * 3600
    event_end = event_start + (READINGS_PER_CUSTOMER - 1) * 3600

    if consent_given:
        # Make consent typically early so most events survive.
        # 85%: consent in first 0–2 hours
        # 15%: consent somewhere within the window (still realistic)
        if random.random() < CONSENT_EARLY_PCT:
            consent_time = event_start + random.randint(0, 2 * 3600)
        else:
            consent_time = event_start + random.randint(0, max(1, event_end - event_start))
    else:
        consent_time = None

    customer = {
        "customerName": f"Customer_{i}",
        "email": email,
        "phone": random_phone(),
        "birthDay": str(datetime(1980, 1, 1) + timedelta(days=random.randint(0, 15000))),
        # This serialNumber represents the Step Trainer device
        "serialNumber": serial,
        "registrationDate": registration,
        "lastUpdateDate": last_update,
        "shareWithResearchAsOfDate": consent_time,
        "shareWithPublicAsOfDate": None,
        "shareWithFriendsAsOfDate": None
    }
    customers.append(customer)

    for j in range(READINGS_PER_CUSTOMER):
        ts = event_start + j * 3600

        # Accelerometer belongs to the PHONE/user (email), not the step trainer device.
        accel = {
            "user": email,         # join key to customers.email
            "timestamp": ts,       # epoch seconds
            "x": round(random.uniform(-2.5, 2.5), 4),
            "y": round(random.uniform(-2.5, 2.5), 4),
            "z": round(random.uniform(-2.5, 2.5), 4),
        }

        # Rare nulls/outliers only
        if random.random() < NULL_RATE:
            accel["x"] = None
        if random.random() < OUTLIER_RATE:
            accel["z"] = 9.5

        accelerometer.append(accel)

        # Step trainer belongs to the DEVICE (serialNumber)
        step_trainer.append({
            "sensorReadingTime": datetime.fromtimestamp(ts).isoformat(),
            "serialNumber": serial,
            "distanceFromObject": random.randint(20, 200)
        })

# Introduce a small number of duplicates (duplicate an accelerometer row)
for _ in range(DUPLICATE_ROWS):
    accelerometer.append(accelerometer[0])

# Force at least 2 deterministic outliers in accelerometer data (if rows exist)
if len(accelerometer) > 15:
    accelerometer[5]["z"] = 9.5
    accelerometer[15]["x"] = -9.5

os.makedirs("data/sample", exist_ok=True)

def write_json_lines(path, rows):
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

write_json_lines("data/sample/customers.json", customers)
write_json_lines("data/sample/accelerometer.json", accelerometer)
write_json_lines("data/sample/step_trainer.json", step_trainer)

print("Tuned synthetic dataset generated (realistic keys: accel=user email, trainer=serialNumber).")