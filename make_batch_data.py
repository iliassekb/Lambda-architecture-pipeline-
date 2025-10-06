import os, random, time, csv

# Output directory and file path
out_dir = "data/batch_input"
os.makedirs(out_dir, exist_ok=True)
out_path = os.path.join(out_dir, "batch-000.csv")

# Set random seed for reproducibility
random.seed(42)

# Generate 50 user IDs (u001, u002, ..., u050)
users = [f"u{n:03d}" for n in range(1, 51)]

# Write CSV file with simulated historical data
with open(out_path, "w", newline="") as f:
    w = csv.writer(f)
    for _ in range(2000):
        uid = random.choice(users)
        etype = random.choices(["view", "purchase"], weights=[0.7, 0.3])[0]
        amount = round(random.uniform(5, 120), 2) if etype == "purchase" else 0.0
        ts = int(time.time()) - random.randint(2*24*3600, 30*24*3600)
        w.writerow([uid, etype, amount, ts])

print(f"Wrote historical CSV -> {out_path}")
