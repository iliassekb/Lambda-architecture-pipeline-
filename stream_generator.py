import os, random, time, csv

# Directory to store streaming data
stream_dir = "data/stream_input"
os.makedirs(stream_dir, exist_ok=True)

# Generate 100 users (u001, u002, ..., u100)
users = [f"u{n:03d}" for n in range(1, 101)]

# Write atomically (temporary file then rename)
def write_atomic(rows, path):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        csv.writer(f).writerows(rows)
    os.replace(tmp, path)

batch_id = 0
while True:
    batch_id += 1
    now = int(time.time())
    rows = []
    for _ in range(random.randint(50, 120)):
        uid = random.choice(users)
        etype = random.choices(["view", "purchase"], weights=[0.8, 0.2])[0]
        amount = round(random.uniform(5, 150), 2) if etype == "purchase" else 0.0
        rows.append([uid, etype, amount, now])

    out_path = os.path.join(stream_dir, f"events-{now}-{batch_id:05d}.csv")
    write_atomic(rows, out_path)
    print(f"Wrote stream file: {out_path}")
    time.sleep(5)
