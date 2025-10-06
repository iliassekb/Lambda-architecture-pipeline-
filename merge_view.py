import os, glob, csv, time
from collections import defaultdict

# Directories for batch, speed, and merged views
BATCH_DIR, SPEED_DIR, VIEW_DIR = "data/serving/batch", "data/serving/speed", "data/serving/view"
os.makedirs(VIEW_DIR, exist_ok=True)

# --- Utility: get the newest subdirectory (latest batch/speed run) ---
def newest_subdir(root):
    subs = [p for p in glob.glob(os.path.join(root, "*")) if os.path.isdir(p)]
    return max(subs, key=os.path.getmtime) if subs else None

# --- Utility: read aggregated files from a directory ---
def read_agg_dir(path):
    if not path or not os.path.isdir(path):
        return {}
    agg = {}
    for p in sorted(glob.glob(os.path.join(path, "part-*"))):
        with open(p) as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) == 3:
                    uid, c, t = parts
                    try:
                        agg[uid] = (int(c), float(t))
                    except:
                        pass
    return agg

# --- Utility: merge two aggregations (batch + speed) ---
def merge(a, b):
    out = defaultdict(lambda: (0, 0.0))
    for src in [a, b]:
        for uid, (c, t) in src.items():
            prev = out[uid]
            out[uid] = (prev[0] + c, prev[1] + t)
    return out

# --- Utility: write merged view safely ---
def write_view(d, path):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "purchase_count_total", "purchase_amount_total"])
        for uid, (c, t) in sorted(d.items()):
            w.writerow([uid, c, f"{t:.2f}"])
    os.replace(tmp, path)

# --- Main serving process ---
print("Serving merger running...")

while True:
    batch_agg = read_agg_dir(newest_subdir(BATCH_DIR))
    speed_agg = read_agg_dir(newest_subdir(SPEED_DIR))
    merged = merge(batch_agg, speed_agg)
    write_view(merged, os.path.join(VIEW_DIR, "merged_view.csv"))
    time.sleep(5)
