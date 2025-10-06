from pyspark import SparkConf, SparkContext
import os, time

# Function to parse each CSV line
def parse_line(line):
    parts = line.strip().split(",")
    if len(parts) != 4:
        return None
    uid, etype, amount_s, ts_s = parts
    if etype != "purchase":
        return None
    try:
        amount = float(amount_s)
    except:
        return None
    return (uid, (1, amount))  # (user_id, (count, total_amount))

# Function to add tuples (sums counts and amounts)
def add_tuples(a, b):
    return (a[0] + b[0], a[1] + b[1])

# Main Spark job
if __name__ == "__main__":
    input_dir, output_dir = "data/batch_input", "data/serving/batch"
    os.makedirs(output_dir, exist_ok=True)

    # Configure Spark context
    sc = SparkContext(conf=SparkConf()
                      .setAppName("BatchLayer-RDD")
                      .setMaster("local[*]"))

    # Read input data
    rdd = sc.textFile(input_dir)

    # Parse, filter, and aggregate
    agg = (
        rdd.map(parse_line)
           .filter(lambda x: x)
           .reduceByKey(add_tuples)
           .map(lambda kv: f"{kv[0]},{kv[1][0]},{kv[1][1]:.2f}")
    )

    # Save aggregated output
    agg.saveAsTextFile(f"{output_dir}/batch-{int(time.time())}")

    sc.stop()
