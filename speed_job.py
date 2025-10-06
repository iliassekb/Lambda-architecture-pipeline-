from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import os

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

# Function to add tuples (aggregate purchases)
def add_tuples(a, b):
    return (a[0] + b[0], a[1] + b[1])

# Function to convert tuple to CSV line
def to_line(kv):
    uid, (c, t) = kv
    return f"{uid},{c},{t:.2f}"

# Main streaming job
if __name__ == "__main__":
    conf = SparkConf().setAppName("SpeedLayer-DStreams").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)  # 10-second micro-batches

    input_dir, output_dir = "data/stream_input", "data/serving/speed"
    os.makedirs(output_dir, exist_ok=True)

    # Read new CSV files as they appear in the input directory
    dstream = (
        ssc.textFileStream(input_dir)
           .map(parse_line)
           .filter(lambda x: x)
           .reduceByKeyAndWindow(add_tuples, None, 60, 10)
           .map(to_line)
           .repartition(1)
    )

    # Save streaming results
    dstream.saveAsTextFiles(f"{output_dir}/speed", "txt")

    # Start streaming
    ssc.start()
    ssc.awaitTermination()
