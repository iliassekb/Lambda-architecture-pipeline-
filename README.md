## Lambda Architecture Pipeline (Batch + Speed + Serving)

This project demonstrates a minimal Lambda Architecture using PySpark:
- **Batch layer**: processes historical data from `data/batch_input` and writes aggregated results to `data/serving/batch`.
- **Speed layer**: consumes streaming CSV files from `data/stream_input` and writes rolling aggregates to `data/serving/speed`.
- **Serving layer**: merges the latest batch and speed outputs into a single view at `data/serving/view/merged_view.csv`.

### Project structure

```
lambda_pipeline/
  data/
    batch_input/
    stream_input/
    serving/
      batch/
      speed/
      view/

batch_job.py          # Batch layer (PySpark RDD job)
speed_job.py          # Speed layer (Spark Streaming DStreams)
merge_view.py         # Serving layer merger
make_batch_data.py    # Generate sample historical data
stream_generator.py   # Generate synthetic streaming events
Dockerfile            # Python 3.10 + OpenJDK 17 + Spark 3.5 base image
docker-compose.yml    # (Optional) Compose services if provided
```

### Prerequisites

- Python 3.10+
- Java (OpenJDK 17) if running Spark locally without Docker
- PySpark (installed automatically in the provided Docker image; can be `pip install pyspark` locally)
- Windows Terminal/PowerShell or any shell

Optional (recommended): Docker and Docker Compose

### Option A: Run with Docker (recommended)

The provided `Dockerfile` installs OpenJDK 17 and Spark 3.5. Build and run an interactive shell:

```bash
docker build -t lambda-pipeline .
docker run --rm -it -v %cd%:/app -w /app lambda-pipeline bash
```

Inside the container, Python and Spark are available on PATH.

### Option B: Run locally (no Docker)

Install dependencies:

```bash
pip install pyspark
```

Ensure Java 17 is installed and `JAVA_HOME` is set if needed.

### Generate sample data

- Historical batch data:

```bash
python make_batch_data.py
```

This creates CSV files under `data/batch_input`.

- Streaming events generator (runs continuously; keep it in its own terminal):

```bash
python stream_generator.py
```

This writes new CSV chunks to `data/stream_input` every ~5 seconds.

### Run the pipeline

Open separate terminals (or tmux panes) for each component.

1) Speed layer (Spark Streaming, micro-batches of 10s):

```bash
python speed_job.py
```

Outputs go to `data/serving/speed/speed-*.txt` directories.

2) Batch layer (on demand; run after generating batch data):

```bash
python batch_job.py
```

Writes to `data/serving/batch/batch-<timestamp>`.

3) Serving layer (merges latest batch + speed into a single CSV every 5s):

```bash
python merge_view.py
```

Produces `data/serving/view/merged_view.csv` with headers:

```
user_id,purchase_count_total,purchase_amount_total
```

### Data format

Both batch and stream generators emit CSV rows with 4 fields:

```
user_id,event_type,amount,unix_timestamp
```

- `event_type` is either `view` or `purchase`.
- Only `purchase` rows are aggregated.

Aggregated outputs are CSV-like lines of:

```
user_id,purchase_count,purchase_amount_total
```

### Common commands (Windows PowerShell)

- Copy command output to clipboard:

```powershell
some-command | clip
```

- If Ctrl+C doesn’t copy in your terminal, try `Ctrl+Shift+C` (Windows Terminal), or enable QuickEdit and press Enter after selection in legacy PowerShell/CMD.

### Notes

- Speed layer uses a 60s window with 10s slide: `reduceByKeyAndWindow(..., 60, 10)`.
- The stream and batch writers use atomic file writes (temporary file + rename) where applicable to avoid partial reads.
- Run the batch job whenever you want to “recompute history”; the serving merger will combine it with the current speed aggregates.

### Troubleshooting

- If Spark can’t find Java, install OpenJDK 17 and ensure `java -version` works.
- On Windows with long paths or spaces, prefer running inside the Docker container to avoid path issues.
- If no streaming output appears, ensure files are appearing under `data/stream_input` and that paths match.


