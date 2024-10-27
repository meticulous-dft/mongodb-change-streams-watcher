import os
import signal
import statistics
import time
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, ReadPreference
from pymongo.server_api import ServerApi
import json

# Load .env file if it exists
load_dotenv()

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/change_streams_sampling.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()
logging.getLogger("pymongo").setLevel(logging.WARNING)

# MongoDB Configuration
CONNECTION_STRING = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DATABASE")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION")

# MongoDB Client Configuration
MAX_WORKERS = min(32, (os.cpu_count() or 1) * 4)
CLIENT_OPTIONS = {
    "maxPoolSize": MAX_WORKERS * 2,
    "minPoolSize": MAX_WORKERS // 2,
    "maxIdleTimeMS": 60000,
    "serverSelectionTimeoutMS": 5000,
    "connectTimeoutMS": 2000,
    "retryWrites": True,
    "retryReads": True,
    "w": "majority",
}

# Change Stream Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
MAX_AWAIT_TIME_MS = int(os.getenv("MAX_AWAIT_TIME_MS", 1000))
CHANGE_STREAM_OPTIONS = {
    "full_document": "default",
    "max_await_time_ms": MAX_AWAIT_TIME_MS,
    "batch_size": BATCH_SIZE,
}

# Test configuration
SAMPLING_RATE = float(os.getenv("SAMPLING_RATE", 0.05))
DOCUMENTS_TO_PROCESS = int(os.getenv("DOCUMENTS_TO_PROCESS", 10001))
WAIT_FILE_PATH = os.getenv("WAIT_FILE_PATH")
EXIT_FILE = "/tmp/change_streams_completed"


class SimpleSampler:
    def __init__(self):
        self.samples = []  # Store latencies
        self.latencies = []  # Store [timestamp, latency] pairs

    def add_sample(self, timestamp, latency):
        self.samples.append(latency)
        self.latencies.append([round(timestamp, 2), round(latency, 2)])

    def get_percentile(self, sorted_samples, percentile):
        """Calculate percentile value from sorted samples"""
        if not sorted_samples:
            return None

        index = int(len(sorted_samples) * percentile / 100)
        return sorted_samples[index]

    def get_stats(self):
        if not self.samples:
            return None

        sorted_samples = sorted(self.samples)
        return {
            "sample_count": len(self.samples),
            "min_latency": sorted_samples[0],
            "p50_latency": self.get_percentile(sorted_samples, 50),
            "p90_latency": self.get_percentile(sorted_samples, 90),
            "p99_latency": self.get_percentile(sorted_samples, 99),
        }


class ChangeStreamMonitor:
    def __init__(self):
        self.sampler = SimpleSampler()
        self.total_documents_processed = 0
        self.start_time = time.time()

    def process_change(self, change):
        now = datetime.now(timezone.utc)
        self.total_documents_processed += 1

        # Sample only a portion of changes
        if self.total_documents_processed % int(1 / SAMPLING_RATE) != 1:
            return

        cluster_time = change.get("clusterTime", datetime.now(timezone.utc))
        operation_time = datetime.fromtimestamp(cluster_time.time, timezone.utc)
        latency = round((now - operation_time).total_seconds() * 1000, 2)
        self.sampler.add_sample(now.timestamp(), latency)

    def print_final_summary(self):
        wall_time = int(time.time() - self.start_time)
        stats = self.sampler.get_stats()

        logger.info("Change stream completed.")
        logger.info(f"Wall clock runtime: {wall_time} seconds")
        logger.info(f"Total documents processed: {self.total_documents_processed}")
        average_throughput = round(self.total_documents_processed / wall_time, 2)

        metadata = {
            "total_processed": self.total_documents_processed,
            "wall_clock_runtime_seconds": wall_time,
            "average_throughput": average_throughput,
        }
        if stats:
            logger.info(f"Latency stats: {stats}")
            metadata = {**metadata, **stats}

        # Save sampled latencies to JSON file
        latencies_file_path = os.path.join("logs", "latencies.json")
        with open(latencies_file_path, "w") as f:
            json.dump(
                {
                    "data": self.sampler.latencies,
                    "metadata": metadata,
                },
                f,
                indent=2,
            )
        logger.info(f"Latencies saved to {latencies_file_path}")

        # Write completion marker file
        completion_time = datetime.now()
        with open(EXIT_FILE, "w") as f:
            f.write(f"Change stream completed at {completion_time}")
        logger.info(f"Wrote completion marker to {EXIT_FILE}")


def watch_changes(collection):
    monitor = ChangeStreamMonitor()

    def signal_handler(signum, frame):
        logger.info("Interrupt received, stopping change stream...")
        monitor.print_final_summary()
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    pipeline = [
        {
            "$project": {
                "documentKey": 1,
                "operationType": 1,
                "clusterTime": 1,
            }
        }
    ]

    try:
        with collection.watch(pipeline, **CHANGE_STREAM_OPTIONS) as stream:
            for change in stream:
                monitor.process_change(change)

                if monitor.total_documents_processed >= DOCUMENTS_TO_PROCESS:
                    logger.info(
                        f"Reached target of {DOCUMENTS_TO_PROCESS } documents. Exiting..."
                    )
                    break
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        raise
    finally:
        monitor.print_final_summary()


def connect_to_mongodb():
    client = MongoClient(CONNECTION_STRING, server_api=ServerApi("1"), **CLIENT_OPTIONS)
    db = client.get_database(DB_NAME, read_preference=ReadPreference.NEAREST)
    collection = db[COLLECTION_NAME]

    return client, collection


def calculate_server_time_offset(client, samples=10) -> float:
    """
    Calculate the time difference between server and client with improved accuracy
    """
    offsets = []
    network_latencies = []

    for _ in range(samples):
        # Get server time before and after to measure request time
        start_time = time.time()
        server_status = client.admin.command("serverStatus")
        end_time = time.time()

        # Use server's system.localTime for better accuracy
        server_time = server_status["localTime"].timestamp()

        # Calculate network latency (round trip time / 2)
        network_latency = (end_time - start_time) / 2
        network_latencies.append(network_latency * 1000)  # Store in ms

        # The server time should be compared to the midpoint of our request
        client_time = start_time + network_latency
        offset = server_time - client_time
        offsets.append(offset)

        time.sleep(0.5)  # Small delay between samples
    logger.info(
        f"Network latencies (ms): median: {statistics.median(network_latencies)}, samples: {network_latencies}"
    )
    logger.info(
        f"Time offsets (s): median: {statistics.median(offsets)}, samples: {offsets}"
    )


def main():
    client, collection = connect_to_mongodb()
    calculate_server_time_offset(client)

    try:
        logger.info(
            f"Starting change stream for all regions... (DB: {DB_NAME}, Collection: {COLLECTION_NAME})"
        )
        watch_changes(collection)
    finally:
        client.close()


if __name__ == "__main__":
    if EXIT_FILE and os.path.exists(EXIT_FILE):
        exit(0)

    if WAIT_FILE_PATH:
        logger.info(f"Waiting for file: {WAIT_FILE_PATH}")
        while not os.path.exists(WAIT_FILE_PATH):
            time.sleep(10)
        logger.info(f"wait file {WAIT_FILE_PATH} found. Proceeding with monitoring.")
    else:
        logger.info("No wait file specified. Proceeding immediately.")

    main()
