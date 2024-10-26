import os
import signal
import time
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, ReadPreference
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError
from bson.timestamp import Timestamp
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
        logging.FileHandler("logs/change_streams_watcher.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()
logging.getLogger("pymongo").setLevel(logging.WARNING)

# MongoDB Configuration
CONNECTION_STRING = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DATABASE")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION")
WAIT_FILE_PATH = os.getenv("WAIT_FILE_PATH")
FULL_DOCUMENT_LOOKUP = os.getenv("FULL_DOCUMENT_LOOKUP", "false").lower() == "true"
DOCUMENTS_TO_PROCESS = os.getenv("DOCUMENTS_TO_PROCESS", 10000)

# Performance tuning
MAX_WORKERS = min(32, (os.cpu_count() or 1) * 4)
BATCH_SIZE = 1000

# MongoDB Client Configuration
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
CHANGE_STREAM_OPTIONS = {
    "full_document": "updateLookup" if FULL_DOCUMENT_LOOKUP else "default",
    "max_await_time_ms": 1000,
    "batch_size": BATCH_SIZE,
}

# Sampling configuration
SAMPLING_RATE = 0.05
LOG_INTERVAL_OPERATIONS = 1000

# Exit file path
EXIT_FILE = "/tmp/change_streams_completed"


class SimpleSampler:
    def __init__(self):
        self.samples = []
        self.latencies = []  # Store [timestamp, latency] pairs

    def add_sample(self, timestamp, latency):
        self.samples.append(latency)
        self.latencies.append([round(timestamp, 2), round(latency, 2)])

    def get_stats(self):
        if not self.samples:
            return None

        sorted_samples = sorted(self.samples)
        return {
            "sample_count": len(self.samples),
            "median_latency": sorted_samples[len(sorted_samples) // 2],
            "min_latency": sorted_samples[0],
            "max_latency": sorted_samples[-1],
        }


class ChangeStreamMonitor:
    def __init__(self):
        self.sampler = SimpleSampler()
        self.total_documents_processed = 0
        self.start_time = time.time()

    def process_change(self, change):
        self.total_documents_processed += 1
        now = datetime.now(timezone.utc)

        if self.first_change_time is None:
            self.first_change_time = now
        self.last_change_time = now

        # Sample only a portion of changes
        if self.total_documents_processed % int(1 / SAMPLING_RATE) != 1:
            return

        cluster_time = change.get("clusterTime", datetime.now(timezone.utc))
        latency = self._calculate_latency(now, cluster_time)
        self.sampler.add_sample(now.timestamp(), latency)

    def _calculate_latency(self, change_time, operation_time):
        return round((change_time - operation_time).total_seconds() * 1000, 2)

    def print_final_summary(self):
        wall_time = int(time.time() - self.start_time)
        stats = self.sampler.get_stats()

        logger.info("Change stream completed.")
        logger.info(f"Wall clock runtime: {wall_time} seconds")
        logger.info(f"Total documents processed: {self.total_documents_processed}")
        average_throughput = round(self.total_documents_processed / wall_time, 2)
        logger.info(f"Average throughput: {average_throughput} documents/second")

        if stats:
            logger.info("Sampling stats:")
            logger.info(f"  Sample count: {stats['sample_count']}")
            logger.info(f"  Median latency: {stats['median_latency']:.2f}ms")
            logger.info(f"  Min latency: {stats['min_latency']:.2f}ms")
            logger.info(f"  Max latency: {stats['max_latency']:.2f}ms")

        # Save sampled latencies to JSON file
        latencies_file_path = os.path.join("logs", "latencies.json")
        metadata = {
            "total_processed": self.total_documents_processed,
            "wall_clock_runtime_seconds": wall_time,
            "average_throughput": average_throughput,
            "sampled_count": len(self.sampler.latencies),
            "median_latency": stats["median_latency"] if stats else None,
            "min_latency": stats["min_latency"] if stats else None,
            "max_latency": stats["max_latency"] if stats else None,
        }

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

    try:
        with collection.watch([], **CHANGE_STREAM_OPTIONS) as stream:
            for change in stream:
                monitor.process_change(change)

                # Log progress
                if (
                    monitor.total_documents_processed > 0
                    and monitor.total_documents_processed % LOG_INTERVAL_OPERATIONS == 0
                ):
                    logger.info(
                        f"Processed {monitor.total_documents_processed} documents"
                    )

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


def main():
    client, collection = connect_to_mongodb()

    try:
        logger.info(
            f"Starting change stream for all regions... (DB: {DB_NAME}, Collection: {COLLECTION_NAME})"
        )
        watch_changes(collection)
    finally:
        client.close()


if __name__ == "__main__":
    if WAIT_FILE_PATH:
        logger.info(f"Waiting for file: {WAIT_FILE_PATH}")
        while not os.path.exists(WAIT_FILE_PATH):
            time.sleep(10)
        logger.info(f"wait file {WAIT_FILE_PATH} found. Proceeding with monitoring.")
    else:
        logger.info("No wait file specified. Proceeding immediately.")

    main()
