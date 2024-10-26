import os
import signal
import time
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, ReadPreference
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError, OperationFailure, NetworkTimeout
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

# Retry configuration
MAX_RETRY_ATTEMPTS = 5
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 60

# Sampling configuration
SAMPLING_RATE = 0.05
LOG_INTERVAL_OPERATIONS = 1000

# Fixed runtime in seconds (5 minutes)
RUNTIME_SECONDS = 300

# Exit file path
EXIT_FILE = "/tmp/run_completed"


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
        self.start_time = time.time()  # Wall clock start time
        self.first_change_time = None  # Time of first change
        self.last_change_time = None  # Time of last change
        self.operation_counts = {"unknown": {"insert": 0, "update": 0, "replace": 0}}

    def process_change(self, change):
        self.total_documents_processed += 1

        # Track change timestamps
        current_time = change.get("clusterTime", datetime.now(timezone.utc))
        if isinstance(current_time, Timestamp):
            current_time = datetime.fromtimestamp(current_time.time, timezone.utc)

        if self.first_change_time is None:
            self.first_change_time = current_time
        self.last_change_time = current_time

        # Sample only a portion of changes
        if self.total_documents_processed % int(1 / SAMPLING_RATE) != 1:
            return

        now = datetime.now(timezone.utc)
        if change["operationType"] in ["insert", "replace", "update"]:
            region = self._get_region(change)
        else:
            region = "unknown"

        if region not in self.operation_counts:
            self.operation_counts[region] = {"insert": 0, "update": 0, "replace": 0}
        self.operation_counts[region][change["operationType"]] += 1

        latency = self._calculate_latency(now, current_time)
        self.sampler.add_sample(now.timestamp(), latency)

        if self.total_documents_processed % LOG_INTERVAL_OPERATIONS == 1:
            logger.info(f"Processed {self.total_documents_processed} documents")

    def _get_region(self, change):
        doc_id = change.get("documentKey", {}).get("_id", "")
        if isinstance(doc_id, str) and "-" in doc_id:
            return doc_id.split("-")[0]
        return "unknown"

    def _calculate_latency(self, change_time, operation_time):
        return round((change_time - operation_time).total_seconds() * 1000, 2)

    def get_change_duration(self):
        """Calculate the duration between first and last change"""
        if self.first_change_time is None or self.last_change_time is None:
            return 0

        duration = (self.last_change_time - self.first_change_time).total_seconds()
        return max(0, round(duration, 2))

    def write_completion_marker(self):
        completion_time = datetime.now()
        with open(EXIT_FILE, "w") as f:
            f.write(f"Change stream completed at {completion_time}")
        logger.info(f"Wrote completion marker to {EXIT_FILE}")

    def print_final_summary(self):
        wall_time = int(time.time() - self.start_time)
        change_duration = self.get_change_duration()
        stats = self.sampler.get_stats()

        logger.info("Change stream completed.")
        logger.info(f"Wall clock runtime: {wall_time} seconds")
        logger.info(f"Change stream duration: {change_duration} seconds")
        logger.info(f"Total documents processed: {self.total_documents_processed}")

        if self.total_documents_processed > 0:
            changes_per_second = self.total_documents_processed / max(
                1, change_duration
            )
            logger.info(
                f"Average throughput: {changes_per_second:.2f} documents/second"
            )

        if stats:
            logger.info("Final sampling stats:")
            logger.info(f"  Sample count: {stats['sample_count']}")
            logger.info(f"  Median latency: {stats['median_latency']:.2f}ms")
            logger.info(f"  Min latency: {stats['min_latency']:.2f}ms")
            logger.info(f"  Max latency: {stats['max_latency']:.2f}ms")

        # Save sampled latencies to JSON file
        latencies_file_path = os.path.join("logs", "latencies.json")
        metadata = {
            "total_processed": self.total_documents_processed,
            "sampled_count": len(self.sampler.latencies),
            "wall_clock_runtime_seconds": wall_time,
            "change_stream_duration_seconds": change_duration,
            "first_change_time": (
                self.first_change_time.isoformat() if self.first_change_time else None
            ),
            "last_change_time": (
                self.last_change_time.isoformat() if self.last_change_time else None
            ),
        }

        if self.total_documents_processed > 0:
            metadata.update(
                {
                    "average_throughput": self.total_documents_processed
                    / max(1, change_duration),
                    "median_latency": stats["median_latency"] if stats else None,
                    "min_latency": stats["min_latency"] if stats else None,
                    "max_latency": stats["max_latency"] if stats else None,
                }
            )

        with open(latencies_file_path, "w") as f:
            json.dump(
                {
                    "data": self.sampler.latencies,
                    "metadata": metadata,
                    "operation_counts": self.operation_counts,
                },
                f,
                indent=2,
            )
        logger.info(f"Latencies saved to {latencies_file_path}")

        # Write completion marker file
        self.write_completion_marker()


def wait_for_load_completion():
    if WAIT_FILE_PATH:
        logger.info(f"Waiting for load completion file: {WAIT_FILE_PATH}")
        while not os.path.exists(WAIT_FILE_PATH):
            logger.debug(f"Waiting for file: {WAIT_FILE_PATH}")
            time.sleep(10)
        logger.info("Load completion file found. Proceeding with monitoring.")
    else:
        logger.info("No wait file specified. Proceeding immediately.")


def connect_to_mongodb():
    if not CONNECTION_STRING:
        raise ValueError("MONGODB_URI environment variable is not set")

    client = MongoClient(CONNECTION_STRING, server_api=ServerApi("1"), **CLIENT_OPTIONS)
    db = client.get_database(DB_NAME, read_preference=ReadPreference.NEAREST)
    collection = db[COLLECTION_NAME]

    try:
        client.admin.command("ping")
        logger.info("Successfully connected to MongoDB")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

    return client, collection


def watch_changes(collection):
    monitor = ChangeStreamMonitor()
    retry_attempts = 0
    resume_token = None
    end_time = time.time() + RUNTIME_SECONDS

    def signal_handler(signum, frame):
        logger.info("Interrupt received, stopping change stream...")
        monitor.print_final_summary()
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    try:
        while time.time() < end_time:
            try:
                with collection.watch(
                    [], **CHANGE_STREAM_OPTIONS, resume_after=resume_token
                ) as stream:
                    for change in stream:
                        if time.time() >= end_time:
                            break
                        monitor.process_change(change)
                        resume_token = stream.resume_token
                        retry_attempts = 0

            except (PyMongoError, OperationFailure, NetworkTimeout) as e:
                retry_attempts += 1
                if retry_attempts > MAX_RETRY_ATTEMPTS:
                    logger.error(f"Max retry attempts reached. Exiting. Error: {e}")
                    break

                retry_delay = min(
                    INITIAL_RETRY_DELAY * (2 ** (retry_attempts - 1)), MAX_RETRY_DELAY
                )
                logger.warning(
                    f"Error occurred: {e}. Retrying in {retry_delay} seconds. Attempt {retry_attempts}/{MAX_RETRY_ATTEMPTS}"
                )
                time.sleep(retry_delay)

    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
    finally:
        monitor.print_final_summary()


def main():
    client, collection = connect_to_mongodb()

    try:
        logger.info(
            f"Starting change stream for all regions... (DB: {DB_NAME}, Collection: {COLLECTION_NAME})"
        )
        logger.info(f"Will run for {RUNTIME_SECONDS} seconds")
        watch_changes(collection)
    finally:
        client.close()


if __name__ == "__main__":
    wait_for_load_completion()
    main()
