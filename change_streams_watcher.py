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
import threading
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

# Sampling Configuration
SAMPLING_RATE = float(os.getenv("SAMPLING_RATE", "0.1"))  # Default 10% sampling
SAMPLING_WINDOW_SIZE = int(os.getenv("SAMPLING_WINDOW_SIZE", "1000"))
MIN_SAMPLES_PER_INTERVAL = int(os.getenv("MIN_SAMPLES_PER_INTERVAL", "100"))

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

# Exit condition configuration
EXIT_FILE = "/tmp/run_completed"
EMPTY_PERIOD_THRESHOLD = 3
EMPTY_PERIOD_DURATION = 30
MIN_DOCUMENTS_PROCESSED = 10000
LOG_INTERVAL_OPERATIONS = 1000

# Retry configuration
MAX_RETRY_ATTEMPTS = 5
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 60

SAMPLING_RATE = 0.05


class SimpleSampler:
    def __init__(self):
        self.samples = []
        self.latencies = []  # Store [timestamp, latency] pairs

    def add_sample(self, timestamp, latency):
        self.samples.append(latency)
        self.latencies.append([round(timestamp, 2), round(latency, 2)])

    def get_stats(self):
        if len(self.samples) == 0:
            return None

        sorted_samples = sorted(self.samples)
        return {
            "sample_count": len(self.samples),
            "median_latency": sorted_samples[len(sorted_samples) // 2],
            "min_latency": sorted_samples[0],
            "max_latency": sorted_samples[-1],
        }


class ChangeStreamMonitor:
    def __init__(self, collection):
        self.collection = collection
        self.sampler = SimpleSampler()
        self.total_documents_processed = 0
        self.empty_periods = 0
        self.exit_flag = threading.Event()
        self.lock = threading.Lock()

    def process_change(self, change):
        with self.lock:
            self.total_documents_processed += 1
            if self.total_documents_processed % int(1 / SAMPLING_RATE) != 1:
                return

            now = datetime.now(timezone.utc)
            if change["operationType"] in ["insert", "replace", "update"]:
                region = self._get_region(change)
            else:
                region = "unknown"

            self.operation_counts[region][change["operationType"]] += 1

            operation_time = change.get("clusterTime", datetime.now(timezone.utc))
            latency = self._calculate_latency(now, operation_time)
            self.sampler.add_sample(now.timestamp, latency)

            if self.total_documents_processed % LOG_INTERVAL_OPERATIONS == 1:
                logger.info(f"processed {self.total_documents_processed} documents")

    def _get_region(self, change):
        doc_id = change.get("documentKey", {}).get("_id", "")

        # Extract region from _id (format: <region>-...)
        if isinstance(doc_id, str) and "-" in doc_id:
            return doc_id.split("-")[0]

        logger.error(f"Could not extract region from _id: {doc_id}")
        return "unknown"

    def _calculate_latency(self, change_time, operation_time):
        if isinstance(operation_time, Timestamp):
            operation_time = datetime.fromtimestamp(operation_time.time, timezone.utc)
        return round((change_time - operation_time).total_seconds() * 1000, 2)

    def check_exit_conditions(self):
        while not self.exit_flag.is_set():
            time.sleep(EMPTY_PERIOD_DURATION)
            with self.lock:
                if time.time() - self.last_change_time > EMPTY_PERIOD_DURATION:
                    self.empty_periods += 1
                    logger.info(f"Empty period detected. Count: {self.empty_periods}")
                    if (
                        self.empty_periods >= EMPTY_PERIOD_THRESHOLD
                        and self.total_documents_processed >= MIN_DOCUMENTS_PROCESSED
                    ):
                        logger.info("Exit conditions met. Stopping change stream...")
                        self.print_final_summary()
                        with open(EXIT_FILE, "w") as f:
                            f.write(f"Change stream completed at {datetime.now()}")
                        self.exit_flag.set()
                else:
                    self.empty_periods = 0

    def start_exit_condition_checker(self):
        self.exit_condition_thread = threading.Thread(target=self.check_exit_conditions)
        self.exit_condition_thread.daemon = True
        self.exit_condition_thread.start()

    def stop_exit_condition_checker(self):
        self.exit_flag.set()
        if self.exit_condition_thread:
            self.exit_condition_thread.join()

    def print_final_summary(self):
        elapsed_time = int(time.time() - self.start_time)
        stats = self.sampler.get_stats()

        logger.info("Change stream completed.")
        logger.info(f"Total runtime: {elapsed_time} seconds")
        logger.info(f"Total documents processed: {self.total_documents_processed}")
        logger.info(
            f"Average throughput: {self.total_documents_processed / elapsed_time:.2f} documents/second"
        )

        if stats:
            logger.info(f"Final sampling stats:")
            logger.info(f"  Sample count: {stats['sample_count']}")
            logger.info(f"  Median latency: {stats['median_latency']:.2f}ms")
            logger.info(f"  Min latency: {stats['min_latency']:.2f}ms")
            logger.info(f"  Max latency: {stats['max_latency']:.2f}ms")

        # Save sampled latencies to JSON file
        latencies_file_path = os.path.join("logs", "latencies.json")
        with open(latencies_file_path, "w") as f:
            json.dump(
                {
                    "data": self.sampler.latencies,
                    "metadata": {
                        "total_processed": self.total_documents_processed,
                        "sampled_count": len(self.sampler.latencies),
                        "total_runtime_seconds": elapsed_time,
                        "average_throughput": self.total_documents_processed
                        / elapsed_time,
                        "median_latency": stats["median_latency"],
                        "min_latency": stats["min_latency"],
                        "max_latency": stats["max_latency"],
                    },
                },
                f,
                indent=2,
            )
        logger.info(f"Latencies saved to {latencies_file_path}")


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
    monitor = ChangeStreamMonitor(collection)
    retry_attempts = 0
    resume_token = None

    def signal_handler(signum, frame):
        logger.info("Interrupt received, stopping change stream...")
        monitor.stop_exit_condition_checker()
        monitor.print_final_summary()
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    monitor.start_exit_condition_checker()

    try:
        while not monitor.exit_flag.is_set():
            try:
                with collection.watch(
                    [], **CHANGE_STREAM_OPTIONS, resume_after=resume_token
                ) as stream:
                    for change in stream:
                        if monitor.exit_flag.is_set():
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
        monitor.stop_exit_condition_checker()
        monitor.print_final_summary()


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
    wait_for_load_completion()
    main()
