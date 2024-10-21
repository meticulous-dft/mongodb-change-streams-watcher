import os
import signal
import time
from datetime import datetime, timezone
from collections import defaultdict
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError, OperationFailure, NetworkTimeout
from bson.timestamp import Timestamp
import threading
import json

# Load .env file if it exists
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/change_streams_watcher.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()

# Disable other loggers
logging.getLogger("pymongo").setLevel(logging.WARNING)

CONNECTION_STRING = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DATABASE")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION")
WAIT_FILE_PATH = os.getenv("WAIT_FILE_PATH")

# Retry configuration
MAX_RETRY_ATTEMPTS = 5
INITIAL_RETRY_DELAY = 1  # seconds
MAX_RETRY_DELAY = 60  # seconds

# Exit condition configuration
EXIT_FILE = "/tmp/run_completed"
EMPTY_PERIOD_THRESHOLD = int(os.getenv("EMPTY_PERIOD_THRESHOLD", 5))
EMPTY_PERIOD_DURATION = int(os.getenv("EMPTY_PERIOD_DURATION", 30))
MIN_DOCUMENTS_PROCESSED = int(
    os.getenv("MIN_DOCUMENTS_PROCESSED", 10000)
)  # Minimum number of documents to process before exiting

# Logging configuration
LOG_INTERVAL_OPERATIONS = int(os.getenv("LOG_INTERVAL_OPERATIONS", 100))
LOG_INTERVAL_SECONDS = int(os.getenv("LOG_INTERVAL_SECONDS", 10))


def wait_for_load_completion():
    if WAIT_FILE_PATH:
        logger.info(f"Waiting for load completion file: {WAIT_FILE_PATH}")
        while not os.path.exists(WAIT_FILE_PATH):
            logger.debug(f"Waiting for file: {WAIT_FILE_PATH}")
            time.sleep(10)  # Check every 10 seconds
        logger.info("Load completion file found. Proceeding with monitoring.")
    else:
        logger.info("No wait file specified. Proceeding immediately.")


def connect_to_mongodb():
    if not CONNECTION_STRING:
        raise ValueError("MONGODB_URI environment variable is not set")
    client = MongoClient(CONNECTION_STRING, server_api=ServerApi("1"))
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return client, collection


def calculate_latency(change_time, operation_time):
    if isinstance(operation_time, Timestamp):
        operation_time = datetime.fromtimestamp(operation_time.time, timezone.utc)
    return round(
        (change_time - operation_time).total_seconds() * 1000, 2
    )  # Convert to milliseconds and round to 2 decimal places


def get_region_from_document(document):
    return document.get("location", "unknown") if document else "unknown"


class ChangeStreamMonitor:
    def __init__(self, collection):
        self.collection = collection
        self.operation_counts = defaultdict(lambda: defaultdict(int))
        self.latencies = defaultdict(lambda: defaultdict(list))
        self.start_time = time.time()
        self.total_documents_processed = 0
        self.last_change_time = time.time()
        self.last_log_time = time.time()
        self.empty_periods = 0
        self.exit_flag = threading.Event()
        self.lock = threading.Lock()
        self.all_latencies = []  # Store all latencies with timestamps

    def process_change(self, change):
        with self.lock:
            operation_time = change.get("clusterTime", datetime.now(timezone.utc))
            change_time = datetime.now(timezone.utc)
            latency = calculate_latency(change_time, operation_time)

            if change["operationType"] in ["insert", "replace", "update"]:
                region = get_region_from_document(change.get("fullDocument"))
            else:
                region = "unknown"

            self.operation_counts[region][change["operationType"]] += 1
            self.latencies[region][change["operationType"]].append(latency)
            self.all_latencies.append(
                (round(time.time(), 2), latency)
            )  # Store rounded timestamp and latency
            self.total_documents_processed += 1
            self.last_change_time = time.time()
            self.empty_periods = 0

            if (
                self.total_documents_processed % LOG_INTERVAL_OPERATIONS == 0
                or time.time() - self.last_log_time >= LOG_INTERVAL_SECONDS
            ):
                self.log_statistics()

    def log_statistics(self):
        elapsed_time = int(time.time() - self.start_time)
        operations_since_last_log = sum(
            sum(counts.values()) for counts in self.operation_counts.values()
        )
        time_since_last_log = time.time() - self.last_log_time
        current_ops_sec = (
            operations_since_last_log / time_since_last_log
            if time_since_last_log > 0
            else 0
        )

        log_lines = [
            f"{elapsed_time} sec: {operations_since_last_log} operations; "
            f"{current_ops_sec:.0f} current ops/sec; "
            f"Total documents processed: {self.total_documents_processed};"
        ]

        for region, ops in self.operation_counts.items():
            for op_type, count in ops.items():
                if count > 0:
                    current_latency = self.latencies[region][op_type][-1]
                    log_lines.append(
                        f" [{region}-{op_type.upper()}: Count={count}, "
                        f"Current Latency={current_latency:.2f}ms] "
                    )

        logger.info("".join(log_lines))

        # Reset counters for the next logging interval
        self.operation_counts.clear()
        self.latencies.clear()
        self.last_log_time = time.time()

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
                        logger.info(f"Exit conditions met. Stopping change stream...")
                        self.print_final_summary()
                        with open(EXIT_FILE, "w") as f:
                            f.write(f"Change stream completed at {datetime.now()}")
                        self.exit_flag.set()
                else:
                    self.empty_periods = 0

    def start_exit_condition_checker(self):
        self.exit_condition_thread = threading.Thread(target=self.check_exit_conditions)
        self.exit_condition_thread.start()

    def stop_exit_condition_checker(self):
        self.exit_flag.set()
        if self.exit_condition_thread:
            self.exit_condition_thread.join()

    def print_final_summary(self):
        elapsed_time = int(time.time() - self.start_time)
        logger.info(f"Change stream completed.")
        logger.info(f"Total runtime: {elapsed_time} seconds")
        logger.info(f"Total documents processed: {self.total_documents_processed}")
        logger.info(
            f"Average throughput: {self.total_documents_processed / elapsed_time:.2f} documents/second"
        )

        # Save all latencies to a JSON file in the logs folder for later analysis
        latencies_file_path = os.path.join("logs", "latencies.json")
        with open(latencies_file_path, "w") as f:
            json.dump(self.all_latencies, f)
        logger.info(f"Latencies saved to {latencies_file_path}")


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
                    [],
                    full_document="updateLookup",
                    resume_after=resume_token,
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
