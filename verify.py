import os
import signal
import time
from datetime import datetime
from collections import defaultdict
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError, OperationFailure, NetworkTimeout
import threading

# Load .env file if it exists
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/change_streams_verify.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()
logging.getLogger("pymongo").setLevel(logging.WARNING)

CONNECTION_STRING = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DATABASE")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION")

# Retry configuration
MAX_RETRY_ATTEMPTS = 5
INITIAL_RETRY_DELAY = 1  # seconds
MAX_RETRY_DELAY = 60  # seconds

# Test configuration
EMPTY_PERIOD_THRESHOLD = int(os.getenv("EMPTY_PERIOD_THRESHOLD", 5))
EMPTY_PERIOD_DURATION = int(os.getenv("EMPTY_PERIOD_DURATION", 30))
MIN_DOCUMENTS_PROCESSED = int(
    os.getenv("MIN_DOCUMENTS_PROCESSED", 10000)
)  # Minimum number of documents to process before exiting
WAIT_FILE_PATH = os.getenv("WAIT_FILE_PATH")
EXIT_FILE = "/tmp/change_streams_completed"
# Logging configuration
LOG_INTERVAL_OPERATIONS = int(os.getenv("LOG_INTERVAL_OPERATIONS", 1000))
LOG_INTERVAL_SECONDS = int(os.getenv("LOG_INTERVAL_SECONDS", 10))


class ChangeStreamMonitor:
    def __init__(self, collection):
        self.collection = collection
        self.operation_counts = defaultdict(lambda: defaultdict(int))
        self.start_time = time.time()
        self.total_documents_processed = 0
        self.last_change_time = time.time()
        self.last_log_time = time.time()
        self.empty_periods = 0
        self.exit_flag = threading.Event()
        self.lock = threading.Lock()

    def process_change(self, change):
        with self.lock:
            if change["operationType"] in ["insert", "replace", "update"]:
                region = change.get("documentKey").get("location", "unknown")

            self.operation_counts[region][change["operationType"]] += 1
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
                    log_lines.append(f" [{region}-{op_type.upper()}: Count={count}]")

        logger.info("".join(log_lines))

        # Reset counters for the next logging interval
        self.operation_counts.clear()
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
        while not monitor.exit_flag.is_set():
            try:
                with collection.watch(
                    pipeline=pipeline,
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


def connect_to_mongodb():
    client = MongoClient(CONNECTION_STRING, server_api=ServerApi("1"))
    db = client[DB_NAME]
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
