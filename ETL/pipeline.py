import argparse
import logging
import sys
import time
from datetime import datetime

# Import trực tiếp từ 2 module
import transform
import load

# Set up Logging
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)-8s %(message)s",
    datefmt = "%H:%M:%S",
    handlers = [logging.StreamHandler(sys.stdout),
                logging.FileHandler(f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log",
                encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Bước 1: Transform
def run_transform():
    log.info("=" * 55)
    log.info("Step 1: Transform")
    log.info("=" * 55)
    t0 = time.time()
    transform.main()
    log.info(f"Transform hoàn thành trong {time.time() - t0:.1f}s\n")

# Bước 2: Load
def run_load():
    log.info("=" * 55)
    log.info("Step 2: Load")
    log.info("=" * 55)
    t0 = time.time()
    load.main()
    log.info(f"Load hoàn thành trong {time.time() - t0:.1f}s\n")

def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline Runner")
    parser.add_argument("--transform", action="store_true", help="Chỉ chạy bước Transform")
    parser.add_argument("--load", action="store_true", help="Chỉ chạy bước Load")
    args = parser.parse_args()

    # Chạy cả 2
    run_all = not args.transform and not args.load

    start = time.time()
    log.info("ETL bắt đầu")

    try:
        if run_all or args.transform:
            run_transform()

        if run_all or args.load:
            run_load()

    except Exception as e:
        log.error(f"Pipeline thất bại: {e}", exc_info=True)
        sys.exit(1)

    elapsed = time.time() - start
    log.info(f"Pipeline hoàn thành trong {elapsed:6.1f}s")


if __name__ == "__main__":
    main()