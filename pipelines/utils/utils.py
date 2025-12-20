from datetime import datetime
import logging
from pathlib import Path
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def persist_bad_records(df, dataset, expectation_name, base_path="../tmp/dq_bad_records"):
    """Persists the bad records from a DataFrame to disk for further analysis.

    Args:
        df (DataFrame): The DataFrame containing bad records.
        dataset (str): The name of the dataset.
        expectation_name (str): The name of the expectation that failed.
        base_path (str): The base path where bad records will be stored.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{base_path}/{dataset}/{expectation_name}"
    if not Path(path).exists():
        Path(path).mkdir(parents=True, exist_ok=True)
    logger.info(f"Persisting bad records to {path} at {timestamp}")
    df.write.mode("overwrite").parquet(path)
