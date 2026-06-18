"""S3 writer module for Lambda ingestion function.

Handles writing Pokemon JSON data to S3 with date-based partitioning
in the Bronze layer of the Medallion Architecture.
"""

import json
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def generate_s3_key(date: str, name: str) -> str:
    """Generate the S3 key path for a Pokemon JSON file.

    Creates a Hive-style date-partitioned path in the bronze layer following:
    bronze/year=YYYY/month=MM/day=DD/{name}.json

    Args:
        date: Execution date string in YYYY-MM-DD format.
        name: Pokemon name (used as the filename).

    Returns:
        The S3 key path string.
    """
    year, month, day = date.split("-")
    return f"bronze/year={year}/month={month}/day={day}/{name}.json"


def write_pokemon_data(bucket: str, execution_date: str, pokemon_name: str, data: dict) -> bool:
    """Write Pokemon data as JSON to S3 with one retry on failure.

    Args:
        bucket: S3 bucket name.
        execution_date: Execution date in YYYY-MM-DD format.
        pokemon_name: Name of the Pokemon (used in the S3 key).
        data: Dictionary containing the Pokemon data to write.

    Returns:
        True if the write succeeded, False otherwise.
    """
    s3_client = boto3.client("s3")
    key = generate_s3_key(execution_date, pokemon_name)
    body = json.dumps(data, ensure_ascii=False)

    for attempt in range(2):
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            logger.info("Successfully wrote %s to s3://%s/%s", pokemon_name, bucket, key)
            return True
        except ClientError as e:
            logger.error(
                "S3 write failed for %s (attempt %d/2): %s",
                pokemon_name,
                attempt + 1,
                e,
            )

    return False
