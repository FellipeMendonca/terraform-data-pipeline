"""Lambda handler for Pokemon data ingestion.

Entry point for the AWS Lambda function that orchestrates fetching
Pokemon data from PokeAPI and writing it to S3 Bronze layer.
Supports timeout monitoring and partial failure handling.
"""

import logging
import os

try:
    # Lambda runtime: files are at the root of the zip
    from pokeapi_client import PokeAPIClient
    from s3_writer import write_pokemon_data
except ImportError:
    # Local/test environment: use full module path via importlib
    import importlib
    _pokeapi_client = importlib.import_module("src.lambda.pokeapi_client")
    _s3_writer = importlib.import_module("src.lambda.s3_writer")
    PokeAPIClient = _pokeapi_client.PokeAPIClient
    write_pokemon_data = _s3_writer.write_pokemon_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_total_timeout_ms(context) -> int:
    """Get the total Lambda timeout in milliseconds from context.

    On first call, the remaining time approximates the full configured timeout.

    Args:
        context: AWS Lambda context object.

    Returns:
        Total timeout in milliseconds.
    """
    return context.get_remaining_time_in_millis()


def _is_timeout_threshold_reached(context, total_timeout_ms: int) -> bool:
    """Check if we've used 80% of the Lambda execution time.

    Args:
        context: AWS Lambda context object.
        total_timeout_ms: The total timeout configured for the Lambda.

    Returns:
        True if remaining time is less than 20% of total timeout.
    """
    remaining_ms = context.get_remaining_time_in_millis()
    threshold = total_timeout_ms * 0.20
    return remaining_ms < threshold


def _fetch_pokemon_list(client: PokeAPIClient) -> list[dict] | None:
    """Fetch the complete list of Pokemon from PokeAPI.

    Args:
        client: Configured PokeAPIClient instance.

    Returns:
        List of Pokemon dicts with 'name' and 'url', or None on failure.
    """
    try:
        pokemon_list = client.get_pokemon_list()
        logger.info("Retrieved %d Pokemon from list endpoint", len(pokemon_list))
        return pokemon_list
    except Exception as e:
        logger.error("Critical failure fetching Pokemon list: %s", str(e))
        return None


def _process_pokemon(
    client: PokeAPIClient,
    pokemon_list: list[dict],
    bucket: str,
    execution_date: str,
    context,
    total_timeout_ms: int,
) -> tuple[int, list[str], bool]:
    """Fetch details for each Pokemon and write to S3.

    Processes Pokemon one at a time, monitoring timeout threshold and
    handling individual failures gracefully.

    Args:
        client: Configured PokeAPIClient instance.
        pokemon_list: List of Pokemon dicts with 'name' key.
        bucket: S3 bucket name for writing.
        execution_date: Date string in YYYY-MM-DD format.
        context: AWS Lambda context for timeout monitoring.
        total_timeout_ms: Total configured timeout in milliseconds.

    Returns:
        Tuple of (successful_count, failed_pokemon_names, timeout_reached).
    """
    success_count = 0
    failed_pokemon: list[str] = []
    timeout_reached = False

    for pokemon_entry in pokemon_list:
        name = pokemon_entry.get("name", "")

        # Check timeout threshold before each fetch
        if _is_timeout_threshold_reached(context, total_timeout_ms):
            timeout_reached = True
            # Log unfetched Pokemon
            unfetched_count = len(pokemon_list) - success_count - len(failed_pokemon)
            logger.warning(
                "Timeout threshold (80%%) reached. "
                "Saved %d Pokemon. %d unfetched.",
                success_count,
                unfetched_count,
            )
            break

        # Fetch individual Pokemon details (by ID or name)
        details = client.get_pokemon_details(name)
        if details is None:
            logger.warning("Failed to fetch details for Pokemon: %s", name)
            failed_pokemon.append(name)
            continue

        # Use the real Pokemon name from the API response for the S3 key
        pokemon_name = details.get("name", name)

        # Write to S3
        write_success = write_pokemon_data(bucket, execution_date, pokemon_name, details)
        if not write_success:
            logger.warning("Failed to write Pokemon to S3: %s", pokemon_name)
            failed_pokemon.append(pokemon_name)
            continue

        success_count += 1

    return success_count, failed_pokemon, timeout_reached


def _determine_status(
    success_count: int,
    failed_pokemon: list[str],
    timeout_reached: bool,
    total_pokemon: int,
) -> str:
    """Determine the execution status based on results.

    Args:
        success_count: Number of successfully processed Pokemon.
        failed_pokemon: List of Pokemon names that failed.
        timeout_reached: Whether the timeout threshold was reached.
        total_pokemon: Total number of Pokemon in the list.

    Returns:
        Status string: "success", "partial_failure", or "failure".
    """
    if timeout_reached:
        return "partial_failure"
    if success_count == 0 and total_pokemon > 0:
        return "failure"
    if failed_pokemon:
        return "partial_failure"
    return "success"


def _register_bronze_partition(bucket: str, year: str, month: str, day: str) -> None:
    """Register a partition in the Glue Catalog bronze_pokemon table.

    This allows Athena to query the data without manual MSCK REPAIR.

    Args:
        bucket: S3 bucket name.
        year: Partition year value.
        month: Partition month value.
        day: Partition day value.
    """
    import boto3

    glue_client = boto3.client("glue")
    database_name = os.environ.get("GLUE_DATABASE_NAME", "")

    if not database_name:
        logger.warning("GLUE_DATABASE_NAME not set, skipping partition registration")
        return

    partition_location = f"s3://{bucket}/bronze/year={year}/month={month}/day={day}/"

    try:
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName="bronze_pokemon",
            PartitionInput={
                "Values": [year, month, day],
                "StorageDescriptor": {
                    "Location": partition_location,
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    },
                },
            },
        )
        logger.info("Registered partition year=%s/month=%s/day=%s", year, month, day)
    except glue_client.exceptions.AlreadyExistsException:
        logger.info("Partition year=%s/month=%s/day=%s already exists", year, month, day)
    except Exception as e:
        logger.warning("Failed to register partition: %s", str(e))


def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point for Pokemon data ingestion.

    Orchestrates the full ingestion flow: fetches Pokemon by Pokedex range
    from PokeAPI, retrieves details for each Pokemon, and writes them to S3 Bronze layer.

    Args:
        event: Input from Step Functions containing:
            - pokedex_start (int): First Pokedex number (e.g., 1)
            - pokedex_end (int): Last Pokedex number (e.g., 151)
            - execution_date (str, optional): Date in YYYY-MM-DD format. Defaults to today.
        context: AWS Lambda context object providing timeout information.

    Returns:
        Dict with status, pokemon_count, failed_pokemon list, and s3_prefix.
    """
    execution_date = event.get("execution_date", "")
    bucket = os.environ.get("S3_BUCKET_NAME", "")
    pokedex_start = event.get("pokedex_start", 1)
    pokedex_end = event.get("pokedex_end", 151)

    # Fallback to current date if execution_date not provided
    if not execution_date:
        from datetime import date
        execution_date = date.today().isoformat()

    logger.info(
        "Starting Pokemon ingestion. execution_date=%s, bucket=%s, range=%d-%d",
        execution_date,
        bucket,
        pokedex_start,
        pokedex_end,
    )

    # Validate inputs
    if not bucket:
        logger.error("Missing S3_BUCKET_NAME environment variable")
        return {
            "status": "failure",
            "pokemon_count": 0,
            "failed_pokemon": [],
            "s3_prefix": "",
        }

    # Capture total timeout at the start of execution
    total_timeout_ms = _get_total_timeout_ms(context)

    # Build S3 prefix from execution date
    year, month, day = execution_date.split("-")
    s3_prefix = f"bronze/year={year}/month={month}/day={day}/"

    # Initialize API client
    client = PokeAPIClient()

    # Build Pokemon list from Pokedex range
    pokemon_list = client.get_pokemon_by_range(pokedex_start, pokedex_end)

    # Process each Pokemon
    success_count, failed_pokemon, timeout_reached = _process_pokemon(
        client=client,
        pokemon_list=pokemon_list,
        bucket=bucket,
        execution_date=execution_date,
        context=context,
        total_timeout_ms=total_timeout_ms,
    )

    # Determine final status
    status = _determine_status(
        success_count=success_count,
        failed_pokemon=failed_pokemon,
        timeout_reached=timeout_reached,
        total_pokemon=len(pokemon_list),
    )

    logger.info(
        "Ingestion complete. status=%s, pokemon_count=%d, failed=%d",
        status,
        success_count,
        len(failed_pokemon),
    )

    # Register partition in Glue Catalog if any data was written
    if success_count > 0:
        _register_bronze_partition(bucket, year, month, day)

    return {
        "status": status,
        "pokemon_count": success_count,
        "failed_pokemon": failed_pokemon,
        "s3_prefix": s3_prefix,
    }
