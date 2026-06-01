"""src/streaming/kafka_consumer_nba.py.

Kafka consumer: analytics

Reads event messages from a Kafka topic and runs the full pipeline:
  - Validates each message against the data contract
  - Computes derived fields (points_scored, shot_quality, clutch_shot)

Start with main() at the bottom.
Work up to see how it all fits together.

Many functions are standard helpers
and should not need project-specific modifications.

Author: Denise Case, Matthew Block
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_nba

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it consumer_yourname.py, and modify your copy.
"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row, read_csv_as_lookup
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_validation.data_contract_nba import (
    CONSUMED_FIELDNAMES,
    EVENTS_REQUIRED_FIELDS,
    validate_required_fields,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("C03", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

COURSE_NAME: Final[str] = "Streaming Data"
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_events.csv"

PLAYERS_CSV: Final[Path] = DATA_DIR / "players.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C03")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "PLAYERS_CSV", PLAYERS_CSV)


# === BASKETBALL DERIVED FIELD LOGIC ===

def compute_points_scored(shot_type: str, is_made: bool) -> int:
    if not is_made:
        return 0
    if shot_type == "3PT": return 3
    if shot_type == "2PT": return 2
    if shot_type == "FT": return 1
    return 0

def compute_shot_quality(distance_ft: float) -> str:
    if distance_ft > 23.75: return "NBA Deep 3"
    if distance_ft >= 15.0: return "Mid-Range"
    if distance_ft >= 4.0: return "Paint"
    return "Restricted Area"

def is_clutch_shot(quarter: str) -> bool:
    return quarter.upper() in ("4", "Q4", "OT", "4TH")

def enrich_event_message(row: dict[str, Any]) -> dict[str, Any]:
    shot_type = str(row.get("shot_type", ""))
    is_made = str(row.get("is_made", "False")).lower() in ("true", "1", "yes", "t")
    distance_ft = float(row.get("distance_ft", 0.0))
    quarter = str(row.get("quarter", ""))
    return {
        **row,
        "points_scored": compute_points_scored(shot_type, is_made),
        "shot_quality_category": compute_shot_quality(distance_ft),
        "clutch_shot_flag": is_clutch_shot(quarter),
    }


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")

    # Create an admin client to check if the topic exists and has messages.
    admin = create_admin_client(settings)

    topic_exists_already = topic_exists(admin, settings.topic)

    if not topic_exists_already:
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    LOG.info(f"Topic {settings.topic!r} exists.")

    # Call a function to get count of messages
    # on the topic before we start consuming
    message_count = get_topic_message_count(admin, settings.topic, settings)

    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        # Exit with a non-zero code to indicate an error.
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so this example reads all available messages.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)

    # call consumer.subscribe() with an on_assign callback
    # to reset offsets to the beginning
    # This ensures the example reads all available messages every time it runs.
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> RunningStats:
    """Initialize output directory, CSV, and stats.

    Returns:
        A RunningStats instance.
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()
    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    return RunningStats()


def load_reference_data() -> tuple[dict[str, str], dict[str, str]]:
    """Load reference data used for message enrichment.

    Returns:
        A tuple containing:
        - player_name_lookup dict.
        - team_name_lookup dict.
    """
    LOG.info("Loading enrichment reference data...")
    player_name_lookup = {
        pid: str(pname) for pid, pname in read_csv_as_lookup(PLAYERS_CSV, key_field="player_id", value_field="player_name").items()
    }
    team_name_lookup = {
        pid: str(tname) for pid, tname in read_csv_as_lookup(PLAYERS_CSV, key_field="player_id", value_field="team_name").items()
    }
    LOG.info(f"Loaded {len(player_name_lookup)} players.")
    return player_name_lookup, team_name_lookup


def process_message(
    row: dict[str, Any],
    *,
    player_name_lookup: dict[str, str],
    team_name_lookup: dict[str, str],
    stats: RunningStats,
) -> dict[str, Any] | None:
    """Process one consumed message.

    Arguments after the asterisk must be passed as keyword arguments.

    Steps:
      - Validate required fields
      - Enrich with derived fields
      - Enrich with additional product and currency metadata
      - Update running statistics

    Arguments:
        row: A raw consumed Kafka message row.
        player_name_lookup: Lookup dict.
        team_name_lookup: Lookup dict.
        stats: Running statistics accumulator.

    Returns:
        The enriched row, or None if validation failed.
    """
    # First, validate the message against the data contract.
    # If validation fails, return None to indicate the message should be rejected.
    errors = validate_required_fields(record=row, required_fields=EVENTS_REQUIRED_FIELDS)
    if errors:
        LOG.warning(f"Validation failed for play {row.get('play_id', '?')}")
        LOG.warning(f"errors={errors}")
        return None

    # Filter for shot events only
    action_type = str(row.get("action_type", ""))
    
    if action_type not in {"Made Shot", "Missed Shot", "Free Throw"}:
        LOG.info(f"Skipping non-shot event: play_id={row.get('play_id')}, action_type={action_type}")
        # Return None to have this non-shot event be counted as "skipped"
        return None

    # Then, enrich the message with derived fields.
    enriched = enrich_event_message(row)
    pid = enriched.get("player_id", "")
    enriched["player_name"] = player_name_lookup.get(pid, "Unknown Player")
    enriched["team_name"] = team_name_lookup.get(pid, "Unknown Team")

    LOG.info(f"shot_quality={enriched['shot_quality_category']}")
    LOG.info(f"clutch_shot={enriched['clutch_shot_flag']}")
    LOG.info(f"points_scored={enriched['points_scored']}")
    LOG.info(f"running_total_pts={stats.total + enriched['points_scored']:.0f}")

    # Update running statistics with the new total.
    stats.update(enriched["points_scored"])
    return enriched


def consume_messages(
    consumer: Any,
    *,
    player_name_lookup: dict[str, str],
    team_name_lookup: dict[str, str],
    stats: RunningStats,
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        consumer: An open Kafka consumer subscribed to the topic.
        player_name_lookup: Lookup dict.
        team_name_lookup: Lookup dict.
        stats: Running statistics accumulator.

    Returns:
        A tuple of (consumed_count, skipped_count).
    """
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        enriched = process_message(
            row,
            player_name_lookup=player_name_lookup,
            team_name_lookup=team_name_lookup,
            stats=stats,
        )

        if enriched is None:
            skipped_count += 1
            LOG.warning("MESSAGE REJECTED")
            LOG.warning(f"play={row.get('play_id', '?')}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        append_csv_row(
            path=OUTPUT_CSV,
            row={field: enriched.get(field, "") for field in CONSUMED_FIELDNAMES},
            fieldnames=CONSUMED_FIELDNAMES,
        )

        consumed_count += 1
        LOG.info("MESSAGE ACCEPTED")
        LOG.info(f"play={enriched['play_id']}")
        LOG.info(f"points={enriched['points_scored']}")
        LOG.info(f"consumed={consumed_count}")
        LOG.info("RUNNING STATS")
        LOG.info(f"total_pts={stats.total:,.0f}")
        LOG.info(f"average_pts={stats.mean:,.2f}")
        LOG.info(f"min_pts={stats.minimum:,.2f}")
        LOG.info(f"max_pts={stats.maximum:,.2f}")

    return consumed_count, skipped_count


def save_artifacts() -> None:
    """Save output artifacts."""
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)

    if stats.count > 0:
        LOG.info(f"  Total points:  {stats.total:,.0f}")
        LOG.info(f"  Average pts: {stats.mean:,.2f}")
        LOG.info(f"  Minimum pts: {stats.minimum:,.2f}")
        LOG.info(f"  Maximum pts: {stats.maximum:,.2f}")

    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    stats = initialize_output()
    player_name_lookup, team_name_lookup = load_reference_data()

    consumed_count = 0
    skipped_count = 0

    try:
        consumed_count, skipped_count = consume_messages(
            consumer,
            player_name_lookup=player_name_lookup,
            team_name_lookup=team_name_lookup,
            stats=stats,
        )
    finally:
        consumer.close()
        LOG.info("Kafka consumer closed.")

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, skipped_count, stats, settings)


# === CONDITIONAL EXECUTION GUARD ===

# WHY: If running this file as a script, then call main().
# This is standard Python "boilerplate".

if __name__ == "__main__":
    main()
