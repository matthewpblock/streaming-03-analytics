"""src/streaming/data_validation/data_contract_nba.py.

Defines what a valid message looks like for this project:
required fields, allowed values, reference table fields,
and output field order.

Use the data/*.csv files as the source of truth for the data contract.

The reusable validation helpers live in core/validation_utils.py.
The domain-specific field rules and validate_sale_record live here.

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it data_contract_yourname.py, and modify your copy
  to adapt the rules for a different domain.
  Then, import from your data_contract instead.
"""

# === DECLARE IMPORTS ===

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from streaming.data_validation.data_validation_nba import validate_distance
from datafun_streaming.data_validation.validation_utils import (
    validate_boolean_text,
    validate_datetime,
    validate_positive_integer,
    validate_required_fields,
)

# === DECLARE REQUIRED FIELDS ===

# This is how Python knows exactly what fields are required
# for a valid message in this project.

# Ensuring messages meet the data contract is a critical part
# of building a reliable streaming pipelines.

# If messages don't meet the contract,
# downstream processes may fail or produce incorrect results.

# === EVENT TABLE FIELDS ===

SHOTS_REQUIRED_FIELDS: Final[list[str]] = [
    "play_id",
    "game_id",
    "timestamp",
    "player_id",
    "shot_type",
    "distance_ft",
    "is_made",
]

SHOTS_OPTIONAL_FIELDS: Final[list[str]] = [
    "quarter",
    "time_remaining",
]

VALID_SHOTS_FIELDNAMES: Final[list[str]] = [
    *SHOTS_REQUIRED_FIELDS,
    *SHOTS_OPTIONAL_FIELDS,
]


# === REFERENCE TABLE FIELDS ===

PLAYERS_REQUIRED_FIELDS: Final[list[str]] = [
    "player_id",
    "player_name",
    "team_name",
    "position",
]

# === ALLOWED VALUES ===

ALLOWED_SHOT_TYPES: Final[set[str]] = {"2PT", "3PT", "FT"}

# === OUTPUT FIELD ORDER ===

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *SHOTS_REQUIRED_FIELDS,
    "player_name",
    "team_name",
    "points_scored",
    "shot_quality_category",
    "clutch_shot_flag",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_SHOTS_FIELDNAMES: Final[list[str]] = [
    *SHOTS_REQUIRED_FIELDS,
    "validation_errors",
]


# === DOMAIN-SPECIFIC VALIDATION ===


def validate_shot_record(
    *,
    record: DataRecordDict,
    valid_player_ids: set[str],
) -> ValidationResult:
    """Validate one shot record against this project's data contract.

    This function can be enhanced.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        record: The message to validate.
        valid_player_ids: The set of valid player_id values from the players reference table.

    Returns:
        A ValidationResult indicating whether the record is valid and any errors found.
    """
    # Initialize an empty list to collect validation errors.
    errors: list[str] = []

    # Validate the required fields, get a list back,
    # and extend the errors list with any errors found.
    # This is a concise form of:
    # required_field_errors = validate_required_fields(record=record, required_fields=SHOTS_REQUIRED_FIELDS)
    # errors.extend(required_field_errors)
    # Use whichever you prefer.
    errors.extend(
        validate_required_fields(record=record, required_fields=SHOTS_REQUIRED_FIELDS)
    )

    if errors:
        # if there are errors,
        # return a ValidationResult with is_valid=False and the list of errors
        # That will exit this function early
        # and skip the rest of the validation checks below.
        return ValidationResult(is_valid=False, errors=errors)

    # If we get here, we know there were no errors
    # based on the validation checks we implemented so far.

    # Now, check the values of specific fields against allowed values or reference tables.
    # If any of these checks fail, add an error message to the errors list.

    # Do an experiment to figure out what the !r does in the f-strings below,
    # and then add a comment to explain it.

    # If the id value in the record is not in the set of valid ids,
    # add an error message to the errors list that includes the invalid value.
    if record["player_id"] not in valid_player_ids:
        errors.append(f"Unknown player_id: {record['player_id']!r}")

    if record["shot_type"] not in ALLOWED_SHOT_TYPES:
        errors.append(f"Invalid shot_type: {record['shot_type']!r}")

    errors.extend(validate_datetime(record["timestamp"]))
    errors.extend(validate_boolean_text(record["is_made"], field_name="is_made"))
    errors.extend(validate_distance(str(record.get("distance_ft", ""))))

    # After all checks, if the errors list is empty, the record is valid.
    # If there are any errors, the record is invalid.

    # Use Python truthiness on a list to see if we're valid.
    # If a list is empty, it is falsy; like it doesn't fully exist.
    # If a list has items, it is truthy; it more practically exists.
    has_errors = bool(errors)  # if there are any errors, this will be True

    # Use the Python not operator.
    is_result_valid = not has_errors  # if there are no errors, the record is valid

    return ValidationResult(is_valid=is_result_valid, errors=errors)


# === OUTPUT HELPERS ===


def keep_shots_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required shot fields in standard order.

    This is used to create the output message for both valid and rejected records.

    Arguments:
        row: The original message as a dict.

    Returns:
        A new dict with only the required fields in the standard order.
    """
    return {field: row.get(field, "") for field in SHOTS_REQUIRED_FIELDS}
