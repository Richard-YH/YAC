import argparse
import csv
from decimal import Decimal, InvalidOperation
import locale
from pathlib import Path
import sys
from typing import Dict, Iterator, List, Sequence, Tuple


SOURCE_COLUMN = "Participant Private ID"
TARGET_COLUMN = "participant"
INTERNAL_PRIVATE_KEY = "__participant_private_id__"
DASS21_FILE_TOKEN = "dass-21"
DASS21_RESPONSE_COLUMN = "Response"
DASS21_SCORE_COLUMN = "DASS-21Score"
DASS21_RESPONSE_TYPE_COLUMN = "Response Type"
DASS21_RESPONSE_TYPE_VALUE = "response"
DASS21_INCLUDED_ITEM_INDEXES = {5, 9, 19, 25, 31, 33, 41}
DASS21_ITEM_OFFSET = Decimal("1")
DASS21_TOTAL_MULTIPLIER = Decimal("2")
GAD_FILE_TOKEN = "gad"
GAD_SCORE_COLUMN = "GAD-7Score"
GAD_QUESTION_KEY_COLUMN = "Question Key"
GAD_START_QUESTION_KEY = "response-2"
GAD_END_QUESTION_KEY = "response-8-quantised"
CUDIT_FILE_TOKEN = "cudit"
CUDIT_SUM_COLUMN = "CUDITSum"
CUDIT_KEY_COLUMN = "Key"
CUDIT_KEY_QUANTISED = "quantised"
IDENTITY_FILE_TOKEN = "identity"
IDENTITY_KEY_VALUE = "value"
DEMOGRAPHIC_FILE_TOKEN = "demographic"
DEMOGRAPHIC_AGE_QUESTION_NORMALIZED = {
    "age",
    "what is your age",
    "what's your age",
    "how old are you",
}
DEMOGRAPHIC_AGE_OUTPUT_COLUMN = "What is your age?"
DEMOGRAPHIC_RACE_OUTPUT_COLUMN = (
    "Which of the following best describes your race or ethnicity"
)
DEMOGRAPHIC_GENDER_OUTPUT_COLUMN = "What is your gender?"
DEMOGRAPHIC_RACE_QUESTION_NORMALIZED = {
    "which of the following best describes your race or ethnicity",
}
DEMOGRAPHIC_GENDER_QUESTION_NORMALIZED = {
    "what is your gender",
}
CANNABIS_BG_FILE_STEM = "cannabisbg"
MOTIVE_FILE_TOKEN = "motive"
I8_FILE_TOKEN = "i-8"
I8_URGENCY_COLUMN = "I-8Urgency"
I8_LACK_OF_PREMEDITATION_COLUMN = "I-8LackOfPremeditation"
I8_LACK_OF_PERSEVERANCE_COLUMN = "I-8LackOfPerseverance"
I8_SENSATION_SEEKING_COLUMN = "I-8SensationSeeking"
I8_URGENCY_INDEXES = {2, 4}
I8_LACK_OF_PREMEDITATION_INDEXES = {6, 8}
I8_LACK_OF_PERSEVERANCE_INDEXES = {10, 12}
I8_SENSATION_SEEKING_INDEXES = {14, 16}
I8_RECODE_BASE = Decimal("5")
CAPE_FILE_TOKEN = "cape"
CAPE_SCORE_COLUMN = "CAPEScore"
CAPE_QUESTION_COLUMN = "Question"
CAPE_BEGIN_MARKER = "BEGIN"
CAPE_END_MARKER = "END"
CAPE_EXCLUDED_QUESTION_KEYWORD = "distressed"
SKIP_COLUMNS = [
    "Response",
    "Participant Device Type",
    "Repeat Key",
    "Local Date and Time",
    "UTC Date and Time",
    "UTC Timestamp",
    "Local Timestamp",
    "Local Timezone",
    "Experiment ID",
    "Experiment Version",
    "Tree Node Key",
    "Schedule ID",
    "Participant Starting Group",
    "Participant Completion Code",
    "Participant External Session ID",
    "Event Index",
    "Participant Device",
    "Participant OS",
    "Participant Browser",
    "Participant Monitor Size",
    "Participant Viewport Size",
    "Checkpoint",
    "Room ID",
    "Room Order",
    "Task Version",
    "questionnaire-fwfy-END QUESTIONNAIRE",
    "questionnaire-fwfy-Used_-quantised",
    "questionnaire-fwfy-Used_-text",
    "branch-vm2q"
]


def candidate_csv_encodings() -> List[str]:
    encodings = ["utf-8-sig", "utf-8", locale.getpreferredencoding(False), "gb18030", "gbk", "cp1252"]
    unique_encodings: List[str] = []
    for encoding in encodings:
        normalized = (encoding or "").strip()
        if not normalized:
            continue
        if normalized not in unique_encodings:
            unique_encodings.append(normalized)
    return unique_encodings


def read_csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    last_decode_error: UnicodeDecodeError | None = None

    for encoding in candidate_csv_encodings():
        try:
            with path.open("r", encoding=encoding, newline="") as infile:
                reader = csv.DictReader(infile)
                if reader.fieldnames is None:
                    raise ValueError(f"{path.name} is missing a header row.")

                rows = list(reader)
                return list(reader.fieldnames), rows
        except UnicodeDecodeError as exc:
            last_decode_error = exc
            continue

    if last_decode_error is not None:
        raise ValueError(
            f"{path.name} could not be decoded with supported encodings: "
            f"{', '.join(candidate_csv_encodings())}"
        ) from last_decode_error
    raise ValueError(f"{path.name} could not be read as CSV.")


def detect_key_column(fieldnames: Sequence[str], path: Path) -> str:
    if TARGET_COLUMN in fieldnames:
        return TARGET_COLUMN
    if SOURCE_COLUMN in fieldnames:
        return SOURCE_COLUMN
    raise ValueError(
        f"{path.name} is missing key column '{TARGET_COLUMN}' "
        f"(or fallback '{SOURCE_COLUMN}')."
    )


def build_data_columns(fieldnames: Sequence[str], key_column: str) -> List[str]:
    skip_columns_set = set(SKIP_COLUMNS)
    excluded = {key_column, TARGET_COLUMN, SOURCE_COLUMN}
    return [
        name
        for name in fieldnames
        if name not in excluded and name not in skip_columns_set
    ]


def find_column_case_insensitive(
    fieldnames: Sequence[str], target_column: str
) -> str | None:
    target_lower = target_column.lower()
    for name in fieldnames:
        if name.lower() == target_lower:
            return name
    return None


def require_column_case_insensitive(
    fieldnames: Sequence[str],
    target_column: str,
    merge_csv: Path,
    context_label: str,
) -> str:
    column_name = find_column_case_insensitive(fieldnames, target_column)
    if column_name is None:
        raise ValueError(
            f"{merge_csv.name} is missing required column '{target_column}' "
            f"for {context_label} aggregation."
        )
    return column_name


def detect_merge_key_column(fieldnames: Sequence[str], merge_csv: Path) -> str:
    if SOURCE_COLUMN in fieldnames:
        return SOURCE_COLUMN
    return detect_key_column(fieldnames, merge_csv)


def iter_window_events(
    rows: Sequence[Dict[str, str]],
    key_column: str,
    response_column: str,
) -> Iterator[Tuple[int, str, str, Dict[str, str], str]]:
    started_participants: set[str] = set()
    finished_participants: set[str] = set()

    for row_number, row in enumerate(rows, start=2):
        participant_value = (row.get(key_column) or "").strip()
        if not participant_value or participant_value in finished_participants:
            continue

        response_value_text = (row.get(response_column) or "").strip()
        response_marker = normalize_marker_token(response_value_text)

        if participant_value not in started_participants:
            if response_marker != CAPE_BEGIN_MARKER:
                continue
            started_participants.add(participant_value)
            yield row_number, "begin", participant_value, row, response_value_text
            continue

        if response_marker == CAPE_END_MARKER:
            finished_participants.add(participant_value)
            yield row_number, "end", participant_value, row, response_value_text
            continue

        if response_marker == CAPE_BEGIN_MARKER:
            # Ignore repeated BEGIN markers before END.
            continue

        yield row_number, "row", participant_value, row, response_value_text


def parse_decimal(value: str, merge_csv: Path, row_number: int) -> Decimal:
    stripped = value.strip()
    if not stripped:
        return Decimal("0")
    try:
        return Decimal(stripped)
    except InvalidOperation as exc:
        raise ValueError(
            f"{merge_csv.name} has non-numeric Response value at row {row_number}: {value!r}"
        ) from exc


def normalize_marker_token(value: str) -> str:
    return value.strip().strip('"').strip("'").upper()


def normalize_question_token(value: str) -> str:
    return " ".join(value.strip().split()).rstrip("?").strip().lower()


def is_demographic_age_question(normalized_question: str) -> bool:
    question_value = normalized_question.strip().lower()
    if not question_value:
        return False
    if question_value in DEMOGRAPHIC_AGE_QUESTION_NORMALIZED:
        return True
    if "old are you" in question_value:
        return True

    tokens = question_value.replace("/", " ").replace("-", " ").split()
    return "age" in tokens


def map_demographic_question_to_column(normalized_question: str) -> str | None:
    if is_demographic_age_question(normalized_question):
        return DEMOGRAPHIC_AGE_OUTPUT_COLUMN
    if normalized_question in DEMOGRAPHIC_RACE_QUESTION_NORMALIZED:
        return DEMOGRAPHIC_RACE_OUTPUT_COLUMN
    if normalized_question in DEMOGRAPHIC_GENDER_QUESTION_NORMALIZED:
        return DEMOGRAPHIC_GENDER_OUTPUT_COLUMN
    return None


def format_decimal(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def aggregate_windowed_response_questions(
    rows: Sequence[Dict[str, str]],
    key_column: str,
    response_column: str,
    response_type_column: str,
    question_column: str,
) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    participant_response_rows: Dict[str, List[Dict[str, str]]] = {}
    first_participant_key: str | None = None
    for _, event, participant_value, row, _ in iter_window_events(
        rows,
        key_column,
        response_column,
    ):
        if event == "begin":
            participant_response_rows[participant_value] = []
            if first_participant_key is None:
                first_participant_key = participant_value
            continue
        if event != "row":
            continue

        response_type_value = (row.get(response_type_column) or "").strip().lower()
        if response_type_value != DASS21_RESPONSE_TYPE_VALUE:
            continue

        participant_response_rows.setdefault(participant_value, []).append(row)

    merge_columns: List[str] = []
    selected_questions: set[str] = set()
    if first_participant_key is not None:
        for row in participant_response_rows.get(first_participant_key, []):
            question_value = (row.get(question_column) or "").strip()
            if not question_value or question_value in selected_questions:
                continue
            selected_questions.add(question_value)
            merge_columns.append(question_value)

    participant_map: Dict[str, Dict[str, str]] = {}
    for participant, response_rows in participant_response_rows.items():
        question_to_response: Dict[str, str] = {}
        for row in response_rows:
            question_value = (row.get(question_column) or "").strip()
            if question_value not in selected_questions or question_value == "":
                continue
            question_to_response[question_value] = row.get(response_column, "")

        participant_map[participant] = {
            question: question_to_response.get(question, "")
            for question in merge_columns
        }

    return merge_columns, participant_map


def aggregate_windowed_keyed_questions(
    rows: Sequence[Dict[str, str]],
    key_column: str,
    response_column: str,
    key_field_column: str,
    key_field_token: str,
    question_column: str,
) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    participant_rows: Dict[str, List[Dict[str, str]]] = {}
    first_participant_key: str | None = None
    target_key = key_field_token.strip().lower()

    for _, event, participant_value, row, _ in iter_window_events(
        rows,
        key_column,
        response_column,
    ):
        if event == "begin":
            participant_rows[participant_value] = []
            if first_participant_key is None:
                first_participant_key = participant_value
            continue
        if event != "row":
            continue

        key_field_value = (row.get(key_field_column) or "").strip().lower()
        if key_field_value != target_key:
            continue

        participant_rows.setdefault(participant_value, []).append(row)

    merge_columns: List[str] = []
    selected_questions: set[str] = set()
    if first_participant_key is not None:
        for row in participant_rows.get(first_participant_key, []):
            question_value = (row.get(question_column) or "").strip()
            if not question_value or question_value in selected_questions:
                continue
            selected_questions.add(question_value)
            merge_columns.append(question_value)

    participant_map: Dict[str, Dict[str, str]] = {}
    for participant, question_rows in participant_rows.items():
        question_to_response: Dict[str, str] = {}
        for row in question_rows:
            question_value = (row.get(question_column) or "").strip()
            if question_value not in selected_questions or question_value == "":
                continue
            question_to_response[question_value] = row.get(response_column, "")

        participant_map[participant] = {
            question: question_to_response.get(question, "")
            for question in merge_columns
        }

    return merge_columns, participant_map


def aggregate_windowed_demographic_questions(
    rows: Sequence[Dict[str, str]],
    key_column: str,
    response_column: str,
    question_column: str,
    key_field_column: str,
) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    merge_columns = [
        DEMOGRAPHIC_AGE_OUTPUT_COLUMN,
        DEMOGRAPHIC_RACE_OUTPUT_COLUMN,
        DEMOGRAPHIC_GENDER_OUTPUT_COLUMN,
    ]

    participant_map: Dict[str, Dict[str, str]] = {}
    for _, event, participant_value, row, response_value_text in iter_window_events(
        rows,
        key_column,
        response_column,
    ):
        if event == "begin":
            participant_map[participant_value] = {column: "" for column in merge_columns}
            continue
        if event != "row":
            continue

        question_value = (row.get(question_column) or "").strip()
        normalized_question = normalize_question_token(question_value)
        target_column = map_demographic_question_to_column(normalized_question)
        if target_column is None:
            continue

        key_value = (row.get(key_field_column) or "").strip().lower()
        if target_column in (
            DEMOGRAPHIC_RACE_OUTPUT_COLUMN,
            DEMOGRAPHIC_GENDER_OUTPUT_COLUMN,
        ) and key_value != CUDIT_KEY_QUANTISED:
            continue

        participant_entry = participant_map.setdefault(
            participant_value,
            {column: "" for column in merge_columns},
        )
        if participant_entry[target_column] == "":
            participant_entry[target_column] = response_value_text

    return merge_columns, participant_map


def read_base_csv(base_csv: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    fieldnames, rows = read_csv_rows(base_csv)

    key_column = detect_key_column(fieldnames, base_csv)
    base_columns = build_data_columns(fieldnames, key_column)

    base_rows: List[Dict[str, str]] = []
    for row in rows:
        participant_value = (row.get(key_column) or "").strip()
        participant_private_id = (row.get(SOURCE_COLUMN) or "").strip()
        normalized_row: Dict[str, str] = {
            TARGET_COLUMN: participant_value,
            INTERNAL_PRIVATE_KEY: participant_private_id or participant_value,
        }
        for column in base_columns:
            normalized_row[column] = row.get(column, "")
        base_rows.append(normalized_row)

    return base_columns, base_rows


def read_merge_csv(merge_csv: Path) -> Tuple[List[str], Dict[str, Dict[str, str]], bool]:
    fieldnames, rows = read_csv_rows(merge_csv)
    merge_name_lower = merge_csv.name.lower()
    merge_stem_lower = merge_csv.stem.strip().lower()

    if DASS21_FILE_TOKEN in merge_name_lower:
        if SOURCE_COLUMN not in fieldnames:
            raise ValueError(
                f"{merge_csv.name} is missing required key column '{SOURCE_COLUMN}' "
                f"for DASS-21 aggregation."
            )

        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "DASS-21",
        )
        response_type_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_TYPE_COLUMN,
            merge_csv,
            "DASS-21",
        )

        participant_adjusted_subtotals: Dict[str, Decimal] = {}
        participant_response_index: Dict[str, int] = {}
        started_participants: set[str] = set()
        finished_participants: set[str] = set()
        for row_number, row in enumerate(rows, start=2):
            participant_value = (row.get(SOURCE_COLUMN) or "").strip()
            if not participant_value or participant_value in finished_participants:
                continue

            response_type_value = (row.get(response_type_column) or "").strip().lower()
            is_response_row = response_type_value == DASS21_RESPONSE_TYPE_VALUE

            if participant_value not in started_participants:
                if not is_response_row:
                    continue
                started_participants.add(participant_value)
                participant_adjusted_subtotals[participant_value] = Decimal("0")
                participant_response_index[participant_value] = 0
            elif not is_response_row:
                finished_participants.add(participant_value)
                continue

            if is_response_row:
                current_index = participant_response_index.get(participant_value, 0) + 1
                participant_response_index[participant_value] = current_index
                if current_index in DASS21_INCLUDED_ITEM_INDEXES:
                    response_value = parse_decimal(
                        row.get(response_column, "") or "",
                        merge_csv,
                        row_number,
                    )
                    adjusted_value = response_value - DASS21_ITEM_OFFSET
                    participant_adjusted_subtotals[participant_value] = (
                        participant_adjusted_subtotals.get(participant_value, Decimal("0"))
                        + adjusted_value
                    )

        participant_map: Dict[str, Dict[str, str]] = {
            participant: {
                DASS21_SCORE_COLUMN: format_decimal(
                    adjusted_subtotal * DASS21_TOTAL_MULTIPLIER
                ),
            }
            for participant, adjusted_subtotal in participant_adjusted_subtotals.items()
        }
        return [DASS21_SCORE_COLUMN], participant_map, True

    if GAD_FILE_TOKEN in merge_name_lower:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "GAD",
        )
        question_key_column = require_column_case_insensitive(
            fieldnames,
            GAD_QUESTION_KEY_COLUMN,
            merge_csv,
            "GAD",
        )

        participant_totals: Dict[str, Decimal] = {}
        participant_item_index: Dict[str, int] = {}
        started_participants: set[str] = set()
        finished_participants: set[str] = set()
        for row_number, row in enumerate(rows, start=2):
            participant_value = (row.get(key_column) or "").strip()
            if not participant_value or participant_value in finished_participants:
                continue

            question_key_value = (row.get(question_key_column) or "").strip().lower()

            if participant_value not in started_participants:
                if question_key_value != GAD_START_QUESTION_KEY:
                    continue
                started_participants.add(participant_value)
                participant_totals[participant_value] = Decimal("0")
                participant_item_index[participant_value] = 1
                current_index = 1
            else:
                current_index = participant_item_index.get(participant_value, 0) + 1
                participant_item_index[participant_value] = current_index

            if current_index % 2 == 1:
                response_value = parse_decimal(
                    row.get(response_column, "") or "",
                    merge_csv,
                    row_number,
                )
                participant_totals[participant_value] = (
                    participant_totals.get(participant_value, Decimal("0")) + response_value
                )

            if question_key_value == GAD_END_QUESTION_KEY:
                finished_participants.add(participant_value)

        participant_map: Dict[str, Dict[str, str]] = {
            participant: {GAD_SCORE_COLUMN: format_decimal(total)}
            for participant, total in participant_totals.items()
        }
        return [GAD_SCORE_COLUMN], participant_map, key_column == SOURCE_COLUMN

    if CUDIT_FILE_TOKEN in merge_name_lower:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "CUDIT",
        )
        key_field_column = require_column_case_insensitive(
            fieldnames,
            CUDIT_KEY_COLUMN,
            merge_csv,
            "CUDIT",
        )

        participant_totals: Dict[str, Decimal] = {}
        for row_number, event, participant_value, row, response_value_text in iter_window_events(
            rows,
            key_column,
            response_column,
        ):
            if event == "begin":
                participant_totals[participant_value] = Decimal("0")
                continue
            if event != "row":
                continue

            key_field_value = (row.get(key_field_column) or "").strip().lower()
            if key_field_value != CUDIT_KEY_QUANTISED:
                continue

            response_value = parse_decimal(response_value_text, merge_csv, row_number)
            participant_totals[participant_value] = (
                participant_totals.get(participant_value, Decimal("0")) + response_value
            )

        participant_map: Dict[str, Dict[str, str]] = {
            participant: {CUDIT_SUM_COLUMN: format_decimal(total)}
            for participant, total in participant_totals.items()
        }
        return [CUDIT_SUM_COLUMN], participant_map, key_column == SOURCE_COLUMN

    if IDENTITY_FILE_TOKEN in merge_name_lower:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "Identity",
        )
        key_field_column = require_column_case_insensitive(
            fieldnames,
            CUDIT_KEY_COLUMN,
            merge_csv,
            "Identity",
        )
        question_column = require_column_case_insensitive(
            fieldnames,
            CAPE_QUESTION_COLUMN,
            merge_csv,
            "Identity",
        )

        merge_columns, participant_map = aggregate_windowed_keyed_questions(
            rows,
            key_column,
            response_column,
            key_field_column,
            IDENTITY_KEY_VALUE,
            question_column,
        )
        return merge_columns, participant_map, key_column == SOURCE_COLUMN

    if DEMOGRAPHIC_FILE_TOKEN in merge_name_lower:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "Demographic",
        )
        question_column = require_column_case_insensitive(
            fieldnames,
            CAPE_QUESTION_COLUMN,
            merge_csv,
            "Demographic",
        )
        key_field_column = require_column_case_insensitive(
            fieldnames,
            CUDIT_KEY_COLUMN,
            merge_csv,
            "Demographic",
        )

        merge_columns, participant_map = aggregate_windowed_demographic_questions(
            rows,
            key_column,
            response_column,
            question_column,
            key_field_column,
        )
        return merge_columns, participant_map, key_column == SOURCE_COLUMN

    if MOTIVE_FILE_TOKEN in merge_name_lower:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "Motive",
        )
        response_type_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_TYPE_COLUMN,
            merge_csv,
            "Motive",
        )
        question_column = require_column_case_insensitive(
            fieldnames,
            CAPE_QUESTION_COLUMN,
            merge_csv,
            "Motive",
        )

        merge_columns, participant_map = aggregate_windowed_response_questions(
            rows,
            key_column,
            response_column,
            response_type_column,
            question_column,
        )
        return merge_columns, participant_map, key_column == SOURCE_COLUMN

    if merge_stem_lower == CANNABIS_BG_FILE_STEM:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "CannabisBG",
        )
        response_type_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_TYPE_COLUMN,
            merge_csv,
            "CannabisBG",
        )
        question_column = require_column_case_insensitive(
            fieldnames,
            CAPE_QUESTION_COLUMN,
            merge_csv,
            "CannabisBG",
        )
        merge_columns, participant_map = aggregate_windowed_response_questions(
            rows,
            key_column,
            response_column,
            response_type_column,
            question_column,
        )
        return merge_columns, participant_map, key_column == SOURCE_COLUMN

    if I8_FILE_TOKEN in merge_name_lower:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "I-8",
        )

        participant_scores: Dict[str, Dict[str, Decimal]] = {}
        participant_score_counts: Dict[str, Dict[str, int]] = {}
        participant_item_index: Dict[str, int] = {}
        relevant_indexes = (
            I8_URGENCY_INDEXES
            | I8_LACK_OF_PREMEDITATION_INDEXES
            | I8_LACK_OF_PERSEVERANCE_INDEXES
            | I8_SENSATION_SEEKING_INDEXES
        )
        for row_number, event, participant_value, _, response_value_text in iter_window_events(
            rows,
            key_column,
            response_column,
        ):
            if event == "begin":
                participant_scores[participant_value] = {
                    I8_URGENCY_COLUMN: Decimal("0"),
                    I8_LACK_OF_PREMEDITATION_COLUMN: Decimal("0"),
                    I8_LACK_OF_PERSEVERANCE_COLUMN: Decimal("0"),
                    I8_SENSATION_SEEKING_COLUMN: Decimal("0"),
                }
                participant_score_counts[participant_value] = {
                    I8_URGENCY_COLUMN: 0,
                    I8_LACK_OF_PREMEDITATION_COLUMN: 0,
                    I8_LACK_OF_PERSEVERANCE_COLUMN: 0,
                    I8_SENSATION_SEEKING_COLUMN: 0,
                }
                participant_item_index[participant_value] = 0
                continue
            if event != "row":
                continue

            current_index = participant_item_index.get(participant_value, 0) + 1
            participant_item_index[participant_value] = current_index
            if current_index not in relevant_indexes:
                continue

            response_value = parse_decimal(response_value_text, merge_csv, row_number)
            participant_score = participant_scores[participant_value]
            participant_score_count = participant_score_counts[participant_value]
            if current_index in I8_URGENCY_INDEXES:
                participant_score[I8_URGENCY_COLUMN] += response_value
                participant_score_count[I8_URGENCY_COLUMN] += 1
            elif current_index in I8_LACK_OF_PREMEDITATION_INDEXES:
                participant_score[I8_LACK_OF_PREMEDITATION_COLUMN] += (
                    I8_RECODE_BASE - response_value
                )
                participant_score_count[I8_LACK_OF_PREMEDITATION_COLUMN] += 1
            elif current_index in I8_LACK_OF_PERSEVERANCE_INDEXES:
                participant_score[I8_LACK_OF_PERSEVERANCE_COLUMN] += (
                    I8_RECODE_BASE - response_value
                )
                participant_score_count[I8_LACK_OF_PERSEVERANCE_COLUMN] += 1
            elif current_index in I8_SENSATION_SEEKING_INDEXES:
                participant_score[I8_SENSATION_SEEKING_COLUMN] += response_value
                participant_score_count[I8_SENSATION_SEEKING_COLUMN] += 1

        participant_map: Dict[str, Dict[str, str]] = {
            participant: {
                I8_URGENCY_COLUMN: format_decimal(
                    scores[I8_URGENCY_COLUMN]
                    / Decimal(max(participant_score_counts[participant][I8_URGENCY_COLUMN], 1))
                ),
                I8_LACK_OF_PREMEDITATION_COLUMN: format_decimal(
                    scores[I8_LACK_OF_PREMEDITATION_COLUMN]
                    / Decimal(
                        max(
                            participant_score_counts[participant][
                                I8_LACK_OF_PREMEDITATION_COLUMN
                            ],
                            1,
                        )
                    )
                ),
                I8_LACK_OF_PERSEVERANCE_COLUMN: format_decimal(
                    scores[I8_LACK_OF_PERSEVERANCE_COLUMN]
                    / Decimal(
                        max(
                            participant_score_counts[participant][
                                I8_LACK_OF_PERSEVERANCE_COLUMN
                            ],
                            1,
                        )
                    )
                ),
                I8_SENSATION_SEEKING_COLUMN: format_decimal(
                    scores[I8_SENSATION_SEEKING_COLUMN]
                    / Decimal(
                        max(
                            participant_score_counts[participant][I8_SENSATION_SEEKING_COLUMN],
                            1,
                        )
                    )
                ),
            }
            for participant, scores in participant_scores.items()
        }
        return [
            I8_URGENCY_COLUMN,
            I8_LACK_OF_PREMEDITATION_COLUMN,
            I8_LACK_OF_PERSEVERANCE_COLUMN,
            I8_SENSATION_SEEKING_COLUMN,
        ], participant_map, key_column == SOURCE_COLUMN

    if CAPE_FILE_TOKEN in merge_name_lower:
        key_column = detect_merge_key_column(fieldnames, merge_csv)
        response_column = require_column_case_insensitive(
            fieldnames,
            DASS21_RESPONSE_COLUMN,
            merge_csv,
            "CAPE",
        )
        question_column = require_column_case_insensitive(
            fieldnames,
            CAPE_QUESTION_COLUMN,
            merge_csv,
            "CAPE",
        )

        participant_totals: Dict[str, Decimal] = {}
        participant_item_index: Dict[str, int] = {}
        for _, event, participant_value, row, response_value_text in iter_window_events(
            rows,
            key_column,
            response_column,
        ):
            if event == "begin":
                participant_totals[participant_value] = Decimal("0")
                participant_item_index[participant_value] = 0
                continue
            if event != "row":
                continue

            question_value = (row.get(question_column) or "").strip().lower()
            if CAPE_EXCLUDED_QUESTION_KEYWORD in question_value:
                continue

            try:
                response_number = Decimal(response_value_text)
            except InvalidOperation:
                # CAPE windows can contain non-numeric status rows; skip them.
                continue

            current_index = participant_item_index.get(participant_value, 0) + 1
            participant_item_index[participant_value] = current_index
            if current_index % 2 == 0:
                # Only sum odd-indexed CAPE responses in the participant window.
                continue

            participant_totals[participant_value] = (
                participant_totals.get(participant_value, Decimal("0")) + response_number
            )

        participant_map: Dict[str, Dict[str, str]] = {
            participant: {CAPE_SCORE_COLUMN: format_decimal(total)}
            for participant, total in participant_totals.items()
        }
        return [CAPE_SCORE_COLUMN], participant_map, key_column == SOURCE_COLUMN

    key_column = detect_key_column(fieldnames, merge_csv)
    merge_columns = build_data_columns(fieldnames, key_column)

    participant_map: Dict[str, Dict[str, str]] = {}
    for row in rows:
        participant_value = (row.get(key_column) or "").strip()
        if not participant_value:
            continue

        row_values = {column: row.get(column, "") for column in merge_columns}
        if participant_value not in participant_map:
            participant_map[participant_value] = row_values
        else:
            # If a participant appears multiple times, keep the latest non-empty values.
            for column, value in row_values.items():
                if value != "":
                    participant_map[participant_value][column] = value

    return merge_columns, participant_map, False


def make_unique_columns(
    columns: Sequence[str], used_columns: set[str], file_label: str
) -> Tuple[List[str], Dict[str, str]]:
    unique_columns: List[str] = []
    column_mapping: Dict[str, str] = {}

    for column in columns:
        candidate = column
        if candidate in used_columns:
            candidate = f"{file_label}_{column}"

        index = 2
        while candidate in used_columns:
            candidate = f"{file_label}_{column}_{index}"
            index += 1

        used_columns.add(candidate)
        unique_columns.append(candidate)
        column_mapping[column] = candidate

    return unique_columns, column_mapping


def merge_csvs(base_csv: Path, merge_csvs_list: Sequence[Path], output_csv: Path) -> None:
    base_columns, base_rows = read_base_csv(base_csv)

    output_columns = [TARGET_COLUMN] + base_columns
    used_columns = set(output_columns)

    for merge_csv in merge_csvs_list:
        merge_columns, participant_map, use_private_id = read_merge_csv(merge_csv)
        unique_columns, column_mapping = make_unique_columns(
            merge_columns, used_columns, merge_csv.stem or "file"
        )
        output_columns.extend(unique_columns)

        for row in base_rows:
            match_key = INTERNAL_PRIVATE_KEY if use_private_id else TARGET_COLUMN
            participant_value = row.get(match_key, "")
            matched_values = participant_map.get(participant_value)
            for source_column in merge_columns:
                output_column = column_mapping[source_column]
                if matched_values is None:
                    row[output_column] = ""
                else:
                    row[output_column] = matched_values.get(source_column, "")

    with output_csv.open("w", encoding="utf-8-sig", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_columns)
        writer.writeheader()
        for row in base_rows:
            writer.writerow({column: row.get(column, "") for column in output_columns})


def run_gui() -> None:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except ImportError as exc:
        raise RuntimeError("GUI mode requires tkinter, but it is not available.") from exc

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    root.update()

    base_file = filedialog.askopenfilename(
        title="Select base CSV file",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
    )
    if not base_file:
        messagebox.showinfo("Cancelled", "No base file selected. Nothing was processed.")
        root.destroy()
        return

    merge_files: Sequence[str] = ()
    should_merge = messagebox.askyesno(
        "Merge Option",
        "Do you want to select additional CSV files to merge?",
    )
    if should_merge:
        merge_files = filedialog.askopenfilenames(
            title="Select CSV files to merge (multi-select allowed)",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )

    default_output = str(Path(base_file).with_name("output.csv"))
    output_file = filedialog.asksaveasfilename(
        title="Select output CSV file",
        defaultextension=".csv",
        initialfile=Path(default_output).name,
        initialdir=str(Path(default_output).parent),
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
    )
    if not output_file:
        messagebox.showinfo("Cancelled", "No output file selected. Nothing was processed.")
        root.destroy()
        return

    try:
        merge_csvs(
            Path(base_file),
            [Path(path) for path in merge_files],
            Path(output_file),
        )
        if merge_files:
            messagebox.showinfo(
                "Done",
                f"Created: {output_file}\nMerged files: {len(merge_files)}",
            )
        else:
            messagebox.showinfo("Done", f"Created base file: {output_file}")
    except Exception as exc:
        messagebox.showerror("Failed", str(exc))
    finally:
        root.destroy()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Use a base CSV and optional extra CSV files, merge data by participant key, "
            "and append each file's columns from left to right in selection order."
        )
    )
    parser.add_argument("base_csv", nargs="?", help="Base CSV file path")
    parser.add_argument(
        "merge_csvs",
        nargs="*",
        help="Extra CSV files to merge (ordered left-to-right in output)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="output.csv",
        help="Output CSV file path (default: output.csv)",
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Use GUI file pickers for base/merge/output files",
    )
    parser.add_argument(
        "--create-base",
        action="store_true",
        help="Create base file only from one CSV (no merge files)",
    )
    args = parser.parse_args()

    if args.gui or not args.base_csv:
        run_gui()
        return

    base_path = Path(args.base_csv)
    merge_paths = [Path(path) for path in args.merge_csvs]
    output_path = Path(args.output)

    if not base_path.exists():
        raise FileNotFoundError(f"Base file not found: {base_path}")
    for merge_path in merge_paths:
        if not merge_path.exists():
            raise FileNotFoundError(f"Merge file not found: {merge_path}")

    if args.create_base and merge_paths:
        raise ValueError("--create-base cannot be used with merge CSV files.")

    if args.create_base:
        merge_csvs(base_path, [], output_path)
        print(f"Created base file: {output_path}")
        return

    merge_csvs(base_path, merge_paths, output_path)
    print(f"Created: {output_path} (merged {len(merge_paths)} file(s))")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        print(error_text, file=sys.stderr)
        try:
            import tkinter as tk
            from tkinter import messagebox

            root = tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            root.update()
            messagebox.showerror("Error", error_text)
            root.destroy()
        except Exception:
            pass
        sys.exit(1) 
