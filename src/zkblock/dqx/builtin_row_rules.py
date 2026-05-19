from pyspark.sql import Column
from pyspark.sql import functions as F
from databricks.labs.dqx.check_funcs import make_condition


def _date_point_sql(point) -> str:
    if isinstance(point, str):
        return point

    point = point or {}
    expression = point.get("sql_expression") or point.get("expression")
    if not expression:
        raise ValueError("Date point requires sql_expression or expression.")
    return str(expression)


def sql_expression(
    column: str,
    sql_expression: str | None = None,
    expression: str | None = None,
    message: str | None = None,
    alias: str | None = None,
) -> Column:
    expr = sql_expression or expression
    if not expr:
        raise ValueError("sql_expression rule requires 'sql_expression' or 'expression'.")
    return make_condition(
        condition=F.expr(expr),
        message=message or expr,
        alias=alias or f"{column}_sql_expression",
    )


def elapsed_days_sql(
    column: str,
    start,
    end,
    sql_expression: str,
    message: str | None = None,
    alias: str | None = None,
) -> Column:
    start_sql = _date_point_sql(start)
    end_sql = _date_point_sql(end)
    elapsed_days_sql_text = f"datediff({end_sql}, {start_sql})"
    rendered = (
        sql_expression
        .replace("{elapsed_days}", elapsed_days_sql_text)
        .replace("{calendar_days}", elapsed_days_sql_text)
        .replace("{actual_days}", elapsed_days_sql_text)
        .replace("{start_date}", start_sql)
        .replace("{end_date}", end_sql)
    )
    return make_condition(
        condition=F.expr(rendered),
        message=message or rendered,
        alias=alias or f"{column}_elapsed_days_sql",
    )


# Checks if the column value is not in the list of exempted values
def values_exempted(column: str, excepted_values: list[str]) -> Column:
    valid = F.col(column).isNull() & F.col(column).isNotNull()
    # Convert all string invalid values to lowercase for case-insensitive comparison
    excepted_list = [v.lower() if isinstance(v, str) else v for v in excepted_values if v is not None]
    valid = F.lower(F.col(column)).isin(excepted_list)
    return make_condition(condition=valid, message=f"{column} invalid value, contains {excepted_values}", alias=f"{column}_values_exempted")


# Checks if the length of the column value is outside the specified min and max bounds
def is_length_invalid(column: str, min_len: int, max_len: int) -> Column:
    """
    Checks if the length of the column value is outside the specified min and max bounds.

    Args:
        column (str): The name of the column to check.
        min_len (int): The minimum length allowed.
        max_len (int): The maximum length allowed.

    Returns:
        Column: A Column object representing the condition.
    """
    cond = (F.length(F.col(column)) < min_len) | (F.length(F.col(column)) > max_len)
    valid = (cond | F.col(column).isNull() | (F.length(F.col(column)) == 0))

    return make_condition(condition=valid, message=f"{column} invalid length", alias=f"{column}_length_invalid")


def is_alphanum_invalid(column: str) -> Column:
    """
    Check if the column value is not alphanumeric.

    Args:
        column (str): The name of the column to check.

    Returns:
        Column: A Column object representing the condition.
    """
    cond = ~F.col(column).rlike(r"^[A-Za-z0-9]+$")
    valid = (cond | F.col(column).isNull() | (F.length(F.col(column)) == 0))  # nulls/blanks are considered invalid

    return make_condition(condition=valid, message=f"{column} non-alphanumeric", alias=f"{column}_alphanum")


# Checks if the column value is in a list of invalid values (case-insensitive)
def is_value_invalid(column: str, invalid_values: list[str]) -> Column:
    """
    Check if the column value is in a list of invalid values (case-insensitive).

    Args:
        column (str): The name of the column to check.
        invalid_values (list[str]): The list of invalid values.

    Returns:
        Column: A Column object representing the condition.
    """
    # Set default invalid values if none provided
    if invalid_values is None:
        invalid_values = ['none', 'na', 'n/a', 'null']
    # Convert all string invalid values to lowercase for case-insensitive comparison
    invalid_set = set([v.lower() if isinstance(v, str) else v for v in invalid_values])
    # Build condition: value is in the set of invalid values (excluding None)
    cond = F.lower(F.col(column)).isin([v for v in invalid_set if v is not None])
    valid = (cond | F.col(column).isNull() | (F.length(F.col(column)) == 0))

    return make_condition(condition=valid, message=f"{column} invalid value", alias=f"{column}_invalid_value")


# Checks if the column value is not in the list of allowed values, with option to allow nulls
def is_allowed_values_invalid(column: str, allowed_values: list[str]) -> Column:
    """
    Checks if the column value is not in the list of allowed values (case-insensitive),
    with option to allow nulls.

    Args:
        column (str): The name of the column to check.
        allowed_values (list[str]): The list of allowed values.

    Returns:
        Column: A Column object representing the condition.
    """
    allowed_values_lower = [v.lower() for v in allowed_values]
    cond = ~F.lower(F.col(column)).isin(allowed_values_lower)
    valid = (cond | F.col(column).isNull() | (F.length(F.col(column)) == 0))
    return make_condition(condition=valid, message=f"{column} contains invalid values", alias=f"{column}_invalid")


def is_sci_notation_invalid(column: str) -> Column:
    """
    Checks if the column value is not in scientific notation.

    Args:
        column (str): The name of the column to check.

    Returns:
        Column: A Column object representing the condition.
    """
    cond = F.col(column).rlike(r"^[+-]?\d+(\.\d+)?[eE][+-]?\d+$")
    valid = ~cond
    valid = (cond | F.col(column).isNull() | (F.length(F.col(column)) == 0))  # nulls/blanks are considered invalid

    return make_condition(condition=valid, message=f"{column} not in scientific notation", alias=f"{column}_sci_notation")


def is_date_format_invalid(
    column: str,
    accepted_formats: list[str],
    allowed_values: list[str] | None = None,
    allow_null_or_blank: bool = True,
) -> Column:
    """
    Checks whether a populated value cannot be parsed using the accepted date formats.

    This is a format check only. Required-date logic should remain a separate
    rule so timeliness logic can compose presence/NA cases independently.
    """
    raw = F.col(column)
    text = F.trim(raw.cast("string"))

    allowed_values = allowed_values or []
    allowed_lower = [str(v).strip().lower() for v in allowed_values if v is not None]

    skip = F.lit(False)
    if allow_null_or_blank:
        skip = raw.isNull() | (text == F.lit(""))
    if allowed_lower:
        skip = skip | F.lower(text).isin(allowed_lower)

    parsed = [F.to_date(text, fmt) for fmt in (accepted_formats or [])]
    parsed_date = F.coalesce(*parsed) if parsed else F.to_date(text)
    invalid = (~skip) & parsed_date.isNull()

    return make_condition(
        condition=invalid,
        message=f"{column} invalid date format",
        alias=f"{column}_date_format_invalid",
    )
