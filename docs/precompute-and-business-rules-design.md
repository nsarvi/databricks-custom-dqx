# Precompute and Business Rules Design

## Purpose

This feature adds a rule-pack level orchestration model for complex row validations, especially PCSL timeliness rules where many T-codes share the same population and validity checks.

The design keeps the framework generic:

- All executable rule IDs remain DQX-compatible row callables.
- Each executable rule must return `make_condition()`.
- The framework does not assume a rule is SQL-based, date-based, or expression-based.
- Business rule grouping is separate from executable DQX rules.

## YAML Sections

A rule pack can now contain these major sections:

```yaml
rule_packs:
  - id: pcsl_2027_timeliness
    rule_version: "1.0"

    emit:
      match_policy: first
      code_separator: "|"

    precompute_rules:
      rules: []

    columns: {}

    business_rules: []

    dataset_rules: []
```

Section meanings:

- `columns`: existing column-level DQX row rules. This behavior is unchanged.
- `dataset_rules`: existing dataset-level DQX rules. This behavior is unchanged.
- `precompute_rules`: reusable row rules that are materialized once per input row.
- `business_rules`: business groupings such as `T100.1`; these are not callable IDs.
- `business_rules[].rules`: executable DQX-compatible row rules.

## Precompute Rules

`precompute_rules` defines reusable checks that many business rules can reference.

Example:

```yaml
precompute_rules:
  rules:
    - id: row.sql_expression
      name: standard_non_part_b_approval_denial
      criticality: error
      message: "Standard non-Part-B approval/denial population"
      arguments:
        sql_expression: >
          `Is this a Primary Part B Drug Request?` = 'N'
          AND `Processing Priority` = 'S'
          AND `Disposition` IN ('F', 'P', 'A')
```

Important semantics:

- `precompute_rules.rules[].id` is a registered row rule callable.
- `name` is required and must be unique within the rule pack.
- The callable can be any DQX-compatible row rule as long as it returns `make_condition()`.
- The result is converted to a boolean by checking whether the returned condition is non-null.
- The boolean is materialized as a technical column before DQX checks run.

Generated technical column names use the constant prefix:

```python
TIMELINESS_PRECOMPUTE_COLUMN_PREFIX = "__zkblock_dqx_"
```

For example:

```text
standard_non_part_b_approval_denial
```

becomes:

```text
__zkblock_dqx_standard_non_part_b_approval_denial
```

These technical columns are dropped from both valid and quarantine dataframes after DQX split.

## Why Precompute Exists

Without precompute, a shared condition may be evaluated inside every T-rule that references it.

With precompute:

1. The shared condition is evaluated once.
2. A boolean technical column is created.
3. Business rules reuse that boolean by name.

This is useful for common checks such as:

- population gates
- extension/no-extension checks
- AOR present or NA checks
- written notice present or NA checks
- reusable date validity checks
- reusable ordering checks

## Business Rules

`business_rules` models business cases such as `T100.1`, `T100.2`, and `T101.1`.

Example:

```yaml
business_rules:
  - id: T100.1
    description: "Untimely verbal notification, AOR present, no extension"
    use_precompute_rules:
      - standard_non_part_b_approval_denial
      - no_extension
      - request_and_verbal_valid
      - aor_valid
      - written_valid
      - verbal_before_written
    rules:
      - id: row.elapsed_days_sql
        name: gt_7_calendar_days
        criticality: error
        message: "Elapsed days greater than 7"
        arguments:
          start:
            sql_expression: >
              greatest(
                to_date(`Date Request Received`),
                to_date(`AOR/Equivalent Notice Receipt Date`)
              )
          end:
            sql_expression: "to_date(`Date verbal notification provided`)"
          sql_expression: "{elapsed_days} > 7"
        metadata:
          error_dictionary_test_cd: "T100.1"
```

Important semantics:

- `business_rules[].id` is a business grouping id, not a callable.
- `business_rules[].rules[].id` is the DQX-compatible callable id.
- `business_rules[].rules[]` are AND-ed together.
- `use_precompute_rules` references `precompute_rules.rules[].name`.
- Optional `when.sql_expression` can provide an extra business gate.
- The emitted code should live in `business_rules[].rules[].metadata.error_dictionary_test_cd`.

## Evaluation Logic

For each business rule, the runtime builds this logical condition:

```text
all referenced precompute booleans
AND optional business_rules[].when condition
AND all business_rules[].rules[] make_condition predicates
```

If the final condition is true, the business rule matches.

The framework emits the first available code from the business rule's executable rules:

```yaml
metadata:
  error_dictionary_test_cd: "T100.1"
```

If no `error_dictionary_test_cd` is present, it falls back to:

```yaml
business_rules[].id
```

## Emit Policy

The pack-level `emit` block controls how matched business rules are emitted.

```yaml
emit:
  match_policy: first
  code_separator: "|"
```

Supported policies:

- `first`: emit the first matching business rule code.
- `all`: emit all matching business rule codes joined by `code_separator`.

This policy applies only to `business_rules`. It does not affect:

- `columns.*.rules`
- `precompute_rules.rules`
- `business_rules[].rules`
- `dataset_rules`

## Runtime Flow

The runtime flow is:

1. Load and merge YAML configs.
2. Resolve `extends`.
3. Materialize `precompute_rules` once per input dataframe.
4. Compile normal column-level DQX rules.
5. Compile one business-rule matrix DQX row rule when `business_rules` exists.
6. Run DQX `apply_checks_and_split`.
7. Drop technical precompute columns from valid/quarantine dataframes.
8. Enrich quarantine metadata for business-rule output codes.

## Extends Behavior

When a rule pack extends another rule pack:

- `columns` keep existing merge behavior.
- `dataset_rules` merge by `id`.
- `business_rules` merge by `id`.
- `precompute_rules.rules` merge by `name` when present, otherwise by `id`.
- scalar values such as `category`, `subcategory`, `rule_type`, `fast_fail`, `emit`, and anchors are overridden by the child.

This allows a base pack to define common precompute rules and broad business rules, while child packs can override or add specific T-cases.

## Depends On

`depends_on` is currently not enforced by the runtime.

It may appear in YAML as metadata or documentation, but it does not currently skip or gate any rule execution.

Dependency-style behavior can be handled downstream in quarantine/result processing, or implemented later as a separate feature.

## Design Rules

Config authors should follow these rules:

- Put physical column checks under `columns`.
- Put reusable shared checks under `precompute_rules`.
- Put T-code/business groupings under `business_rules`.
- Put executable business checks under `business_rules[].rules`.
- Do not put DQX callable IDs in `business_rules[].id`.
- Do put DQX callable IDs in `business_rules[].rules[].id`.
- Store emitted business codes in `business_rules[].rules[].metadata.error_dictionary_test_cd`.

## Minimal Example

```yaml
rule_packs:
  - id: pcsl_2027_timeliness
    rule_version: "1.0"
    row_anchor_column: ORG_DETRMN_NUM

    emit:
      match_policy: first
      code_separator: "|"

    precompute_rules:
      rules:
        - id: row.sql_expression
          name: standard_population
          criticality: error
          message: "Standard population"
          arguments:
            sql_expression: "`Processing Priority` = 'S'"

    business_rules:
      - id: T100.1
        use_precompute_rules:
          - standard_population
        rules:
          - id: row.elapsed_days_sql
            name: gt_7_calendar_days
            criticality: error
            message: "Elapsed days greater than 7"
            arguments:
              start:
                sql_expression: "to_date(`Date Request Received`)"
              end:
                sql_expression: "to_date(`Date verbal notification provided`)"
              sql_expression: "{elapsed_days} > 7"
            metadata:
              error_dictionary_test_cd: "T100.1"
```

