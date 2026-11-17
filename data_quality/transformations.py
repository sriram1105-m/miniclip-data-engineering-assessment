
from pyspark.sql import functions as F


def apply_transformations(df, rules):
    """Apply configured transformations to a Spark DataFrame."""

    for rule in rules["transformations"]:
        field = rule["field"]
        operation = rule["operation"]
        event_types = rule.get("event_types", [])

        if field not in df.columns:
            continue

        applies = (
            F.col("event-type").isin(event_types)
            if event_types
            else F.lit(True)
        )

        if operation == "uppercase":
            transformed = F.upper(F.col(field))

        elif operation == "map_values":
            mapping = rule["mapping"]

            if not mapping:
                raise ValueError(f"Empty mapping for field: {field}")

            mapping_expr = F.create_map(
                *[
                    item
                    for key, value in mapping.items()
                    for item in (F.lit(key), F.lit(value))
                ]
            )

            transformed = F.coalesce(
                F.element_at(mapping_expr, F.col(field)),
                F.col(field)
            )

        else:
            raise ValueError(
                f"Unsupported transformation operation: {operation}"
            )

        df = df.withColumn(
            field,
            F.when(applies, transformed).otherwise(F.col(field))
        )

    return df
