from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col, count


def evaluate_expectation(
        df: DataFrame,
        rule: str
        ):
    """evaluates a given expectation rule on the dataframe.

    Args:
        df (DataFrame): input dataframe
        rule (str): expression to evaluate
    """
    total = df.count()
    failed = df.filter(f"NOT ({rule})").count()

    success_rate = (total - failed) / total if total > 0 else 1.0
    status = "PASS" if failed ==0 else "FAIL"

    return {
        "total_records": total,
        "failed_records": failed,
        "success_rate": success_rate,
        "status": status
        }


def evaluate_sql_expectation(df: DataFrame, rule: str):
    """Evaluates a SQL-based expectation on the given DataFrame.

    Args:
        df (DataFrame): The input DataFrame to evaluate.
        rule (str): The SQL expression representing the expectation.
    """
    total = df.count()
    failed_df = df.filter(expr(f"NOT {(rule)}"))
    failed = failed_df.count()
    return total, failed_df, failed

def evaluate_column_range(df, column, min_value, max_value):
    """Evaluates if the values in a specified column fall within a given range.

    Args:
        df (DataFrame): The input DataFrame to evaluate.
        column (str): The column name to check.
        min_value (float): The minimum acceptable value.
        max_value (float): The maximum acceptable value.
    """
    condition = None

    if min_value is not None:
        condition = expr(f"{column} >= {min_value}")
    
    if max_value is not None:
        condition = condition & (col(column) <= max_value)

    total = df.count()
    failed_df = df.filter(~condition)
    failed = failed_df.count()
    return total, failed_df, failed

def evaluate_duplicate_rows(df, subset = None):
    """Evaluates if there are duplicate rows in the DataFrame.

    Args:
        df (DataFrame): The input DataFrame to evaluate.
        subset (list, optional): List of columns to consider for identifying duplicates. 
                                 If None, all columns are considered. Defaults to None.
    """
    total = df.count()
    if subset:
        duplicate_df = df.groupBy(subset).count().filter(col("count") > 1)
    else:
        duplicate_df = df.groupBy(df.columns).count().filter(col("count") > 1)
    
    failed_df = df.join(duplicate_df.select(subset), on=subset, how='inner')
    
    failed = duplicate_df.count()
    return total, failed_df, failed

def evaluate_primary_key(df, pk_columns):
    """Evaluates if the specified columns form a primary key (i.e., no duplicates).

    Args:
        df (DataFrame): The input DataFrame to evaluate.
        pk_columns (str): columns that should form the primary key.
    """
    total = df.count()
    duplicate_df = df.groupBy(pk_columns).count().filter(col("count") > 1)
    failed_df = df.join(duplicate_df.select(pk_columns), on=pk_columns, how='inner')
    failed = duplicate_df.count()
    return total, failed_df, failed
