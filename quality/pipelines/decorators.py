from functools import wraps
from quality.pipelines.registry import EXPECTATION_REGISTRY, VIEW_REGISTRY
from quality.pipelines.expectations import evaluate_duplicate_rows, evaluate_expectation, evaluate_primary_key, evaluate_sql_expectation, evaluate_column_range
from quality.pipelines.utils.utils import persist_bad_records
from quality.pipelines.database.session import SessionLocal, init_db
from quality.pipelines.database.models import DataQualityMetric

def expect(name: str, rule: str, severity: str = "ERROR"):
    """
    Usage:
    @dp.expect("valid_artist_name", "artist_name IS NOT NULL")
    """
    def decorator(func):
        EXPECTATION_REGISTRY[func.__name__].append(
            {   
                "type": "sql",
                "name": name, 
                "rule": rule,
                "severity": severity
            }
        )
        return func
    return decorator

def expect_primary_key(name, columns: str, severity: str = "ERROR"):
    """
    Usage:
    @dp.expect_primary_key("id")
    """
    def decorator(func):
        EXPECTATION_REGISTRY[func.__name__].append(
            {
                "type": "primary_key",
                "name": name,
                "columns": columns,
                "severity": severity
            }
        )
        return func
    return decorator

def expect_no_duplicates(name, columns: list = None, severity: str = "ERROR"):
    """
    Usage:
    @dp.expect_no_duplicates("no_duplicates", ["id", "name"])
    """
    def decorator(func):
        EXPECTATION_REGISTRY[func.__name__].append(
            {
                "type": "no_duplicates",
                "name": name,
                "columns": columns,
                "severity": severity
            }
        )
        return func
    return decorator

def expect_column_range(name, column, min_value=None, max_value=None, severity="ERROR"):
    def decorator(func):
        EXPECTATION_REGISTRY[func.__name__].append({
            "type": "range",
            "name": name,
            "column": column,
            "min": min_value,
            "max": max_value,
            "severity": severity,
        })
        return func
    return decorator

def materialized_view(comment: str = None, fail_fast: bool = True):
    def decorator(func):
        VIEW_REGISTRY[func.__name__] = comment

        @wraps(func)
        def wrapper(*args, **kwargs):
            init_db()
            session = SessionLocal()

            df = func(*args, **kwargs)
            expectations = EXPECTATION_REGISTRY.get(func.__name__, [])

            errors = []

            for exp in expectations:
                if exp["type"] == "sql":
                    total, failed_df, failed = evaluate_sql_expectation(df, exp["rule"])

                elif exp["type"] == "range":
                    total, failed_df, failed = evaluate_column_range(
                        df, exp["column"], exp["min"], exp["max"]
                    )

                elif exp["type"] == "duplicate":
                    total, failed_df, failed = evaluate_duplicate_rows(
                        df, exp["columns"]
                    )

                elif exp["type"] == "primary_key":
                    total, failed_df, failed = evaluate_primary_key(
                        df, exp["columns"]
                    )

                status = "PASS" if failed == 0 else "FAIL"

                metric = DataQualityMetric(
                    dataset_name=func.__name__,
                    expectation_name=exp["name"],
                    rule=str(exp),
                    total_records=total,
                    failed_records=failed,
                    success_ratio=(total - failed) / total if total else 1.0,
                    status=status,
                )
                session.add(metric)

                # ✅ Persist bad records PER expectation
                if failed > 0 and failed_df is not None:
                    persist_bad_records(
                        df=failed_df,
                        dataset=func.__name__,
                        expectation_name=exp["name"],
                    )

                # ✅ Fail-fast tracking
                if failed > 0 and exp.get("severity", "ERROR") == "ERROR":
                    errors.append(exp["name"])

            session.commit()
            session.close()

            if errors and fail_fast:
                raise RuntimeError(
                    f"Data quality failed for {func.__name__}: {errors}"
                )

            return df

        return wrapper
    return decorator

