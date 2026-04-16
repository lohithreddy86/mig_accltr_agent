"""
Status Tracking Decorator for PySpark Procedures

Replaces the repeated PL/SQL pattern of:
  INSERT INTO <status_table> (Name, Dwld_Date, Start_Date) VALUES (...);
  COMMIT;
  ... procedure body ...
  UPDATE <status_table> SET Status = 'Y', End_Date = ... WHERE ...;
  COMMIT;
  EXCEPTION
    ROLLBACK;
    UPDATE <status_table> SET Status = 'N', Error = ... WHERE ...;
    COMMIT;

The status table name is detected from the input and parameterized in
the generated code — not hardcoded to any specific table.
"""


_STATUS_TRACKER_TEMPLATE = '''
import logging
from datetime import datetime
from functools import wraps

logger = logging.getLogger(__name__)

# Status tracking table — detected from the source PL/SQL input.
# Change this if the target Spark/Trino environment uses a different table.
STATUS_TABLE = "{status_table}"


def track_status(procedure_name: str):
    """Decorator that wraps a PySpark procedure with status tracking.

    Inserts a status record at the start, updates it to 'Y' on success
    or 'N' with error message on failure.

    Args:
        procedure_name: The name to record in the status table
    """
    def decorator(func):
        @wraps(func)
        def wrapper(spark, *args, **kwargs):
            now = datetime.now()
            dwld_date = now.replace(second=0, microsecond=0)  # TRUNC(SYSDATE, 'MI')

            # Insert start status
            try:
                spark.sql(f"""
                    INSERT INTO {{STATUS_TABLE}}
                    (name, dwld_date, start_date)
                    VALUES ('{{procedure_name}}',
                            TIMESTAMP '{{dwld_date.strftime("%Y-%m-%d %H:%M:%S")}}',
                            CURRENT_TIMESTAMP())
                """)
                logger.info(f"[{{procedure_name}}] Started")
            except Exception as e:
                logger.warning(f"[{{procedure_name}}] Could not insert start status: {{e}}")

            # Execute procedure
            try:
                result = func(spark, *args, **kwargs)

                # Update success status
                spark.sql(f"""
                    UPDATE {{STATUS_TABLE}}
                    SET status = 'Y', end_date = CURRENT_TIMESTAMP()
                    WHERE name = '{{procedure_name}}'
                      AND CAST(dwld_date AS DATE) = CURRENT_DATE()
                      AND status IS NULL
                """)
                logger.info(f"[{{procedure_name}}] Completed successfully")
                return result

            except Exception as e:
                error_msg = str(e).replace("'", "''")[:4000]
                try:
                    spark.sql(f"""
                        UPDATE {{STATUS_TABLE}}
                        SET status = 'N',
                            end_date = CURRENT_TIMESTAMP(),
                            error = '{{error_msg}}'
                        WHERE name = '{{procedure_name}}'
                          AND CAST(dwld_date AS DATE) = CURRENT_DATE()
                          AND status IS NULL
                    """)
                except Exception:
                    pass
                logger.error(f"[{{procedure_name}}] Failed: {{e}}")
                raise

        return wrapper
    return decorator
'''


def generate_status_tracker(status_table: str = "status_tracking") -> str:
    """Return the status tracking decorator code with the table name filled in.

    Args:
        status_table: The fully-qualified status table name detected from input.
    """
    return _STATUS_TRACKER_TEMPLATE.format(status_table=status_table)


def generate_procedure_wrapper(procedure_name: str, status_name: str,
                                body_code: str, has_params: bool = False,
                                params: list[str] = None) -> str:
    """Generate a complete PySpark function with status tracking.

    Args:
        procedure_name: Python function name
        status_name: Status name recorded in archive_status table
        body_code: The translated procedure body
        has_params: Whether the function takes parameters beyond spark
        params: List of parameter names

    Returns:
        Complete Python function string
    """
    params_str = ', '.join(params) if params else ''
    func_params = f"spark, {params_str}" if params_str else "spark"

    return f'''
@track_status("{status_name}")
def {procedure_name}({func_params}):
    """Converted from Oracle PL/SQL procedure {procedure_name}."""
{_indent(body_code, 4)}
'''


def _indent(text: str, spaces: int) -> str:
    """Indent all lines of text by the specified number of spaces."""
    prefix = ' ' * spaces
    lines = text.split('\n')
    return '\n'.join(prefix + line if line.strip() else '' for line in lines)
