"""
Stock Data Pipeline — Entry Point Module.

This module is the single entry point for the entire stock data pipeline.
It can be invoked in two ways:

    1. AWS Lambda:  AWS invokes `lambda_handler(event, context)` on a scheduled
                    cron trigger (defined in template.yaml) or via manual invocation.

    2. Local CLI:   Run directly with `python -m src.stock_pipeline.app` for local
                    development and testing.

All pipeline orchestration logic (ingestion, transforms, writes) lives in
`pipeline.py`. This file only handles:
    - Logging setup
    - Lambda handler (entry point)
    - Local __main__ execution block

SAM Template Reference:
    Handler: src.stock_pipeline.app.lambda_handler   (template.yaml line 198)
"""

import logging
from datetime import datetime, timezone

from .pipeline import StockPipeline


# ============================================================
# LOGGING CONFIGURATION
#
# Configures the root logger for the entire pipeline.
# Format: "INFO src.stock_pipeline.app [LAMBDA] Starting..."
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# AWS LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    AWS Lambda handler — the main entry point when deployed to AWS.

    This function is invoked by AWS Lambda on a cron schedule
    (default: weekdays at 2:00 PM UTC) or via manual trigger.

    The event payload can optionally contain:
        {
            "stock_symbols": ["IBM", "AAPL", "GOOG"]
        }

    If no symbols are specified, defaults to ["IBM"].

    Args:
        event (dict): Lambda invocation event. May contain 'stock_symbols' key.
        context (LambdaContext): AWS Lambda runtime context (timeout, memory, etc.).

    Returns:
        dict: Response with statusCode, body message, and ingestion results.

    Raises:
        Exception: Re-raised if pipeline execution fails (triggers Lambda retry/alarm).
    """
    # Record the execution start time in UTC for duration tracking
    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA] Execution triggered at %s",
        execution_start_time.isoformat(),
    )

    # Extract stock symbols from the event payload, or use default
    is_dict = isinstance(event, dict)
    stock_symbols = event.get("stock_symbols", ["IBM"]) if is_dict else ["IBM"]

    try:
        # Initialize the full pipeline (Spark, Extractors, Loaders, etc.)
        stock_pipeline = StockPipeline()

        # Execute the end-to-end Medallion ETL pipeline:
        #   1. Ingestion  → Bronze (raw JSON from APIs to S3)
        #   2. Processing → Silver (cleaned, validated, enriched DataFrames)
        #   3. Gold Build → Gold  (joined business dataset)
        results = stock_pipeline.run(
            execution_start_time=execution_start_time,
            stock_symbols=stock_symbols,
        )

        # Calculate and log total execution duration
        execution_end_time = datetime.now(timezone.utc)
        total_duration = (execution_end_time - execution_start_time).total_seconds()

        logger.info(
            "[LAMBDA] Execution completed successfully in %.2f seconds.",
            total_duration,
        )

        return {
            "statusCode": 200,
            "body": "Stock pipeline executed successfully.",
            "results": results,
        }

    except Exception as e:
        # Log failure with duration, then re-raise to trigger Lambda error alarm
        execution_end_time = datetime.now(timezone.utc)
        total_duration = (execution_end_time - execution_start_time).total_seconds()

        logger.exception(
            "[LAMBDA] Execution failed after %.2f seconds: %s",
            total_duration,
            e,
        )

        # Re-raise so AWS Lambda marks the invocation as FAILED
        # This triggers the CloudWatch error alarm defined in template.yaml
        raise


# ============================================================
# LOCAL EXECUTION ENTRY POINT
#
# Usage:
#   python -m src.stock_pipeline.app
#
# This simulates a Lambda invocation with an empty event/context
# for local development and testing.
# ============================================================

if __name__ == "__main__":
    logger.info("[LOCAL] Executing Stock Data Pipeline locally.")

    try:
        # Simulate a Lambda invocation with empty event and context
        lambda_handler({}, {})
        logger.info("[LOCAL] Execution completed successfully.")

    except Exception as e:
        logger.exception("[LOCAL] Execution failed: %s", e)
        raise