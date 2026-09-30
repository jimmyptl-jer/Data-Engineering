"""
Stock Data Pipeline — Entry Point Module.

This module is the entry point for the Bronze → Silver → Gold transform job.
Ingestion (API → Bronze) runs separately in the Lambdas under `ingestion/`.
It can be invoked in two ways:

    1. AWS Lambda:  AWS invokes `lambda_handler(event, context)` on a scheduled
                    cron trigger (defined in template.yaml) or via manual invocation.

    2. Local CLI:   Run directly with `python -m src.stock_pipeline.app` for local
                    development and testing.

All pipeline orchestration logic (transforms, writes) lives in
`pipeline.py`. This file only handles:
    - Logging setup
    - Lambda handler (entry point)
    - Local __main__ execution block

SAM Template Reference:
    Handler: src.stock_pipeline.app.lambda_handler
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

    This function is invoked by the Step Functions workflow in template.yaml
    (default schedule: weekdays at 2:00 PM UTC) after the ingestion Lambdas
    have written Bronze, or via manual trigger.

    The event payload can optionally contain:
        {
            "run_id": "batch_20260930_140000"
        }

    `run_id` selects the Bronze run to process and must match the run_id
    given to the ingestion Lambdas. If omitted, the pipeline falls back to
    `batch_<YYYYmmdd_HHMMSS>` of this execution.

    Args:
        event (dict): Lambda invocation event. May contain a 'run_id' key.
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

    # Bronze run to process, shared with the ingestion Lambdas
    run_id = event.get("run_id") if isinstance(event, dict) else None

    try:
        # Initialize the full pipeline (Spark, Extractors, Loaders, etc.)
        stock_pipeline = StockPipeline()

        # Execute the Medallion transform stages:
        #   1. Processing → Silver (cleaned, validated, enriched DataFrames)
        #   2. Gold Build → Gold  (joined business dataset)
        results = stock_pipeline.run(
            execution_start_time=execution_start_time,
            run_id=run_id,
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