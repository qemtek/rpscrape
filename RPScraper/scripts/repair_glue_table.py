#!/usr/bin/env python3
"""
Runs MSCK REPAIR TABLE on the specified AWS Glue table using Athena.
"""

import time
import logging


from settings import (
    boto3_session,
    AWS_GLUE_DB,
    AWS_RPSCRAPE_TABLE_NAME,
    S3_BUCKET
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration for Athena query
ATHENA_QUERY_OUTPUT_LOCATION = f"s3://{S3_BUCKET}/athena_query_results/msck_repair/"
QUERY_REPAIR_TABLE = f"MSCK REPAIR TABLE {AWS_RPSCRAPE_TABLE_NAME};"


def run_msck_repair():
    """Executes MSCK REPAIR TABLE query on Athena and waits for completion."""
    logger.info(f"Target Glue Database: {AWS_GLUE_DB}")
    logger.info(f"Target Glue Table: {AWS_RPSCRAPE_TABLE_NAME}")
    logger.info(f"Athena Query Output Location: {ATHENA_QUERY_OUTPUT_LOCATION}")
    logger.info(f"Executing query: {QUERY_REPAIR_TABLE}")

    athena_client = boto3_session.client('athena')

    response = athena_client.start_query_execution(
        QueryString=QUERY_REPAIR_TABLE,
        QueryExecutionContext={
            'Database': AWS_GLUE_DB
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_QUERY_OUTPUT_LOCATION,
        }
    )
    query_execution_id = response['QueryExecutionId']
    logger.info(f"Started MSCK REPAIR TABLE query. Execution ID: {query_execution_id}")

    # Poll for query completion
    while True:
        try:
            query_status_response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = query_status_response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                logger.info(f"Query finished with status: {status}")
                if status == 'FAILED':
                    error_message = query_status_response['QueryExecution']['Status'].get('StateChangeReason', 'No specific error message provided.')
                    logger.error(f"Query failed. Reason: {error_message}")
                return status == 'SUCCEEDED'
            else:
                logger.info(f"Query status: {status}. Waiting...")
                time.sleep(5)  # Wait for 5 seconds before checking again
        except Exception as e:
            logger.error(f"Error checking query status for {query_execution_id}. Error: {e}")
            return False

if __name__ == "__main__":
    logger.info("Starting MSCK REPAIR TABLE script...")
    success = run_msck_repair()
    if success:
        logger.info("MSCK REPAIR TABLE completed successfully.")
    else:
        logger.error("MSCK REPAIR TABLE did not complete successfully.")
