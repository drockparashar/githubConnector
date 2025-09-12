from datetime import timedelta
from typing import Any, Callable, Dict, Sequence

from temporalio import workflow
from application_sdk.workflows import WorkflowInterface
from application_sdk.observability.logger_adaptor import get_logger

from .activities import SourceSenseActivities

logger = get_logger(__name__)
workflow.logger = logger


@workflow.defn
class SourceSenseWorkflow(WorkflowInterface):
    """
    Orchestrates the metadata extraction process for the SourceSense GitHub app.
    """

    activities_cls = SourceSenseActivities

    @staticmethod
    def get_activities(activities: SourceSenseActivities) -> Sequence[Callable[..., Any]]:
        """
        Registers all the activities that are part of this workflow.
        """
        return [
            activities.get_workflow_args,
            activities.preflight_check,
            activities.fetch_repositories,
            activities.transform_data,
            activities.upload_to_atlan,
        ]

    @workflow.run
    async def run(self, workflow_config: Dict[str, Any]) -> None:
        """
        Defines the execution flow for the metadata extraction workflow.
        """
        workflow_args = await workflow.execute_activity_method(
            self.activities_cls.get_workflow_args,
            workflow_config,
            start_to_close_timeout=timedelta(minutes=1),
        )

        logger.info("Starting workflow execution for SourceSense.")

        await workflow.execute_activity_method(
            self.activities_cls.preflight_check,
            args=[workflow_args],
            start_to_close_timeout=timedelta(minutes=2),
        )
        logger.info("Preflight checks passed successfully.")

        # --- START CHANGE ---
        # Capture the result from fetch_repositories, which now contains the local path
        fetch_result = await workflow.execute_activity_method(
            self.activities_cls.fetch_repositories,
            args=[workflow_args],
            start_to_close_timeout=timedelta(minutes=15),
        )
        # --- END CHANGE ---

        if fetch_result:
            repo_stats = fetch_result.get("stats")
            # --- START CHANGE ---
            # Get the local_path from the result
            local_parquet_path = fetch_result.get("local_path")
            logger.info(f"Successfully fetched raw data to local path: {local_parquet_path}")

            # Add the local path to workflow_args for the next step
            workflow_args["local_parquet_path"] = local_parquet_path
            # --- END CHANGE ---

            transform_stats = await workflow.execute_activity_method(
                self.activities_cls.transform_data,
                args=[workflow_args],
                start_to_close_timeout=timedelta(minutes=10),
            )
            logger.info(f"Successfully transformed data: {transform_stats}")
            
            if transform_stats:
                await workflow.execute_activity_method(
                    self.activities_cls.upload_to_atlan,
                    args=[workflow_args],
                    start_to_close_timeout=timedelta(minutes=10),
                )
                logger.info("Upload to Atlan completed successfully.")

        logger.info("Workflow execution finished.")