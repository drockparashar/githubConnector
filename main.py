import asyncio

from application_sdk.application import BaseApplication # CORRECTED IMPORT
from application_sdk.common.error_codes import ApiError
from application_sdk.observability.logger_adaptor import get_logger

# Import the custom classes from the 'app' module
from app.activities import SourceSenseActivities
from app.client import GitHubClient
from app.handler import GitHubHandler
from app.transformer import GitHubTransformer
from app.workflow import SourceSenseWorkflow

logger = get_logger(__name__)

async def main():
    """
    Main entry point for the SourceSense GitHub metadata extraction application.
    This function initializes and runs the application, setting up the workflow,
    worker, and server components.
    """
    try:
        application = BaseApplication(
            name="sourcesense",
            client_class=GitHubClient,
            handler_class=GitHubHandler,
        )

        # Set up the workflow and its associated activities
        await application.setup_workflow(
            workflow_and_activities_classes=[
                (SourceSenseWorkflow, SourceSenseActivities)
            ],
        )

        # Start the Temporal worker
        await application.start_worker()

        # Set up and start the FastAPI server to handle UI and triggers
        await application.setup_server(
            workflow_class=SourceSenseWorkflow,
        )
        await application.start_server()

    except ApiError as e:
        logger.error("Failed to start the application server.", exc_info=True)
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())

