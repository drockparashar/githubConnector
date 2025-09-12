import os
import asyncio
from typing import Any, Dict, List, Optional, cast
import daft

from application_sdk.activities.metadata_extraction.base import (
    BaseMetadataExtractionActivities,
    BaseMetadataExtractionActivitiesState,
)
from application_sdk.activities.common.models import ActivityStatistics
from application_sdk.activities.common.utils import (
    build_output_path,
    get_workflow_id,
    get_workflow_run_id,
    get_object_store_prefix,
)
from application_sdk.common.error_codes import ActivityError
from application_sdk.constants import TEMPORARY_PATH
from application_sdk.inputs.parquet import ParquetInput
from application_sdk.outputs.json import JsonOutput
from application_sdk.services import AtlanStorage, ObjectStore, StateStore, StateType
from temporalio import activity

from .client import GitHubClient
from .handler import GitHubHandler
from .transformer import GitHubTransformer

from application_sdk.observability.logger_adaptor import get_logger

logger = get_logger(__name__)
activity.logger = logger


class SourceSenseActivities(BaseMetadataExtractionActivities):
    """
    Activities for extracting metadata from the GitHub API.
    """

    def __init__(self):
        super().__init__(
            client_class=GitHubClient,
            handler_class=GitHubHandler,
            transformer_class=GitHubTransformer,
        )

    @activity.defn
    async def get_workflow_args(
        self, workflow_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Overrides the base SDK activity to add a retry loop to resolve a race condition.
        """
        workflow_id = workflow_config.get("workflow_id", get_workflow_id())
        if not workflow_id:
            raise ValueError("workflow_id is required in workflow_config")

        max_retries = 5
        retry_delay_seconds = 2

        for attempt in range(max_retries):
            try:
                workflow_args = await StateStore.get_state(workflow_id, StateType.WORKFLOWS)
                
                workflow_args["output_prefix"] = workflow_args.get("output_prefix", TEMPORARY_PATH)
                workflow_args["output_path"] = os.path.join(
                    workflow_args["output_prefix"], build_output_path()
                )
                workflow_args["workflow_id"] = workflow_id
                workflow_args["workflow_run_id"] = get_workflow_run_id()
                logger.info(f"Successfully retrieved workflow arguments on attempt {attempt + 1}.")
                return workflow_args
            
            except Exception as e:
                if "not found" in str(e).lower():
                    logger.warning(
                        f"Config file not yet available on attempt {attempt + 1}/{max_retries}. Retrying..."
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay_seconds)
                    else:
                        logger.error("Failed to retrieve workflow arguments after all retries.")
                        raise
                else:
                    logger.error(f"An unexpected error occurred while getting workflow arguments: {e}")
                    raise
        
        raise Exception("Failed to retrieve workflow arguments after all retries.")

    @activity.defn
    async def fetch_repositories(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Fetches metadata, writes it to a LOCAL Parquet file, and returns the path.
        """
        state = cast(BaseMetadataExtractionActivitiesState, await self._get_state(workflow_args))
        handler: GitHubHandler = state.handler

        # --- START FIX: Add cleanup step ---
        # Manually clean up the output directory from any previous runs to prevent conflicts.
        output_path = workflow_args.get("output_path")
        if os.path.exists(output_path):
            import shutil
            shutil.rmtree(output_path)
            logger.info(f"Cleaned up previous run data from: {output_path}")
        # --- END FIX ---
        
        raw_data_list: List[Dict[str, Any]] = await handler.fetch_repositories_metadata(
            owner=workflow_args.get("metadata", {}).get("owner")
        )

        if not raw_data_list:
            logger.warning("No repositories found, skipping file write.")
            return None

        flattened_data = []
        for repo in raw_data_list:
            flat_repo = repo.copy()
            if 'owner' in flat_repo and isinstance(flat_repo['owner'], dict):
                flat_repo['owner_login'] = flat_repo['owner'].get('login')
            del flat_repo['owner']
            flattened_data.append(flat_repo)

        raw_dataframe = daft.from_pylist(flattened_data)
        
        local_raw_data_path = os.path.join(output_path, "raw", "REPOSITORY")
        os.makedirs(local_raw_data_path, exist_ok=True)

        try:
            raw_dataframe.write_parquet(root_dir=local_raw_data_path, write_mode="overwrite")
            logger.info(f"Successfully wrote Parquet files to local temp directory: {local_raw_data_path}")
            
            stats = ActivityStatistics(
                total_record_count=len(flattened_data),
                chunk_count=1,
                typename="REPOSITORY",
            )
            
            return {"stats": stats, "local_path": local_raw_data_path}

        except Exception as e:
            logger.error(f"Error during fetch and save process: {e}", exc_info=True)
            raise

    @activity.defn
    async def transform_data(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """
        Reads raw Parquet data from a LOCAL path, transforms it, and writes JSON back to object store.
        """
        state = cast(BaseMetadataExtractionActivitiesState, await self._get_state(workflow_args))
        if not state.transformer:
            raise ValueError("Transformer is not initialized in the activity state.")

        try: # --- START CHANGE: Add try block to reveal hidden errors ---
            local_parquet_path = workflow_args.get("local_parquet_path")
            if not local_parquet_path:
                raise ValueError("Local Parquet path was not provided to transform_data.")

            logger.info(f"Reading raw data directly from local path: {local_parquet_path}")
            # Create the glob path to find all .parquet files in the directory
            glob_path = os.path.join(local_parquet_path, "*.parquet")
            
            # IMPORTANT: Normalize the path to use only forward slashes for cross-platform compatibility
            normalized_glob_path = glob_path.replace("\\", "/")

            raw_dataframe = daft.read_parquet(normalized_glob_path)

            if raw_dataframe.is_empty():
                logger.warning("Raw data dataframe is empty, skipping transformation.")
                return None
            
            output_path = workflow_args.get("output_path")
            output_prefix = workflow_args.get("output_prefix")

            transformed_dataframe = state.transformer.transform_metadata(
                dataframe=raw_dataframe, **workflow_args
            )

            if not transformed_dataframe or transformed_dataframe.is_empty():
                logger.warning("Transformation resulted in an empty dataframe.")
                return None

            transformed_output = JsonOutput(
                output_prefix=output_prefix,
                output_path=output_path,
                output_suffix="transformed",
                typename="REPOSITORY",
            )

            await transformed_output.write_daft_dataframe(transformed_dataframe)
            
            logger.info(f"Successfully wrote transformed data to object store at {transformed_output.get_full_path()}")
            return await transformed_output.get_statistics()

        except Exception as e: # --- START CHANGE: Add except block to log the error ---
            logger.error(f"An error occurred during the transform_data activity: {e}", exc_info=True)
            raise # Re-raise the exception so the activity still fails
    
    @activity.defn
    async def upload_to_atlan(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """
        Uploads the transformed data from the object store to Atlan.
        """
        migration_prefix = workflow_args["output_path"]
        logger.info(f"Starting migration from object store with prefix: {migration_prefix}")
        
        upload_stats = await AtlanStorage.migrate_from_objectstore_to_atlan(prefix=migration_prefix)

        logger.info(
            f"Atlan upload completed: {upload_stats.migrated_files} files uploaded, "
            f"{upload_stats.failed_migrations} failed"
        )

        if upload_stats.failures:
            logger.error(f"Upload failed with {len(upload_stats.failures)} errors")
            for failure in upload_stats.failures:
                logger.error(f"Upload error: {failure}")
            raise ActivityError(
                f"{ActivityError.ATLAN_UPLOAD_ERROR}: Atlan upload failed with {len(upload_stats.failures)} errors."
            )

        return ActivityStatistics(
            total_record_count=upload_stats.migrated_files,
            chunk_count=upload_stats.total_files,
            typename="atlan-upload-completed",
        )

