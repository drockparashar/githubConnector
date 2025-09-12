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
        Fetches metadata, uploads it to the object store, and returns the object store path.
        """
        logger.info("--- Starting fetch_repositories activity ---")
        state = cast(BaseMetadataExtractionActivitiesState, await self._get_state(workflow_args))
        handler: GitHubHandler = state.handler

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
        
        output_path = workflow_args.get("output_path")
        logger.info(f"[FETCH] Workflow 'output_path': {output_path}")
        
        local_raw_data_path = os.path.join(output_path, "raw", "REPOSITORY")
        logger.info(f"[FETCH] Writing Parquet data to LOCAL path: {local_raw_data_path}")
        os.makedirs(local_raw_data_path, exist_ok=True)

        try:
            raw_dataframe.write_parquet(root_dir=local_raw_data_path, write_mode="overwrite")
            
            await asyncio.sleep(1)

            object_store_destination = get_object_store_prefix(local_raw_data_path)
            logger.info(f"[FETCH] Calculated OBJECT STORE destination path: {object_store_destination}")
            
            await ObjectStore.upload_prefix(
                source=local_raw_data_path,
                destination=object_store_destination
            )
            logger.info(f"[FETCH] Successfully uploaded local directory to object store.")

            stats = ActivityStatistics(
                total_record_count=len(flattened_data),
                chunk_count=1,
                typename="REPOSITORY",
            )
            
            logger.info(f"[FETCH] Returning object_store_path to workflow: {object_store_destination}")
            logger.info("--- Finished fetch_repositories activity ---")
            return {"stats": stats, "object_store_path": object_store_destination}

        except Exception as e:
            logger.error(f"Error during fetch and save process: {e}", exc_info=True)
            raise

    @activity.defn
    async def transform_data(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """
        Reads raw Parquet data from the object store, transforms it, and writes JSON back.
        """
        logger.info("--- Starting transform_data activity ---")
        state = cast(BaseMetadataExtractionActivitiesState, await self._get_state(workflow_args))
        if not state.transformer:
            raise ValueError("Transformer is not initialized in the activity state.")

        object_store_path = workflow_args.get("object_store_path")
        logger.info(f"[TRANSFORM] Received OBJECT STORE path to read from: {object_store_path}")
        if not object_store_path:
            raise ValueError("Object store path was not provided to transform_data.")
            
        output_prefix = workflow_args.get("output_prefix")
        logger.info(f"[TRANSFORM] Received 'output_prefix' for local download: {output_prefix}")

        logger.info(f"[TRANSFORM] Initializing ParquetInput with path='{object_store_path}' and input_prefix='{output_prefix}'")
        raw_input = ParquetInput(path=object_store_path, input_prefix=output_prefix)

        raw_dataframe = await raw_input.get_daft_dataframe()

        if raw_dataframe.is_empty():
            logger.warning("Raw data dataframe from object store is empty, skipping transformation.")
            return None

        output_path = workflow_args.get("output_path")

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
        logger.info("--- Finished transform_data activity ---")
        return await transformed_output.get_statistics()
    
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

