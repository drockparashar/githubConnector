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
        handler = cast(GitHubHandler, state.handler)
        if not handler:
            raise ValueError("Handler is not initialized in the activity state.")

        # --- START FIX: Add cleanup step ---
        # Manually clean up the output directory from any previous runs to prevent conflicts.
        import shutil
        output_path = workflow_args.get("output_path")
        if output_path and os.path.exists(output_path):
            shutil.rmtree(output_path)
            logger.info(f"Cleaned up previous run data from: {output_path}")
        # --- END FIX ---
        
        raw_data_list: List[Dict[str, Any]] = await handler.fetch_repositories_metadata(
            owner=workflow_args.get("metadata", {}).get("owner")
        )

        if not raw_data_list:
            logger.warning("No repositories found, skipping file write.")
            return None

        logger.info(f"Processing {len(raw_data_list)} repositories")

        flattened_data: List[Dict[str, Any]] = []
        for i, repo in enumerate(raw_data_list):
            flat_repo = repo.copy()
            if 'owner' in flat_repo and isinstance(flat_repo['owner'], dict):
                owner_dict = flat_repo['owner']
                flat_repo['owner_login'] = owner_dict['login'] if 'login' in owner_dict else None
                del flat_repo['owner']
            else:
                flat_repo['owner_login'] = None
                if 'owner' in flat_repo:
                    del flat_repo['owner']
            flattened_data.append(flat_repo)
            
            # Debug: log first few items
            if i < 3:
                logger.info(f"Processed repo {i}: {flat_repo.get('name', 'unnamed')} with keys: {list(flat_repo.keys())}")
        
        logger.info(f"Flattened {len(flattened_data)} repositories")

        raw_dataframe = daft.from_pylist(flattened_data)
        
        # Debug: Check if dataframe has data
        logger.info(f"Created dataframe with {raw_dataframe.count_rows()} rows and {len(raw_dataframe.columns)} columns")
        logger.info(f"DataFrame columns: {raw_dataframe.columns}")
        
        if not output_path:
            raise ValueError("output_path is required in workflow_args")
        
        local_raw_data_path = os.path.join(output_path, "raw", "REPOSITORY")
        os.makedirs(local_raw_data_path, exist_ok=True)

        try:
            # Use a specific file path instead of root_dir to ensure files are written
            parquet_file_path = os.path.join(local_raw_data_path, "repositories.parquet")
            logger.info(f"About to write parquet file to: {parquet_file_path}")
            logger.info(f"Dataframe type: {type(raw_dataframe)}, has {raw_dataframe.count_rows()} rows")
            
            # Skip Daft write completely and use pandas directly for Windows compatibility
            try:
                logger.info("Using pandas approach directly to avoid Windows file locking issues")
                
                # Convert Daft dataframe to pandas
                materialized_df = raw_dataframe.collect()
                pandas_df = materialized_df.to_pandas()
                
                # Write using pandas
                pandas_df.to_parquet(parquet_file_path, engine='pyarrow')
                
                # Verify the file was written successfully
                if os.path.exists(parquet_file_path):
                    file_size = os.path.getsize(parquet_file_path)
                    logger.info(f"Successfully wrote parquet file, size: {file_size} bytes")
                    
                    if file_size == 0:
                        raise ValueError("Parquet file was written but has 0 bytes")
                else:
                    raise FileNotFoundError(f"Parquet file was not created at {parquet_file_path}")
                    
            except Exception as write_error:
                logger.error(f"Failed to write parquet with pandas: {write_error}")
                
                # Last resort: try different file path in case of path issues
                logger.info("Attempting alternative file path approach")
                
                alternative_path = os.path.join(local_raw_data_path, f"repositories_{get_workflow_run_id()}.parquet")
                logger.info(f"Trying alternative path: {alternative_path}")
                
                # Ensure we have fresh dataframe conversion
                materialized_df = raw_dataframe.collect()
                pandas_df = materialized_df.to_pandas()
                pandas_df.to_parquet(alternative_path, engine='pyarrow')
                
                if os.path.exists(alternative_path):
                    final_size = os.path.getsize(alternative_path)
                    logger.info(f"Successfully wrote parquet using alternative path, size: {final_size} bytes")
                    # Update the path for later use
                    parquet_file_path = alternative_path
                else:
                    raise Exception("Failed to write parquet file with any approach")
            
            stats = ActivityStatistics(
                total_record_count=raw_dataframe.count_rows(),
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
            
            # Check what files exist in the directory for debugging
            if os.path.exists(local_parquet_path):
                files_in_dir = os.listdir(local_parquet_path)
                logger.info(f"Files in directory {local_parquet_path}: {files_in_dir}")
            else:
                raise FileNotFoundError(f"Directory {local_parquet_path} does not exist")
            
            # Look for the specific parquet file we created
            import glob
            parquet_file_path = os.path.join(local_parquet_path, "repositories.parquet")
            if os.path.exists(parquet_file_path):
                logger.info(f"Found specific parquet file: {parquet_file_path}")
                raw_dataframe = daft.read_parquet(parquet_file_path)
            else:
                # Fallback: try to find any parquet files
                recursive_pattern = os.path.join(local_parquet_path, "**", "*.parquet")
                parquet_files = glob.glob(recursive_pattern, recursive=True)
                logger.info(f"Found parquet files with recursive search: {parquet_files}")
                
                if not parquet_files:
                    raise FileNotFoundError(f"No parquet files found in {local_parquet_path}")
                
                # Read the files
                raw_dataframe = daft.read_parquet(parquet_files)

            # Check if dataframe is empty using count_rows for unmaterialized dataframes
            if raw_dataframe.count_rows() == 0:
                logger.warning("Raw data dataframe is empty, skipping transformation.")
                return None
            
            output_path = workflow_args.get("output_path")
            output_prefix = workflow_args.get("output_prefix")

            # Extract workflow args to avoid duplicate keyword arguments
            # Ensure connection fields have default values to prevent None errors
            connection_name = workflow_args.get("connection_name") or "github-default"
            connection_qualified_name = workflow_args.get("connection_qualified_name") or "default/github/connection"
            
            transform_kwargs = {
                "typename": "REPOSITORY",
                "dataframe": raw_dataframe,
                "workflow_id": workflow_args.get("workflow_id", ""),
                "workflow_run_id": workflow_args.get("workflow_run_id", ""),
                "connection": {
                    "connection_name": connection_name,
                    "connection_qualified_name": connection_qualified_name
                }
            }

            transformed_dataframe = state.transformer.transform_metadata(**transform_kwargs)

            if not transformed_dataframe or transformed_dataframe.count_rows() == 0:
                logger.warning("Transformation resulted in an empty dataframe.")
                return None

            transformed_output = JsonOutput(
                output_prefix=output_prefix,
                output_path=output_path,
                output_suffix="transformed",
                typename="REPOSITORY",
            )

            logger.info(f"About to write transformed data with output_prefix: {output_prefix}, output_path: {output_path}")
            await transformed_output.write_daft_dataframe(transformed_dataframe)
            
            # Add debugging to check what files were actually created
            if output_path:
                local_json_files = glob.glob(os.path.join(output_path, "**", "*.json"), recursive=True)
                logger.info(f"Local JSON files created: {local_json_files}")
                
                # Check object store path
                object_store_prefix = get_object_store_prefix(output_path)
                logger.info(f"Object store prefix should be: {object_store_prefix}")
            else:
                logger.warning("output_path is None, cannot check created files")
            
            logger.info(f"Successfully wrote transformed data to object store")
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
        output_path = workflow_args["output_path"]
        
        # The JsonOutput uses get_object_store_prefix to determine where files are uploaded
        from application_sdk.activities.common.utils import get_object_store_prefix
        
        if output_path:
            migration_prefix = get_object_store_prefix(output_path)
            logger.info(f"Starting migration from object store with prefix: {migration_prefix}")
            logger.info(f"Original output_path was: {output_path}")
        else:
            logger.error("output_path is None in upload_to_atlan")
            raise ValueError("output_path is required for upload_to_atlan")
        
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

