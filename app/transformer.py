from typing import Any, Dict, Optional, Type

from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.transformers.atlas import AtlasTransformer
from application_sdk.transformers.common.utils import build_atlas_qualified_name

logger = get_logger(__name__)


class GitHubRepository:
    """
    Represents a GitHub Repository entity in Atlan.

    This helper class handles the transformation of a raw GitHub repository dictionary
    into the correct Atlan entity format, following the pattern from the sample apps.
    It does not use or return any `pyatlan` asset objects directly.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforms a raw GitHub repository dictionary into Atlan entity attributes.

        Args:
            obj: A dictionary containing the raw GitHub repository metadata.

        Returns:
            A dictionary containing the transformed 'attributes' and 'custom_attributes'.
        """
        connection_qualified_name = obj.get("connection_qualified_name", "")

        # Clean text fields to remove problematic Unicode characters
        def clean_text(text):
            if not text:
                return text
            if isinstance(text, str):
                # Replace problematic Unicode characters with safe alternatives
                return text.encode('ascii', 'ignore').decode('ascii')
            return text

        # Attributes that map to the generic 'Resource' asset type in Atlan
        attributes = {
            "name": clean_text(obj.get("name")),
            "qualifiedName": build_atlas_qualified_name(
                connection_qualified_name, obj.get("full_name", "")
            ),
            "connectionQualifiedName": connection_qualified_name,
            "description": clean_text(obj.get("description")),
            "sourceUrl": obj.get("html_url"),
        }

        # GitHub-specific fields that will be stored as custom metadata in Atlan
        custom_attributes = {
            "github_owner": obj.get("owner", {}).get("login"),
            "github_is_private": obj.get("private", False),
            "github_is_fork": obj.get("fork", False),
            "github_stargazers_count": obj.get("stargazers_count", 0),
            "github_watchers_count": obj.get("watchers_count", 0),
            "github_forks_count": obj.get("forks_count", 0),
            "github_open_issues_count": obj.get("open_issues_count", 0),
            "github_language": clean_text(obj.get("language")),
        }

        return {
            "attributes": attributes,
            "custom_attributes": custom_attributes,
        }


class GitHubTransformer(AtlasTransformer):
    """
    A transformer for converting raw GitHub API data into Atlan entities.
    """

    def __init__(self, connector_name: str, tenant_id: str, **kwargs: Any):
        """
        Initializes the transformer and registers the custom entity helper class.
        """
        super().__init__(connector_name, tenant_id, **kwargs)

        # Register our custom GitHubRepository helper for the "REPOSITORY" typename.
        self.entity_class_definitions = {"REPOSITORY": GitHubRepository}
        # We set the connector_type to a generic value since 'github' is not a native Atlan type.
        self.connector_type = "api" 

    def transform_row(
        self,
        typename: str,
        data: Dict[str, Any],
        workflow_id: str,
        workflow_run_id: str,
        entity_class_definitions: Optional[Dict[str, Type[Any]]] = None,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]:
        """
        This method is overridden from the SDK's AtlasTransformer to correctly
        process the dictionary from our helper class into the final JSON entity format.
        This implementation directly mirrors the working example from the MySQL connector.
        """
        typename = typename.upper()
        self.entity_class_definitions = (
            entity_class_definitions or self.entity_class_definitions
        )

        connection_qualified_name = kwargs.get("connection_qualified_name", None)
        connection_name = kwargs.get("connection_name", None)

        # Provide default values if connection information is None
        if not connection_qualified_name:
            connection_qualified_name = "default/github/connection"
        if not connection_name:
            connection_name = "github-default"

        data.update(
            {
                "connection_qualified_name": connection_qualified_name,
                "connection_name": connection_name,
            }
        )

        creator = self.entity_class_definitions.get(typename)
        if creator:
            try:
                entity_attributes = creator.get_attributes(data)
                
                # The base AtlasTransformer has a helper to enrich with standard metadata
                enriched_data = self._enrich_entity_with_metadata(
                    workflow_id, workflow_run_id, data
                )

                entity_attributes["attributes"].update(enriched_data["attributes"])
                entity_attributes["custom_attributes"].update(
                    enriched_data["custom_attributes"]
                )

                entity = {
                    # We are creating a generic 'Resource' asset in Atlan
                    "typeName": "Resource",
                    "attributes": entity_attributes["attributes"],
                    "customAttributes": entity_attributes["custom_attributes"],
                    "status": "ACTIVE",
                }

                return entity
            except Exception as e:
                logger.error(
                    "Error transforming {} entity: {}",
                    typename,
                    str(e),
                    extra={"data": data},
                )
                return None
        else:
            logger.error(f"Unknown typename: {typename}")
            return None

