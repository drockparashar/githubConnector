from typing import Any, Dict, List

from application_sdk.handlers.base import BaseHandler
from application_sdk.observability.logger_adaptor import get_logger
from .client import GitHubClient

logger = get_logger(__name__)


class GitHubHandler(BaseHandler):
    """
    Handler for GitHub metadata extraction logic. It uses the GitHubClient
    to interact with the API and implements the business logic for the app.
    """

    def __init__(self, client: GitHubClient):
        super().__init__(client=client)
        self.client: GitHubClient = client

    async def load(self, config: Dict[str, Any]) -> None:
        """
        CORRECTED: This method is now robust and handles two different payload
        structures sent by the SDK server for the /auth and /check endpoints.
        """
        logger.info("Loading GitHub handler and client...")
        
        credentials_to_load = {}
        # Scenario 1: The config itself is the credentials dict (from /check endpoint)
        if "token" in config:
            credentials_to_load = config
        # Scenario 2: The credentials are in a nested object (from /auth endpoint)
        elif "credentials" in config:
            credentials_to_load = config.get("credentials", {})

        if not credentials_to_load:
            raise ValueError("Credentials payload is empty or malformed.")

        await self.client.load(credentials=credentials_to_load)
        logger.info("GitHub handler and client loaded successfully.")

    async def test_auth(self, **kwargs: Any) -> bool:
        """
        Tests authentication by making a simple call to the GitHub API.
        """
        logger.info("Testing GitHub API authentication...")
        is_authenticated = await self.client.test_authentication()
        if not is_authenticated:
            raise ValueError("Authentication failed. The provided Personal Access Token is invalid or expired.")
        
        return True

    async def preflight_check(self, workflow_args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Verifies the connection and that the target repository owner exists.
        """
        owner = workflow_args.get("metadata", {}).get("owner")
        if not owner:
            raise ValueError("GitHub 'owner' not found in configuration metadata.")
        
        logger.info(f"Performing preflight check for owner: {owner}")
        
        if not await self.client.check_owner_exists(owner):
            raise ConnectionError(f"GitHub user or organization '{owner}' not found.")
        
        logger.info("Preflight check successful.")
        return {"status": "success", "message": f"Successfully connected and found owner '{owner}'."}

    async def fetch_repositories_metadata(self, owner: str) -> List[Dict[str, Any]]:
        """
        Uses the client to get repositories and returns the raw metadata.
        """
        logger.info(f"Fetching repositories for owner: {owner}")
        repos = await self.client.get_repositories(owner=owner)
        logger.info(f"Fetched {len(repos)} repositories.")
        return repos

