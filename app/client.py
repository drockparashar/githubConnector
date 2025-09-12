from typing import Any, Dict, List, Optional
import httpx

from application_sdk.clients.base import BaseClient
from application_sdk.observability.logger_adaptor import get_logger

logger = get_logger(__name__)


class GitHubClient(BaseClient):
    """
    Client to interact with the GitHub API, handling authentication and requests.
    """
    BASE_URL = "https://api.github.com"

    async def load(self, **kwargs: Any) -> None:
        """
        Loads credentials and sets up authentication headers for GitHub API requests.
        The personal access token is expected in the 'token' key of the credentials.
        """
        credentials = kwargs.get("credentials", {})
        auth_token = credentials.get("token")
        if not auth_token:
            raise ValueError("GitHub personal access token is required in credentials.")

        self.http_headers = {
            "Authorization": f"Bearer {auth_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        logger.info("GitHub client loaded with authentication headers.")

    async def get_paginated_results(self, url: str) -> List[Dict[str, Any]]:
        """
        Fetches all pages for a given GitHub API endpoint.

        Args:
            url: The initial URL to fetch.

        Returns:
            A list containing all items from all pages.
        """
        all_results = []
        while url:
            response = await self.execute_http_get_request(url=url)
            if not response or response.status_code != 200:
                logger.error(f"Failed to fetch data from {url}. Status: {response.status_code if response else 'N/A'}")
                break
            
            all_results.extend(response.json())
            
            # Check for the 'next' link in the Link header for pagination
            url = response.links.get("next", {}).get("url")
        return all_results

    async def get_repositories(self, owner: str) -> List[Dict[str, Any]]:
        """
        Fetches a list of repositories for a given user or organization.

        Args:
            owner: The GitHub username or organization name.

        Returns:
            A list of repositories.
        """
        url = f"{self.BASE_URL}/users/{owner}/repos"
        return await self.get_paginated_results(url)

    async def test_authentication(self) -> bool:
        """
        Tests the provided credentials by making a simple call to the GitHub API.

        Returns:
            True if authentication is successful, False otherwise.
        """
        response = await self.execute_http_get_request(f"{self.BASE_URL}/user")
        return response is not None and response.status_code == 200

    async def check_owner_exists(self, owner: str) -> bool:
        """
        Checks if a given GitHub user or organization exists.

        Args:
            owner: The GitHub username or organization name.

        Returns:
            True if the owner exists, False otherwise.
        """
        response = await self.execute_http_get_request(f"{self.BASE_URL}/users/{owner}")
        return response is not None and response.status_code == 200

