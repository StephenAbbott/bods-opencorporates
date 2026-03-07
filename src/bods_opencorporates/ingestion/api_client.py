"""OpenCorporates REST API client.

Fetches company, officer, and relationship data from the OpenCorporates
API (v0.4) and returns typed dataclass instances.

See: https://api.opencorporates.com/documentation/API-Reference
"""

from __future__ import annotations

import logging
import time
from typing import Iterator

import requests

from bods_opencorporates.ingestion.models import OCCompany, OCOfficer

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.opencorporates.com/v0.4"
DEFAULT_RATE_LIMIT_DELAY = 1.0  # seconds between requests
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0


class OpenCorporatesAPIError(Exception):
    """Raised when the OpenCorporates API returns an error."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


class OpenCorporatesAPI:
    """Client for the OpenCorporates REST API.

    Handles authentication, pagination, rate limiting, and retries.

    Usage:
        api = OpenCorporatesAPI(api_token="your_token")
        company = api.get_company("gb", "00445790")
        officers = list(api.get_officers("gb", "00445790"))
    """

    def __init__(
        self,
        api_token: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY,
    ):
        self.api_token = api_token
        self.base_url = base_url.rstrip("/")
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self._last_request_time: float = 0

        if api_token:
            self.session.params = {"api_token": api_token}

    def get_company(self, jurisdiction: str, company_number: str) -> OCCompany:
        """Fetch a single company by jurisdiction and company number.

        Args:
            jurisdiction: OpenCorporates jurisdiction code (e.g., 'gb', 'us_de').
            company_number: The company's registration number.

        Returns:
            An OCCompany instance.

        Raises:
            OpenCorporatesAPIError: If the API returns an error.
        """
        url = f"{self.base_url}/companies/{jurisdiction}/{company_number}"
        data = self._request(url)
        return OCCompany.from_api_dict(data["results"]["company"])

    def search_companies(
        self,
        query: str,
        jurisdiction: str | None = None,
        per_page: int = 100,
    ) -> Iterator[OCCompany]:
        """Search for companies matching a query.

        Args:
            query: Search string.
            jurisdiction: Optional jurisdiction filter.
            per_page: Results per page (max 100).

        Yields:
            OCCompany instances.
        """
        params: dict = {"q": query, "per_page": min(per_page, 100)}
        if jurisdiction:
            params["jurisdiction_code"] = jurisdiction

        url = f"{self.base_url}/companies/search"
        yield from self._paginate_companies(url, params)

    def get_officers(
        self,
        jurisdiction: str,
        company_number: str,
    ) -> Iterator[OCOfficer]:
        """Fetch all officers for a company.

        Args:
            jurisdiction: OpenCorporates jurisdiction code.
            company_number: The company's registration number.

        Yields:
            OCOfficer instances.
        """
        url = f"{self.base_url}/companies/{jurisdiction}/{company_number}/officers"
        page = 1

        while True:
            params = {"page": page}
            try:
                data = self._request(url, params=params)
            except OpenCorporatesAPIError as e:
                if e.status_code == 404:
                    return
                raise

            officers = data.get("results", {}).get("officers", [])
            if not officers:
                break

            for officer_data in officers:
                try:
                    yield OCOfficer.from_api_dict(
                        officer_data, jurisdiction, company_number
                    )
                except (KeyError, ValueError) as e:
                    logger.warning("Skipping invalid officer data: %s", e)

            # Check for next page
            total_pages = data.get("results", {}).get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1

    def search_officers(
        self,
        query: str,
        jurisdiction: str | None = None,
        per_page: int = 100,
    ) -> Iterator[OCOfficer]:
        """Search for officers matching a query.

        Args:
            query: Search string.
            jurisdiction: Optional jurisdiction filter.
            per_page: Results per page.

        Yields:
            OCOfficer instances.
        """
        params: dict = {"q": query, "per_page": min(per_page, 100)}
        if jurisdiction:
            params["jurisdiction_code"] = jurisdiction

        url = f"{self.base_url}/officers/search"
        page = 1

        while True:
            params["page"] = page
            try:
                data = self._request(url, params=params)
            except OpenCorporatesAPIError as e:
                if e.status_code == 404:
                    return
                raise

            officers = data.get("results", {}).get("officers", [])
            if not officers:
                break

            for officer_data in officers:
                try:
                    # Extract jurisdiction and company number from the officer data
                    officer_obj = officer_data.get("officer", officer_data)
                    company = officer_obj.get("company", {})
                    jur = company.get("jurisdiction_code", "")
                    num = str(company.get("company_number", ""))
                    yield OCOfficer.from_api_dict(officer_data, jur, num)
                except (KeyError, ValueError) as e:
                    logger.warning("Skipping invalid officer data: %s", e)

            total_pages = data.get("results", {}).get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1

    def get_company_network(
        self,
        jurisdiction: str,
        company_number: str,
    ) -> dict:
        """Fetch the corporate network for a company.

        Returns the raw network data dict from the API.
        """
        url = f"{self.base_url}/companies/{jurisdiction}/{company_number}/network"
        data = self._request(url)
        return data.get("results", {})

    def _paginate_companies(
        self, url: str, params: dict
    ) -> Iterator[OCCompany]:
        """Paginate through company search results."""
        page = 1

        while True:
            params["page"] = page
            try:
                data = self._request(url, params=params)
            except OpenCorporatesAPIError as e:
                if e.status_code == 404:
                    return
                raise

            companies = data.get("results", {}).get("companies", [])
            if not companies:
                break

            for company_data in companies:
                try:
                    yield OCCompany.from_api_dict(company_data)
                except (KeyError, ValueError) as e:
                    logger.warning("Skipping invalid company data: %s", e)

            total_pages = data.get("results", {}).get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1

    def _request(self, url: str, params: dict | None = None) -> dict:
        """Make a rate-limited, retrying request to the API.

        Returns:
            The parsed JSON response dict.

        Raises:
            OpenCorporatesAPIError: On HTTP errors after retries exhausted.
        """
        self._rate_limit()

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429:
                    # Rate limited - wait and retry
                    wait = RETRY_BACKOFF ** (attempt + 1)
                    logger.warning(
                        "Rate limited by API, waiting %.1f seconds (attempt %d/%d)",
                        wait,
                        attempt + 1,
                        MAX_RETRIES,
                    )
                    time.sleep(wait)
                    continue

                if response.status_code >= 500:
                    # Server error - retry with backoff
                    wait = RETRY_BACKOFF ** (attempt + 1)
                    logger.warning(
                        "Server error %d, retrying in %.1f seconds (attempt %d/%d)",
                        response.status_code,
                        wait,
                        attempt + 1,
                        MAX_RETRIES,
                    )
                    time.sleep(wait)
                    continue

                # Client error - don't retry
                raise OpenCorporatesAPIError(
                    response.status_code,
                    response.text[:500],
                )

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF ** (attempt + 1)
                    logger.warning(
                        "Request failed (%s), retrying in %.1f seconds",
                        e,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    raise OpenCorporatesAPIError(0, str(e)) from e

        raise OpenCorporatesAPIError(0, "Max retries exhausted")

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        if self.rate_limit_delay <= 0:
            return

        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()
