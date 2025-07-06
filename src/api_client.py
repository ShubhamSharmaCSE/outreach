"""
Asynchronous external API client with retry logic and error handling.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
from structlog import get_logger

from .models import OperationType, ProviderConfig, SyncResult, SyncStatus, RecordData
from .rate_limiter import RateLimiterManager
from .schema_transformer import SchemaTransformer, get_default_mappings

logger = get_logger(__name__)


class APIError(Exception):
    """Base exception for API errors."""
    pass


class RateLimitError(APIError):
    """Raised when rate limit is exceeded."""
    pass


class AuthenticationError(APIError):
    """Raised when authentication fails."""
    pass


class ValidationError(APIError):
    """Raised when data validation fails."""
    pass


class ExternalAPIClient:
    """
    Asynchronous client for external API operations with rate limiting,
    retry logic, and schema transformation.
    """
    
    def __init__(
        self,
        provider_config: ProviderConfig,
        rate_limiter: RateLimiterManager,
        schema_transformer: SchemaTransformer
    ):
        self.config = provider_config
        self.rate_limiter = rate_limiter
        self.schema_transformer = schema_transformer
        self.client: Optional[httpx.AsyncClient] = None
        self._auth_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout_seconds,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
        )
        await self._authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.client:
            await self.client.aclose()
    
    async def _authenticate(self) -> None:
        """Authenticate with the external provider."""
        auth_config = self.config.auth_config
        
        if auth_config.auth_type == "oauth2":
            await self._oauth2_authenticate()
        elif auth_config.auth_type == "api_key":
            self._auth_token = auth_config.credentials.get("api_key")
        elif auth_config.auth_type == "basic":
            # Basic auth handled in request headers
            pass
        else:
            raise AuthenticationError(f"Unsupported auth type: {auth_config.auth_type}")
    
    async def _oauth2_authenticate(self) -> None:
        """Perform OAuth2 authentication."""
        auth_config = self.config.auth_config
        credentials = auth_config.credentials
        
        if not auth_config.token_url:
            raise AuthenticationError("OAuth2 token URL not configured")
        
        token_data = {
            "grant_type": "client_credentials",
            "client_id": credentials.get("client_id"),
            "client_secret": credentials.get("client_secret")
        }
        
        if auth_config.refresh_token:
            token_data["grant_type"] = "refresh_token"
            token_data["refresh_token"] = auth_config.refresh_token
        
        try:
            response = await self.client.post(
                auth_config.token_url,
                data=token_data
            )
            response.raise_for_status()
            
            token_info = response.json()
            self._auth_token = token_info.get("access_token")
            
            # Calculate token expiration
            expires_in = token_info.get("expires_in", 3600)
            self._token_expires_at = datetime.utcnow().timestamp() + expires_in - 300  # 5min buffer
            
            logger.info("OAuth2 authentication successful", provider=self.config.name)
            
        except httpx.HTTPError as e:
            logger.error("OAuth2 authentication failed", provider=self.config.name, error=str(e))
            raise AuthenticationError(f"OAuth2 authentication failed: {e}")
    
    async def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for requests."""
        headers = {}
        auth_config = self.config.auth_config
        
        # Check if token needs refresh
        if (self._token_expires_at and 
            datetime.utcnow().timestamp() > self._token_expires_at):
            await self._authenticate()
        
        if auth_config.auth_type == "oauth2":
            if self._auth_token:
                headers["Authorization"] = f"Bearer {self._auth_token}"
        elif auth_config.auth_type == "api_key":
            # Different providers use different header names
            if self.config.provider_type.value == "salesforce":
                headers["Authorization"] = f"Bearer {self._auth_token}"
            elif self.config.provider_type.value == "hubspot":
                headers["Authorization"] = f"Bearer {self._auth_token}"
            else:
                headers["X-API-Key"] = self._auth_token
        elif auth_config.auth_type == "basic":
            credentials = auth_config.credentials
            username = credentials.get("username")
            password = credentials.get("password")
            if username and password:
                import base64
                auth_string = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers["Authorization"] = f"Basic {auth_string}"
        
        return headers
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        before_sleep=before_sleep_log(logger, "warning")
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        
        # Check rate limit
        if not await self.rate_limiter.can_make_request(self.config.name):
            raise RateLimitError(f"Rate limit exceeded for provider {self.config.name}")
        
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "application/json"
        
        try:
            response = await self.client.request(
                method=method,
                url=endpoint,
                json=data,
                params=params,
                headers=headers
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                logger.warning("Rate limited by provider", provider=self.config.name)
                raise RateLimitError("Rate limited by provider")
            
            # Handle authentication errors
            if response.status_code == 401:
                logger.warning("Authentication failed", provider=self.config.name)
                # Try to re-authenticate once
                await self._authenticate()
                headers = await self._get_auth_headers()
                response = await self.client.request(
                    method=method,
                    url=endpoint,
                    json=data,
                    params=params,
                    headers=headers
                )
            
            response.raise_for_status()
            
            # Handle different response types
            if response.headers.get("content-type", "").startswith("application/json"):
                return response.json()
            else:
                return {"status": "success", "data": response.text}
                
        except httpx.HTTPError as e:
            logger.error(
                "HTTP request failed",
                provider=self.config.name,
                method=method,
                endpoint=endpoint,
                error=str(e)
            )
            raise APIError(f"HTTP request failed: {e}")
    
    async def create_record(self, record_data: RecordData) -> Dict[str, Any]:
        """Create a new record in the external system."""
        
        # Transform data to external format
        mappings = get_default_mappings(self.config.provider_type.value)
        transformed_data = self.schema_transformer.transform_record(
            record_data,
            mappings,
            direction="internal_to_external"
        )
        
        # Provider-specific endpoint logic
        if self.config.provider_type.value == "salesforce":
            endpoint = "/services/data/v52.0/sobjects/Contact"
        elif self.config.provider_type.value == "hubspot":
            endpoint = "/crm/v3/objects/contacts"
        elif self.config.provider_type.value == "pipedrive":
            endpoint = "/v1/persons"
        else:
            endpoint = "/contacts"  # Default endpoint
        
        logger.info(
            "Creating record",
            provider=self.config.name,
            endpoint=endpoint,
            data_fields=len(transformed_data)
        )
        
        return await self._make_request("POST", endpoint, data=transformed_data)
    
    async def read_record(self, record_id: str) -> Dict[str, Any]:
        """Read a record from the external system."""
        
        # Provider-specific endpoint logic
        if self.config.provider_type.value == "salesforce":
            endpoint = f"/services/data/v52.0/sobjects/Contact/{record_id}"
        elif self.config.provider_type.value == "hubspot":
            endpoint = f"/crm/v3/objects/contacts/{record_id}"
        elif self.config.provider_type.value == "pipedrive":
            endpoint = f"/v1/persons/{record_id}"
        else:
            endpoint = f"/contacts/{record_id}"
        
        logger.info("Reading record", provider=self.config.name, record_id=record_id)
        
        return await self._make_request("GET", endpoint)
    
    async def update_record(self, record_id: str, record_data: RecordData) -> Dict[str, Any]:
        """Update an existing record in the external system."""
        
        # Transform data to external format
        mappings = get_default_mappings(self.config.provider_type.value)
        transformed_data = self.schema_transformer.transform_record(
            record_data,
            mappings,
            direction="internal_to_external"
        )
        
        # Provider-specific endpoint logic
        if self.config.provider_type.value == "salesforce":
            endpoint = f"/services/data/v52.0/sobjects/Contact/{record_id}"
            method = "PATCH"
        elif self.config.provider_type.value == "hubspot":
            endpoint = f"/crm/v3/objects/contacts/{record_id}"
            method = "PATCH"
        elif self.config.provider_type.value == "pipedrive":
            endpoint = f"/v1/persons/{record_id}"
            method = "PUT"
        else:
            endpoint = f"/contacts/{record_id}"
            method = "PUT"
        
        logger.info(
            "Updating record",
            provider=self.config.name,
            record_id=record_id,
            method=method,
            data_fields=len(transformed_data)
        )
        
        return await self._make_request(method, endpoint, data=transformed_data)
    
    async def delete_record(self, record_id: str) -> Dict[str, Any]:
        """Delete a record from the external system."""
        
        # Provider-specific endpoint logic
        if self.config.provider_type.value == "salesforce":
            endpoint = f"/services/data/v52.0/sobjects/Contact/{record_id}"
        elif self.config.provider_type.value == "hubspot":
            endpoint = f"/crm/v3/objects/contacts/{record_id}"
        elif self.config.provider_type.value == "pipedrive":
            endpoint = f"/v1/persons/{record_id}"
        else:
            endpoint = f"/contacts/{record_id}"
        
        logger.info("Deleting record", provider=self.config.name, record_id=record_id)
        
        return await self._make_request("DELETE", endpoint)
    
    async def execute_operation(self, operation_type: OperationType, **kwargs) -> Dict[str, Any]:
        """Execute a CRUD operation based on type."""
        
        if operation_type == OperationType.CREATE:
            return await self.create_record(kwargs["record_data"])
        elif operation_type == OperationType.READ:
            return await self.read_record(kwargs["record_id"])
        elif operation_type == OperationType.UPDATE:
            return await self.update_record(kwargs["record_id"], kwargs["record_data"])
        elif operation_type == OperationType.DELETE:
            return await self.delete_record(kwargs["record_id"])
        else:
            raise ValueError(f"Unsupported operation type: {operation_type}")
