"""
Pydantic models for the sync service.
Defines data structures for operations, providers, and transformations.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator


class OperationType(str, Enum):
    """Supported CRUD operations."""
    CREATE = "CREATE"
    READ = "READ"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class SyncStatus(str, Enum):
    """Status of sync operations."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class ProviderType(str, Enum):
    """Supported external providers."""
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    PIPEDRIVE = "pipedrive"
    CUSTOM = "custom"


class AuthConfig(BaseModel):
    """Authentication configuration for external providers."""
    auth_type: str = Field(..., description="Type of authentication (oauth2, api_key, basic)")
    credentials: Dict[str, Any] = Field(..., description="Authentication credentials")
    token_url: Optional[str] = Field(None, description="OAuth2 token URL")
    refresh_token: Optional[str] = Field(None, description="OAuth2 refresh token")


class ProviderConfig(BaseModel):
    """Configuration for external API providers."""
    name: str = Field(..., description="Provider identifier")
    provider_type: ProviderType = Field(..., description="Type of provider")
    base_url: str = Field(..., description="Base URL for the provider API")
    rate_limit_per_minute: int = Field(1000, description="Rate limit per minute")
    burst_limit: int = Field(100, description="Burst capacity")
    timeout_seconds: int = Field(30, description="Request timeout")
    max_retries: int = Field(3, description="Maximum retry attempts")
    auth_config: AuthConfig = Field(..., description="Authentication configuration")
    
    class Config:
        schema_extra = {
            "example": {
                "name": "salesforce_prod",
                "provider_type": "salesforce",
                "base_url": "https://api.salesforce.com",
                "rate_limit_per_minute": 1000,
                "burst_limit": 100,
                "timeout_seconds": 30,
                "max_retries": 3,
                "auth_config": {
                    "auth_type": "oauth2",
                    "credentials": {
                        "client_id": "your_client_id",
                        "client_secret": "your_client_secret"
                    },
                    "token_url": "https://api.salesforce.com/oauth2/token"
                }
            }
        }


class SchemaMapping(BaseModel):
    """Schema mapping between internal and external formats."""
    internal_field: str = Field(..., description="Internal field name")
    external_field: str = Field(..., description="External field name")
    transformer: Optional[str] = Field(None, description="Transformation function name")
    required: bool = Field(True, description="Whether field is required")


class RecordData(BaseModel):
    """Generic record data structure."""
    id: Optional[str] = Field(None, description="Record identifier")
    data: Dict[str, Any] = Field(..., description="Record data")
    schema_version: str = Field("1.0", description="Schema version")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "record_123",
                "data": {
                    "name": "John Doe",
                    "email": "john@example.com",
                    "company": "Acme Corp"
                },
                "schema_version": "1.0"
            }
        }


class SyncOperation(BaseModel):
    """Sync operation request."""
    id: UUID = Field(default_factory=uuid4, description="Operation ID")
    operation: OperationType = Field(..., description="Type of operation")
    provider: str = Field(..., description="Target provider name")
    record_id: Optional[str] = Field(None, description="External record ID")
    record_data: Optional[RecordData] = Field(None, description="Record data for CREATE/UPDATE")
    priority: int = Field(5, ge=1, le=10, description="Operation priority (1=highest, 10=lowest)")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    scheduled_at: Optional[datetime] = Field(None, description="Scheduled execution time")
    
    @validator('record_data')
    def validate_record_data(cls, v, values):
        """Validate that record_data is provided for CREATE/UPDATE operations."""
        operation = values.get('operation')
        if operation in [OperationType.CREATE, OperationType.UPDATE] and v is None:
            raise ValueError(f"record_data is required for {operation} operations")
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "operation": "UPDATE",
                "provider": "salesforce_prod",
                "record_id": "sf_003xx000004TmiQ",
                "record_data": {
                    "data": {
                        "name": "Jane Smith",
                        "email": "jane@example.com",
                        "phone": "+1-555-0123"
                    }
                },
                "priority": 3
            }
        }


class SyncResult(BaseModel):
    """Result of a sync operation."""
    operation_id: UUID = Field(..., description="Operation ID")
    status: SyncStatus = Field(..., description="Operation status")
    provider: str = Field(..., description="Provider name")
    external_id: Optional[str] = Field(None, description="External record ID")
    started_at: Optional[datetime] = Field(None, description="Processing start time")
    completed_at: Optional[datetime] = Field(None, description="Processing completion time")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    retry_count: int = Field(0, description="Number of retry attempts")
    response_data: Optional[Dict[str, Any]] = Field(None, description="Provider response data")
    
    class Config:
        schema_extra = {
            "example": {
                "operation_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "COMPLETED",
                "provider": "salesforce_prod",
                "external_id": "sf_003xx000004TmiQ",
                "started_at": "2024-01-15T10:30:00Z",
                "completed_at": "2024-01-15T10:30:02Z",
                "retry_count": 0,
                "response_data": {
                    "id": "sf_003xx000004TmiQ",
                    "success": True
                }
            }
        }


class QueueMetrics(BaseModel):
    """Queue metrics for monitoring."""
    pending_operations: int = Field(..., description="Number of pending operations")
    processing_operations: int = Field(..., description="Number of processing operations")
    failed_operations: int = Field(..., description="Number of failed operations")
    completed_operations_last_hour: int = Field(..., description="Operations completed in last hour")
    average_processing_time_seconds: float = Field(..., description="Average processing time")
    error_rate_percentage: float = Field(..., description="Error rate percentage")


class ProviderMetrics(BaseModel):
    """Provider-specific metrics."""
    provider_name: str = Field(..., description="Provider name")
    requests_per_minute: int = Field(..., description="Current requests per minute")
    rate_limit_utilization: float = Field(..., description="Rate limit utilization percentage")
    average_response_time_ms: float = Field(..., description="Average response time in milliseconds")
    success_rate_percentage: float = Field(..., description="Success rate percentage")
    last_request_at: Optional[datetime] = Field(None, description="Last request timestamp")


class HealthStatus(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Overall health status")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Health check timestamp")
    version: str = Field("1.0.0", description="Service version")
    redis_connected: bool = Field(..., description="Redis connection status")
    active_providers: int = Field(..., description="Number of active providers")
    queue_metrics: QueueMetrics = Field(..., description="Queue metrics")
    provider_metrics: List[ProviderMetrics] = Field(..., description="Provider metrics")
