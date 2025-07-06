"""
Schema transformation and validation pipeline.
Handles data mapping between internal and external formats.
"""

from typing import Any, Callable, Dict, List, Optional
from datetime import datetime

from pydantic import BaseModel, ValidationError
from structlog import get_logger

from .models import RecordData, SchemaMapping

logger = get_logger(__name__)


class TransformationError(Exception):
    """Raised when data transformation fails."""
    pass


class SchemaTransformer:
    """Handles schema transformation between internal and external formats."""
    
    def __init__(self):
        self.transformers: Dict[str, Callable] = {
            'to_upper': lambda x: str(x).upper() if x else None,
            'to_lower': lambda x: str(x).lower() if x else None,
            'to_string': lambda x: str(x) if x is not None else None,
            'to_int': lambda x: int(x) if x is not None else None,
            'to_float': lambda x: float(x) if x is not None else None,
            'to_bool': lambda x: bool(x) if x is not None else None,
            'format_phone': self._format_phone,
            'format_email': self._format_email,
            'format_date': self._format_date,
            'clean_html': self._clean_html,
            'truncate_255': lambda x: str(x)[:255] if x else None,
            'remove_special_chars': self._remove_special_chars,
        }
    
    def _format_phone(self, phone: Any) -> Optional[str]:
        """Format phone number to E.164 format."""
        if not phone:
            return None
        
        # Simple phone formatting - in production, use a proper library
        phone_str = str(phone).strip()
        # Remove all non-digits
        digits = ''.join(filter(str.isdigit, phone_str))
        
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits.startswith('1'):
            return f"+{digits}"
        else:
            return phone_str  # Return original if can't format
    
    def _format_email(self, email: Any) -> Optional[str]:
        """Format and validate email address."""
        if not email:
            return None
        
        email_str = str(email).strip().lower()
        # Basic email validation
        if '@' in email_str and '.' in email_str.split('@')[-1]:
            return email_str
        else:
            return None
    
    def _format_date(self, date_value: Any) -> Optional[str]:
        """Format date to ISO format."""
        if not date_value:
            return None
        
        if isinstance(date_value, datetime):
            return date_value.isoformat()
        elif isinstance(date_value, str):
            try:
                # Try parsing common date formats
                from dateutil.parser import parse
                parsed_date = parse(date_value)
                return parsed_date.isoformat()
            except Exception:
                return date_value  # Return original if can't parse
        else:
            return str(date_value)
    
    def _clean_html(self, html: Any) -> Optional[str]:
        """Remove HTML tags from text."""
        if not html:
            return None
        
        import re
        html_str = str(html)
        # Simple HTML tag removal
        clean_text = re.sub(r'<[^>]+>', '', html_str)
        return clean_text.strip()
    
    def _remove_special_chars(self, text: Any) -> Optional[str]:
        """Remove special characters, keep alphanumeric and spaces."""
        if not text:
            return None
        
        import re
        text_str = str(text)
        clean_text = re.sub(r'[^a-zA-Z0-9\s]', '', text_str)
        return clean_text.strip()
    
    def add_transformer(self, name: str, func: Callable) -> None:
        """Add a custom transformation function."""
        self.transformers[name] = func
        logger.info("Custom transformer added", name=name)
    
    def transform_value(self, value: Any, transformer_name: str) -> Any:
        """Apply a transformation to a value."""
        if transformer_name not in self.transformers:
            raise TransformationError(f"Unknown transformer: {transformer_name}")
        
        try:
            return self.transformers[transformer_name](value)
        except Exception as e:
            logger.error(
                "Transformation failed",
                transformer=transformer_name,
                value=value,
                error=str(e)
            )
            raise TransformationError(f"Transformation '{transformer_name}' failed: {e}")
    
    def transform_record(
        self,
        record_data: RecordData,
        mappings: List[SchemaMapping],
        direction: str = "internal_to_external"
    ) -> Dict[str, Any]:
        """
        Transform record data using schema mappings.
        
        Args:
            record_data: Input record data
            mappings: List of field mappings
            direction: "internal_to_external" or "external_to_internal"
            
        Returns:
            Transformed data dictionary
        """
        input_data = record_data.data
        output_data = {}
        
        for mapping in mappings:
            if direction == "internal_to_external":
                source_field = mapping.internal_field
                target_field = mapping.external_field
            else:
                source_field = mapping.external_field
                target_field = mapping.internal_field
            
            # Get source value
            source_value = input_data.get(source_field)
            
            # Check if field is required
            if mapping.required and source_value is None:
                raise TransformationError(f"Required field '{source_field}' is missing")
            
            # Apply transformation if specified
            if mapping.transformer and source_value is not None:
                try:
                    transformed_value = self.transform_value(source_value, mapping.transformer)
                except TransformationError:
                    if mapping.required:
                        raise
                    else:
                        # Skip optional field if transformation fails
                        logger.warning(
                            "Skipping optional field due to transformation error",
                            field=source_field,
                            transformer=mapping.transformer
                        )
                        continue
            else:
                transformed_value = source_value
            
            # Set target value
            if transformed_value is not None:
                output_data[target_field] = transformed_value
        
        logger.debug(
            "Record transformation completed",
            direction=direction,
            input_fields=len(input_data),
            output_fields=len(output_data)
        )
        
        return output_data


class SchemaValidator:
    """Validates data against predefined schemas."""
    
    def __init__(self):
        self.schemas: Dict[str, BaseModel] = {}
    
    def register_schema(self, provider: str, schema_class: BaseModel) -> None:
        """Register a validation schema for a provider."""
        self.schemas[provider] = schema_class
        logger.info("Schema registered", provider=provider, schema=schema_class.__name__)
    
    def validate_data(self, provider: str, data: Dict[str, Any]) -> BaseModel:
        """
        Validate data against provider schema.
        
        Args:
            provider: Provider identifier
            data: Data to validate
            
        Returns:
            Validated data model instance
            
        Raises:
            ValidationError: If data doesn't match schema
        """
        if provider not in self.schemas:
            logger.warning("No schema registered for provider", provider=provider)
            # Return a generic validated model
            return RecordData(data=data)
        
        schema_class = self.schemas[provider]
        
        try:
            validated_data = schema_class(**data)
            logger.debug("Data validation successful", provider=provider)
            return validated_data
        except ValidationError as e:
            logger.error(
                "Data validation failed",
                provider=provider,
                errors=e.errors(),
                data=data
            )
            raise


# Common provider schemas

class SalesforceContact(BaseModel):
    """Salesforce Contact schema."""
    FirstName: Optional[str] = None
    LastName: str
    Email: Optional[str] = None
    Phone: Optional[str] = None
    AccountId: Optional[str] = None
    Title: Optional[str] = None
    Department: Optional[str] = None


class HubSpotContact(BaseModel):
    """HubSpot Contact schema."""
    firstname: Optional[str] = None
    lastname: str
    email: Optional[str] = None
    phone: Optional[str] = None
    company: Optional[str] = None
    jobtitle: Optional[str] = None


class PipedriveContact(BaseModel):
    """Pipedrive Person schema."""
    name: str
    email: Optional[List[str]] = None
    phone: Optional[List[str]] = None
    org_id: Optional[int] = None


def get_default_mappings(provider: str) -> List[SchemaMapping]:
    """Get default field mappings for common providers."""
    
    mappings = {
        'salesforce': [
            SchemaMapping(internal_field='first_name', external_field='FirstName'),
            SchemaMapping(internal_field='last_name', external_field='LastName', required=True),
            SchemaMapping(internal_field='email', external_field='Email', transformer='format_email'),
            SchemaMapping(internal_field='phone', external_field='Phone', transformer='format_phone'),
            SchemaMapping(internal_field='company_id', external_field='AccountId'),
            SchemaMapping(internal_field='title', external_field='Title'),
        ],
        'hubspot': [
            SchemaMapping(internal_field='first_name', external_field='firstname'),
            SchemaMapping(internal_field='last_name', external_field='lastname', required=True),
            SchemaMapping(internal_field='email', external_field='email', transformer='format_email'),
            SchemaMapping(internal_field='phone', external_field='phone', transformer='format_phone'),
            SchemaMapping(internal_field='company_name', external_field='company'),
            SchemaMapping(internal_field='title', external_field='jobtitle'),
        ],
        'pipedrive': [
            SchemaMapping(internal_field='full_name', external_field='name', required=True),
            SchemaMapping(internal_field='email', external_field='email', transformer='format_email'),
            SchemaMapping(internal_field='phone', external_field='phone', transformer='format_phone'),
            SchemaMapping(internal_field='organization_id', external_field='org_id', transformer='to_int'),
        ]
    }
    
    return mappings.get(provider, [])
