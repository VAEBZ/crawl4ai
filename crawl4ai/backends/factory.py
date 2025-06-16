"""
Backend factory for creating storage backends with clean dependency injection.

This module provides a factory pattern for creating different storage backends
with proper configuration management and validation.
"""

import logging
from typing import Any, Dict, Optional, Union

from . import (
    BackendConfigurationError,
    BackendType,
    StorageBackend,
)
from .dynamodb import DynamoDBBackend
from .mem0 import Mem0Backend

logger = logging.getLogger(__name__)


class BackendFactory:
    """
    Factory for creating storage backends with configuration validation.

    This factory provides a clean interface for creating different storage
    backends while handling configuration validation and error handling.
    """

    @staticmethod
    def create_backend(
        backend_type: Union[BackendType, str],
        config: Optional[Dict[str, Any]] = None,
    ) -> StorageBackend:
        """
        Create a storage backend instance with proper configuration.

        Args:
            backend_type: Type of backend to create
            config: Configuration dictionary for the backend

        Returns:
            Configured storage backend instance

        Raises:
            BackendConfigurationError: If configuration is invalid
            ValueError: If backend type is not supported
        """
        # Normalize backend type
        if isinstance(backend_type, str):
            try:
                backend_type = BackendType(backend_type.lower())
            except ValueError:
                raise ValueError(
                    f"Unsupported backend type: {backend_type}. "
                    f"Supported types: {[bt.value for bt in BackendType]}"
                )

        config = config or {}

        try:
            if backend_type == BackendType.DYNAMODB:
                return BackendFactory._create_dynamodb_backend(config)
            elif backend_type == BackendType.MEM0:
                return BackendFactory._create_mem0_backend(config)
            elif backend_type == BackendType.SQLITE:
                # Legacy support - could import the original SQLite manager
                raise NotImplementedError(
                    "SQLite backend not implemented in new architecture. "
                    "Use DynamoDB or Mem0 backends."
                )
            else:
                raise ValueError(f"Unsupported backend type: {backend_type}")

        except Exception as e:
            error_msg = f"Failed to create {backend_type.value} backend: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise BackendConfigurationError(error_msg, backend_type.value) from e

    @staticmethod
    def _create_dynamodb_backend(config: Dict[str, Any]) -> DynamoDBBackend:
        """
        Create and configure DynamoDB backend.

        Args:
            config: DynamoDB configuration

        Returns:
            Configured DynamoDBBackend instance
        """
        # Validate required configuration
        validated_config = BackendFactory._validate_dynamodb_config(config)

        logger.info(
            f"Creating DynamoDB backend with table: "
            f"{validated_config.get('table_name', 'crawl4ai-results')}"
        )

        return DynamoDBBackend(**validated_config)

    @staticmethod
    def _create_mem0_backend(config: Dict[str, Any]) -> Mem0Backend:
        """
        Create and configure Mem0 backend.

        Args:
            config: Mem0 configuration

        Returns:
            Configured Mem0Backend instance
        """
        # Validate required configuration
        validated_config = BackendFactory._validate_mem0_config(config)

        logger.info(
            f"Creating Mem0 backend with DynamoDB tables: "
            f"vectors={validated_config['dynamodb_config'].get('table_name', 'mem0-vectors')}, "
            f"graph={validated_config['dynamodb_config'].get('graph_table_name', 'mem0-graph')}"
        )

        return Mem0Backend(**validated_config)

    @staticmethod
    def _validate_dynamodb_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and normalize DynamoDB configuration.

        Args:
            config: Raw configuration dictionary

        Returns:
            Validated configuration dictionary

        Raises:
            BackendConfigurationError: If configuration is invalid
        """
        validated = {}

        # Table name
        validated["table_name"] = config.get("table_name", "crawl4ai-results")

        # Region
        validated["region_name"] = config.get("region_name", "us-east-1")

        # Endpoint URL (for local DynamoDB)
        if "endpoint_url" in config:
            validated["endpoint_url"] = config["endpoint_url"]

        # AWS credentials
        if "aws_access_key_id" in config:
            validated["aws_access_key_id"] = config["aws_access_key_id"]
        if "aws_secret_access_key" in config:
            validated["aws_secret_access_key"] = config["aws_secret_access_key"]

        # Validate table name format
        table_name = validated["table_name"]
        if not table_name or not isinstance(table_name, str):
            raise BackendConfigurationError(
                "DynamoDB table_name must be a non-empty string", "dynamodb"
            )

        return validated

    @staticmethod
    def _validate_mem0_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and normalize Mem0 configuration.

        Args:
            config: Raw configuration dictionary

        Returns:
            Validated configuration dictionary

        Raises:
            BackendConfigurationError: If configuration is invalid
        """
        validated = {}

        # API key (optional for OSS version)
        if "api_key" in config:
            validated["api_key"] = config["api_key"]

        # Graph memory
        validated["enable_graph"] = config.get("enable_graph", True)

        # DynamoDB configuration for Mem0's vector store
        dynamodb_config = config.get("dynamodb_config", {})
        validated["dynamodb_config"] = {
            "table_name": dynamodb_config.get("table_name", "mem0-vectors"),
            "graph_table_name": dynamodb_config.get("graph_table_name", "mem0-graph"),
            "region_name": dynamodb_config.get("region_name", "us-east-1"),
        }

        # Add endpoint URL if specified
        if "endpoint_url" in dynamodb_config:
            validated["dynamodb_config"]["endpoint_url"] = dynamodb_config[
                "endpoint_url"
            ]

        # Add AWS credentials if specified
        for key in ["aws_access_key_id", "aws_secret_access_key"]:
            if key in dynamodb_config:
                validated["dynamodb_config"][key] = dynamodb_config[key]

        # Custom Mem0 configuration
        if "mem0_config" in config:
            validated["config"] = config["mem0_config"]

        return validated

    @staticmethod
    def get_supported_backends() -> list[str]:
        """
        Get list of supported backend types.

        Returns:
            List of supported backend type strings
        """
        return [backend_type.value for backend_type in BackendType]

    @staticmethod
    def create_from_url(backend_url: str) -> StorageBackend:
        """
        Create backend from URL-style configuration.

        Args:
            backend_url: URL-style backend specification
                Examples:
                - "dynamodb://table-name?region=us-east-1"
                - "mem0://vectors-table?graph_table=graph-table&region=us-east-1"

        Returns:
            Configured storage backend instance

        Raises:
            BackendConfigurationError: If URL format is invalid
        """
        try:
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(backend_url)
            backend_type = parsed.scheme

            if not backend_type:
                raise BackendConfigurationError(
                    f"Invalid backend URL format: {backend_url}", "unknown"
                )

            # Parse query parameters
            query_params = parse_qs(parsed.query)
            config = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}

            # Add path as table name for DynamoDB
            if backend_type == "dynamodb" and parsed.path:
                config["table_name"] = parsed.path.lstrip("/")

            # Add path as vector table for Mem0
            if backend_type == "mem0" and parsed.path:
                config.setdefault("dynamodb_config", {})
                config["dynamodb_config"]["table_name"] = parsed.path.lstrip("/")

            logger.info(
                f"Creating backend from URL: {backend_type} with config: {config}"
            )

            return BackendFactory.create_backend(backend_type, config)

        except Exception as e:
            error_msg = f"Failed to parse backend URL '{backend_url}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise BackendConfigurationError(error_msg, "unknown") from e
