#!/usr/bin/env python3
"""
Configuration file for the Rucio workflow package.

This module provides configuration management for the workflow package,
including default settings, environment-specific configurations, and
validation.
"""

import os
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class RucioConfig:
    """Configuration settings for Rucio client."""
    
    # Rucio server settings
    auth_host: str = "https://voatlasrucio-auth-prod.cern.ch"
    rucio_host: str = "https://voatlasrucio-server-prod.cern.ch"
    auth_type: str = "x509"
    ca_cert: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    
    # Account settings
    account: str = "pilot"
    
    # Timeout settings (seconds)
    auth_timeout: int = 300
    request_timeout: int = 600
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Logging settings
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass  
class WorkflowConfig:
    """Configuration settings for workflow execution."""
    
    # Default RSE for file registration
    default_rse: str = "ATLAS_DISK"
    
    # Dataset settings
    default_dataset_lifetime_days: int = 30
    dataset_state_open: str = "OPEN"
    dataset_state_closed: str = "CLOSED"
    
    # File settings
    default_scope: str = "user.pilot"
    checksum_algorithm: str = "adler32"
    
    # Batch processing
    batch_size: int = 100
    
    # Monitoring
    enable_performance_monitoring: bool = True
    enable_detailed_logging: bool = False


class ConfigManager:
    """Manages configuration for the Rucio workflow package."""
    
    def __init__(self):
        self.rucio_config = RucioConfig()
        self.workflow_config = WorkflowConfig()
        self._load_from_environment()
        
    def _load_from_environment(self):
        """Load configuration from environment variables."""
        
        # Rucio client configuration
        if os.getenv("RUCIO_AUTH_HOST"):
            self.rucio_config.auth_host = os.getenv("RUCIO_AUTH_HOST")
            
        if os.getenv("RUCIO_RUCIO_HOST"):
            self.rucio_config.rucio_host = os.getenv("RUCIO_RUCIO_HOST")
            
        if os.getenv("RUCIO_AUTH_TYPE"):
            self.rucio_config.auth_type = os.getenv("RUCIO_AUTH_TYPE")
            
        if os.getenv("RUCIO_ACCOUNT"):
            self.rucio_config.account = os.getenv("RUCIO_ACCOUNT")
            
        if os.getenv("X509_USER_CERT"):
            self.rucio_config.client_cert = os.getenv("X509_USER_CERT")
            
        if os.getenv("X509_USER_KEY"):
            self.rucio_config.client_key = os.getenv("X509_USER_KEY")
            
        if os.getenv("X509_CERT_DIR"):
            self.rucio_config.ca_cert = os.getenv("X509_CERT_DIR")
            
        # Workflow configuration
        if os.getenv("RUCIO_DEFAULT_RSE"):
            self.workflow_config.default_rse = os.getenv("RUCIO_DEFAULT_RSE")
            
        if os.getenv("RUCIO_DEFAULT_SCOPE"):
            self.workflow_config.default_scope = os.getenv("RUCIO_DEFAULT_SCOPE")
            
        # Numeric settings
        try:
            if os.getenv("RUCIO_AUTH_TIMEOUT"):
                self.rucio_config.auth_timeout = int(os.getenv("RUCIO_AUTH_TIMEOUT"))
                
            if os.getenv("RUCIO_REQUEST_TIMEOUT"):
                self.rucio_config.request_timeout = int(os.getenv("RUCIO_REQUEST_TIMEOUT"))
                
            if os.getenv("RUCIO_MAX_RETRIES"):
                self.rucio_config.max_retries = int(os.getenv("RUCIO_MAX_RETRIES"))
                
            if os.getenv("RUCIO_BATCH_SIZE"):
                self.workflow_config.batch_size = int(os.getenv("RUCIO_BATCH_SIZE"))
                
        except ValueError as e:
            logging.warning(f"Invalid numeric environment variable: {e}")
            
        # Boolean settings
        if os.getenv("RUCIO_ENABLE_PERFORMANCE_MONITORING"):
            self.workflow_config.enable_performance_monitoring = (
                os.getenv("RUCIO_ENABLE_PERFORMANCE_MONITORING").lower() in ["true", "1", "yes"]
            )
            
        if os.getenv("RUCIO_ENABLE_DETAILED_LOGGING"):
            self.workflow_config.enable_detailed_logging = (
                os.getenv("RUCIO_ENABLE_DETAILED_LOGGING").lower() in ["true", "1", "yes"]
            )
            
    def get_rucio_client_config(self) -> Dict[str, Any]:
        """Get configuration dictionary for Rucio client initialization."""
        config = {
            "rucio_host": self.rucio_config.rucio_host,
            "auth_host": self.rucio_config.auth_host,
            "auth_type": self.rucio_config.auth_type,
            "account": self.rucio_config.account,
            "timeout": self.rucio_config.request_timeout,
        }
        
        # Add certificate settings if available
        if self.rucio_config.client_cert:
            config["creds"] = {
                "client_cert": self.rucio_config.client_cert,
                "client_key": self.rucio_config.client_key or self.rucio_config.client_cert,
            }
            
        if self.rucio_config.ca_cert:
            config["ca_cert"] = self.rucio_config.ca_cert
            
        return config
        
    def setup_logging(self):
        """Set up logging based on configuration."""
        log_level = getattr(logging, self.rucio_config.log_level.upper())
        
        if self.workflow_config.enable_detailed_logging:
            log_level = logging.DEBUG
            
        logging.basicConfig(
            level=log_level,
            format=self.rucio_config.log_format
        )
        
        # Set Rucio client logging level
        rucio_logger = logging.getLogger("rucio")
        rucio_logger.setLevel(logging.WARNING)  # Reduce Rucio noise
        
    def validate_config(self) -> bool:
        """Validate the current configuration."""
        errors = []
        
        # Check required settings
        if not self.rucio_config.auth_host:
            errors.append("auth_host is required")
            
        if not self.rucio_config.rucio_host:
            errors.append("rucio_host is required")
            
        if not self.rucio_config.account:
            errors.append("account is required")
            
        # Check certificate files exist if specified
        if self.rucio_config.client_cert and not os.path.exists(self.rucio_config.client_cert):
            errors.append(f"client_cert file does not exist: {self.rucio_config.client_cert}")
            
        if self.rucio_config.client_key and not os.path.exists(self.rucio_config.client_key):
            errors.append(f"client_key file does not exist: {self.rucio_config.client_key}")
            
        # Check numeric ranges
        if self.rucio_config.auth_timeout <= 0:
            errors.append("auth_timeout must be positive")
            
        if self.rucio_config.request_timeout <= 0:
            errors.append("request_timeout must be positive")
            
        if self.rucio_config.max_retries < 0:
            errors.append("max_retries must be non-negative")
            
        if self.workflow_config.batch_size <= 0:
            errors.append("batch_size must be positive")
            
        if errors:
            for error in errors:
                logging.error(f"Configuration error: {error}")
            return False
            
        return True
        
    def print_config_summary(self):
        """Print a summary of the current configuration."""
        print("=" * 50)
        print("RUCIO WORKFLOW CONFIGURATION SUMMARY")
        print("=" * 50)
        
        print(f"Rucio Host: {self.rucio_config.rucio_host}")
        print(f"Auth Host: {self.rucio_config.auth_host}")
        print(f"Auth Type: {self.rucio_config.auth_type}")
        print(f"Account: {self.rucio_config.account}")
        print(f"Default RSE: {self.workflow_config.default_rse}")
        print(f"Default Scope: {self.workflow_config.default_scope}")
        print(f"Batch Size: {self.workflow_config.batch_size}")
        print(f"Log Level: {self.rucio_config.log_level}")
        
        cert_status = "✓" if self.rucio_config.client_cert else "✗"
        print(f"X.509 Certificate: {cert_status}")
        
        perf_status = "✓" if self.workflow_config.enable_performance_monitoring else "✗"
        print(f"Performance Monitoring: {perf_status}")
        
        print("=" * 50)


# Global configuration instance
config_manager = ConfigManager()


def get_config() -> ConfigManager:
    """Get the global configuration manager instance."""
    return config_manager


def setup_logging():
    """Set up logging using the global configuration."""
    config_manager.setup_logging()


# Environment configuration helpers
def is_development_environment() -> bool:
    """Check if running in development environment."""
    return os.getenv("RUCIO_ENV", "").lower() in ["dev", "development"]


def is_production_environment() -> bool:
    """Check if running in production environment."""
    return os.getenv("RUCIO_ENV", "").lower() in ["prod", "production"]


def get_environment_name() -> str:
    """Get the current environment name."""
    return os.getenv("RUCIO_ENV", "unknown")


if __name__ == "__main__":
    # Demonstration of configuration management
    config = get_config()
    
    print("Testing configuration management...")
    config.print_config_summary()
    
    print(f"\nEnvironment: {get_environment_name()}")
    print(f"Development: {is_development_environment()}")
    print(f"Production: {is_production_environment()}")
    
    print(f"\nConfiguration valid: {config.validate_config()}")
