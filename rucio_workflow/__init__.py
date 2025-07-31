"""
Rucio Workflow Package

A Python package for managing Rucio dataset and file operations, 
inspired by PanDA-Rucio interactions.

This package provides:
- Dataset management (create, open, close)
- File registration with existing PFNs
- File-to-dataset association
- Workflow orchestration
"""

from .dataset_manager import DatasetManager
from .file_manager import FileManager
from .workflow_orchestrator import WorkflowOrchestrator
from .utils import RucioUtils, ValidationUtils
from .exceptions import (
    RucioWorkflowError,
    DatasetError,
    FileRegistrationError,
    ValidationError
)

__version__ = "1.0.0"
__author__ = "Xin Zhao"

__all__ = [
    "DatasetManager",
    "FileManager", 
    "WorkflowOrchestrator",
    "RucioUtils",
    "ValidationUtils",
    "RucioWorkflowError",
    "DatasetError",
    "FileRegistrationError",
    "ValidationError"
]
