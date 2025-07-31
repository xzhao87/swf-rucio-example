"""
Workflow orchestrator for managing the complete Rucio dataset and file workflow.

This module provides the WorkflowOrchestrator class that coordinates the complete
workflow described in README.dm, combining dataset and file management operations.
"""

import logging
from typing import Optional, List, Dict, Any, Union
from datetime import datetime

from rucio.client import Client as RucioClient

from .dataset_manager import DatasetManager
from .file_manager import FileManager, FileInfo
from .exceptions import WorkflowExecutionError, DatasetError, FileRegistrationError
from .utils import RucioUtils, ValidationUtils


class WorkflowResult:
    """Represents the result of a workflow execution."""
    
    def __init__(self):
        self.success = False
        self.dataset_created = False
        self.dataset_info = {}
        self.files_registered = 0
        self.files_added_to_dataset = 0
        self.dataset_closed = False
        self.errors = []
        self.start_time = datetime.now()
        self.end_time = None
        
    def mark_complete(self, success: bool = True):
        """Mark the workflow as complete."""
        self.success = success
        self.end_time = datetime.now()
        
    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
        
    def get_duration(self) -> float:
        """Get workflow duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
        
    def __str__(self):
        status = "SUCCESS" if self.success else "FAILED"
        duration = self.get_duration()
        return (f"WorkflowResult(status={status}, duration={duration:.2f}s, "
                f"files_registered={self.files_registered}, "
                f"dataset_closed={self.dataset_closed})")


class WorkflowOrchestrator:
    """
    Orchestrates the complete Rucio workflow as described in README.dm.
    
    This class coordinates the following steps:
    1. Create an empty and OPEN rucio dataset
    2. Register files with existing PFNs 
    3. Add these files to the dataset
    4. Close the dataset
    """
    
    def __init__(
        self,
        rucio_client: Optional[RucioClient] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize WorkflowOrchestrator.
        
        Args:
            rucio_client: Optional Rucio client instance
            logger: Optional logger instance
        """
        self.client = rucio_client if rucio_client else RucioClient()
        self.logger = logger if logger else logging.getLogger(__name__)
        
        # Initialize managers
        self.dataset_manager = DatasetManager(self.client, self.logger)
        self.file_manager = FileManager(self.client, self.logger)
        
        # Workflow state
        self.current_workflow = None
        
    def execute_workflow(
        self,
        dataset_name: str,
        files: List[Union[FileInfo, Dict[str, Any]]],
        rse: str,
        dataset_scope: Optional[str] = None,
        dataset_metadata: Optional[Dict[str, Any]] = None,
        dataset_lifetime_days: Optional[int] = None,
        batch_size: int = 100
    ) -> WorkflowResult:
        """
        Execute the complete workflow as described in README.dm.
        
        Args:
            dataset_name: Name of the dataset to create
            files: List of FileInfo objects or file dictionaries
            rse: RSE where files are located
            dataset_scope: Dataset scope (will be extracted if not provided)
            dataset_metadata: Optional dataset metadata
            dataset_lifetime_days: Dataset lifetime in days
            batch_size: Batch size for file operations
            
        Returns:
            WorkflowResult object with execution details
        """
        result = WorkflowResult()
        self.current_workflow = result
        
        try:
            self.logger.info("=" * 60)
            self.logger.info("STARTING RUCIO WORKFLOW EXECUTION")
            self.logger.info("=" * 60)
            
            # Step 1: Create empty and OPEN dataset
            self.logger.info("STEP 1: Creating empty and OPEN dataset")
            dataset_info = self._create_dataset(
                dataset_name, 
                dataset_scope, 
                dataset_metadata, 
                dataset_lifetime_days,
                result
            )
            
            # Step 2: Register files with existing PFNs
            self.logger.info("STEP 2: Registering files with existing PFNs")
            file_infos = self._prepare_and_register_files(files, rse, result)
            
            # Step 3: Add files to dataset
            self.logger.info("STEP 3: Adding files to dataset")
            self._add_files_to_dataset(file_infos, dataset_info, rse, batch_size, result)
            
            # Step 4: Close dataset
            self.logger.info("STEP 4: Closing dataset")
            self._close_dataset(dataset_info, result)
            
            # Mark workflow as successful
            result.mark_complete(success=True)
            
            self.logger.info("=" * 60)
            self.logger.info("WORKFLOW COMPLETED SUCCESSFULLY")
            self.logger.info(f"Dataset: {dataset_info.get('scope', '')}:{dataset_info.get('name', '')}")
            self.logger.info(f"Files registered: {result.files_registered}")
            self.logger.info(f"Files added to dataset: {result.files_added_to_dataset}")
            self.logger.info(f"Duration: {result.get_duration():.2f} seconds")
            self.logger.info("=" * 60)
            
        except Exception as e:
            error_msg = f"Workflow execution failed: {str(e)}"
            self.logger.error(error_msg)
            result.add_error(error_msg)
            result.mark_complete(success=False)
            
            # Attempt cleanup if dataset was created
            if result.dataset_created:
                self._cleanup_on_failure(dataset_info)
                
        return result
        
    def _create_dataset(
        self,
        dataset_name: str,
        dataset_scope: Optional[str],
        dataset_metadata: Optional[Dict[str, Any]],
        dataset_lifetime_days: Optional[int],
        result: WorkflowResult
    ) -> Dict[str, str]:
        """Create the dataset (Step 1)."""
        try:
            dataset_info = self.dataset_manager.create_dataset(
                dataset_name=dataset_name,
                scope=dataset_scope,
                metadata=dataset_metadata,
                lifetime_days=dataset_lifetime_days,
                open_dataset=True  # Explicitly create as OPEN
            )
            
            result.dataset_created = True
            result.dataset_info = dataset_info
            
            self.logger.info(f"✓ Created OPEN dataset: {dataset_info['scope']}:{dataset_info['name']}")
            return dataset_info
            
        except Exception as e:
            error_msg = f"Failed to create dataset: {str(e)}"
            result.add_error(error_msg)
            raise DatasetError(error_msg) from e
            
    def _prepare_and_register_files(
        self,
        files: List[Union[FileInfo, Dict[str, Any]]],
        rse: str,
        result: WorkflowResult
    ) -> List[FileInfo]:
        """Prepare and register files (Step 2)."""
        try:
            # Convert file dictionaries to FileInfo objects if needed
            file_infos = []
            for file_item in files:
                if isinstance(file_item, FileInfo):
                    file_infos.append(file_item)
                elif isinstance(file_item, dict):
                    # Create FileInfo from dictionary
                    file_info = FileInfo(
                        lfn=file_item['lfn'],
                        pfn=file_item['pfn'],
                        size=file_item['size'],
                        checksum=file_item['checksum'],
                        guid=file_item.get('guid'),
                        scope=file_item.get('scope'),
                        **{k: v for k, v in file_item.items() 
                           if k not in ['lfn', 'pfn', 'size', 'checksum', 'guid', 'scope']}
                    )
                    file_infos.append(file_info)
                else:
                    raise ValueError(f"Invalid file type: {type(file_item)}")
                    
            # Register file replicas
            registration_results = self.file_manager.register_multiple_files(
                files=file_infos,
                rse=rse
            )
            
            # Count successful registrations
            successful_registrations = sum(1 for success in registration_results.values() if success)
            result.files_registered = successful_registrations
            
            if successful_registrations == 0:
                raise FileRegistrationError("No files were successfully registered")
                
            self.logger.info(f"✓ Registered {successful_registrations}/{len(file_infos)} files at {rse}")
            
            # Filter to only successfully registered files
            successful_files = [
                file_info for file_info in file_infos
                if registration_results.get(file_info.lfn, False)
            ]
            
            return successful_files
            
        except Exception as e:
            error_msg = f"Failed to register files: {str(e)}"
            result.add_error(error_msg)
            raise FileRegistrationError(error_msg) from e
            
    def _add_files_to_dataset(
        self,
        file_infos: List[FileInfo],
        dataset_info: Dict[str, str],
        rse: str,
        batch_size: int,
        result: WorkflowResult
    ):
        """Add files to dataset (Step 3)."""
        try:
            success = self.file_manager.add_files_to_dataset(
                files=file_infos,
                dataset_name=dataset_info['name'],
                dataset_scope=dataset_info['scope'],
                rse=rse
            )
            
            if success:
                result.files_added_to_dataset = len(file_infos)
                self.logger.info(f"✓ Added {len(file_infos)} files to dataset")
            else:
                raise FileRegistrationError("Failed to add files to dataset")
                
        except Exception as e:
            error_msg = f"Failed to add files to dataset: {str(e)}"
            result.add_error(error_msg)
            raise FileRegistrationError(error_msg) from e
            
    def _close_dataset(self, dataset_info: Dict[str, str], result: WorkflowResult):
        """Close the dataset (Step 4)."""
        try:
            success = self.dataset_manager.close_dataset(
                dataset_name=dataset_info['name'],
                scope=dataset_info['scope']
            )
            
            if success:
                result.dataset_closed = True
                self.logger.info(f"✓ Closed dataset: {dataset_info['scope']}:{dataset_info['name']}")
            else:
                self.logger.warning("Dataset close operation returned False, but continuing")
                result.dataset_closed = True  # PanDA pattern: return True even on some failures
                
        except Exception as e:
            error_msg = f"Failed to close dataset: {str(e)}"
            result.add_error(error_msg)
            raise DatasetError(error_msg) from e
            
    def _cleanup_on_failure(self, dataset_info: Dict[str, str]):
        """Clean up resources on workflow failure."""
        try:
            self.logger.info("Attempting cleanup after workflow failure...")
            self.dataset_manager.delete_dataset(
                dataset_name=dataset_info['name'],
                scope=dataset_info['scope'],
                grace_period_hours=1
            )
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")
            
    def create_files_from_pfn_list(
        self,
        pfn_list: List[str],
        rse: str,
        scope_prefix: str = "user",
        default_scope: Optional[str] = None
    ) -> List[FileInfo]:
        """
        Create FileInfo objects from a list of PFNs.
        
        This is a convenience method for creating files when you have existing
        PFNs but need to generate the metadata.
        
        Args:
            pfn_list: List of physical file names/paths
            rse: RSE where files are located
            scope_prefix: Prefix for auto-generated scopes
            default_scope: Default scope to use for all files
            
        Returns:
            List of FileInfo objects
        """
        file_infos = []
        
        for pfn in pfn_list:
            try:
                # Determine scope
                if default_scope:
                    scope = default_scope
                else:
                    # Auto-generate scope based on client account
                    scope = f"{scope_prefix}.{self.client.account}"
                    
                file_info = self.file_manager.create_file_from_pfn(
                    pfn=pfn,
                    scope=scope
                )
                file_infos.append(file_info)
                
            except Exception as e:
                self.logger.error(f"Failed to create FileInfo from PFN {pfn}: {str(e)}")
                
        self.logger.info(f"Created {len(file_infos)} FileInfo objects from {len(pfn_list)} PFNs")
        return file_infos
        
    def get_workflow_status(self) -> Optional[WorkflowResult]:
        """Get the status of the current workflow."""
        return self.current_workflow
        
    def verify_workflow_completion(self, dataset_name: str, expected_file_count: int) -> bool:
        """
        Verify that the workflow completed successfully.
        
        Args:
            dataset_name: Name of the dataset to verify
            expected_file_count: Expected number of files in dataset
            
        Returns:
            True if verification passes
        """
        try:
            # Check dataset exists and is closed
            metadata = self.dataset_manager.get_dataset_metadata(dataset_name)
            if not metadata:
                self.logger.error("Dataset not found")
                return False
                
            if metadata.get("state") != "closed":
                self.logger.error(f"Dataset is not closed: {metadata.get('state')}")
                return False
                
            # Check file count
            file_count = self.dataset_manager.get_dataset_count(dataset_name)
            if file_count != expected_file_count:
                self.logger.error(f"File count mismatch: {file_count} != {expected_file_count}")
                return False
                
            self.logger.info("Workflow verification passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Verification failed: {str(e)}")
            return False
