"""
Test module for Rucio workflow package.

This module provides unit tests and integration tests for the workflow components.
"""

import unittest
import tempfile
import os
import logging
from unittest.mock import Mock, MagicMock, patch
from typing import List, Dict, Any

# Import the modules to test
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rucio_workflow.dataset_manager import DatasetManager
from rucio_workflow.file_manager import FileManager, FileInfo
from rucio_workflow.workflow_orchestrator import WorkflowOrchestrator, WorkflowResult
from rucio_workflow.utils import RucioUtils, ValidationUtils, MetadataUtils
from rucio_workflow.exceptions import (
    DatasetError, FileRegistrationError, ValidationError
)


class TestRucioUtils(unittest.TestCase):
    """Test utility functions."""
    
    def test_extract_scope(self):
        """Test scope extraction with both colon and dot formats."""
        
        # Test explicit colon format: scope:name
        scope, name = RucioUtils.extract_scope("user.pilot:dataset1")
        self.assertEqual(scope, "user.pilot")
        self.assertEqual(name, "dataset1")
        
        # Test colon format with complex names
        scope, name = RucioUtils.extract_scope("user.pilot:my.complex.dataset.name")
        self.assertEqual(scope, "user.pilot")
        self.assertEqual(name, "my.complex.dataset.name")
        
        # Test data scope with colon
        scope, name = RucioUtils.extract_scope("data.atlas:mc.run123.output")
        self.assertEqual(scope, "data.atlas")
        self.assertEqual(name, "mc.run123.output")
        
        # Test inferred dot format for user datasets
        scope, name = RucioUtils.extract_scope("user.pilot.dataset2")
        self.assertEqual(scope, "user.pilot")
        self.assertEqual(name, "dataset2")
        
        # Test inferred dot format for user datasets with complex names
        scope, name = RucioUtils.extract_scope("user.pilot.my.complex.dataset")
        self.assertEqual(scope, "user.pilot")
        self.assertEqual(name, "my.complex.dataset")
        
        # Test group datasets
        scope, name = RucioUtils.extract_scope("group.atlas.dataset3")
        self.assertEqual(scope, "group.atlas")
        self.assertEqual(name, "dataset3")
        
        # Test group datasets with complex names
        scope, name = RucioUtils.extract_scope("group.atlas.mc.run456.output")
        self.assertEqual(scope, "group.atlas")
        self.assertEqual(name, "mc.run456.output")
        
        # Test data scope inferred format
        scope, name = RucioUtils.extract_scope("data.atlas.mc.run123.output")
        self.assertEqual(scope, "data.atlas.mc.run123")
        self.assertEqual(name, "output")
        
        # Test mc scope
        scope, name = RucioUtils.extract_scope("mc.atlas.simulation.job456.dataset")
        self.assertEqual(scope, "mc.atlas.simulation.job456")
        self.assertEqual(name, "dataset")
        
        # Test edge cases
        with self.assertRaises(ValidationError):
            # No dots or colons
            RucioUtils.extract_scope("invalid")
            
        # Test with trailing slash
        scope, name = RucioUtils.extract_scope("user.pilot.dataset/", strip_slash=True)
        self.assertEqual(scope, "user.pilot")
        self.assertEqual(name, "dataset")
        
    def test_generate_guid(self):
        """Test GUID generation."""
        guid = RucioUtils.generate_guid()
        self.assertIsInstance(guid, str)
        self.assertEqual(len(guid), 36)  # UUID format
        
    def test_format_guid(self):
        """Test GUID formatting."""
        # Test with dashes
        guid = "12345678-1234-1234-1234-123456789012"
        formatted = RucioUtils.format_guid(guid)
        self.assertEqual(formatted, guid)
        
        # Test without dashes
        guid_no_dash = "12345678123412341234123456789012"
        formatted = RucioUtils.format_guid(guid_no_dash)
        self.assertEqual(formatted, "12345678-1234-1234-1234-123456789012")
        
    def test_generate_vuid(self):
        """Test VUID generation."""
        vuid = RucioUtils.generate_vuid("user.test", "dataset1")
        self.assertIsInstance(vuid, str)
        self.assertEqual(len(vuid), 36)
        self.assertRegex(vuid, r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')


class TestValidationUtils(unittest.TestCase):
    """Test validation utilities."""
    
    def test_validate_dataset_name(self):
        """Test dataset name validation."""
        # Valid names
        self.assertTrue(ValidationUtils.validate_dataset_name("user.test.dataset"))
        self.assertTrue(ValidationUtils.validate_dataset_name("group.atlas:container"))
        
        # Invalid names
        with self.assertRaises(ValidationError):
            ValidationUtils.validate_dataset_name("")
        with self.assertRaises(ValidationError):
            ValidationUtils.validate_dataset_name("dataset with spaces")
            
    def test_validate_checksum(self):
        """Test checksum validation."""
        # Valid checksums
        self.assertTrue(ValidationUtils.validate_checksum("md5:abcdef1234567890abcdef1234567890"))
        self.assertTrue(ValidationUtils.validate_checksum("ad:12345678"))
        
        # Invalid checksums
        with self.assertRaises(ValidationError):
            ValidationUtils.validate_checksum("invalid:format")
        with self.assertRaises(ValidationError):
            ValidationUtils.validate_checksum("md5:invalidhash")


class TestFileInfo(unittest.TestCase):
    """Test FileInfo class."""
    
    def test_file_info_creation(self):
        """Test FileInfo object creation."""
        file_info = FileInfo(
            lfn="test_file.root",
            pfn="/path/to/test_file.root",
            size=1024,
            checksum="ad:12345678",
            scope="user.test"
        )
        
        self.assertEqual(file_info.lfn, "test_file.root")
        self.assertEqual(file_info.pfn, "/path/to/test_file.root")
        self.assertEqual(file_info.size, 1024)
        self.assertEqual(file_info.checksum, "ad:12345678")
        self.assertEqual(file_info.scope, "user.test")
        self.assertIsNotNone(file_info.guid)
        
    def test_to_rucio_dict(self):
        """Test conversion to Rucio dictionary format."""
        file_info = FileInfo(
            lfn="test_file.root",
            pfn="/path/to/test_file.root",
            size=1024,
            checksum="ad:12345678",
            scope="user.test",
            guid="12345678-1234-1234-1234-123456789012"
        )
        
        rucio_dict = file_info.to_rucio_dict("TEST_RSE")
        
        self.assertEqual(rucio_dict["scope"], "user.test")
        self.assertEqual(rucio_dict["name"], "test_file.root")
        self.assertEqual(rucio_dict["bytes"], 1024)
        self.assertEqual(rucio_dict["pfn"], "/path/to/test_file.root")
        self.assertEqual(rucio_dict["adler32"], "12345678")
        self.assertEqual(rucio_dict["meta"]["guid"], "12345678-1234-1234-1234-123456789012")


class MockRucioClient:
    """Mock Rucio client for testing."""
    
    def __init__(self):
        self.account = "testuser"
        self.datasets = {}
        self.files = {}
        self.replicas = {}
        
    def add_dataset(self, scope, name, meta=None):
        dataset_id = f"{scope}:{name}"
        self.datasets[dataset_id] = {
            "scope": scope,
            "name": name,
            "meta": meta or {},
            "is_open": True,
            "did_type": "DATASET"
        }
        
    def set_status(self, scope, name, open=True):
        dataset_id = f"{scope}:{name}"
        if dataset_id in self.datasets:
            self.datasets[dataset_id]["is_open"] = open
            
    def set_metadata(self, scope, name, key, value):
        dataset_id = f"{scope}:{name}"
        if dataset_id in self.datasets:
            self.datasets[dataset_id]["meta"][key] = value
            
    def get_metadata(self, scope, name):
        dataset_id = f"{scope}:{name}"
        if dataset_id in self.datasets:
            return self.datasets[dataset_id].copy()
        raise Exception("DataIdentifierNotFound")
        
    def add_replicas(self, rse, files):
        for file_dict in files:
            file_id = f"{file_dict['scope']}:{file_dict['name']}"
            self.replicas[file_id] = {
                "rse": rse,
                "file": file_dict
            }
            
    def add_files_to_dataset(self, scope, name, files, rse=None):
        dataset_id = f"{scope}:{name}"
        if dataset_id not in self.datasets:
            raise Exception("DataIdentifierNotFound")
            
        for file_dict in files:
            file_id = f"{file_dict['scope']}:{file_dict['name']}"
            self.files[file_id] = {
                "dataset": dataset_id,
                "file": file_dict
            }
            
    def list_files(self, scope, name, long=False):
        dataset_id = f"{scope}:{name}"
        for file_id, file_data in self.files.items():
            if file_data["dataset"] == dataset_id:
                file_info = file_data["file"].copy()
                file_info.update({
                    "adler32": file_info.get("adler32", "12345678"),
                    "events": 1000,
                    "guid": file_info.get("meta", {}).get("guid", "test-guid")
                })
                yield file_info


class TestDatasetManager(unittest.TestCase):
    """Test DatasetManager class."""
    
    def setUp(self):
        """Set up test environment."""
        self.mock_client = MockRucioClient()
        self.logger = logging.getLogger("test")
        self.dataset_manager = DatasetManager(self.mock_client, self.logger)
        
    def test_create_dataset(self):
        """Test dataset creation."""
        result = self.dataset_manager.create_dataset(
            dataset_name="user.test.dataset1",
            metadata={"test": "value"}
        )
        
        self.assertIn("duid", result)
        self.assertIn("vuid", result)
        self.assertEqual(result["scope"], "user.test")
        self.assertEqual(result["name"], "dataset1")  # Fixed: name is just the last part
        
        # Check dataset was created in mock client
        dataset_id = f"{result['scope']}:{result['name']}"
        self.assertIn(dataset_id, self.mock_client.datasets)
        
    def test_close_dataset(self):
        """Test dataset closing."""
        # Create dataset first
        self.dataset_manager.create_dataset("user.test.dataset2")
        
        # Close it
        result = self.dataset_manager.close_dataset("user.test.dataset2")
        self.assertTrue(result)
        
        # Check it's closed in mock client
        dataset = self.mock_client.datasets["user.test:dataset2"]  # Fixed: correct key
        self.assertFalse(dataset["is_open"])
        
    def test_get_dataset_metadata(self):
        """Test getting dataset metadata."""
        # Create dataset first
        self.dataset_manager.create_dataset(
            "user.test.dataset3",
            metadata={"campaign": "test_campaign"}
        )
        
        # Get metadata
        metadata = self.dataset_manager.get_dataset_metadata("user.test.dataset3")
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata["scope"], "user.test")
        self.assertEqual(metadata["name"], "dataset3")  # Fixed: name is just the last part
        self.assertEqual(metadata["state"], "open")
        
    def test_create_dataset_colon_format(self):
        """Test dataset creation with colon format."""
        result = self.dataset_manager.create_dataset(
            dataset_name="user.test:my.complex.dataset.name",
            metadata={"test": "colon_format"}
        )
        
        self.assertIn("duid", result)
        self.assertIn("vuid", result)
        self.assertEqual(result["scope"], "user.test")
        self.assertEqual(result["name"], "my.complex.dataset.name")
        
        # Check dataset was created in mock client
        dataset_id = f"{result['scope']}:{result['name']}"
        self.assertIn(dataset_id, self.mock_client.datasets)


class TestFileManager(unittest.TestCase):
    """Test FileManager class."""
    
    def setUp(self):
        """Set up test environment."""
        self.mock_client = MockRucioClient()
        self.logger = logging.getLogger("test")
        self.file_manager = FileManager(self.mock_client, self.logger)
        
    def test_register_file_replica(self):
        """Test file replica registration."""
        file_info = FileInfo(
            lfn="test_file.root",
            pfn="/path/to/test_file.root",
            size=1024,
            checksum="ad:12345678",
            scope="user.test"
        )
        
        result = self.file_manager.register_file_replica(file_info, "TEST_RSE")
        self.assertTrue(result)
        
        # Check file was registered in mock client
        file_id = f"{file_info.scope}:{file_info.lfn}"
        self.assertIn(file_id, self.mock_client.replicas)
        
    def test_add_files_to_dataset(self):
        """Test adding files to dataset."""
        # Create dataset first
        self.mock_client.add_dataset("user.test", "test_dataset")
        
        # Create file info
        file_info = FileInfo(
            lfn="test_file.root",
            pfn="/path/to/test_file.root",
            size=1024,
            checksum="ad:12345678",
            scope="user.test"
        )
        
        result = self.file_manager.add_files_to_dataset(
            [file_info],
            "test_dataset",
            "user.test"
        )
        self.assertTrue(result)
        
        # Check file was added to dataset
        file_id = f"{file_info.scope}:{file_info.lfn}"
        self.assertIn(file_id, self.mock_client.files)


class TestWorkflowOrchestrator(unittest.TestCase):
    """Test WorkflowOrchestrator class."""
    
    def setUp(self):
        """Set up test environment."""
        self.mock_client = MockRucioClient()
        self.logger = logging.getLogger("test")
        self.orchestrator = WorkflowOrchestrator(self.mock_client, self.logger)
        
    def test_execute_workflow(self):
        """Test complete workflow execution."""
        # Prepare test files
        files = [
            {
                "lfn": "test_file1.root",
                "pfn": "/path/to/test_file1.root", 
                "size": 1024,
                "checksum": "ad:12345678",
                "scope": "user.test"
            },
            {
                "lfn": "test_file2.root",
                "pfn": "/path/to/test_file2.root",
                "size": 2048, 
                "checksum": "ad:87654321",
                "scope": "user.test"
            }
        ]
        
        # Execute workflow
        result = self.orchestrator.execute_workflow(
            dataset_name="user.test.workflow_dataset",
            files=files,
            rse="TEST_RSE"
        )
        
        # Check result
        self.assertTrue(result.success)
        self.assertTrue(result.dataset_created)
        self.assertEqual(result.files_registered, 2)
        self.assertEqual(result.files_added_to_dataset, 2)
        self.assertTrue(result.dataset_closed)
        
        # Verify workflow completion
        self.assertTrue(
            self.orchestrator.verify_workflow_completion(
                "user.test.workflow_dataset",
                2
            )
        )


if __name__ == "__main__":
    # Configure logging for tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run tests
    unittest.main(verbosity=2)
