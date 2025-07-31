#!/usr/bin/env python3
"""
Advanced example showing individual component usage and custom workflows.

This script demonstrates:
- Using individual managers separately
- Custom error handling
- Advanced file operations
- Monitoring and logging
"""

import logging
import sys
import os
from typing import List, Dict, Any

# Add the parent directory to the path to import our package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rucio_workflow import DatasetManager, FileManager, FileInfo
from rucio_workflow.utils import RucioUtils, ValidationUtils
from rucio_workflow.exceptions import DatasetError, FileRegistrationError


def setup_logging():
    """Set up detailed logging."""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def demonstrate_individual_managers():
    """Demonstrate using individual managers separately."""
    logger = logging.getLogger("advanced_example")
    
    logger.info("=" * 60)
    logger.info("DEMONSTRATING INDIVIDUAL MANAGER USAGE")
    logger.info("=" * 60)
    
    # Initialize managers
    dataset_manager = DatasetManager(logger=logger)
    file_manager = FileManager(logger=logger)
    
    try:
        # Step 1: Dataset operations
        logger.info("Step 1: Dataset Operations")
        logger.info("-" * 30)
        
        dataset_name = "user.advanced.test_dataset"
        
        # Create dataset with custom metadata
        dataset_info = dataset_manager.create_dataset(
            dataset_name=dataset_name,
            metadata={
                "project": "advanced_example",
                "version": "1.0",
                "author": "test_user"
            },
            lifetime_days=7
        )
        logger.info(f"Created dataset: {dataset_info}")
        
        # Get dataset metadata
        metadata = dataset_manager.get_dataset_metadata(dataset_name)
        logger.info(f"Dataset metadata: {metadata}")
        
        # Step 2: File operations
        logger.info("\nStep 2: File Operations")
        logger.info("-" * 30)
        
        # Create file info objects
        files = []
        for i in range(3):
            file_info = FileInfo(
                lfn=f"advanced_file_{i:03d}.dat",
                pfn=f"/storage/path/advanced_file_{i:03d}.dat",
                size=1024 * (i + 1),
                checksum=f"ad:{(12345678 + i):08x}",
                scope="user.advanced",
                events=1000 * (i + 1),
                file_type="data"
            )
            files.append(file_info)
            
        logger.info(f"Created {len(files)} file info objects")
        
        # Register files individually
        rse = "ADVANCED_TEST_RSE"
        for file_info in files:
            success = file_manager.register_file_replica(file_info, rse)
            logger.info(f"Registered {file_info.lfn}: {success}")
            
        # Add files to dataset
        success = file_manager.add_files_to_dataset(files, dataset_name)
        logger.info(f"Added files to dataset: {success}")
        
        # List files in dataset
        dataset_files = dataset_manager.list_dataset_files(dataset_name, long_format=True)
        logger.info(f"Files in dataset: {len(dataset_files)}")
        for lfn, attrs in dataset_files.items():
            logger.info(f"  {lfn}: {attrs['fsize']} bytes")
            
        # Step 3: Close dataset
        logger.info("\nStep 3: Close Dataset")
        logger.info("-" * 30)
        
        close_success = dataset_manager.close_dataset(dataset_name)
        logger.info(f"Dataset closed: {close_success}")
        
        # Verify final state
        final_metadata = dataset_manager.get_dataset_metadata(dataset_name)
        logger.info(f"Final dataset state: {final_metadata.get('state')}")
        
    except Exception as e:
        logger.error(f"Error in individual manager demonstration: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


def demonstrate_utility_functions():
    """Demonstrate utility function usage."""
    logger = logging.getLogger("utils_demo")
    
    logger.info("=" * 60)
    logger.info("DEMONSTRATING UTILITY FUNCTIONS")
    logger.info("=" * 60)
    
    # Scope extraction
    test_datasets = [
        "user.johndoe:my_dataset",
        "user.johndoe.my_dataset2", 
        "group.atlas.physics_data",
        "mc16_13TeV:EVNT.12345._001.pool.root"
    ]
    
    logger.info("Scope Extraction:")
    for dataset in test_datasets:
        scope, name = RucioUtils.extract_scope(dataset)
        logger.info(f"  {dataset} -> scope: {scope}, name: {name}")
        
    # GUID and VUID generation
    logger.info("\nGUID and VUID Generation:")
    guid = RucioUtils.generate_guid()
    logger.info(f"  Generated GUID: {guid}")
    
    vuid = RucioUtils.generate_vuid("user.test", "example_dataset")
    logger.info(f"  Generated VUID: {vuid}")
    
    # Validation examples
    logger.info("\nValidation Examples:")
    
    # Valid cases
    try:
        ValidationUtils.validate_dataset_name("user.test.valid_dataset")
        logger.info("  ✓ Valid dataset name passed")
    except Exception as e:
        logger.error(f"  ✗ Unexpected validation error: {e}")
        
    try:
        ValidationUtils.validate_checksum("md5:abcdef1234567890abcdef1234567890")
        logger.info("  ✓ Valid MD5 checksum passed")
    except Exception as e:
        logger.error(f"  ✗ Unexpected validation error: {e}")
        
    # Invalid cases
    try:
        ValidationUtils.validate_dataset_name("invalid dataset name")
        logger.error("  ✗ Invalid dataset name should have failed")
    except Exception:
        logger.info("  ✓ Invalid dataset name correctly rejected")
        
    try:
        ValidationUtils.validate_checksum("invalid:checksum")
        logger.error("  ✗ Invalid checksum should have failed")
    except Exception:
        logger.info("  ✓ Invalid checksum correctly rejected")


def demonstrate_error_handling():
    """Demonstrate error handling and recovery."""
    logger = logging.getLogger("error_demo")
    
    logger.info("=" * 60) 
    logger.info("DEMONSTRATING ERROR HANDLING")
    logger.info("=" * 60)
    
    dataset_manager = DatasetManager(logger=logger)
    
    # Test dataset creation with invalid name
    try:
        dataset_manager.create_dataset("invalid dataset name")
        logger.error("Should have raised ValidationError")
    except Exception as e:
        logger.info(f"✓ Correctly caught error: {type(e).__name__}: {e}")
        
    # Test getting metadata for non-existent dataset
    try:
        metadata = dataset_manager.get_dataset_metadata("non.existent.dataset")
        if metadata is None:
            logger.info("✓ Correctly returned None for non-existent dataset")
        else:
            logger.error("Should have returned None")
    except Exception as e:
        logger.info(f"✓ Correctly handled error: {type(e).__name__}: {e}")
        
    # Test file creation with invalid parameters
    try:
        FileInfo(
            lfn="",  # Invalid empty LFN
            pfn="/valid/path",
            size=1024,
            checksum="ad:12345678"
        )
        logger.error("Should have raised ValidationError")
    except Exception as e:
        logger.info(f"✓ Correctly caught file validation error: {type(e).__name__}: {e}")


def demonstrate_performance_monitoring():
    """Demonstrate performance monitoring and logging."""
    logger = logging.getLogger("performance_demo")
    
    logger.info("=" * 60)
    logger.info("DEMONSTRATING PERFORMANCE MONITORING") 
    logger.info("=" * 60)
    
    import time
    
    # Simulate batch file operations
    file_manager = FileManager(logger=logger)
    
    # Create many file objects
    start_time = time.time()
    files = []
    for i in range(100):
        file_info = FileInfo(
            lfn=f"perf_test_{i:03d}.dat",
            pfn=f"/storage/perf_test_{i:03d}.dat",
            size=1024 * 1024,  # 1MB each
            checksum=f"ad:{(12345678 + i):08x}",
            scope="user.perf"
        )
        files.append(file_info)
        
    creation_time = time.time() - start_time
    logger.info(f"Created {len(files)} FileInfo objects in {creation_time:.3f} seconds")
    
    # Simulate batch registration
    start_time = time.time()
    # Note: This would fail with a real client, but demonstrates the timing
    try:
        results = file_manager.register_multiple_files(files, "PERF_TEST_RSE", batch_size=50)
        registration_time = time.time() - start_time
        logger.info(f"Batch registration simulation took {registration_time:.3f} seconds")
    except Exception as e:
        registration_time = time.time() - start_time
        logger.info(f"Registration simulation (with expected error) took {registration_time:.3f} seconds")
        logger.info(f"Expected error: {type(e).__name__}")


def main():
    """Main execution function."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Advanced Rucio Workflow Examples")
    
    try:
        # Run all demonstrations
        demonstrate_utility_functions()
        print()  # Add spacing
        
        demonstrate_error_handling()
        print()
        
        demonstrate_performance_monitoring()
        print()
        
        # Note: Commented out as it requires a real Rucio client
        # demonstrate_individual_managers()
        
        logger.info("Advanced examples completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Unexpected error in advanced examples: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
