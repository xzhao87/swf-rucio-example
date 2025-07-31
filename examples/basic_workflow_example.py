#!/usr/bin/env python3
"""
Example script demonstrating the complete Rucio workflow as described in README.dm.

This script shows how to:
1. Create an empty and OPEN rucio dataset
2. Register files with existing PFNs
3. Add these files to the dataset
4. Close the dataset
"""

import logging
import sys
import os
from typing import List, Dict, Any

# Add the parent directory to the path to import our package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rucio_workflow import WorkflowOrchestrator, FileInfo
from rucio_workflow.exceptions import WorkflowExecutionError


def setup_logging(level=logging.INFO):
    """Set up logging configuration."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('workflow_example.log')
        ]
    )


def create_example_files() -> List[Dict[str, Any]]:
    """
    Create example file information.
    
    In a real scenario, these would be files that already exist on storage
    with known PFNs, but Rucio doesn't know about them yet.
    """
    files = [
        {
            "lfn": "example_data_001.root",
            "pfn": "root://storage.example.com:1094//data/example_data_001.root",
            "size": 1024 * 1024 * 100,  # 100 MB
            "checksum": "ad:12345678",
            "scope": "user.testuser",
            "metadata": {
                "events": 10000,
                "campaign": "test_campaign_2024"
            }
        },
        {
            "lfn": "example_data_002.root", 
            "pfn": "root://storage.example.com:1094//data/example_data_002.root",
            "size": 1024 * 1024 * 150,  # 150 MB
            "checksum": "ad:87654321",
            "scope": "user.testuser",
            "metadata": {
                "events": 15000,
                "campaign": "test_campaign_2024"
            }
        },
        {
            "lfn": "example_data_003.root",
            "pfn": "root://storage.example.com:1094//data/example_data_003.root", 
            "size": 1024 * 1024 * 80,   # 80 MB
            "checksum": "ad:abcdef12",
            "scope": "user.testuser",
            "metadata": {
                "events": 8000,
                "campaign": "test_campaign_2024"
            }
        }
    ]
    
    return files


def main():
    """Main execution function."""
    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Rucio Workflow Example")
        logger.info("=" * 50)
        
        # Configuration
        dataset_name = "user.testuser.example_dataset_2024"
        rse = "EXAMPLE_RSE"  # Replace with actual RSE name
        
        # Create workflow orchestrator
        # Note: In production, you would pass a real Rucio client
        orchestrator = WorkflowOrchestrator(logger=logger)
        
        # Prepare example files
        files = create_example_files()
        logger.info(f"Prepared {len(files)} example files")
        
        # Dataset metadata
        dataset_metadata = {
            "campaign": "test_campaign_2024",
            "task_id": "12345",
            "description": "Example dataset for workflow demonstration"
        }
        
        # Execute the complete workflow
        logger.info("Executing complete Rucio workflow...")
        result = orchestrator.execute_workflow(
            dataset_name=dataset_name,
            files=files,
            rse=rse,
            dataset_metadata=dataset_metadata,
            dataset_lifetime_days=30,  # Keep for 30 days
            batch_size=100
        )
        
        # Display results
        logger.info("=" * 50)
        logger.info("WORKFLOW RESULTS")
        logger.info("=" * 50)
        logger.info(f"Success: {result.success}")
        logger.info(f"Dataset Created: {result.dataset_created}")
        logger.info(f"Dataset Info: {result.dataset_info}")
        logger.info(f"Files Registered: {result.files_registered}")
        logger.info(f"Files Added to Dataset: {result.files_added_to_dataset}")
        logger.info(f"Dataset Closed: {result.dataset_closed}")
        logger.info(f"Duration: {result.get_duration():.2f} seconds")
        
        if result.errors:
            logger.error("Errors encountered:")
            for error in result.errors:
                logger.error(f"  - {error}")
                
        if result.success:
            logger.info("Workflow completed successfully!")
            
            # Verify the workflow
            logger.info("Verifying workflow completion...")
            verification_result = orchestrator.verify_workflow_completion(
                dataset_name, len(files)
            )
            logger.info(f"Verification result: {verification_result}")
            
        else:
            logger.error("Workflow failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
        
    return 0


if __name__ == "__main__":
    sys.exit(main())
