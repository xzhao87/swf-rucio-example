# Rucio Workflow Package

A comprehensive Python package for managing Rucio workflows, designed to simplify dataset creation, file registration, and workflow orchestration in high-energy physics data management systems.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Command Line Examples](#command-line-examples)
- [Python API Examples](#python-api-examples)
- [Configuration](#configuration)

## Overview

The Rucio Workflow package provides a high-level interface for common Rucio operations, making it easier to:

- Create and manage datasets
- Register files with existing Physical File Names (PFNs)

## Features

- **Dataset Management**: Create, open, close, and manage Rucio datasets with proper metadata
- **File Registration**: Register files that already exist on storage with custom PFNs
- **Workflow Orchestration**: Complete end-to-end workflow execution
- **Command Line Tools**: Easy-to-use CLI for common operations
- **Python API**: Programmatic access for integration into larger systems
- **Configuration Management**: Flexible configuration via environment variables
- **Error Handling**: Comprehensive error reporting and validation

## Installation

### Prerequisites

- Python 3.7 or higher
- Valid X.509 certificate for authentication
- Access to a Rucio instance
- Properly configured Rucio client

### Install from Source

```bash
# Clone the repository
git clone https://github.com/xzhao87/swf-rucio-example.git
cd swf-rucio-example

# create a virtual env 
python3 -m venv venv
source venv/bin/activate

# Install the package
pip install -e .

# Or install with development and test dependencies
pip install -e ".[dev,test]"

# Set up Rucio environment using provided script
source setup_rucio_env.sh
```

### Verify Installation

```bash
rucio-workflow --help
```

## Command Line Examples

### Basic Workflow: Create Dataset → Register File → Attach File → Close Dataset

#### 1. Create a New Dataset
```bash
# Basic dataset creation
rucio-workflow create-dataset user.yourusername:analysis.dataset.2025

# With custom lifetime (in days) and metadata
rucio-workflow create-dataset user.yourusername:analysis.dataset.2025 \
  --lifetime 60 \
  --metadata '{"project": "higgs_analysis", "version": "v1.0"}' \
  --verbose
```

#### 2. Register an Existing File
```bash
# Create a file list JSON for your existing file
cat > existing_file.json << 'EOF'
[
  {
    "lfn": "my_analysis_output.root",
    "pfn": "root://test.com:1094//testpath/testdir/my_analysis_output.root",
    "size": 10485760,
    "checksum": "ad:deadbeef12345678",
    "scope": "user.yourusername"
  }
]
EOF

# Register the existing file
rucio-workflow register-files --rse TEST_RSE --file-list existing_file.json --verbose
```

#### 3. Attach File to Open Dataset
```bash
# Attach the registered file to your dataset
rucio-workflow attach-files --dataset user.yourusername:analysis.dataset.2025 --file-list existing_file.json
```

#### 4. Close Dataset
```bash
# Close the dataset (no more files can be added)
rucio-workflow close-dataset user.yourusername:analysis.dataset.2025 --verbose
```

### Additional Command Examples

#### Multiple File Registration

(The following examples use fake files and RSE names, please make necessary changes if you want to run a real test)

```bash
# Create a file list for multiple files
cat > multiple_files.json << 'EOF'
[
  {
    "lfn": "data_file_001.root",
    "pfn": "root://test.com:1094//testpath/data/data_file_001.root",
    "size": 2048576,
    "checksum": "ad:a1b2c3d4e5f6",
    "scope": "user.yourusername"
  },
  {
    "lfn": "data_file_002.root", 
    "pfn": "root://test.com:1094//testpath/data/data_file_002.root",
    "size": 1048576,
    "checksum": "ad:e5f6g7h8i9j0",
    "scope": "user.yourusername"
  }
]
EOF

# Register multiple files at once
rucio-workflow register-files --rse TEST_RSE --file-list multiple_files.json --verbose
```

#### Check Dataset Status
```bash
# List created datasets
rucio list-dids user.yourusername:*analysis*

# Check dataset contents
rucio list-content user.yourusername:analysis.dataset.2025

# Get dataset metadata
rucio get-metadata user.yourusername:analysis.dataset.2025
```

## Python API Examples

### Set up the environment 

To use the APIs without installing the whole package, follow the following steps 

```bash
# add rucio client into your environment, e.g.  
source rucio client setup file from cvmfs

# set up relevant environment variables pointing to your rucio instance, refer to the setup_rucio_env.sh file 

# make sure swf-rucio-example package is in your PYTHONPATH 
export PYTHONPATH=<path-to-the-swf-rucio-example>:$PYTHONPATH
```

### Basic Workflow: Create Dataset → Register File → Attach File → Close Dataset

```python
from rucio_workflow import DatasetManager, FileManager, FileInfo

# Initialize managers
dataset_manager = DatasetManager()
file_manager = FileManager()

# 1. Create a new dataset
dataset_name = "user.yourusername:api.analysis.dataset.2025"
result = dataset_manager.create_dataset(
    dataset_name=dataset_name,
    metadata={
        "project": "higgs_analysis", 
        "version": "v1.0",
        "contact": "yourusername@example.com"
    },
    lifetime_days=60
)
print(f"Created dataset: {result['duid']}")
print(f"Dataset state: {result['state']}")

# 2. Register an existing file
file_info = FileInfo(
    lfn="my_analysis_output.root",
    pfn="root://test.com:1094//testpath/testdir/my_analysis_output.root",
    size=10485760,  # 10MB
    checksum="ad:deadbeef12345678",
    scope="user.yourusername"
)

# Register the file replica
success = file_manager.register_file_replica(file_info, "DAQ_DISK_3")
print(f"File registered: {success}")

# 3. Attach file to the open dataset
attachment_success = file_manager.add_files_to_dataset([file_info], dataset_name)
print(f"File attached to dataset: {attachment_success}")

# 4. Close the dataset
close_success = dataset_manager.close_dataset(dataset_name)
print(f"Dataset closed: {close_success}")

# Verify final state
final_metadata = dataset_manager.get_dataset_metadata(dataset_name)
print(f"Final dataset state: {final_metadata['state']}")
print(f"Dataset contains {len(dataset_manager.list_dataset_files(dataset_name))} files")
```

### Advanced Example with Multiple Files

```python
from rucio_workflow import DatasetManager, FileManager, FileInfo

def process_multiple_files():
    """Example of processing multiple existing files."""
    
    # Initialize managers
    dataset_manager = DatasetManager()
    file_manager = FileManager()
    
    # Create dataset for batch processing
    dataset_name = "user.yourusername:batch.analysis.2025"
    dataset_info = dataset_manager.create_dataset(
        dataset_name=dataset_name,
        metadata={
            "project": "batch_analysis",
            "production_tag": "v2.1",
            "systematic": "nominal"
        },
        lifetime_days=90
    )
    print(f"Created batch dataset: {dataset_info['duid']}")
    
    # Define multiple existing files
    existing_files = [
        FileInfo(
            lfn=f"batch_output_{i:03d}.root",
            pfn=f"root://dcintdoor.sdcc.bnl.gov:1094//pnfs/sdcc.bnl.gov/batch/batch_output_{i:03d}.root",
            size=1048576 * (i + 1),  # Varying sizes
            checksum=f"ad:{i:08x}deadbeef",
            scope="user.yourusername"
        )
        for i in range(5)  # 5 files
    ]
    
    # Register all files
    registration_results = []
    for file_info in existing_files:
        success = file_manager.register_file_replica(file_info, "DAQ_DISK_3")
        registration_results.append(success)
        print(f"Registered {file_info.lfn}: {success}")
    
    successful_registrations = sum(registration_results)
    print(f"Successfully registered {successful_registrations}/{len(existing_files)} files")
    
    # Attach all successfully registered files to dataset
    successfully_registered_files = [
        file_info for file_info, success in zip(existing_files, registration_results) if success
    ]
    
    if successfully_registered_files:
        attachment_success = file_manager.add_files_to_dataset(
            successfully_registered_files, 
            dataset_name
        )
        print(f"Attached {len(successfully_registered_files)} files to dataset: {attachment_success}")
    
    # Get dataset status
    files_in_dataset = dataset_manager.list_dataset_files(dataset_name)
    print(f"Dataset now contains {len(files_in_dataset)} files")
    
    # Close dataset when done
    close_success = dataset_manager.close_dataset(dataset_name)
    print(f"Dataset closed: {close_success}")
    
    return {
        "dataset_name": dataset_name,
        "files_registered": successful_registrations,
        "files_in_dataset": len(files_in_dataset),
        "dataset_closed": close_success
    }

# Run the example
if __name__ == "__main__":
    result = process_multiple_files()
    print(f"Batch processing completed: {result}")
```

### Error Handling Example

```python
from rucio_workflow import DatasetManager, FileManager, FileInfo
from rucio_workflow.exceptions import DatasetError, FileRegistrationError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def robust_dataset_workflow():
    """Example with comprehensive error handling."""
    
    dataset_manager = DatasetManager()
    file_manager = FileManager()
    
    dataset_name = "user.yourusername:robust.test.2025"
    
    try:
        # 1. Create dataset with error handling
        try:
            result = dataset_manager.create_dataset(
                dataset_name=dataset_name,
                metadata={"test": "robust_handling"},
                lifetime_days=30
            )
            logger.info(f"Dataset created successfully: {result['duid']}")
        except DatasetError as e:
            logger.error(f"Failed to create dataset: {e}")
            return False
        
        # 2. Register file with error handling
        file_info = FileInfo(
            lfn="test_file.root",
            pfn="root://dcintdoor.sdcc.bnl.gov:1094//pnfs/test/test_file.root",
            size=1024,
            checksum="ad:testchecksum",
            scope="user.yourusername"
        )
        
        try:
            success = file_manager.register_file_replica(file_info, "DAQ_DISK_3")
            if success:
                logger.info("File registered successfully")
            else:
                logger.warning("File registration returned False")
        except FileRegistrationError as e:
            logger.error(f"Failed to register file: {e}")
            return False
        
        # 3. Attach file to dataset
        try:
            attachment_success = file_manager.add_files_to_dataset([file_info], dataset_name)
            logger.info(f"File attachment: {attachment_success}")
        except Exception as e:
            logger.error(f"Failed to attach file to dataset: {e}")
        
        # 4. Close dataset
        try:
            close_success = dataset_manager.close_dataset(dataset_name)
            logger.info(f"Dataset close: {close_success}")
        except DatasetError as e:
            logger.error(f"Failed to close dataset: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in workflow: {e}")
        return False

# Run robust example
if __name__ == "__main__":
    success = robust_dataset_workflow()
    print(f"Robust workflow completed successfully: {success}")
```

## Configuration

### Environment Variables

```bash
# Rucio server configuration
export RUCIO_RUCIO_HOST=https://rucio-server.example.com
export RUCIO_AUTH_HOST=https://rucio-auth.example.com
export RUCIO_ACCOUNT=your_account

# Authentication
export X509_USER_CERT=/path/to/cert.pem
export X509_USER_KEY=/path/to/key.pem
export X509_CERT_DIR=/etc/grid-security/certificates

# Workflow defaults
export RUCIO_DEFAULT_RSE=TEST_RSE
export RUCIO_DEFAULT_SCOPE=user.yourusername

# Performance tuning
export RUCIO_BATCH_SIZE=100
export RUCIO_REQUEST_TIMEOUT=600
export RUCIO_MAX_RETRIES=3

# Logging
export RUCIO_LOG_LEVEL=INFO
export RUCIO_ENABLE_DETAILED_LOGGING=false
```

### Configuration via Python

```python
from rucio_workflow.config import get_config

# Get configuration manager
config = get_config()

# Print current configuration
config.print_config_summary()

# Validate configuration
if config.validate_config():
    print("Configuration is valid")
else:
    print("Configuration has errors")
```

---

**Version**: 0.1.0  
**License**: Apache License 2.0  
**Author**: Xin Zhao (xzhao@bnl.gov)
