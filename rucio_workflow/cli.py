#!/usr/bin/env python3
"""
Command-line interface for the Rucio Workflow package.

This module provides a CLI for common operations without requiring
direct Python scripting.
"""

import sys
import argparse
import logging
import json
from typing import List, Dict, Any

from .workflow_orchestrator import WorkflowOrchestrator
from .dataset_manager import DatasetManager
from .file_manager import FileManager, FileInfo
from .config import get_config, setup_logging
from .exceptions import RucioWorkflowError


def setup_cli_logging(verbose: bool = False):
    """Set up logging for CLI operations."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(levelname)s: %(message)s'
    )


def create_dataset_command(args):
    """Handle create-dataset command."""
    try:
        manager = DatasetManager()
        
        metadata = {}
        if args.metadata:
            metadata = json.loads(args.metadata)
            
        result = manager.create_dataset(
            dataset_name=args.dataset_name,
            metadata=metadata,
            lifetime_days=args.lifetime
        )
        
        print(f"Dataset created successfully: {args.dataset_name}")
        if args.verbose:
            print(f"Details: {result}")
            
    except Exception as e:
        print(f"Error creating dataset: {e}", file=sys.stderr)
        return 1
        
    return 0


def close_dataset_command(args):
    """Handle close-dataset command."""
    try:
        manager = DatasetManager()
        success = manager.close_dataset(args.dataset_name)
        
        if success:
            print(f"Dataset closed successfully: {args.dataset_name}")
        else:
            print(f"Failed to close dataset: {args.dataset_name}", file=sys.stderr)
            return 1
            
    except Exception as e:
        print(f"Error closing dataset: {e}", file=sys.stderr)
        return 1
        
    return 0


def register_files_command(args):
    """Handle register-files command."""
    try:
        # Read file list from JSON file or stdin
        if args.file_list == "-":
            file_data = json.load(sys.stdin)
        else:
            with open(args.file_list, 'r') as f:
                file_data = json.load(f)
                
        # Convert to FileInfo objects
        files = []
        for file_info in file_data:
            files.append(FileInfo(
                lfn=file_info["lfn"],
                pfn=file_info["pfn"],
                size=file_info["size"],
                checksum=file_info["checksum"],
                scope=file_info.get("scope"),
                events=file_info.get("events"),
                file_type=file_info.get("file_type", "data")
            ))
            
        # Register files
        manager = FileManager()
        results = manager.register_multiple_files(files, args.rse)
        
        successful = sum(1 for r in results if r)
        print(f"Registered {successful}/{len(files)} files successfully")
        
        if args.verbose:
            for i, (file_info, success) in enumerate(zip(files, results)):
                status = "✓" if success else "✗"
                print(f"  {status} {file_info.lfn}")
                
    except Exception as e:
        print(f"Error registering files: {e}", file=sys.stderr)
        return 1
        
    return 0


def attach_files_command(args):
    """Handle attach-files command."""
    try:
        # Read file list from JSON file or stdin
        if args.file_list == "-":
            file_data = json.load(sys.stdin)
        else:
            with open(args.file_list, 'r') as f:
                file_data = json.load(f)
                
        # Convert file data to the format expected by attach_files
        files = []
        for file_info in file_data:
            if isinstance(file_info, dict):
                # Support different input formats
                if 'scope' in file_info and 'name' in file_info:
                    # Direct scope/name format
                    files.append({
                        'scope': file_info['scope'],
                        'name': file_info['name']
                    })
                elif 'scope' in file_info and 'lfn' in file_info:
                    # scope/lfn format
                    files.append({
                        'scope': file_info['scope'],
                        'name': file_info['lfn']
                    })
                elif 'lfn' in file_info:
                    # Extract scope from lfn if needed
                    lfn = file_info['lfn']
                    if '.' in lfn and lfn.count('.') >= 2:
                        # Assume format like "scope.rest_of_name"
                        scope = file_info.get('scope', lfn.split('.')[0] + '.' + lfn.split('.')[1])
                        files.append({
                            'scope': scope,
                            'name': lfn
                        })
                    else:
                        # Use provided scope or default
                        scope = file_info.get('scope', 'user.unknown')
                        files.append({
                            'scope': scope,
                            'name': lfn
                        })
                else:
                    print(f"Warning: Skipping file entry without proper scope/name: {file_info}", file=sys.stderr)
                    continue
            else:
                print(f"Warning: Skipping non-dictionary entry: {file_info}", file=sys.stderr)
                continue
                
        if not files:
            print("No valid files found in file list", file=sys.stderr)
            return 1
            
        # Attach files to dataset
        manager = DatasetManager()
        result = manager.attach_files(
            dataset_name=args.dataset_name,
            files=files,
            rse=args.rse
        )
        
        print(f"Successfully attached {result['files_attached']} files to dataset {result['dataset']}")
        
        if args.rse:
            print(f"Validated files on RSE: {args.rse}")
            
        if args.verbose:
            print("Attached files:")
            for file_info in result['files']:
                print(f"  {file_info['scope']}:{file_info['name']}")
                
    except Exception as e:
        print(f"Error attaching files: {e}", file=sys.stderr)
        return 1
        
    return 0


def execute_workflow_command(args):
    """Handle execute-workflow command."""
    try:
        # Read file list
        if args.file_list == "-":
            file_data = json.load(sys.stdin)
        else:
            with open(args.file_list, 'r') as f:
                file_data = json.load(f)
                
        # Execute workflow
        orchestrator = WorkflowOrchestrator()
        result = orchestrator.execute_workflow(
            dataset_name=args.dataset_name,
            files=file_data,
            rse=args.rse,
            metadata=json.loads(args.metadata) if args.metadata else None
        )
        
        if result.success:
            print(f"Workflow completed successfully")
            print(f"Dataset: {result.dataset_name}")
            print(f"Files registered: {len(result.registered_files)}")
            print(f"Files added to dataset: {len(result.files_added_to_dataset)}")
        else:
            print(f"Workflow failed at step: {result.failed_step}", file=sys.stderr)
            print(f"Error: {result.error_message}", file=sys.stderr)
            return 1
            
    except Exception as e:
        print(f"Error executing workflow: {e}", file=sys.stderr)
        return 1
        
    return 0


def list_dataset_command(args):
    """Handle list-dataset command."""
    try:
        manager = DatasetManager()
        
        if args.files:
            # List files in dataset
            files = manager.list_dataset_files(args.dataset_name, long_format=args.long)
            
            if args.long:
                print(f"Files in dataset {args.dataset_name}:")
                for lfn, attrs in files.items():
                    size = attrs.get('fsize', 'unknown')
                    events = attrs.get('events', 'unknown')
                    print(f"  {lfn} (size: {size}, events: {events})")
            else:
                for lfn in files:
                    print(lfn)
        else:
            # Show dataset metadata
            metadata = manager.get_dataset_metadata(args.dataset_name)
            if metadata:
                print(f"Dataset: {args.dataset_name}")
                for key, value in metadata.items():
                    print(f"  {key}: {value}")
            else:
                print(f"Dataset not found: {args.dataset_name}", file=sys.stderr)
                return 1
                
    except Exception as e:
        print(f"Error listing dataset: {e}", file=sys.stderr)
        return 1
        
    return 0


def config_command(args):
    """Handle config command."""
    try:
        config = get_config()
        
        if args.validate:
            valid = config.validate_config()
            if valid:
                print("Configuration is valid")
            else:
                print("Configuration has errors", file=sys.stderr)
                return 1
        else:
            config.print_config_summary()
            
    except Exception as e:
        print(f"Error with configuration: {e}", file=sys.stderr)
        return 1
        
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Rucio Workflow CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a dataset
  rucio-workflow create-dataset user.pilot.test_dataset

  # Register files from JSON
  rucio-workflow register-files --rse ATLAS_DISK --file-list files.json

  # Attach existing files to a dataset
  rucio-workflow attach-files user.pilot.test_dataset --file-list files.json --rse ATLAS_DISK

  # Execute complete workflow  
  rucio-workflow execute-workflow user.pilot.output --rse ATLAS_DISK --file-list files.json

  # List dataset contents
  rucio-workflow list-dataset user.pilot.test_dataset --files --long

  # Check configuration
  rucio-workflow config --validate
        """
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Create dataset command
    create_parser = subparsers.add_parser("create-dataset", help="Create a new dataset")
    create_parser.add_argument(
        "dataset_name", 
        help="Dataset name in format 'scope.name' or 'scope:name' (e.g., 'user.pilot.dataset' or 'user.pilot:dataset.name')"
    )
    create_parser.add_argument("--metadata", help="Dataset metadata as JSON string")
    create_parser.add_argument("--lifetime", type=int, default=30, help="Dataset lifetime in days")
    create_parser.set_defaults(func=create_dataset_command)
    
    # Close dataset command
    close_parser = subparsers.add_parser("close-dataset", help="Close a dataset")
    close_parser.add_argument(
        "dataset_name", 
        help="Dataset name in format 'scope.name' or 'scope:name'"
    )
    close_parser.set_defaults(func=close_dataset_command)
    
    # Register files command
    register_parser = subparsers.add_parser("register-files", help="Register files")
    register_parser.add_argument("--rse", required=True, help="RSE name")
    register_parser.add_argument("--file-list", required=True, help="JSON file with file list (use - for stdin)")
    register_parser.set_defaults(func=register_files_command)
    
    # Attach files command
    attach_parser = subparsers.add_parser("attach-files", help="Attach existing files to a dataset")
    attach_parser.add_argument(
        "dataset_name",
        help="Dataset name in format 'scope.name' or 'scope:name'"
    )
    attach_parser.add_argument("--file-list", required=True, help="JSON file with file list (use - for stdin)")
    attach_parser.add_argument("--rse", help="RSE name for validation (optional)")
    attach_parser.set_defaults(func=attach_files_command)
    
    # Execute workflow command
    workflow_parser = subparsers.add_parser("execute-workflow", help="Execute complete workflow")
    workflow_parser.add_argument(
        "dataset_name", 
        help="Dataset name in format 'scope.name' or 'scope:name'"
    )
    workflow_parser.add_argument("--rse", required=True, help="RSE name")
    workflow_parser.add_argument("--file-list", required=True, help="JSON file with file list (use - for stdin)")
    workflow_parser.add_argument("--metadata", help="Dataset metadata as JSON string")
    workflow_parser.set_defaults(func=execute_workflow_command)
    
    # List dataset command
    list_parser = subparsers.add_parser("list-dataset", help="List dataset information")
    list_parser.add_argument("dataset_name", help="Name of the dataset")
    list_parser.add_argument("--files", action="store_true", help="List files in dataset")
    list_parser.add_argument("--long", action="store_true", help="Show detailed information")
    list_parser.set_defaults(func=list_dataset_command)
    
    # Config command
    config_parser = subparsers.add_parser("config", help="Show or validate configuration")
    config_parser.add_argument("--validate", action="store_true", help="Validate configuration")
    config_parser.set_defaults(func=config_command)
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
        
    # Set up logging
    setup_cli_logging(args.verbose)
    
    # Execute command
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
