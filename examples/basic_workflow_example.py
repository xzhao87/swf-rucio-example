from rucio_workflow import DatasetManager, FileManager, FileInfo

# Initialize managers
dataset_manager = DatasetManager()
file_manager = FileManager()

# 1. Create a new dataset
dataset_name = "<scope name>:<file name>"
result = dataset_manager.create_dataset(dataset_name=dataset_name)
print(f"Created dataset: {result['duid']}")

# Get dataset metadata to check state
metadata = dataset_manager.get_dataset_metadata(dataset_name)
print(f"Dataset state: {metadata['state']}")

# 2. Register an existing file
file_info = FileInfo(
    lfn="<logical file name>",
    pfn="root://test.com:1094/testpath/testdir/<logical file name>",
    size=10240,
    checksum="ad:28000001",
    scope="<scope name>"
)

# Register the file replica
success = file_manager.register_file_replica(file_info, "TEST_RSE")
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

