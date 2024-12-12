import argparse
from azure.storage.blob import BlobServiceClient, ContainerClient

def change_blob_tiers(account_name, account_key, container_name, tier, prefix=None, file_extension=None):
    """
    Change the access tier of blobs in a specified Azure Storage container.

    Parameters:
    - account_name (str): The name of the Azure Storage account.
    - account_key (str): The key for the Azure Storage account.
    - container_name (str): The name of the container whose blobs' tiers will be changed.
    - tier (str): The desired access tier for the blobs ('Hot', 'Cool', 'Cold' or 'Archive').
    - prefix (str): Optional. The virtual folder prefix to filter blobs.
    - file_extension (str): Optional. The file extension to filter blobs (e.g., '.fastq.gz').
    """
    # Construct the blob service client using the account name and key
    account_url = f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # Set blob tier for blobs in the container, filtered by prefix if specified
    for blob in container_client.list_blobs(name_starts_with=prefix):
        # Skip if file extension doesn't match
        if file_extension and not blob.name.endswith(file_extension):
            continue
            
        blob_client = container_client.get_blob_client(blob)
        
        # Get blob properties to check the blob type
        properties = blob_client.get_blob_properties()
        
        # Only change tier for block blobs
        if properties.blob_type == "BlockBlob":
            try:
                blob_client.set_standard_blob_tier(tier)
                print(f"Updated tier for blob: {blob.name}")
            except Exception as e:
                print(f"Failed to update tier for blob {blob.name}: {str(e)}")
        else:
            print(f"Skipping {blob.name}: Not a block blob (type: {properties.blob_type})")

def main():
    parser = argparse.ArgumentParser(description='Change Azure Blob Storage access tier')
    parser.add_argument('--account-name', required=True, help='Azure Storage account name')
    parser.add_argument('--account-key', required=True, help='Azure Storage account key')
    parser.add_argument('--container-name', required=True, help='Container name')
    parser.add_argument('--tier', choices=['Hot', 'Cool', 'Cold', 'Archive'], default='Cold',
                      help='Desired access tier (Hot, Cool, Cold or Archive). Defaults to Cold')
    parser.add_argument('--prefix', help='Optional virtual folder prefix (e.g., "folder1/subfolder/")')
    parser.add_argument('--file-extension', help='Optional file extension filter (e.g., ".fastq.gz")')

    args = parser.parse_args()
    
    try:
        change_blob_tiers(args.account_name, args.account_key, 
                         args.container_name, args.tier, args.prefix,
                         args.file_extension)
        print("Successfully updated all blob tiers")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()

# Usage:
# Run this script from the command line with the following arguments:
# python change_access_tier.py --account-name your-account-name --account-key your-account-key --container-name your-container-name --tier Cool --prefix folder1/subfolder/ --file-extension .fastq.gz
