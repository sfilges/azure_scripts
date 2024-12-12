from azure.storage.blob import BlobServiceClient

# Source account details
source_connection_string = "Source_Connection_String"
source_container_name = "source-container"
source_blob_name = "blob-name"

# Destination account details
destination_connection_string = "Destination_Connection_String"
destination_container_name = "destination-container"

# Connect to source
source_service = BlobServiceClient.from_connection_string(source_connection_string)
source_blob_client = source_service.get_blob_client(container=source_container_name, blob=source_blob_name)

# Download blob
blob_data = source_blob_client.download_blob().readall()

# Connect to destination
destination_service = BlobServiceClient.from_connection_string(destination_connection_string)
destination_blob_client = destination_service.get_blob_client(container=destination_container_name, blob=source_blob_name)

# Upload blob to destination
destination_blob_client.upload_blob(blob_data)
