import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

def upload_file_to_azure(local_file_path, blob_name):
    load_dotenv()  # Load .env file
    
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME")

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    print(f"Uploading {local_file_path} to Azure...")

    with open(local_file_path, "rb") as data:
        container_client.upload_blob(
            name=blob_name,
            data=data,
            overwrite=True
        )

    print(f"Upload complete. Blob name: {blob_name}")

if __name__ == "__main__":
    # Upload your local market data file
    local_path = os.path.join("data", "market_data_raw.csv")
    blob_name = "market_data_raw.csv"
    
    upload_file_to_azure(local_path, blob_name)
