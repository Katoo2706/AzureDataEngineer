# pip install azure-storage-blob azure-identity
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


class AzureStorage:
    def __init__(self, storage_account: str, access_token: str):
        self.storage_account = storage_account

        # DefaultAzureCredential uses environment variables, managed identities, or shared token cache
        self.account_url = f"https://{self.storage_account}.blob.core.windows.net"
        self.token = access_token

        # Create a BlobServiceClient using DefaultAzureCredential
        # self.credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(account_url=self.account_url,
                                                     credential=self.token)

    def read_all_files_blob(self, container_name: str):
        # Create a ContainerClient to work with a specific container
        container_client = self.blob_service_client.get_container_client(container_name)

        # List the blobs in the container
        blob = container_client.list_blobs()

        # Iterate through the blobs and find the CSV file
        for blob in blob:
            print(blob.name)

    def get_file(self, container_name: str, blob_name: str):
        blob_client = self.blob_service_client.get_blob_client(container=container_name,
                                                               blob=blob_name)
        # Download the CSV file to a bytes buffer # in temporary memory
        download_stream = blob_client.download_blob()
        csv_data = download_stream.readall()

        import io
        # Read the CSV data from the buffer into a Pandas DataFrame
        df = pd.read_csv(io.BytesIO(csv_data))

    def upload_file_blob(self, container_name: str, local_path: str, local_file_name: str):
        """
        Upload a file to container in Azure Storage Account
        :param container_name: The container name
        :param local_path: Local path, for example: /folder_name
        :param local_file_name: file.csv
        :return:
        """
        blob_client = self.blob_service_client.get_blob_client(container=container_name,
                                                               blob=local_file_name)

        upload_file_path = os.path.join(local_path, local_file_name)

        # Upload the created file
        try:
            with open(file=upload_file_path, mode="rb") as data:
                print("Uploading to Azure Storage as blob:" + local_file_name)
                blob_client.upload_blob(data)
        except ResourceExistsError:
            print(upload_file_path + " already existed, overwriting...")
            with open(file=upload_file_path, mode="rb") as data:
                blob_client.upload_blob(data, overwrite=True)

        print("Upload file successfully")

    def upload_fol_blob(self,
                        container_name: str,
                        local_path: str):
        """
        Upload both file or folder to container

        :param container_name:
        :param local_path:
        :return: message
        """
        for root, _, files in os.walk(local_path):
            for file in files:
                if file != ".DS_Store":
                    local_file_path = os.path.join(root, file)

                    # get relative path from local file path
                    blob_name = os.path.relpath(local_file_path, local_path)
                    try:
                        self.upload_file_blob(container_name=container_name,
                                              local_path=local_path,
                                              local_file_name=blob_name)
                    except Exception as e:
                        print(e)

        # local_path = local_path
        # file_list = os.listdir(local_path)
        # for file in file_list:
        #     file_path = os.path.join(local_path, file)
        #     if os.path.isfile(file_path):
        #         if file != ".DS_Store":
        #             azure_ins.upload_blob(container_name=container_name,
        #                                   local_path=local_path,
        #                                   local_file_name=file)  # file as blob
        #     if os.path.isdir(file_path):
        #         for _file in os.listdir(file_path):
        #             if _file != ".DS_Store":
        #                 azure_ins.upload_blob(container_name=container_name,
        #                                       local_path=local_path,
        #                                       local_file_name=f"{file}/{_file}")  # file is in sub-folder
