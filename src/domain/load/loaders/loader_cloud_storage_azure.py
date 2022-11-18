import logging
import pandas as pd

from azure.storage.filedatalake import DataLakeServiceClient
from domain.load.loaders.loader import Loader
from domain.load.loaders.loader_cloud_storage import LoaderCloudStorage
from domain.load.loaders.loader_file import LoaderFile

class LoaderCloudStorageAzure(LoaderCloudStorage):
    bucket_url: str
    file_path: str
    file_name: str
    service_client: DataLakeServiceClient

    def __init__(self,
        bucket_url: str,
        file_path: str,
        file_name: str
    ):
        self.bucket_url = bucket_url
        self.file_path = file_path
        self.file_name = file_name

    def init_azure_service_client(self) -> DataLakeServiceClient:
        storage_account_key = 'test'
        self.service_client = DataLakeServiceClient(account_url=self.bucket_url, credential=storage_account_key)

    def upload_to_adls(self) -> bool:
        # https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python
        self.init_azure_service_client()

        # TODO: Change this to the right destination(s)
        fs_client = self.service_client.get_file_system_client(file_system="my-file-system")
        directory_client = fs_client.get_directory_client("my-directory")

        file_client = directory_client.create_file(self.file_name)
        local_file = open(f"{self.file_path}\{self.file_name}", "r")
        file_content = local_file.read()

        file_client.append_data(data=file_content, offset=0, length=len(file_content))
        file_client.flush_data(len(file_content))

        return True

    def load(self,
        data: pd.DataFrame
    ) -> bool:

        upload_success = True
        file_success = super().load(data)

        if (file_success):
            logging.info(f"Uploading {self.file_name} to {self.bucket_url}")
            upload_success = self.upload_to_adls()
        else:
            upload_success = False

        return upload_success