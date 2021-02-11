import logging
import requests
import os
from azure.storage.filedatalake import DataLakeServiceClient
import boto3
from botocore.exceptions import NoCredentialsError
import azure.functions as func
import sys
import time
from azure.storage.blob import BlobServiceClient


def initialize_storage_account(storage_account_name, storage_account_key):
    try:
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        logging.info(e)

def download_file_from_directory(source, path, file_name):
    try:
        file_system_client = service_client.get_file_system_client(file_system=source)
        # directory_client = file_system_client.get_directory_client("callback")
        local_file = open(path, 'wb')
        file_client = file_system_client.get_file_client(file_name)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        local_file.write(downloaded_bytes)
        local_file.close()
        logging.info(os.listdir(path))
    except Exception as e:
        logging.info(e)

def delete_file_from_directory(source, f_name):
    try:
        file_system_client = service_client.get_file_system_client(file_system=source)
        file_client = file_system_client.delete_file(f_name)
    except Exception as e:
        logging.info(e)

def copy_file_from_directory(source, account_name, file_name, container_name):
    try:
        CONNECTION_STRING = os.environ["adlcertadls2storage"]
    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)

    status = None
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    source_blob = f"https://{account_name}.blob.core.windows.net/{source}/{file_name}"
    copied_blob = blob_service_client.get_blob_client(container_name, file_name)
    logging.info(copied_blob)
    # Copy started
    copied_blob.start_copy_from_url(source_blob)
    for i in range(10):
        props = copied_blob.get_blob_properties()
        logging.info(props)
        status = props.copy.status
        logging.info("Copy status: " + status)
        if status == "success":
            # Copy finished
            return status
            break
        time.sleep(10)

    if status != "success":
        # if not finished after 100s, cancel the operation
        props = copied_blob.get_blob_properties()
        logging.info(props.copy.status)
        copy_id = props.copy.id
        copied_blob.abort_copy(copy_id)
        props = copied_blob.get_blob_properties()
        logging.info(props.copy.status)
        return status

def upload_to_aws(key, path, f_name, bucket):
    ACCESS_KEY = os.getenv('ACCESS_KEY')
    SECRET_KEY = key
    local_file = path
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, f_name)
        logging.info("Upload Successful")
        return True
    except FileNotFoundError:
        logging.info("The file was not found")
        return False
    except NoCredentialsError:
        logging.info("Credentials not available")
        return False

def get_MSI_Token():
    identity_endpoint = os.environ["MSI_ENDPOINT"]
    identity_header = os.environ["MSI_SECRET"]
    logging.info('IDENTITY_ENDPOINT %s', identity_endpoint)
    resource_uri = "https://vault.azure.net"
    token_auth_uri = f"{identity_endpoint}?resource={resource_uri}&api-version=2017-09-01"
    head_msi = {'secret':identity_header}

    resp = requests.get(token_auth_uri, headers=head_msi)
    logging.info('MSI REPSONSE %s', resp)
    access_token = resp.json()['access_token']

    return access_token

def get_kv(access_token, secret_name):
    uri = "https://xxxxxx.vault.azure.net/secrets/" + secret_name
    logging.info(uri)
    headers = {
        "Authorization": 'Bearer ' + access_token
    }
    search_params = {
        'api-version': '2016-10-01'
    }
    try:
        response = requests.get(uri, params=search_params, headers=headers)
        # data = response.text
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except Exception as err:
        logging.info(f'Other error occurred: {err}')
    else:
        data_json = response.json()
        return data_json['value']

def main(myblob: func.InputStream):
    archive = os.getenv('ARCHIVE_CONTAINER')
    source_container = os.getenv('SOURCE_CONTAINER')
    token = get_MSI_Token()
    account_name = os.getenv('STORAGE_ACCOUNT_NAME')
    account_key = get_kv(token, 'storageaccountkey')
    f_name = myblob.name.split('/')[-1]
    logging.info(f_name)
    f_dir = myblob.name.split('/')[:-1]
    logging.info(f_dir)
    path = '/tmp/'
    complete_path = path + f_name
    
    logging.info(f"Python blob trigger function processed blob \n"
                f"Name: {f_name}\n"
                f"DIR: {f_dir}\n"
                f"Blob Size: {myblob.length} bytes")
    logging.info(f"Initalizing Storage Account")
    initialize_storage_account(account_name,account_key)

    logging.info(f"calling download_file_from_directory() function")
    download_file_from_directory(source_container, complete_path, f_name)

    logging.info(os.listdir(path))
    logging.info("Calling upload_to_aws function")
    secret_key = get_kv(token, 'ACCESSKEYRBI')
    bucket_name = os.getenv('Rbi_bucket_name')
    uploaded = upload_to_aws(secret_key, complete_path, f_name, bucket_name)
    logging.info(f"File Uploaded {uploaded}")


    if uploaded:
        logging.info(f"Copying file to {archive}")
        status = copy_file_from_directory(source_container, account_name,f_name, archive)

        if status == "success":
            logging.info(f"Deleting File from ADLS")
            delete_file_from_directory(source_container, f_name)
