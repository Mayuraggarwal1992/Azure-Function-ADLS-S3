# Azure-Function-ADLS-S3
This function shows how to move resources from Azure ADLS to AWS S3
- Get ADLS service client using Storage account Access keys.
- Download the file in /tmp folder.
- Will call the upload function to move files to AWS S3.
- Move the source file from parent directory to archive container.
- Delete the source file present in parent directory.
