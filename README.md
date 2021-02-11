# Azure-Function-ADLS-S3

This function will do the following:
- Get ADLS service client using Storage account Access keys.
- Download the file in /tmp folder.
- Will call the upload function to move files to S3.
- Move the source file from parent directory to archive container.
- Delete the source file present in parent directory.
