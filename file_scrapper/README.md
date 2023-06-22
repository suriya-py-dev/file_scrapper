# File Scrapper
[![PyPI version](https://badge.fury.io/py/your-module-name.svg)](https://badge.fury.io/py/file_scrapper)

# Description

Module is used to upload the documents, media and images to cloud which is like AWS and GCS.

## Features

    - Media and document will be upload to AWS and images will be upload to GCS.
    - If you want to upload all the files to particular cloud you can 
      use all_to_aws or all_to_gcs depending on your requirement

## Usage

    - Install your package using the below command
       **pip install file_scrapper**

    -  set up the ENV variables to connect AWS and GCS
    -  AWS_ACCESS_KEY - this is used to store the access key
    -  AWS_SECRET_KEY - this is used to store the secret key
    -  GCS_PROJCT_ID  - this is used to store GCS project id.
    
    -  from file_scrapper.file_walker.walker import FileWalker
        walker = FileWalker()
        walker.walk(your_dir_name, gcs_bucket_name, aws_bucket_name)
    -  Upload files only to AWS:
       walker.walk(your_dir_name, aws_bucket_name=bucket_name, all_to_aws=True)
