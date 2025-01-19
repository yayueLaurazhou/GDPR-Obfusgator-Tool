This is a python tool used to process data being ingested to AWS and intercept personally identifiable information (PII). 

It accepts CSV, JSON or Parquet format files in AWS S3 bucket, and it will create a byte-stream object containing an exact copy of the input file but with the
sensitive data replaced with obfuscated strings.

****Installation****

**Clone the Repository:**

git clone https://github.com/yayueLaurazhou/GDPR-Obfusgator-Tool.git
cd GDPR-Obfusgator-Tool

**Install Dependencies:**

Install the required Python libraries listed in requirements.txt:

pip install -r requirements.txt


****Usage****

Use the obfuscator_tool.py obfuscate_file function with the following command:

import obfuscate_file from obfuscator_tool

obscured_byte_stream = obfuscator_file(event)


The event must be a dictionary with given S3 file location and field to obscure

Example:
  
  {
        file_to_obfuscate": "s3://my_bucket/file1.csv",
        "pii_fields": ["name", "email_address"]
  }
