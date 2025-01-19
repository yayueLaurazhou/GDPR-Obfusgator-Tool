This is a python tool used to process data being ingested to AWS and intercept personally identifiable information (PII). 

It accepts CSV, JSON or Parquet format files in AWS S3 bucket, and it will create a byte-stream object containing an exact copy of the input file but with the sensitive data replaced with obfuscated strings.

****Installation****

**Clone the Repository:**

```
git clone https://github.com/yayueLaurazhou/GDPR-Obfusgator-Tool.git
cd GDPR-Obfusgator-Tool
```

**Install Dependencies:**

Install the required Python libraries listed in requirements.txt:

```pip install -r requirements.txt```

**Run Tests:**

To validate the functionality of the tool, you can run the tests provided in the test.py file. Use the following command:

```pytest test.py```

The test.py file contains unit tests and integration tests designed to verify:

File Processing: Ensures that input files are correctly read and obfuscated.

AWS S3 Integration: Tests the interaction with AWS S3 to ensure files are correctly accessed and written back

****Usage****

Use the obfuscator_tool.py obfuscate_file function with the following command:

```
import obfuscate_file from obfuscator_tool

obscured_byte_stream = obfuscator_file(event)
```


The event must be a dictionary with given S3 file location and field to obscure

Example:
  
  {
        file_to_obfuscate": "s3://my_bucket/file1.csv", # besides csv, can also be JSON or Parquet files
        "pii_fields": ["name", "email_address"]
  }

The obscured_byte_stream is of io.BytesIO type, which contains the Byte-stream of the obfuscated file content


You can invoke the function in your AWS LAMBDA function or EC2 instance for file obfuscation purpose. 
