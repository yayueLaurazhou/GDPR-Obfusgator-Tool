This is a python tool used to process data being ingested to AWS and intercept personally identifiable information (PII). 

It accepts CSV, JSON or Parquet format files in AWS S3 bucket, and it will create a byte-stream object containing an exact copy of the input file but with the sensitive data replaced with obfuscated strings.

You can invoke the function in your AWS LAMBDA function or EC2 instance for file obfuscation purpose. 

****Installation****

**Clone the Repository:**

```
git clone https://github.com/yayueLaurazhou/GDPR-Obfusgator-Tool.git
cd GDPR-Obfusgator-Tool
```

**Install Dependencies:**

Install the required Python libraries listed in requirements.txt:

```pip install -r -W requirements.txt```

**Run Tests:**

To validate the functionality of the tool, you can run the tests provided in the test.py file. Use the following command:

```pytest test.py```

The test.py file contains unit tests and integration tests designed to verify:

File Processing: Ensures that input files are correctly read and obfuscated.

AWS S3 Integration: Tests the interaction with AWS S3 to ensure files are correctly accessed and written back

****Usage****

You must have a file in CSV, JSON, or Parquet format in S3 bucket to use this function 
After uploading the file in S3 bucket, you will need to pass the S3 URI to the function. The S3 URI is in the format:
```s3://<bucket_name>/<file_key>```
For example, if your bucket is named my-obfuscation-bucket and the file is named student_data.csv, the S3 URI would be:
```s3://my-obfuscation-bucket/student_data.csv```

Below are 2 examples of how to use the function

1. Command line usage

--file: Required argument for the S3 file path.
--pii-fields: Required argument for the list of PII fields to obfuscate. You can pass multiple fields separated by spaces.

For example, to Obfuscate a CSV file from S3, obfuscate name and email_address field:
```python obfuscate_tool.py --file "s3://my_bucket/file1.csv" --pii-fields name email_address```

2. Integrate the function in your project by invoking the obfuscator_tool.py obfuscate_file function in other file:

```
import obfuscate_file from obfuscator_tool

obscured_byte_stream = obfuscator_file(event)
```

The event must be a dictionary with given S3 file location and field to obscure

Example:
```
  {
        "file_to_obfuscate": "s3://my_bucket/file1.csv", # besides csv, can also be JSON or Parquet files
        "pii_fields": ["name", "email_address"]
  }
```

The obscured_byte_stream is of io.BytesIO type, which contains the Byte-stream of the obfuscated file content

****Accessing the Obfuscated File Output****

When you run the obfuscation tool, the obfuscated file content is returned as a byte stream (io.BytesIO). The byte stream can be handled in various ways, depending on what you want to do with the obfuscated data. Below are examples of how to access and save the output.

1. Reading the Byte Stream Directly:

If you want to read the contents of the byte stream directly:

```
# Access the byte stream data
data = obfuscated_file.getvalue()

# Print the raw byte data (for debugging purposes, usually not recommended for large files)
print(data)

# If it's a text-based format (e.g., CSV, JSON), you can decode it:
decoded_data = data.decode('utf-8')
print(decoded_data)  # This will print the obfuscated data in human-readable form
```

This can be useful if you just need to preview the obfuscated data without saving it to a file.

2. Sending the Byte Stream to Another Service:
If you want to upload the obfuscated file back to S3 or send it via an API, you can pass the BytesIO object directly to another function that accepts file-like objects:

```
import boto3

# Initialize S3 client
s3_client = boto3.client('s3')

# Save the obfuscated content back to S3
s3_client.put_object(Bucket='your_bucket_name', Key='obfuscated_output.csv', Body=obfuscated_file)
```

****Working with Different File Formats****

The tool handles multiple file formats (CSV, JSON, and Parquet). The byte stream will contain the obfuscated content in the same format as the input file. You can use appropriate libraries (e.g., pandas for CSV, json for JSON, or pyarrow for Parquet) to further process or inspect the content if necessary.


