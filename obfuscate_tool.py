# obfuscate_tool_v2.py
import boto3
import csv
import json
import io
import logging
import pandas as pd
import argparse
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def obfuscate_file(event: dict):
    """
    Main function to handle file obfuscation for CSV, JSON, and Parquet.

    Args:
        event (dict): JSON input with S3 file location and PII fields to obfuscate.
                      Example:
                      {
                          "file_to_obfuscate": "s3://my_bucket/file1.csv",
                          "pii_fields": ["name", "email_address"]
                      }

    Returns:
        io.BytesIO: Byte-stream of the obfuscated file content.
    """
   # Extract input parameters
    s3_file_path = event.get("file_to_obfuscate")
    pii_fields = event.get("pii_fields", [])

    # Validate input
    if not s3_file_path:
        raise ValueError("'file_to_obfuscate' must be provided in the input JSON.")
    if not pii_fields or not isinstance(pii_fields, list):
        raise ValueError("'pii_fields' must be a non-empty list.")

    # Extract S3 bucket and key
    try:
        s3_bucket, s3_key = parse_s3_path(s3_file_path)
    except ValueError as e:
        raise ValueError(f"Invalid S3 file path: {s3_file_path}. {str(e)}")

    # Determine file type
    file_type = get_file_type(s3_key)
    if file_type not in ["csv", "json", "parquet"]:
        raise ValueError(f"Unsupported file type: {file_type}. Supported types are: 'csv', 'json', 'parquet'.")

    # Download the file from S3
    try:
        file_content = download_file_from_s3(s3_bucket, s3_key)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            raise FileNotFoundError(f"The file '{s3_key}' does not exist in bucket '{s3_bucket}'.")
        raise  # Re-raise the original exception if it's not a "NoSuchKey" error

    # Obfuscate based on file type
    if file_type == "csv":
        obfuscated_data = process_csv(file_content, pii_fields)
        return convert_to_csv_stream(obfuscated_data)
    elif file_type == "json":
        obfuscated_data = process_json(file_content, pii_fields)
        return convert_to_json_stream(obfuscated_data)
    elif file_type == "parquet":
        obfuscated_data = process_parquet(file_content, pii_fields)
        return convert_to_parquet_stream(obfuscated_data)


def parse_s3_path(s3_path: str):
    """
    Parses the S3 path into bucket name and key.

    Args:
        s3_path (str): S3 path string in the format s3://bucket/key.

    Returns:
        tuple: (bucket_name, key)
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("Invalid S3 path. It must start with 's3://'.")
    
    path_parts = s3_path[5:].split("/", 1)
    if len(path_parts) != 2:
        raise ValueError("S3 path must include both bucket and key.")
    
    return path_parts[0], path_parts[1]


def get_file_type(s3_key: str):
    """
    Determines the file type based on the S3 key extension.

    Args:
        s3_key (str): The S3 key of the file.

    Returns:
        str: File type ('csv', 'json', 'parquet').
    """
    if s3_key.endswith(".csv"):
        return "csv"
    elif s3_key.endswith(".json"):
        return "json"
    elif s3_key.endswith(".parquet"):
        return "parquet"
    else:
        return "unknown"


def download_file_from_s3(bucket: str, key: str):
    """
    Downloads a file from S3 and returns its content as a string or byte-stream.

    Args:
        bucket (str): S3 bucket name.
        key (str): S3 object key.

    Returns:
        str or bytes: File content.
    """
    s3 = boto3.client("s3")
    logger.info(f"Downloading file from S3: bucket={bucket}, key={key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def process_csv(file_content: str, pii_fields: list):
    """
    Processes the CSV content and obfuscates the specified fields.

    Args:
        file_content (str): The original CSV content as a string.
        pii_fields (list): List of fields to obfuscate.

    Returns:
        list: List of obfuscated rows.
    """
    input_stream = io.StringIO(file_content.decode("utf-8"))
    reader = csv.DictReader(input_stream)
    obfuscated_rows = []

    for row in reader:
        for field in pii_fields:
            if field in row:
                row[field] = "***"  # obfuscated text
        obfuscated_rows.append(row)

    return {"header": reader.fieldnames, "rows": obfuscated_rows}


def process_json(file_content: bytes, pii_fields: list):
    """
    Processes the JSON content and obfuscates the specified fields.

    Args:
        file_content (bytes): The original JSON content as bytes.
        pii_fields (list): List of fields to obfuscate.

    Returns:
        list: List of obfuscated JSON records.
    """
    records = json.loads(file_content.decode("utf-8"))
    for record in records:
        for field in pii_fields:
            if field in record:
                record[field] = "***"  # obfuscated text
    return records


def process_parquet(file_content: bytes, pii_fields: list):
    """
    Processes the Parquet content and obfuscates the specified fields.

    Args:
        file_content (bytes): The original Parquet content as bytes.
        pii_fields (list): List of fields to obfuscate.

    Returns:
        pd.DataFrame: Obfuscated DataFrame.
    """
    input_stream = io.BytesIO(file_content)
    df = pd.read_parquet(input_stream)

    for field in pii_fields:
        if field in df.columns:
            df[field] = "***"  # obfuscated text
    return df


def convert_to_csv_stream(obfuscated_data: dict):
    """
    Converts obfuscated CSV data into a byte-stream.

    Args:
        obfuscated_data (dict): Obfuscated CSV data.

    Returns:
        io.BytesIO: Byte-stream of the CSV data.
    """
    output_stream = io.StringIO()
    writer = csv.DictWriter(output_stream, fieldnames=obfuscated_data["header"],lineterminator='\n')
    writer.writeheader()
    writer.writerows(obfuscated_data["rows"])
    return io.BytesIO(output_stream.getvalue().encode("utf-8"))


def convert_to_json_stream(obfuscated_data: list):
    """
    Converts obfuscated JSON data into a byte-stream.

    Args:
        obfuscated_data (list): Obfuscated JSON data.

    Returns:
        io.BytesIO: Byte-stream of the JSON data.
    """
    return io.BytesIO(json.dumps(obfuscated_data, indent=2).encode("utf-8"))


def convert_to_parquet_stream(obfuscated_data: pd.DataFrame):
    """
    Converts obfuscated Parquet data into a byte-stream.

    Args:
        obfuscated_data (pd.DataFrame): Obfuscated Parquet data.

    Returns:
        io.BytesIO: Byte-stream of the Parquet data.
    """
    output_stream = io.BytesIO()
    obfuscated_data.to_parquet(output_stream, index=False)
    output_stream.seek(0)
    return output_stream


def main():
    parser = argparse.ArgumentParser(description="Obfuscate files in S3 bucket.")
    parser.add_argument("--file", required=True, help="S3 path to the file to obfuscate (e.g., s3://bucket/file.csv)")
    parser.add_argument("--pii-fields", required=True, nargs='+', help="List of PII fields to obfuscate (e.g., name email_address)")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Prepare the event dictionary
    event = {
        "file_to_obfuscate": args.file,
        "pii_fields": args.pii_fields
    }

    try:
        obfuscated_file = obfuscate_file(event)
        print(f"Obfuscated file content: {obfuscated_file.getvalue()}")
    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()