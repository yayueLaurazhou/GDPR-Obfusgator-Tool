import pytest
import io
import json
import pandas as pd
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError
from obfuscate_tool import (
    obfuscate_file,
    parse_s3_path,
    get_file_type,
    process_csv,
    process_json,
    process_parquet,
    convert_to_csv_stream,
    convert_to_json_stream,
    convert_to_parquet_stream,
)

SAMPLE_EVENT_CSV = {
    "file_to_obfuscate": "s3://test-bucket/sample.csv",
    "pii_fields": ["name", "email_address"]
}
SAMPLE_EVENT_JSON = {
    "file_to_obfuscate": "s3://test-bucket/sample.json",
    "pii_fields": ["name", "email"]
}
SAMPLE_EVENT_PARQUET = {
    "file_to_obfuscate": "s3://test-bucket/sample.parquet",
    "pii_fields": ["name", "email"]
}

SAMPLE_CSV = """student_id,name,course,graduation_date,email_address
1234,John Smith,Software,2024-03-31,j.smith@email.com
5678,Jane Doe,Data Science,2024-06-30,jane.doe@email.com"""
EXPECTED_CSV_OUTPUT = """student_id,name,course,graduation_date,email_address
1234,***,Software,2024-03-31,***
5678,***,Data Science,2024-06-30,***"""

SAMPLE_JSON = [
    {"id": 1, "name": "John Smith", "email": "john.smith@email.com"},
    {"id": 2, "name": "Jane Doe", "email": "jane.doe@email.com"}
]
EXPECTED_JSON_OUTPUT = [
    {"id": 1, "name": "***", "email": "***"},
    {"id": 2, "name": "***", "email": "***"}
]

SAMPLE_PARQUET = pd.DataFrame([
    {"id": 1, "name": "John Smith", "email": "john.smith@email.com"},
    {"id": 2, "name": "Jane Doe", "email": "jane.doe@email.com"}
])
EXPECTED_PARQUET_OUTPUT = pd.DataFrame([
    {"id": 1, "name": "***", "email": "***"},
    {"id": 2, "name": "***", "email": "***"}
])

# Mock S3 setup
@pytest.fixture
def s3_mock_setup():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        s3.put_object(Bucket="test-bucket", Key="sample.csv", Body=SAMPLE_CSV)
        s3.put_object(Bucket="test-bucket", Key="sample.json", Body=json.dumps(SAMPLE_JSON))
        buffer = io.BytesIO()
        SAMPLE_PARQUET.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket="test-bucket", Key="sample.parquet", Body=buffer.read())
        yield s3


# Test parse_s3_path
def test_parse_s3_path_valid():
    bucket, key = parse_s3_path("s3://test-bucket/sample.csv")
    assert bucket == "test-bucket"
    assert key == "sample.csv"

def test_parse_s3_path_invalid():
    with pytest.raises(ValueError):
        parse_s3_path("invalid_path/sample.csv")


# Test file type detection
def test_get_file_type():
    assert get_file_type("sample.csv") == "csv"
    assert get_file_type("sample.json") == "json"
    assert get_file_type("sample.parquet") == "parquet"
    assert get_file_type("sample.txt") == "unknown"


# Test process_csv
def test_process_csv():
    obfuscated_data = process_csv(SAMPLE_CSV.encode("utf-8"), ["name", "email_address"])
    output_stream = convert_to_csv_stream(obfuscated_data).getvalue().decode("utf-8")
    assert output_stream.strip() == EXPECTED_CSV_OUTPUT.strip()


# Test process_json
def test_process_json():
    obfuscated_data = process_json(json.dumps(SAMPLE_JSON).encode("utf-8"), ["name", "email"])
    output_stream = json.loads(convert_to_json_stream(obfuscated_data).getvalue().decode("utf-8"))
    assert output_stream == EXPECTED_JSON_OUTPUT


# Test process_parquet
def test_process_parquet():
    buffer = io.BytesIO()
    SAMPLE_PARQUET.to_parquet(buffer, index=False)
    buffer.seek(0)

    obfuscated_data = process_parquet(buffer.read(), ["name", "email"])
    assert obfuscated_data.equals(EXPECTED_PARQUET_OUTPUT)

    output_stream = convert_to_parquet_stream(obfuscated_data)
    result_df = pd.read_parquet(output_stream)
    assert result_df.equals(EXPECTED_PARQUET_OUTPUT)


# End-to-end test for obfuscate_file
@pytest.mark.parametrize("event,expected_output", [
    (SAMPLE_EVENT_CSV, EXPECTED_CSV_OUTPUT),
    (SAMPLE_EVENT_JSON, json.dumps(EXPECTED_JSON_OUTPUT, indent=2)),
    (SAMPLE_EVENT_PARQUET, EXPECTED_PARQUET_OUTPUT)
])
def test_obfuscate_file(event, expected_output, s3_mock_setup):
    output_stream = obfuscate_file(event)
    file_type = get_file_type(event["file_to_obfuscate"])

    if file_type == "csv":
        result = output_stream.getvalue().decode("utf-8")
        assert result.strip() == expected_output.strip()
    elif file_type == "json":
        result = json.loads(output_stream.getvalue().decode("utf-8"))
        assert result == json.loads(expected_output)
    elif file_type == "parquet":
        result_df = pd.read_parquet(output_stream)
        assert result_df.equals(expected_output)


# ************** New Tests Added *************
# Error-Handling Tests
def test_missing_file_to_obfuscate():
    invalid_event = {"pii_fields": ["name", "email"]}
    with pytest.raises(ValueError, match="'file_to_obfuscate' must be provided"):
        obfuscate_file(invalid_event)

def test_empty_pii_fields():
    invalid_event = {"file_to_obfuscate": "s3://test-bucket/sample.csv", "pii_fields": []}
    with pytest.raises(ValueError, match="'pii_fields' must be a non-empty list"):
        obfuscate_file(invalid_event)

def test_invalid_s3_path():
    invalid_event = {"file_to_obfuscate": "invalid_path/sample.csv", "pii_fields": ["name"]}
    with pytest.raises(ValueError, match="Invalid S3 file path"):
        obfuscate_file(invalid_event)

def test_unsupported_file_type():
    invalid_event = {"file_to_obfuscate": "s3://test-bucket/sample.txt", "pii_fields": ["name"]}
    with pytest.raises(ValueError, match="Unsupported file type"):
        obfuscate_file(invalid_event)

def test_s3_file_not_found(s3_mock_setup):
    invalid_event = {"file_to_obfuscate": "s3://test-bucket/nonexistent.csv", "pii_fields": ["name"]}
    with pytest.raises(FileNotFoundError, match="does not exist in bucket"):
        obfuscate_file(invalid_event)

