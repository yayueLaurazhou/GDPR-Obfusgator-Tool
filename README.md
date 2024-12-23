This is a python tool used to process data being ingested to AWS and intercept personally identifiable information (PII). 
It accepts CSV, JSON or Parquet format files in AWS S3 bucket, and it will create a byte-stream object containing an exact copy of the input file but with the
sensitive data replaced with obfuscated strings.
