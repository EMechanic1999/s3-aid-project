import boto3
import os
import re  # Import regex module

# Load environment variables
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.environ.get("S3_BUCKET")

if not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and S3_BUCKET):
    raise ValueError("Missing environment variable(s). Please check your .env file.")

# Initialize S3 client with credentials from environment variables
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def upload_file():
    """Upload the file 'myfile.txt' from D: drive to the specified S3 key in the 'b-wing/' prefix."""
    local_file_path = r"D:\myfile.txt"  
    s3_key = "b-wing/myfile.txt" 

    try:
        print(f"Uploading '{local_file_path}' to 's3://{S3_BUCKET}/{s3_key}'")
        s3.upload_file(local_file_path, S3_BUCKET, s3_key)
        print(f"Successfully uploaded '{local_file_path}' to 's3://{S3_BUCKET}/{s3_key}'")
    except FileNotFoundError:
        print(f"Local file '{local_file_path}' not found.")
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            print(f"Access denied when uploading to '{s3_key}'.")
        else:
            print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def list_files():
    """List all files in the 'b-wing/' prefix of the S3 bucket."""
    prefix = 'b-wing/'  # Directory to list files from

    try:
        print(f"Listing files in the '{prefix}' directory of the S3 bucket '{S3_BUCKET}'")
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

        if 'Contents' in response:
            print(f"Files found in the '{prefix}' directory:")
            for item in response['Contents']:
                print(item['Key'])  # Print the key (path) of each file
        else:
            print(f"No files found in the '{prefix}' directory.")
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            print(f"Access denied when listing files in '{prefix}'.")
        else:
            print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def list_files_with_filter(regex_pattern):
    """List files in the 'b-wing/' prefix of the S3 bucket that match the provided regex pattern."""
    prefix = 'b-wing/'  # Directory to list files from

    try:
        print(f"Listing files in the '{prefix}' directory of the S3 bucket '{S3_BUCKET}' matching regex '{regex_pattern}'")
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

        if 'Contents' in response:
            print(f"Files matching the regex pattern '{regex_pattern}':")
            pattern = re.compile(regex_pattern)  # Compile the regex pattern for matching
            matched_files = [item['Key'] for item in response['Contents'] if pattern.search(item['Key'])]

            if matched_files:
                for file in matched_files:
                    print(file)  # Print the matched file
            else:
                print(f"No files matched the regex '{regex_pattern}'.")
        else:
            print(f"No files found in the '{prefix}' directory.")
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            print(f"Access denied when listing files in '{prefix}'.")
        else:
            print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def delete_files_matching_regex(regex_pattern):
    """Delete files in the 'b-wing/' prefix of the S3 bucket that match the provided regex pattern."""
    prefix = 'b-wing/'  # Directory to list files from

    try:
        print(f"Deleting files in the '{prefix}' directory of the S3 bucket '{S3_BUCKET}' matching regex '{regex_pattern}'")
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

        if 'Contents' in response:
            pattern = re.compile(regex_pattern)  # Compile the regex pattern for matching
            matched_files = [item['Key'] for item in response['Contents'] if pattern.search(item['Key'])]

            if matched_files:
                for file in matched_files:
                    print(f"Deleting '{file}'")
                    s3.delete_object(Bucket=S3_BUCKET, Key=file)  # Delete each matched file
                print(f"Successfully deleted {len(matched_files)} file(s) matching the regex '{regex_pattern}'")
            else:
                print(f"No files matched the regex '{regex_pattern}'.")
        else:
            print(f"No files found in the '{prefix}' directory.")
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            print(f"Access denied when deleting files in '{prefix}'.")
        else:
            print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    upload_file()

    list_files()

    regex_pattern = r".*\.txt$" 
    list_files_with_filter(regex_pattern)

    delete_files_matching_regex(regex_pattern)