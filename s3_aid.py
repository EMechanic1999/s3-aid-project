import boto3
import os

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


    # Then, list all files in the 'b-wing/' prefix
    list_files()