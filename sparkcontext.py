import boto3
import os
import asyncio
from botocore.exceptions import ClientError
from typing import Optional

async def upload_file(s3_client, file_path: str, bucket: str, s3_path: Optional[str] = None) -> bool:
    file_name = os.path.basename(file_path)
    s3_key = f"{s3_path}/{file_name}" if s3_path else file_name
    
    try:
        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            bucket,
            s3_key
        )
        print(f"Uploaded: {file_name}")
        return True
    except ClientError as e:
        print(f"Error uploading {file_name}: {e}")
        return False

async def upload_directory(
    bucket_name: str,
    local_dir: str = ".",
    s3_folder: Optional[str] = None,
    region: str = "us-east-1"
) -> None:
    s3_client = boto3.client('s3', region_name=region)
    
    tasks = []
    for file_name in os.listdir(local_dir):
        file_path = os.path.join(local_dir, file_name)
        if os.path.isfile(file_path):
            task = upload_file(s3_client, file_path, bucket_name, s3_folder)
            tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    successful = sum(1 for r in results if r)
    print(f"\nUploaded {successful}/{len(results)} files")

if __name__ == "__main__":
    # Configuration
    config = {
        "bucket_name": "your-bucket-name",
        "local_dir": ".", # Current directory
        "s3_folder": "destination/folder", # Optional
        "region": "us-east-1"
    }
    
    asyncio.run(upload_directory(**config))
