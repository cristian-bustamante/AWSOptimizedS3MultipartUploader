"""
AWS S3 Optimized Multipart Uploader
----------------------------------

A high-performance tool for uploading large files to Amazon S3 using multipart uploads
with automatic chunk size optimization and resume capability.

Author: Cristian Bustamante R.
Created: 2024-10-24
Version: 1.0

Features:
- Automatic chunk size optimization based on file size
- Parallel upload with configurable number of workers
- Resume capability for interrupted uploads
- Accurate progress tracking and speed calculation
- Robust error handling and retry logic
"""
import argparse
import boto3
import math
import os
import concurrent.futures
import sys
import time
import json
import hashlib
import threading
import re
import textwrap
from typing import List, Dict, Optional
from dataclasses import dataclass
from botocore.client import Config


@dataclass
class ChunkMetrics:
    """Stores metrics for individual chunk uploads"""
    chunk_number: int
    start_time: float
    end_time: float
    size: int
    speed_mbps: float


class MultipartUploader:
    """
    Handles large file uploads to S3 with the following features:
    - Automatic chunk size optimization based on file size
    - Parallel upload with configurable number of workers
    - Resume capability for interrupted uploads
    - Accurate progress tracking and speed calculation
    - Robust error handling and retry logic
    """

    # Optimal chunk sizes mapped to file size thresholds (in bytes)
    CHUNK_SIZE_MAPPING = [
        (0, 8_388_608),              # 8 MB for files up to 1GB
        (1_073_741_824, 16_777_216),  # 16 MB for files 1GB-10GB
        (10_737_418_240, 33_554_432),  # 32 MB for files 10GB-50GB
        (53_687_091_200, 67_108_864),  # 64 MB for files 50GB+
    ]

    def __init__(
        self,
        file_path: str,
        bucket: str,
        key: str,
        max_workers: int,
        profile: Optional[str] = None,
        role_arn: Optional[str] = None,
        session_duration: int = 43200
    ) -> None:
        """
        Initialize the uploader with the given parameters.

        Args:
            file_path: Path to the file to upload
            bucket: Target S3 bucket name
            key: Target S3 key (path in bucket)
            max_workers: Maximum number of concurrent upload threads
            profile: Optional AWS profile name to use for credentials
            role_arn: Optional ARN of the role to assume
            session_duration: Duration in seconds for assumed role session (default: 12 hours)
        """
        self.file_path = file_path
        self.bucket = bucket
        self.key = key
        self.file_size = os.path.getsize(file_path)
        self.chunk_size = self._get_optimal_chunk_size()
        self.max_workers = max_workers

        # Configure boto3 client with optimized settings
        config = Config(
            max_pool_connections=max_workers,  # Match connection pool to workers
            tcp_keepalive=True,
            retries={'max_attempts': 5}  # More aggressive retry strategy
        )

        # Get AWS session with appropriate credentials
        aws_session = self._get_aws_session(profile, role_arn, session_duration)

        # Initialize S3 client with optimized configuration
        self.s3_client = aws_session.client(
            's3',
            config=config,
            use_ssl=True
        )

        # Initialize tracking variables
        self.metrics: List[ChunkMetrics] = []
        self.state_file = self._get_state_file_path()
        self._upload_progress_lock = threading.Lock()
        self._total_bytes_transferred = 0
        self._session_bytes_transferred = 0  # Track only this session's bytes
        self.start_time = None
        self._existing_parts_size = 0  # Track size of previously uploaded parts

    def _get_aws_session(
        self,
        profile: Optional[str] = None,
        role_arn: Optional[str] = None,
        session_duration: int = 43200
    ) -> boto3.Session:
        """
        Create AWS session with appropriate credentials based on provided parameters.

        Args:
            profile: AWS CLI profile name to use for credentials
            role_arn: ARN of the role to assume
            session_duration: Duration in seconds for assumed role session

        Returns:
            boto3.Session: Configured AWS session with appropriate credentials

        Raises:
            botocore.exceptions.ProfileNotFound: If specified profile doesn't exist
            botocore.exceptions.ClientError: If role assumption fails
        """
        try:
            # Create initial session with profile if specified
            session = boto3.Session(profile_name=profile) if profile else boto3.Session()

            if profile:
                print(f"Using AWS profile: {profile}")
            else:
                print("Using default AWS credentials")

            # If role ARN is provided, assume role
            if role_arn:
                sts_client = session.client('sts')
                try:
                    response = sts_client.assume_role(
                        RoleArn=role_arn,
                        RoleSessionName=f'S3UploaderSession-{int(time.time())}',
                        DurationSeconds=session_duration
                    )

                    print(f"Assumed role: {role_arn}")

                    # Create new session with temporary credentials
                    return boto3.Session(
                        aws_access_key_id=response['Credentials']['AccessKeyId'],
                        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                        aws_session_token=response['Credentials']['SessionToken']
                    )
                except Exception as e:
                    raise Exception(f"Failed to assume role {role_arn}: {str(e)}")

            return session
        except Exception as e:
            raise Exception(f"Failed to create AWS session: {str(e)}")

    def _get_optimal_chunk_size(self) -> int:
        """Calculate the optimal chunk size based on file size."""
        for size_threshold, chunk_size in self.CHUNK_SIZE_MAPPING:
            if self.file_size < size_threshold or size_threshold == self.CHUNK_SIZE_MAPPING[-1][0]:
                return chunk_size
        return self.CHUNK_SIZE_MAPPING[-1][1]

    def _get_state_file_path(self) -> str:
        """Generate a unique state file path based on upload parameters."""
        # Create unique identifier for this upload configuration
        upload_id = hashlib.md5(
            f"{self.file_path}:{self.bucket}:{self.key}:{self.chunk_size}".encode()
        ).hexdigest()
        return os.path.join(os.path.dirname(self.file_path), f".upload_state_{upload_id}.json")

    def _calculate_existing_parts_size(self, existing_parts: Dict[int, str]) -> int:
        """Calculate total size of previously uploaded parts."""
        total_size = 0
        for part_number in existing_parts.keys():
            start_byte = (part_number - 1) * self.chunk_size
            chunk_size = min(self.chunk_size, self.file_size - start_byte)
            total_size += chunk_size
        return total_size

    def _load_state(self) -> Optional[Dict]:
        """Load the upload state from file if it exists."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load state file: {e}")
        return None

    def _save_state(self, upload_id: str, completed_parts: List[Dict]) -> None:
        """Save the current upload state to file."""
        state = {
            'upload_id': upload_id,
            'completed_parts': completed_parts,
            'file_size': self.file_size,
            'last_modified': os.path.getmtime(self.file_path)
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f)

    def _clean_state(self) -> None:
        """Remove the state file after successful upload."""
        if os.path.exists(self.state_file):
            os.remove(self.state_file)

    def validate_inputs(self) -> None:
        """Validate input parameters and file accessibility."""
        if not os.path.exists(self.file_path):
            raise ValueError(f"File not found: {self.file_path}")

        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except Exception as e:
            raise ValueError(f"Unable to access bucket {self.bucket}: {str(e)}")

    def list_existing_parts(self, upload_id: str) -> Dict[int, str]:
        """
        List already uploaded parts for the given upload ID.
        Handles pagination for uploads with many parts.
        """
        try:
            parts = []
            paginator = self.s3_client.get_paginator('list_parts')
            for page in paginator.paginate(Bucket=self.bucket, Key=self.key, UploadId=upload_id):
                if 'Parts' in page:
                    parts.extend(page['Parts'])
            return {part['PartNumber']: part['ETag'] for part in parts}
        except Exception as e:
            print(f"Warning: Could not list parts: {e}")
            return {}

    def _update_progress(self, chunk_size: int, is_resumed_part: bool = False):
        """
        Thread-safe progress update with separate tracking for new and resumed parts.

        Args:
            chunk_size: Size of the chunk being processed
            is_resumed_part: Whether this chunk was previously uploaded
        """
        with self._upload_progress_lock:
            self._total_bytes_transferred += chunk_size
            if not is_resumed_part:
                self._session_bytes_transferred += chunk_size

            if self.start_time is None:
                self.start_time = time.time()

            elapsed_time = time.time() - self.start_time
            total_progress = (self._total_bytes_transferred / self.file_size) * 100

            # Calculate speed based only on this session's transferred bytes
            if self._session_bytes_transferred > 0:
                current_speed = (self._session_bytes_transferred / 1024 / 1024) / elapsed_time * 8  # Mbps
            else:
                current_speed = 0

            # Calculate ETA based on remaining new bytes and current speed
            remaining_new_bytes = self.file_size - self._existing_parts_size - self._session_bytes_transferred
            if current_speed > 0:
                eta_seconds = remaining_new_bytes / (current_speed * 1024 * 1024 / 8)
                eta_str = f"ETA: {int(eta_seconds)}s"
            else:
                eta_str = "ETA: calculating..."

            print(f"\rProgress: {total_progress:.1f}% | Current Speed: {current_speed:.2f} Mbps | {eta_str}", end="")
            sys.stdout.flush()

    def upload_chunk(self, chunk_number: int, start_byte: int, upload_id: str, existing_parts: Dict[int, str]) -> Dict:
        """
        Upload a single chunk to S3 with optimized handling and retry logic.

        Args:
            chunk_number: Zero-based chunk number
            start_byte: Starting byte position in file
            upload_id: Multipart upload ID
            existing_parts: Dictionary of already uploaded parts

        Returns:
            Dict containing PartNumber and ETag of uploaded chunk
        """
        part_number = chunk_number + 1

        # Skip if chunk was already uploaded
        if part_number in existing_parts:
            chunk_size = min(self.chunk_size, self.file_size - start_byte)
            self._update_progress(chunk_size, is_resumed_part=True)
            return {
                'PartNumber': part_number,
                'ETag': existing_parts[part_number]
            }

        chunk_size = min(self.chunk_size, self.file_size - start_byte)

        # Implement retry logic with exponential backoff
        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                # Use memory-efficient file reading
                with open(self.file_path, 'rb') as f:
                    f.seek(start_byte)
                    start_time = time.time()

                    response = self.s3_client.upload_part(
                        Bucket=self.bucket,
                        Key=self.key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=f.read(chunk_size)
                    )

                    end_time = time.time()
                    duration = end_time - start_time
                    speed_mbps = (chunk_size / 1024 / 1024) / duration * 8

                    self.metrics.append(ChunkMetrics(
                        chunk_number=part_number,
                        start_time=start_time,
                        end_time=end_time,
                        size=chunk_size,
                        speed_mbps=speed_mbps
                    ))

                    self._update_progress(chunk_size)

                    return {
                        'PartNumber': part_number,
                        'ETag': response['ETag']
                    }
            except Exception as e:
                retries += 1
                if retries == max_retries:
                    raise Exception(f"Failed to upload part {part_number} after {max_retries} attempts: {str(e)}")
                time.sleep(2 ** retries)  # Exponential backoff

    def upload(self) -> None:
        """
        Perform optimized multipart upload with intelligent chunk handling.
        Handles the complete upload process including resume capability.
        """
        self.validate_inputs()
        chunk_count = math.ceil(self.file_size / self.chunk_size)

        print(f"\nStarting upload of {self.file_size / (1024*1024):.2f} MB")
        print(f"Chunk size: {self.chunk_size / (1024*1024):.0f} MB")
        print(f"Number of chunks: {chunk_count}")
        print(f"Max concurrent uploads: {self.max_workers}")

        # Check for existing upload state
        state = self._load_state()
        upload_id = None if not state else state.get('upload_id')

        try:
            if upload_id:
                # Verify upload ID is still valid
                self.s3_client.list_parts(
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=upload_id
                )
                print("Resuming previous upload")
            else:
                # Start new upload
                mpu = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
                upload_id = mpu['UploadId']
                print("Starting new upload")

            # Get list of already uploaded parts
            existing_parts = self.list_existing_parts(upload_id)
            if existing_parts:
                self._existing_parts_size = self._calculate_existing_parts_size(existing_parts)
                print(f"Found {len(existing_parts)} previously uploaded parts ({self._existing_parts_size / (1024*1024):.2f} MB)")

            # Use ThreadPoolExecutor for I/O-bound tasks
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self.upload_chunk,
                        chunk_number,
                        chunk_number * self.chunk_size,
                        upload_id,
                        existing_parts
                    )
                    for chunk_number in range(chunk_count)
                ]

                completed_parts = []
                for future in concurrent.futures.as_completed(futures):
                    part = future.result()
                    completed_parts.append(part)
                    self._save_state(upload_id, completed_parts)

            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=upload_id,
                MultipartUpload={'Parts': sorted(completed_parts, key=lambda x: x['PartNumber'])}
            )

            self._clean_state()
            self.display_summary()

        except Exception as e:
            print(f"\nError during upload: {str(e)}")
            print(f"You can resume this upload later using the same command.")
            raise

    def display_summary(self) -> None:
        """Display upload summary with both individual and parallel performance metrics."""
        if not self.metrics:  # No metrics if all chunks were skipped
            print("\n\nUpload completed (all chunks were already uploaded)")
            return

        # Only consider metrics from this session for speed calculations
        session_metrics = [m for m in self.metrics if m.speed_mbps > 0]  # Speed > 0 indicates new upload
        if not session_metrics:
            print("\n\nNo new parts uploaded in this session")
            return

        total_session_size = sum(metric.size for metric in session_metrics)
        session_start = min(m.start_time for m in session_metrics)
        session_end = max(m.end_time for m in session_metrics)
        total_time = session_end - session_start

        # Calculate parallel throughput
        time_windows = []
        current_window_start = session_start
        while current_window_start < session_end:
            window_end = current_window_start + 1.0  # 1-second windows
            window_bytes = sum(
                m.size for m in session_metrics
                if m.start_time <= window_end and m.end_time >= current_window_start
            )
            if window_bytes > 0:
                time_windows.append((window_bytes / 1024 / 1024) * 8)  # Convert to Mbps
            current_window_start = window_end

        # Calculate parallel performance statistics
        parallel_min_speed = min(time_windows) if time_windows else 0
        parallel_max_speed = max(time_windows) if time_windows else 0
        parallel_avg_speed = sum(time_windows) / len(time_windows) if time_windows else 0

        print("\n\nUpload Summary:")
        print("-" * 50)
        print(f"Total File Size: {self.file_size / (1024*1024):.2f} MB")
        print(f"Previously Uploaded: {self._existing_parts_size / (1024*1024):.2f} MB")
        print(f"Uploaded This Session: {total_session_size / (1024*1024):.2f} MB")
        print(f"Session Upload Time: {total_time:.2f} seconds")
        print(f"Effective Session Speed: {(total_session_size / 1024 / 1024) / total_time * 8:.2f} Mbps")
        print(f"Chunk Size: {self.chunk_size / (1024*1024):.0f} MB")

        if len(session_metrics) > 0:
            print("\nIndividual Chunk Performance:")
            chunk_speeds = [m.speed_mbps for m in session_metrics]
            print(f"Min Chunk Speed: {min(chunk_speeds):.2f} Mbps")
            print(f"Max Chunk Speed: {max(chunk_speeds):.2f} Mbps")
            print(f"Avg Chunk Speed: {sum(chunk_speeds)/len(chunk_speeds):.2f} Mbps")

            print("\nParallel Performance (1-second windows):")
            print(f"Min Throughput: {parallel_min_speed:.2f} Mbps")
            print(f"Max Throughput: {parallel_max_speed:.2f} Mbps")
            print(f"Avg Throughput: {parallel_avg_speed:.2f} Mbps")


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments

    Raises:
        ArgumentError: If invalid argument combinations or values are provided
    """
    parser = argparse.ArgumentParser(
        description='Optimized Resumable S3 Multipart Upload',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Authentication Examples:
            ----------------------
            1. Using default credentials:
                %(prog)s myfile.dat my-bucket path/to/file
                
            2. Using a specific profile:
                %(prog)s myfile.dat my-bucket path/to/file --profile dev
                
            3. Assuming a role:
                %(prog)s myfile.dat my-bucket path/to/file --role-arn arn:aws:iam::123456789012:role/S3UploadRole
                
            4. Assuming a role using a specific profile:
                %(prog)s myfile.dat my-bucket path/to/file --profile dev --role-arn arn:aws:iam::123456789012:role/S3UploadRole
        ''')
    )

    # Required arguments
    parser.add_argument('file_path',
                        help='Path to the file to upload')
    parser.add_argument('bucket',
                        help='Target S3 bucket name')
    parser.add_argument('key',
                        help='Target S3 key (path/filename in bucket)')

    # Optional arguments
    parser.add_argument('--max-workers',
                        type=int,
                        default=10,
                        help='Maximum number of parallel uploads (default: 10)')

    # AWS authentication options
    auth_group = parser.add_argument_group('AWS Authentication Options')
    auth_group.add_argument('--profile',
                            default="",
                            help='AWS CLI profile to use for credentials')
    auth_group.add_argument('--role-arn',
                            default="",
                            help='ARN of the IAM role to assume')
    auth_group.add_argument('--session-duration',
                            type=int,
                            default=43200,
                            help='Duration in seconds for assumed role session (default: 43200 [12 hours])')

    args = parser.parse_args()

    # Validate arguments
    if args.max_workers < 1:
        parser.error("Max workers must be at least 1")

    if args.session_duration < 900 or args.session_duration > 43200:
        parser.error("Session duration must be between 900 seconds (15 minutes) and 43200 seconds (12 hours)")

    # Validate file path
    if not os.path.exists(args.file_path):
        parser.error(f"File not found: {args.file_path}")

    # Validate AWS role ARN format if provided
    if args.role_arn:
        arn_pattern = r'^arn:aws:iam::\d{12}:role/[\w+=,.@-]+$'
        if not re.match(arn_pattern, args.role_arn):
            parser.error("Invalid role ARN format. Expected format: arn:aws:iam::123456789012:role/RoleName")

    return args


def main():
    """Main entry point."""
    try:
        args = parse_args()

        uploader = MultipartUploader(
            args.file_path,
            args.bucket,
            args.key,
            args.max_workers,
            profile=args.profile,
            role_arn=args.role_arn,
            session_duration=args.session_duration if args.role_arn else None
        )
        uploader.upload()

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
