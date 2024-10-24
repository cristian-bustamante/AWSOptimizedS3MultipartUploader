# Optimized S3 Multipart Uploader

A Python-based solution for efficient and resilient large file uploads to Amazon S3, featuring intelligent chunking, parallel processing, and robust resume capabilities.

## Description

This project provides an optimized solution for uploading large files to Amazon S3 by implementing several performance-enhancing strategies. It addresses common challenges in large file uploads such as network interruptions, timeout issues, and inefficient resource utilization. The implementation uses Python's concurrent processing capabilities and AWS's multipart upload API to achieve better performance and reliability.

## Key Benefits

- **Resume Capability**: Interruption-resistant uploads with automatic state tracking

  - Saves upload progress to allow resuming from the last successful chunk
  - Maintains upload state across sessions
  - Verifies file integrity before resuming

- **Optimized Performance**:

  - Dynamic chunk size selection based on file size
  - Parallel upload with configurable worker count
  - Connection pool optimization
  - TCP keepalive for long-running uploads

- **Flexible Authentication**:

  - Support for default AWS credentials
  - Custom AWS profile selection
  - Cross-account access via role assumption
  - Configurable role session duration

- **Advanced Monitoring**:
  - Real-time progress tracking
  - Speed calculations
  - ETA estimates
  - Detailed performance metrics
  - Session-specific statistics

## Implementation Challenges

1. **Chunk Size Optimization**

   - Finding the balance between too many small chunks (API overhead) and too few large chunks (reduced parallelism)
   - Implementing dynamic chunk sizing based on file size
   - Handling the AWS limit of 10,000 parts per upload

2. **State Management**

   - Designing a reliable state tracking mechanism
   - Handling edge cases in resume scenarios
   - Ensuring state file consistency

3. **Resource Management**

   - Managing memory usage with large files
   - Optimizing thread pool size
   - Handling connection pooling effectively

4. **Error Handling**
   - Implementing robust retry logic
   - Managing timeouts
   - Handling various AWS API errors
   - Ensuring cleanup of incomplete uploads

## Logical Flow

1. **Initialization**

   - Validate input parameters
   - Calculate optimal chunk size based on file size
   - Configure AWS client with optimized settings
   - Set up authentication (profile/role)

2. **State Check**

   - Look for existing upload state
   - Validate state file integrity
   - Verify upload ID if resuming

3. **Upload Process**

   - Create new multipart upload or resume existing
   - List any previously uploaded parts
   - Initialize thread pool
   - Distribute chunks to worker threads
   - Track progress and update state

4. **Chunk Upload**

   - Read chunk from file
   - Upload to S3 with retry logic
   - Update progress and metrics
   - Save state after each chunk

5. **Completion**
   - Verify all parts uploaded successfully
   - Complete multipart upload
   - Clean up state file
   - Display performance metrics

## Usage

```bash
# Basic usage with default credentials
python s3_uploader.py myfile.dat my-bucket path/to/file

# Using a specific AWS profile
python s3_uploader.py myfile.dat my-bucket path/to/file --profile dev

# Assuming a role
python s3_uploader.py myfile.dat my-bucket path/to/file --role-arn arn:aws:iam::123456789012:role/S3UploadRole

# Combining profile with role assumption
python s3_uploader.py myfile.dat my-bucket path/to/file --profile dev --role-arn arn:aws:iam::123456789012:role/S3UploadRole
```

## Performance Considerations

- **Chunk Size**: Automatically optimized based on file size:

  - 8 MB for files up to 1GB
  - 16 MB for files 1GB-10GB
  - 32 MB for files 10GB-50GB
  - 64 MB for files 50GB+

- **Parallel Processing**: Default of 10 concurrent uploads, configurable via `--max-workers`

- **Network Optimization**: Uses connection pooling and TCP keepalive

## Requirements

- Python 3.6+
- boto3
- AWS credentials configured (either default, profile, or role-based)
- Appropriate S3 permissions

## Limitations and Considerations

- Memory usage scales with chunk size and number of workers
- Network bandwidth and latency significantly impact performance
- AWS service limits apply (e.g., maximum parts per upload)
- Not optimized for extremely small files

## Conclusion

This implementation serves as an exploration of optimization strategies for S3 uploads rather than production-ready code. It demonstrates several important concepts:

- The importance of chunk size optimization
- Benefits of parallel processing in I/O-bound operations
- Value of robust state management for long-running operations
- Impact of connection reuse and keepalive
- Flexibility in AWS authentication methods

While this code provides a foundation for understanding these concepts, a production implementation would need additional features such as:

- Comprehensive logging
- Monitoring integration
- Better error reporting
- Configuration management
- Test coverage
- Documentation
- CI/CD integration
- Security hardening

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
