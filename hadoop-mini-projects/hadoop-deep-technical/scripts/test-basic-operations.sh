#!/bin/bash

# Test basic HDFS operations against the MinIO-backed metadata service
# This script demonstrates the WebHDFS API compatibility

BASE_URL="http://localhost:8080/webhdfs/v1"

echo "🧪 Testing HDFS Metadata Operations on MinIO"
echo "============================================="
echo ""

# Test 1: Create directory
echo "1. Creating directory /user/test..."
response=$(curl -s -X PUT "${BASE_URL}/user/test?op=MKDIRS")
echo "Response: $response"
echo ""

# Test 2: List root directory
echo "2. Listing root directory..."
response=$(curl -s "${BASE_URL}/?op=LISTSTATUS")
echo "Response: $response"
echo ""

# Test 3: Create a file
echo "3. Creating file /user/test/sample.txt..."
response=$(curl -s -X PUT "${BASE_URL}/user/test/sample.txt?op=CREATE" \
    -H "Content-Type: application/octet-stream" \
    --data "Hello HDFS on MinIO!")
echo "Response: $response"
echo ""

# Test 4: Get file status
echo "4. Getting file status for /user/test/sample.txt..."
response=$(curl -s "${BASE_URL}/user/test/sample.txt?op=GETFILESTATUS")
echo "Response: $response"
echo ""

# Test 5: List directory contents
echo "5. Listing /user/test directory..."
response=$(curl -s "${BASE_URL}/user/test?op=LISTSTATUS")
echo "Response: $response"
echo ""

# Test 6: Rename operation (most complex)
echo "6. Renaming /user/test/sample.txt to /user/test/renamed.txt..."
response=$(curl -s -X PUT "${BASE_URL}/user/test/sample.txt?op=RENAME&destination=/user/test/renamed.txt")
echo "Response: $response"
echo ""

# Test 7: Verify rename
echo "7. Listing directory after rename..."
response=$(curl -s "${BASE_URL}/user/test?op=LISTSTATUS")
echo "Response: $response"
echo ""

# Test 8: Delete file
echo "8. Deleting file /user/test/renamed.txt..."
response=$(curl -s -X DELETE "${BASE_URL}/user/test/renamed.txt?op=DELETE")
echo "Response: $response"
echo ""

# Test 9: Delete directory
echo "9. Deleting directory /user/test..."
response=$(curl -s -X DELETE "${BASE_URL}/user/test?op=DELETE&recursive=true")
echo "Response: $response"
echo ""

# Test 10: Health check
echo "10. Checking service health..."
response=$(curl -s "${BASE_URL%/webhdfs/v1}/api/v1/health")
echo "Response: $response"
echo ""

echo "✅ Basic operations test completed!"
echo ""
echo "🔍 Next steps:"
echo "- Check Grafana dashboard for performance metrics"
echo "- Monitor PostgreSQL for metadata storage"
echo "- Examine MinIO console for object storage"
echo "- Run performance benchmarks with larger datasets"
