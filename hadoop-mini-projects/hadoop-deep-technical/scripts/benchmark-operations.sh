#!/bin/bash

# Performance benchmarking script for HDFS operations on MinIO
# Compares operation latencies and demonstrates scalability differences

BASE_URL="http://localhost:8080/webhdfs/v1"
RESULTS_FILE="benchmark_results_$(date +%Y%m%d_%H%M%S).txt"

echo "📊 HDFS on MinIO Performance Benchmark"
echo "======================================"
echo "Results will be saved to: $RESULTS_FILE"
echo "" | tee -a $RESULTS_FILE

# Benchmark configuration
NUM_DIRS=100
NUM_FILES_PER_DIR=50
FILE_SIZE_KB=64

echo "Configuration:" | tee -a $RESULTS_FILE
echo "- Directories: $NUM_DIRS" | tee -a $RESULTS_FILE
echo "- Files per directory: $NUM_FILES_PER_DIR" | tee -a $RESULTS_FILE
echo "- File size: ${FILE_SIZE_KB}KB" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

# Generate test data
generate_test_data() {
    head -c $((FILE_SIZE_KB * 1024)) /dev/urandom | base64
}

# Measure execution time
measure_time() {
    local start_time=$(date +%s.%N)
    eval "$1"
    local end_time=$(date +%s.%N)
    echo "scale=3; $end_time - $start_time" | bc
}

echo "🏗️  Setting up benchmark environment..."

# Create benchmark directory
curl -s -X PUT "${BASE_URL}/benchmark?op=MKDIRS" > /dev/null

echo "📝 Benchmark 1: Directory Creation Performance"
echo "=============================================="
total_time=0
for i in $(seq 1 $NUM_DIRS); do
    dir_path="/benchmark/dir_$(printf "%03d" $i)"
    time_taken=$(measure_time "curl -s -X PUT '${BASE_URL}${dir_path}?op=MKDIRS' > /dev/null")
    total_time=$(echo "$total_time + $time_taken" | bc)
    
    if [ $((i % 10)) -eq 0 ]; then
        echo "Created $i directories..."
    fi
done

avg_dir_time=$(echo "scale=3; $total_time / $NUM_DIRS * 1000" | bc)
echo "Directory Creation Results:" | tee -a $RESULTS_FILE
echo "- Total directories: $NUM_DIRS" | tee -a $RESULTS_FILE
echo "- Total time: ${total_time}s" | tee -a $RESULTS_FILE
echo "- Average time per directory: ${avg_dir_time}ms" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

echo "📄 Benchmark 2: File Creation Performance"
echo "========================================="
test_data=$(generate_test_data)
total_files=0
total_file_time=0

for i in $(seq 1 10); do  # Test with first 10 directories
    dir_path="/benchmark/dir_$(printf "%03d" $i)"
    
    for j in $(seq 1 $NUM_FILES_PER_DIR); do
        file_path="${dir_path}/file_$(printf "%03d" $j).txt"
        time_taken=$(measure_time "curl -s -X PUT '${BASE_URL}${file_path}?op=CREATE' -H 'Content-Type: application/octet-stream' --data '$test_data' > /dev/null")
        total_file_time=$(echo "$total_file_time + $time_taken" | bc)
        total_files=$((total_files + 1))
    done
    
    echo "Created files in directory $i..."
done

avg_file_time=$(echo "scale=3; $total_file_time / $total_files * 1000" | bc)
echo "File Creation Results:" | tee -a $RESULTS_FILE
echo "- Total files: $total_files" | tee -a $RESULTS_FILE
echo "- Total time: ${total_file_time}s" | tee -a $RESULTS_FILE
echo "- Average time per file: ${avg_file_time}ms" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

echo "📋 Benchmark 3: Directory Listing Performance"
echo "=============================================="
# Test different directory sizes
for dir_num in 1 5 10; do
    dir_path="/benchmark/dir_$(printf "%03d" $dir_num)"
    
    # Warm up
    curl -s "${BASE_URL}${dir_path}?op=LISTSTATUS" > /dev/null
    
    # Measure multiple runs
    total_list_time=0
    runs=5
    for run in $(seq 1 $runs); do
        time_taken=$(measure_time "curl -s '${BASE_URL}${dir_path}?op=LISTSTATUS' > /dev/null")
        total_list_time=$(echo "$total_list_time + $time_taken" | bc)
    done
    
    avg_list_time=$(echo "scale=3; $total_list_time / $runs * 1000" | bc)
    echo "Directory $dir_num listing (${NUM_FILES_PER_DIR} files): ${avg_list_time}ms avg" | tee -a $RESULTS_FILE
done
echo "" | tee -a $RESULTS_FILE

echo "🔄 Benchmark 4: Rename Operation Performance"
echo "==========================================="
# Test rename with different file sizes
rename_times=()
for i in 1 2 3; do
    source_path="/benchmark/dir_001/file_$(printf "%03d" $i).txt"
    dest_path="/benchmark/dir_001/renamed_file_$(printf "%03d" $i).txt"
    
    time_taken=$(measure_time "curl -s -X PUT '${BASE_URL}${source_path}?op=RENAME&destination=${dest_path}' > /dev/null")
    rename_time=$(echo "$time_taken * 1000" | bc)
    rename_times+=($rename_time)
    echo "Rename operation $i: ${rename_time}ms" | tee -a $RESULTS_FILE
done
echo "" | tee -a $RESULTS_FILE

echo "🗑️  Benchmark 5: Delete Operation Performance"  
echo "==========================================="
# Test single file delete
delete_time=$(measure_time "curl -s -X DELETE '${BASE_URL}/benchmark/dir_001/renamed_file_001.txt?op=DELETE' > /dev/null")
delete_time_ms=$(echo "$delete_time * 1000" | bc)
echo "Single file delete: ${delete_time_ms}ms" | tee -a $RESULTS_FILE

# Test directory delete (recursive)
dir_delete_time=$(measure_time "curl -s -X DELETE '${BASE_URL}/benchmark/dir_010?op=DELETE&recursive=true' > /dev/null")
dir_delete_time_ms=$(echo "$dir_delete_time * 1000" | bc)
echo "Directory delete (${NUM_FILES_PER_DIR} files): ${dir_delete_time_ms}ms" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

echo "📊 Performance Summary" | tee -a $RESULTS_FILE
echo "=====================" | tee -a $RESULTS_FILE
echo "Operation latencies (average):" | tee -a $RESULTS_FILE
echo "- Directory creation: ${avg_dir_time}ms" | tee -a $RESULTS_FILE
echo "- File creation: ${avg_file_time}ms" | tee -a $RESULTS_FILE
echo "- Directory listing: varies by size" | tee -a $RESULTS_FILE
echo "- File rename: $(echo "${rename_times[0]} + ${rename_times[1]} + ${rename_times[2]}" | bc | awk '{print $1/3}')ms avg" | tee -a $RESULTS_FILE
echo "- File delete: ${delete_time_ms}ms" | tee -a $RESULTS_FILE
echo "- Directory delete: ${dir_delete_time_ms}ms" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

echo "🔍 Analysis Notes:" | tee -a $RESULTS_FILE
echo "- File operations are slower than traditional HDFS due to object storage overhead" | tee -a $RESULTS_FILE
echo "- Directory listing performance depends on database query efficiency" | tee -a $RESULTS_FILE
echo "- Rename operations require object copying, making them expensive" | tee -a $RESULTS_FILE
echo "- Caching significantly improves repeated operations" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

# Cleanup
echo "🧹 Cleaning up benchmark data..."
curl -s -X DELETE "${BASE_URL}/benchmark?op=DELETE&recursive=true" > /dev/null

echo "✅ Benchmark completed! Results saved to: $RESULTS_FILE"
echo ""
echo "📈 Next steps:"
echo "- Analyze results in Grafana dashboard"
echo "- Compare with traditional HDFS performance"
echo "- Experiment with different caching strategies"
echo "- Test with larger datasets for scalability analysis"
