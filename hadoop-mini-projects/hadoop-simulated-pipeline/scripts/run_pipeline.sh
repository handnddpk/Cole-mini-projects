#!/bin/bash
"""
Complete Pipeline Execution Script
Runs the full data pipeline from data generation to analysis
"""

set -e

echo "=== Hadoop HDFS Simulated Data Pipeline ==="
echo "Starting pipeline execution..."

# Step 1: Generate sample data
echo "Step 1: Generating sample data..."
python3 /scripts/generate_data.py

# Step 2: Create HDFS directories
echo "Step 2: Setting up HDFS directories..."
hdfs dfs -mkdir -p /input
hdfs dfs -mkdir -p /output
hdfs dfs -mkdir -p /temp

# Step 3: Upload data to HDFS
echo "Step 3: Uploading data to HDFS..."
hdfs dfs -put /data/input/*.csv /input/

# Step 4: Verify data upload
echo "Step 4: Verifying data upload..."
hdfs dfs -ls /input/
echo "Total data size in HDFS:"
hdfs dfs -du -s -h /input/

# Step 5: Compile and run MapReduce job
echo "Step 5: Running MapReduce job for sales analysis..."

# Create Java source directory
mkdir -p /tmp/mapreduce
cd /tmp/mapreduce

# Create the MapReduce job (Sales by Category)
cat > SalesAnalysis.java << 'EOF'
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesAnalysis {

    public static class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        private Text category = new Text();
        private DoubleWritable amount = new DoubleWritable();
        
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            
            // Skip header line
            if (line.startsWith("transaction_id")) {
                return;
            }
            
            try {
                String[] fields = line.split(",");
                if (fields.length >= 7) {
                    String cat = fields[3].trim(); // category column
                    double totalAmount = Double.parseDouble(fields[6].trim()); // total_amount column
                    
                    category.set(cat);
                    amount.set(totalAmount);
                    context.write(category, amount);
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Skip malformed lines
                System.err.println("Skipping malformed line: " + line);
            }
        }
    }
    
    public static class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        private DoubleWritable result = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            
            double sum = 0;
            int count = 0;
            
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            
            result.set(sum);
            context.write(key, result);
            
            // Also write count information
            Text countKey = new Text(key.toString() + "_COUNT");
            DoubleWritable countValue = new DoubleWritable(count);
            context.write(countKey, countValue);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sales analysis");
        
        job.setJarByClass(SalesAnalysis.class);
        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesReducer.class);
        job.setReducerClass(SalesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
EOF

# Compile the MapReduce job
echo "Compiling MapReduce job..."
javac -classpath $(hadoop classpath) SalesAnalysis.java
jar cf sales-analysis.jar SalesAnalysis*.class

# Run the MapReduce job
echo "Executing MapReduce job..."
hdfs dfs -rm -r -f /output/sales-summary
hadoop jar sales-analysis.jar SalesAnalysis /input /output/sales-summary

# Step 6: Display results
echo "Step 6: Pipeline Results"
echo "======================="

echo "Sales Summary by Category:"
hdfs dfs -cat /output/sales-summary/part-r-00000 | sort -k2 -nr

echo ""
echo "HDFS Storage Summary:"
hdfs dfs -df -h

echo ""
echo "Directory Contents:"
hdfs dfs -ls /output/sales-summary/

echo ""
echo "=== Pipeline Execution Complete ==="
echo "Access the results at:"
echo "- HDFS Web UI: http://localhost:9870"
echo "- Resource Manager: http://localhost:8088"
echo "- Results location: /output/sales-summary in HDFS"
