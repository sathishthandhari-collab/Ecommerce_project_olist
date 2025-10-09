import boto3
import time
import os
from datetime import datetime, timedelta
import random

class OlistStreamingSimulator:
    def __init__(self, bucket_name, batch_dir):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.batch_dir = batch_dir
        self.batch_files = sorted([f for f in os.listdir(batch_dir) if f.endswith('.csv')])
        
    def simulate_business_hours_timing(self):
        """Simulate realistic e-commerce traffic patterns"""
        current_hour = datetime.now().hour
        
        # Brazilian business hours (10AM-8PM) = higher frequency
        if 10 <= current_hour <= 20:
            return random.randint(30, 120)  # 30s - 2min intervals
        # Off-hours = lower frequency  
        else:
            return random.randint(300, 900)  # 5-15min intervals
            
    def upload_batch_to_s3(self, batch_file):
        """Upload batch file to S3 to trigger Snowpipe"""
        try:
            # Add timestamp to avoid conflicts
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
            s3_key = f"orders/streaming_data/{timestamp}_{batch_file}"
            
            local_path = os.path.join(self.batch_dir, batch_file)
            
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            print(f"✅ Uploaded {batch_file} as {s3_key}")
            
            return s3_key
            
        except Exception as e:
            print(f"❌ Failed to upload {batch_file}: {str(e)}")
            return None
            
    def start_streaming(self, max_batches=None):
        """Start the streaming simulation"""
        print(f"🚀 Starting Olist streaming simulation...")
        print(f"📁 Found {len(self.batch_files)} batch files")
        
        batches_to_process = self.batch_files[:max_batches] if max_batches else self.batch_files
        
        for i, batch_file in enumerate(batches_to_process):
            # Upload to S3 (triggers Snowpipe)
            s3_key = self.upload_batch_to_s3(batch_file)
            
            if s3_key:
                print(f"📊 Batch {i+1}/{len(batches_to_process)} processed")
            
            # Wait based on business hours simulation
            if i < len(batches_to_process) - 1:  # Don't wait after last batch
                wait_time = self.simulate_business_hours_timing()
                print(f"⏳ Waiting {wait_time}s for next batch...")
                time.sleep(wait_time)
        
        print("🎉 Streaming simulation completed!")

# Usage
if __name__ == "__main__":
    simulator = OlistStreamingSimulator(
        bucket_name='olist-streaming-bucket',
        batch_dir=r'C:\Users\Sathish\OneDrive\Desktop\DA\Projects\olist_data\streaming_batches'
    )
    
    # Start with first 20 batches for testing
    simulator.start_streaming(max_batches=20)
