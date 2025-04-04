#!/usr/bin/env python3
import os
import random
import threading
import argparse
from pathlib import Path
from time import time
from multiprocessing import cpu_count

class DummyDataGenerator:
    def __init__(self, output_dir, num_files, file_size_kb, thread_count=None, node_id=0, node_count=1):
        self.output_dir = Path(output_dir)
        self.num_files = num_files
        self.file_size_bytes = file_size_kb * 1024
        self.thread_count = thread_count or cpu_count()
        self.lock = threading.Lock()
        self.node_id = node_id
        self.node_count = node_count
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_random_data(self, size):
        """Generate random binary data of given size"""
        return bytes(random.getrandbits(8) for _ in range(size))

    def create_file(self, file_num):
        """Create a single dummy file"""
        global_num = (file_num * self.node_count) + self.node_id
        file_path = self.output_dir / f"dummy_n{self.node_id}_{global_num}.dat"
        
        # Check if file exists (in case of overlapping runs)
        if file_path.exists():
            with self.lock:
                print(f"Warning: {file_path} already exists, skipping")
            return
        try:
            with open(file_path, 'wb') as f:
                f.write(self.generate_random_data(self.file_size_bytes))
            with self.lock:
                print(f"Created {file_path} ({self.file_size_bytes/1024:.2f} KB)")
        except Exception as e:
            print(f"Error creating {file_path}: {e}")

    def run(self):
        """Run the generation process with threading"""
        print(f"Starting generation of {self.num_files} files ({self.file_size_bytes/1024:.2f} KB each)")
        print(f"Using {self.thread_count} threads")
        
        start_time = time()
        threads = []
        
        # Distribute files across threads
        files_per_thread = self.num_files // self.thread_count
        remaining_files = self.num_files % self.thread_count
        
        for i in range(self.thread_count):
            # Calculate files for this thread
            files_to_create = files_per_thread + (1 if i < remaining_files else 0)
            if files_to_create == 0:
                continue
                
            # Create and start thread
            start_num = i * files_per_thread + min(i, remaining_files)
            t = threading.Thread(
                target=self.create_files_batch,
                args=(start_num, files_to_create)
            )
            threads.append(t)
            t.start()
        
        # Wait for all threads to complete
        for t in threads:
            t.join()
            
        total_size_gb = (self.num_files * self.file_size_bytes) / (1024**3)
        elapsed = time() - start_time
        print(f"\nCompleted in {elapsed:.2f} seconds")
        print(f"Total data generated: {total_size_gb:.2f} GB")
        print(f"Throughput: {total_size_gb/elapsed:.2f} GB/s")

    def create_files_batch(self, start_num, count):
        """Create a batch of files (used by threads)"""
        for i in range(start_num, start_num + count):
            self.create_file(i)

def main():
    parser = argparse.ArgumentParser(description='Parallel dummy data generator')
    parser.add_argument('output_dir', help='Output directory for dummy files')
    parser.add_argument('-n', '--num-files', type=int, default=100,
                       help='Number of files to generate per node (default: 100)')
    parser.add_argument('-s', '--size-kb', type=int, default=10240,
                       help='Size of each file in KB (default: 10240 = 10MB)')
    parser.add_argument('-t', '--threads', type=int,
                       help='Number of threads to use (default: CPU count)')
    parser.add_argument('--node-id', type=int, default=0,
                       help='Node identifier (0-based) for distributed runs')
    parser.add_argument('--node-count', type=int, default=1,
                       help='Total number of nodes in distributed run')
    
    args = parser.parse_args()
    
    generator = DummyDataGenerator(
        output_dir=args.output_dir,
        num_files=args.num_files,
        file_size_kb=args.size_kb,
        thread_count=args.threads,
        node_id=args.node_id,
        node_count=args.node_count
    )
    generator.run()

if __name__ == '__main__':
    main()
