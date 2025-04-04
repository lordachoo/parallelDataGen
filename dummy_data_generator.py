#!/usr/bin/env python3
import os
import random
import threading
import argparse
import json
import sys
import platform

# Optional psutil import for enhanced system metrics
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
from pathlib import Path
from time import time
from datetime import datetime
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
        self.status_file = self.output_dir / ".dummy_data_status.json"
        self.files_created = 0
        self.start_time = None
        self.last_update_time = None
        
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
                self.files_created += 1
                if self.files_created % 10 == 0:  # Update status every 10 files
                    self.update_shared_status()
        except Exception as e:
            print(f"Error creating {file_path}: {e}")

    def update_shared_status(self):
        """Update shared status file with current progress"""
        import json
        from datetime import datetime
        
        status = {
            'nodes': {},
            'timestamp': datetime.utcnow().isoformat(),
            'total_files': self.num_files * self.node_count,
            'file_size_kb': self.file_size_bytes / 1024,
            'aggregate_stats': {
                'total_throughput_mb_s': 0.0,
                'active_nodes': 0,
                'total_files_created': 0
            }
        }
        
        try:
            # Read existing status if available
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    existing = json.load(f)
                    if 'nodes' in existing:
                        status['nodes'].update(existing['nodes'])
            
            # Update our node's status
            now = datetime.utcnow()
            current_time = now.isoformat()
            
            # Calculate throughput if we have previous data
            throughput = None
            if self.last_update_time and self.files_created > 0:
                time_diff = (now - self.last_update_time).total_seconds()
                if time_diff > 0:
                    data_mb = (self.file_size_bytes * 10) / (1024 * 1024)  # 10 files since last update
                    throughput = data_mb / time_diff  # MB/s
            
            status['nodes'][str(self.node_id)] = {
                'files_created': self.files_created,
                'percent_complete': (self.files_created / self.num_files) * 100,
                'last_update': current_time,
                'throughput_mb_s': throughput,
                'node_metadata': {
                    'node_id': self.node_id,
                    'node_count': self.node_count,
                    'thread_count': self.thread_count,
                    'file_size_kb': self.file_size_bytes / 1024,
                    'target_files': self.num_files,
                    'hostname': os.uname().nodename if hasattr(os, 'uname') else 'unknown',
                    'start_time': self.start_time.isoformat() if self.start_time else None,
                    'python_version': '.'.join(map(str, sys.version_info[:3])),
                    'platform': sys.platform,
                    'cpu_model': platform.processor(),
                    **({
                        'system_memory_gb': round(psutil.virtual_memory().total / (1024**3), 2),
                        'available_memory_gb': round(psutil.virtual_memory().available / (1024**3), 2),
                        'cpu_cores': psutil.cpu_count(logical=False),
                        'cpu_threads': psutil.cpu_count(logical=True)
                    } if PSUTIL_AVAILABLE else {
                        'psutil_missing': True,
                        'cpu_cores': os.cpu_count() or 'unknown',
                        'cpu_threads': os.cpu_count() or 'unknown'
                    })
                }
            }
            
            self.last_update_time = now
            
            # Calculate aggregate stats
            total_throughput = 0.0
            active_nodes = 0
            total_files = 0
            
            for node_id, node_data in status['nodes'].items():
                if node_data.get('throughput_mb_s') is not None:
                    total_throughput += node_data['throughput_mb_s']
                    active_nodes += 1
                total_files += node_data['files_created']
            
            status['aggregate_stats'] = {
                'total_throughput_mb_s': round(total_throughput, 2),
                'active_nodes': active_nodes,
                'total_files_created': total_files,
                'percent_complete': round((total_files / status['total_files']) * 100, 1)
            }
            
            # Write back to file
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not update status file - {e}")

    def run(self):
        """Run the generation process with threading"""
        self.start_time = datetime.utcnow()
        self.last_update_time = self.start_time
        # Write initial status
        self.update_shared_status()
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
        # Final status update
        self.update_shared_status()
        print(f"\nCompleted in {elapsed:.2f} seconds")
        
        # Print summary from all nodes if available
        try:
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    status = json.load(f)
                    total_created = sum(n['files_created'] for n in status['nodes'].values())
                    print(f"\nCluster-wide status:")
                    print(f"- Total files created: {total_created}/{status['total_files']}")
                    print(f"- Completion: {(total_created/status['total_files'])*100:.1f}%")
        except Exception as e:
            print(f"Warning: Could not read cluster status - {e}")
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
