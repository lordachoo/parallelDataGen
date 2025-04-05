# Parallel Dummy Data Generator

A Python script for generating large volumes of dummy data in parallel across multiple threads and nodes.

## Features

- Parallel file generation using multiple threads
- Distributed execution across multiple nodes
- Configurable file size and count
- Real-time progress monitoring
- Throughput statistics (MB/s and files/s)
- Per-node and cluster-wide status reporting
- Atomic file operations to prevent corruption

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-repo/parallel-data-generator.git
cd parallel-data-generator
```

2. Install dependencies:
```bash
pip install psutil  # Optional for enhanced system metrics
```

## Usage

### Basic Single-Node Operation
```bash
python dummy_data_generator.py /path/to/output -n 100 -s 1024
```

### Multi-Node Operation
Run on each node (with unique node-id):
```bash
# On node 0:
python dummy_data_generator.py /shared/output -n 100 --node-id 0 --node-count 3

# On node 1:
python dummy_data_generator.py /shared/output -n 100 --node-id 1 --node-count 3

# On node 2:
python dummy_data_generator.py /shared/output -n 100 --node-id 2 --node-count 3
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `output_dir` | Output directory for generated files | (required) |
| `-n`, `--num-files` | Number of files to generate per node | 100 |
| `-s`, `--size-kb` | Size of each file in KB | 10240 (10MB) |
| `-t`, `--threads` | Number of threads to use | CPU count |
| `--node-id` | Node identifier (0-based) | 0 |
| `--node-count` | Total number of nodes in cluster | 1 |

## Status Files

Each node creates its own status file:
- `.dummy_data_status_nodeX.json` (where X is node-id)

Status files contain:
- Current progress (files created)
- Throughput metrics (MB/s and files/s)
- Node metadata (hardware specs, configuration)
- Timestamp of last update

## Monitoring Progress

### View Individual Node Status
```bash
watch -n 1 cat /path/to/output/.dummy_data_status_node0.json
```

### View Cluster Status
The script automatically shows cluster-wide status when complete, or you can run:
```bash
python -c "from dummy_data_generator import DummyDataGenerator; print(DummyDataGenerator('/path/to/output', 1, 1).get_cluster_status())"
```

#### Example Output

- Example parallel run on a SMALL, slow SBC cluster (5 nodes)

```bash
$ pdsh -w sbc[0-4] '/data/software/dummy-data-generator/parallelDataGen.py -n 1000 -s 100 -t 8 --node-id ${HOSTNAME:3} --node-count 5 /data/software/dummy-data-generator/testOut'
sbc1: Starting generation of 1000 files (100.00 KB each)
sbc1: Using 8 threads
sbc1: Created /data/software/dummy-data-generator/testOut/dummy_n1_1.dat (100.00 KB)
...
sbc3: Created /data/software/dummy-data-generator/testOut/dummy_n3_1873.dat (100.00 KB)
sbc3: 
sbc3: Completed in 13.88 seconds
sbc3: 
sbc3: Cluster-wide status:
sbc3: - Total files created: 4940/5000
sbc3: - Completion: 98.8%
sbc3: - Active nodes: 5/5
sbc3: - Total throughput: 112.39 MB/s
sbc3: - Files per second: 1150.91 files/s
sbc3: Total data generated: 0.10 GB
sbc3: Throughput: 0.01 GB/s
sbc4: Created /data/software/dummy-data-generator/testOut/dummy_n4_1794.dat (100.00 KB)
...
sbc0: Created /data/software/dummy-data-generator/testOut/dummy_n0_3120.dat (100.00 KB)
sbc0: 
sbc0: Completed in 14.33 seconds
sbc0: 
sbc0: Cluster-wide status:
sbc0: - Total files created: 5000/5000
sbc0: - Completion: 100.0%
sbc0: - Active nodes: 5/5
sbc0: - Total throughput: 336.45 MB/s
sbc0: - Files per second: 3445.27 files/s
sbc0: Total data generated: 0.10 GB
sbc0: Throughput: 0.01 GB/s
```

## Performance Tips

1. For best performance:
   - Use fast storage (SSD/NVMe)
   - Match thread count to available CPU cores
   - Distribute load across multiple nodes

2. Expected performance:
   - SSD: 200-500 MB/s per SSD per node
   - HDD: 50-150 MB/s per HDD per node
   - Scale linearly with node count

## Troubleshooting

**Error: File already exists**
- Solution: Use a clean output directory or delete existing files

**Warning: Could not update node status file**
- Solution: Check directory permissions and storage space

**Low throughput**
- Solution:
  - Verify storage performance
  - Reduce thread count if CPU-bound
  - Check for network latency (if using shared storage)

## CHANGELOG

### Version 1.1
- Performance improvements:
  - Pre-generate and reuse random data buffer instead of generating for each file
  - Increased file write buffer size to 1MB for better I/O performance
  - Improved files per second calculation for more accurate reporting
- Added version number and improved help output formatting