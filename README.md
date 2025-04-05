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
| `--direct-io` | Use direct I/O for file writes (bypasses OS cache) | False |

**Note:** When using `--direct-io`, the file size (`-s`) will be automatically rounded up to the nearest multiple of 4KB (4096 bytes) to meet direct I/O alignment requirements. This means the actual file size may be larger than specified.

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

- Example parallel run using `pdsh` on a SMALL, slow SBC cluster (5 nodes)

```bash
[root@sbc0 parallelDataGen]# pdsh -w sbc[0-4] '/data/software/parallelDataGen/parallelDataGen -n 1000 -s 1 -t 4 --node-id ${HOSTNAME:3} --node-count 5 /data/software/dummy-data-generator/testOut1'
sbc0: Starting generation of 1000 files (1.00 KB each)
sbc0: Using 4 threads
sbc0: Created /data/software/dummy-data-generator/testOut1/dummy_n0_0.dat (1.00 KB)
sbc0: Created /data/software/dummy-data-generator/testOut1/dummy_n0_1250.dat (1.00 KB)
sbc0: Created /data/software/dummy-data-generator/testOut1/dummy_n0_3750.dat (1.00 KB)
sbc0: Created /data/software/dummy-data-generator/testOut1/dummy_n0_1255.dat (1.00 KB)
sbc0: Created /data/software/dummy-data-generator/testOut1/dummy_n0_5.dat (1.00 KB)
...
sbc0: Created /data/software/dummy-data-generator/testOut/dummy_n0_3120.dat (100.00 KB)
sbc1: Created /data/software/dummy-data-generator/testOut1/dummy_n1_1246.dat (1.00 KB)
sbc1: 
sbc1: Completed in 13.97 seconds
sbc1: 
sbc1: Cluster-wide status:
sbc1: - Total files created: 5000/5000
sbc1: - Completion: 100.0%
sbc1: - Active nodes: 5/5
sbc1: - Total throughput: 2.49 MB/s
sbc1: - Files per second: 553.77 files/s
sbc1: Total data generated: 0.00 GB
sbc1: Throughput: 0.00 GB/s
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

### Version 1.2
- Added robust direct I/O support:
  - Implemented memory-mapped buffers for proper alignment
  - Automatic size adjustment to meet 4KB alignment requirements
  - Improved error handling for direct I/O operations
  - Achieved up to 640 MB/s throughput with direct I/O on large files
- Performance optimizations:
  - Switched to os.urandom() for faster random data generation
  - Improved buffer reuse across threads
  - Added direct I/O support for bypassing OS cache (use `--direct-io`)

### Version 1.1
- Performance improvements:
  - Pre-generate and reuse random data buffer instead of generating for each file
  - Increased file write buffer size to 1MB for better I/O performance
  - Improved files per second calculation for more accurate reporting
- Added version number and improved help output formatting