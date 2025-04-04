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

## Performance Tips

1. For best performance:
   - Use fast storage (SSD/NVMe)
   - Match thread count to available CPU cores
   - Distribute load across multiple nodes

2. Expected performance:
   - SSD: 200-500 MB/s per node
   - HDD: 50-150 MB/s per node
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

## License

[MIT License](LICENSE)
