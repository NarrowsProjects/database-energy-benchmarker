# Energy Benchmarking Suite for Databases
A Rust-based benchmarking tool to measure and compare energy efficiency between MongoDB and PostgreSQL when handling hierarchical data with different access patterns.

## Features

- Comparative analysis of energy consumption
- Hierarchical JSON data generation (3, 5, 10 levels deep)
- Power monitoring integration via Intel PCM
- Read-heavy & write-heavy workload simulations
- Indexed vs non-indexed query comparisons
- CSV output for detailed analysis

## Prerequisites

- Rust 1.83+ & Cargo
- MongoDB 8.06+
- PostgreSQL 17.4+
- Intel PCM (Power Control Mechanism)

## Installation

1. Clone repository:
   ```bash
    https://github.com/NarrowsProjects/database-energy-benchmarker.git  
    cd energy-benchmark
   ```

2. Build with optimizations:
   ```bash
   cargo build --release
   ```

## Usage

1. Generate test data (10k documents per depth):
   ```bash
   cargo run --bin generate_data
   ```

2. Run full benchmark suite:
   ```bash
   cargo run --release --bin energy_benchmark
   ```

## Test Types

| Test ID | Description                     | Indexes | Workload Ratio |
|---------|---------------------------------|---------|----------------|
| 1A      | Read-heavy baseline            | No      | 5:1 (R/W)      |
| 1B      | Write-heavy baseline           | No      | 1:5 (R/W)      |
| 2A      | Read-heavy with indexing       | Yes     | 5:1 (R/W)      |
| 2B      | Write-heavy with indexing      | Yes     | 1:5 (R/W)      |
| C1      | Idle power consumption control | N/A     | N/A            |

## Configuration

Adjust in `main.rs`:
```rust
let depths = [3, 5, 10];      // Nesting levels
let epochs = [1,2,3];             // Test repetitions 
let num_docs = 10000;         // Documents per test
```

## Results

Output CSV files stored in:
```
src/energy_benchmarks/
  ├── MongoDB_read_heavy_depth_3_epoch_1.csv
  ├── PostgreSQL_write_heavy__depth_5_epoch_1_with_index.csv
  └── Control_Test_C1_epoch_1.csv
```

## Extending with New Databases

1. Implement the `Database` trait:
   ```rust
   #[async_trait]
   impl Database for NewDB {
      async fn connect(&mut self) -> Result<()> { ... }
      async fn insert_test_data(&self) -> Result<()> { ... }
      async fn connect(&mut self) -> Result<(), Box<dyn Error>> { ... }
      async fn disconnect(&mut self) -> Result<(), Box<dyn Error>> { ... }
      async fn clean_database(&self) -> Result<(), Box<dyn Error>> { ... }
      async fn insert_test_data(
         &self, 
         batch_size: usize, 
         test_data: Vec<Value>
         ) -> Result<(), Box<dyn Error>> { ... }
      async fn run_queries(
         &self,
         depth: u8,
         num_reads: usize,
         num_writes: usize,
         use_index: bool,
         num_docs: usize,
         measurement_name: &str
      ) -> Result<(), Box<dyn Error>> { ... }

   }

   ```

2. Add to databases vector in main.rs:
   ```rust
   let mut databases: Vec<Box<dyn Database>> = vec![
       Box::new(MongoDB::new()),
       Box::new(PostgreSQL::new()),
       Box::new(NewDB::new())
   ];
   ```

## Limitations
- Requires root access for power monitoring
- Assumes local DB instances
