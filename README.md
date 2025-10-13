# BenchBase Fork For Aurora DSQL

We have made this repository available for AWS customers to run TPC-C benchmarking against the newly launched [Amazon Aurora DSQL](https://aws.amazon.com/rds/aurora/dsql/).

## Why Use This Aurora DSQL Fork?

This fork applies performance best practices and minor PostgreSQL compatibility adjustments to ensure optimal results with Aurora DSQL Clusters::

### 1. **IAM Integration and Easy Connection**
- Seamless integration with AWS IAM for secure, credential-free database connections
- No need to manage database passwords or connection strings manually

### 2. **Distributed Load and Execution**
- Built-in support for distributing benchmark workloads across multiple availability zones
- Distributing workloads improves performance and provides more realistic benchmark results

### 3. **High performance connection management**
- Intelligent strategy that leverages Aurora DSQL’s connection architecture
- Optimized connection reuse patterns for sustained benchmark execution

### 4. **Enhanced Command Line Support**
- Complete configuration via command line parameters, eliminating the need to modify XML files
- Simplified workflow: configure everything through CLI arguments for faster setup and automation

### 5. **Asynchronous Index Creation**
- Aurora DSQL only supports asynchronous index creation, and this repository respects that requirement
- Proper handling of Aurora DSQL's async-only index creation to prevent blocking operations

### 6. **Foreign Key Constraint Compatibility**
- Aurora DSQL currently does not support foreign key constraints, and this fork automatically handles this limitation
- Schema definitions are optimized to work without foreign key constraints while maintaining data integrity through application logic

These enhancements ensure optimal performance and reliability when benchmarking Aurora DSQL, providing results that accurately reflect the database's capabilities in real-world scenarios.



## Quickstart

To clone and build BenchBase using the auroradsql profile,
```bash
git clone --depth 1 https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking.git
cd aurora-dsql-benchbase-benchmarking
./mvnw clean package -P auroradsql
```
This produces artifacts in the `target` folder, which can be extracted,
```bash
cd target
tar xvzf benchbase-auroradsql.tgz
cd benchbase-auroradsql
```

### Prerequisites

Before running the benchmark, you need to create an Aurora DSQL cluster and ensure proper AWS credentials are configured:

#### 1. Configure AWS Credentials

**For EC2 instances:**
- Ensure your EC2 instance has an IAM role attached with the necessary Aurora DSQL permissions
- The role should include policies for `dsql:*` actions

**For development desktop/local environment:**
```bash
# Set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_SESSION_TOKEN=your_session_token  # if using temporary credentials
```

#### 2. Create Aurora DSQL Cluster (Optional)
If you don't already have an Aurora DSQL cluster, create one:
```bash
aws dsql create-cluster --region ${REGION}
```

#### 3. Run the Benchmark
```bash
export CLUSTER_ENDPOINT=<cluster_id>.dsql.<region>.on.aws
export REGION=<region>
java -jar benchbase.jar -b tpcc -c config/auroradsql/sample_tpcc_config.xml --create=true --load=true --execute=true --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" --region ${REGION}
```

The default configuration will setup a TPC-C run for 200 warehouses. To learn more about the config file changes and the benchmarking results, checkout this [wiki](https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking/wiki#loading-data-and-running-tpc-c-against-an-aurora-dsql-cluster).


## Advanced: Multi-Instance Distributed Benchmarking across AZs

To maximize performance, use this multi-instance approach instead of the single-instance Quickstart method. This distributed approach provides more realistic results by spreading the workload across multiple EC2 instances in different availability zones.

TPC-C benchmarking against Aurora DSQL follows a three-step approach when distributing the benchmark:

**(The following examples demonstrate distributing 200 warehouses across 3 instances in different availability zones:)**
> **Important**: Each loader/executor command should be run on a separate EC2 instance located in a different availability zone to properly distribute the workload and achieve realistic benchmark results.



### Phase 1: Schema and Item Initialization

**Purpose**: Set up the database structure and load shared reference data that all warehouses will use.

**What this phase does**:
- Creates all database tables and their schema
- Creates indexes asynchronously (Aurora DSQL requirement)
- Loads the shared `item` table with 100,000 items used by all warehouses
- Does NOT load warehouse-specific data (that happens in Phase 2)

**Run this phase only once** before starting the distributed warehouse loading:

```bash 
java \
    -jar benchbase.jar \
    -b tpcc \
    -c config/auroradsql/sample_tpcc_config.xml \
    --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" \
    --region ${REGION} \
    --skipMainDataLoad true \
    --scalefactor 200 \
    --create true \
    --load true \
    --execute false
```

### Phase 2: Distributed Warehouse Loading

Run multiple instances to load warehouse data in parallel. Each instance loads a subset of warehouses using stride-based distribution:

##### Loader 1 - Warehouses [1,4,7,10,...,199] (67 warehouses):

```bash 
java \
    -jar benchbase.jar \
    -b tpcc \
    -c config/auroradsql/sample_tpcc_config.xml \
    --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" \
    --region ${REGION} \
    --scalefactor 200 \
    --startWarehouseIndex 1 \
    --endWarehouseIndex 200 \
    --stride 3 \
    --loaderThreads 70 \
    --skipItemLoad true \
    --create false \
    --load true \
    --execute false \
    --clear false
```

#### Loader 2 - Warehouses [2,5,8,...,200] (67 warehouses):
```bash 
java \
    -jar benchbase.jar \
    -b tpcc \
    -c config/auroradsql/sample_tpcc_config.xml \
    --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" \
    --region ${REGION} \
    --scalefactor 200 \
    --startWarehouseIndex 2 \
    --endWarehouseIndex 200 \
    --stride 3 \
    --loaderThreads 70 \
    --skipItemLoad true \
    --create false \
    --load true \
    --execute false \
    --clear false
```

#### Loader 3 - Warehouses [3,6,9,...,198] (66 warehouses):
```bash
java \
    -jar benchbase.jar \
    -b tpcc \
    -c config/auroradsql/sample_tpcc_config.xml \
    --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" \
    --region ${REGION} \
    --scalefactor 200 \
    --startWarehouseIndex 3 \
    --endWarehouseIndex 200 \
    --stride 3 \
    --loaderThreads 70 \
    --skipItemLoad true \
    --create false \
    --load true \
    --execute false \
    --clear false
```

### Phase 3: Distributed Benchmark Execution

Run multiple instances to execute the benchmark workload. Each instance operates on its assigned warehouse subset:


#### Executor 1 - Warehouses [1,4,7,10,...,199] (67 warehouses):

```bash 
java \
    -jar benchbase.jar \
    -b tpcc \
    -c config/auroradsql/sample_tpcc_config.xml \
    --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" \
    --region ${REGION} \
    --scalefactor 200 \
    --startWarehouseIndex 1 \
    --endWarehouseIndex 200 \
    --terminals 67 \
    --stride 3 \
    --create false \
    --load false \
    --execute true \
    --clear false
```

#### Executor 2 - Warehouses [2,5,8,...,200] (67 warehouses):

```bash
java \
    -jar benchbase.jar \
    -b tpcc \
    -c config/auroradsql/sample_tpcc_config.xml \
    --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" \
    --region ${REGION} \
    --scalefactor 200 \
    --startWarehouseIndex 2 \
    --endWarehouseIndex 200 \
    --terminals 67 \
    --stride 3 \
    --create false \
    --load false \
    --execute true \
    --clear false
```

#### Executor 3 - Warehouses [3,6,9,...,198] (66 warehouses):

```bash
java \
    -jar benchbase.jar \
    -b tpcc \
    -c config/auroradsql/sample_tpcc_config.xml \
    --url "jdbc:postgresql://${CLUSTER_ENDPOINT}:5432/postgres?sslmode=require&ApplicationName=tpcc&reWriteBatchedInserts=true" \
    --region ${REGION} \
    --scalefactor 200 \
    --startWarehouseIndex 3 \
    --endWarehouseIndex 200 \
    --terminals 66 \
    --stride 3 \
    --create false \
    --load false \
    --execute true \
    --clear false
```

### Key Parameters

#### Loader Parameters (Phases 1 & 2)

| Parameter | Description | Usage |
|-----------|-------------|-------|
| `--scalefactor` | Total number of warehouses in the database | Should be the same across all instances |
| `--startWarehouseIndex` | First warehouse ID for this instance (1-based) | Different for each loader instance |
| `--endWarehouseIndex` | Last warehouse ID to consider | Usually same as scalefactor |
| `--stride` | Step size between warehouses | Used for distribution (e.g., stride=3 for 3-way split) |
| `--loaderThreads` | Threads for data loading | Controls loading parallelism |
| `--region` | AWS region for Aurora DSQL | Required for Aurora DSQL connections |
| `--create` | Create database tables and schema | Use `true` only in Phase 1 |
| `--load` | Load data into tables | Use `true` in Phase 1 and Phase 2 |
| `--clear` | Clear existing data | Usually `false` for distributed loading |
| `--skipItemLoad` | Skip item table loading | Use `true` except in Phase 1 |
| `--skipMainDataLoad` | Skip warehouse data loading | Use `true` in Phase 1 only |

#### Executor Parameters (Phase 3)

| Parameter | Description | Usage |
|-----------|-------------|-------|
| `--scalefactor` | Total number of warehouses in the database | Should be the same across all instances |
| `--startWarehouseIndex` | First warehouse ID for this instance (1-based) | Different for each executor instance |
| `--endWarehouseIndex` | Last warehouse ID to consider | Usually same as scalefactor |
| `--stride` | Step size between warehouses | Used for distribution (e.g., stride=3 for 3-way split) |
| `--terminals` | Number of concurrent terminals | Should match number of warehouses this instance handles |
| `--region` | AWS region for Aurora DSQL | Required for Aurora DSQL connections |
| `--execute` | Execute benchmark workload | Use `true` only in Phase 3 |
| `--create` | Create database tables and schema | Use `false` in Phase 3 |
| `--load` | Load data into tables | Use `false` in Phase 3 |
| `--clear` | Clear existing data | Usually `false` for distributed execution |

## Benchmark Results and Performance Evaluation

### Individual Instance Results

Each executor instance generates its own benchmark results file containing performance metrics for its assigned warehouse subset.

**Result File Locations:**
- Results are saved in the current directory with timestamps
- File format: `results_<timestamp>.csv` or `results_<timestamp>.json`
- Each instance produces independent result files

### Aggregating Distributed Results

When evaluating overall Aurora DSQL performance, you need to merge results from all executor instances.

#### Manual Aggregation Method

1. **Collect Result Files**: Gather all result files from each executor instance
2. **Sum Throughput**: Add TPS values from all instances for total cluster throughput
3. **Weighted Average Latency**: Calculate weighted averages based on transaction volumes
4. **Combine Transaction Counts**: Sum successful/failed transactions across all instances

#### Example Aggregation Calculation

For 3 executor instances with results:
- **Instance 1**: 1,200 TPS, 67 warehouses
- **Instance 2**: 1,180 TPS, 67 warehouses  
- **Instance 3**: 1,150 TPS, 66 warehouses

**Total Cluster Performance**: 3,530 TPS across 200 warehouses
