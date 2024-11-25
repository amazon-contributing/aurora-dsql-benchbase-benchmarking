# BenchBase Fork For Aurora DSQL

BenchBase (formerly [OLTPBench](https://github.com/oltpbenchmark/oltpbench/)) is a Multi-DBMS SQL Benchmarking Framework via JDBC. This fork of benchbase has a sample config file to run a TPC-C workload on an Aurora DSQL cluster.

**Table of Contents**

- [Before you begin](#before-you-begin)
- [Quickstart](#quickstart)
- [Description](#description)
- [Usage Guide](#usage-guide)

---

## Before you begin

### Uderstanding Benchbase: A Benchmarking Framework

[Benchbase](https://github.com/cmu-db/benchbase) is an open-source benchmarking tool designed to assess the performance of relational and NoSQL databases under different workloads. It was originally developed by the Carnegie Mellon Database Group and is widely used to evaluate database systems, both in academic and industry settings. The tool simplifies the process of setting up and running benchmarks by providing a standard interface and a range of workloads, such as TPC-C, TPC-H, etc.

### Install Java and Maven

The benchbase package we will be using requires [Java 21](https://docs.aws.amazon.com/corretto/latest/corretto-21-ug/downloads-list.html) and  [Maven](https://maven.apache.org/install.html).

### Have a DSQL Cluster ready

If you haven’t done it already, go ahead and create a cluster to use for the TPC-C benchmarking following this guide [LINK to creating clusters]


## Quickstart

### Step 1: Setting up Benchbase

1. We will be using a forked version of the original Benchbase repository that can be pulled from the amazon-contributing github repo using the following command:

```bash
git clone --depth 1 https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking.git
cd aurora-dsql-benchbase-benchmarking
./mvnw clean package -P auroradsql
```

2. After cloning and building the package:

```bash
cd target
tar xvzf benchbase-auroradsql.tgz
cd benchbase-auroradsql
```

### Step 2: Loading TPC-C data

We have added sample configuration and ddl files to the repository, allowing you to run TPC-C benchmarks against an Aurora DSQL cluster.

1. Edit the `config/auroradsql/sample_tpcc_config.xml file`:

This file contains various settings that can be adjusted based on how users want to run the benchmarking test. Before loading any data into the table, replace `localhost` in the `<url></url>` tag with your Aurora DSQL cluster endpoint.

Next, set the username and the password token inside the `<username></username>` and `<password></password>` tags. If you don’t know how to generate a password token, follow this guide [LINK to token generation].

We have also added automatic password/token generation using IAM authentication in our custom Benchbase implementation. To use it, simply leave the `<password></password>` field empty. To understand where the credentials and region information are fetched from, checkout these libraries:
- [DefaultCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html)
- [DefaultAwsRegionProviderChain](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/regions/providers/DefaultAwsRegionProviderChain.html)

Set the `<scalefactor></scalefactor>` to the number of TPC-C warehouses you would like to create and run the benchmark on.

Finally, set the workload parameters. These settings will be used by the workload in the next step. Change the number of `<terminals></terminals>` to determine how many concurrent connections to make. These connections are evenly spread over the number of warehouses. Note that more terminals mean more memory usage.

2. Save the config file and run the following command to create tables and load data:
```shell
java -jar benchbase.jar -b tpcc -c config/auroradsql/sample_tpcc_config.xml --create=true --load=true --execute=false
```

We separated the loading and test execution into two steps. You can set the `--execute` flag to `true` to have the benchmarking run immediately after the data is loaded.


Step 3: Running TPC-C and Interpreting Results

1. Run the benchmark using the following command:

```shell
java -jar benchbase.jar -b tpcc -c config/auroradsql/sample_tpcc_config.xml --create=false --load=false --execute=true
```

Once the workload has finished running, Benchbase will output a histogram in the terminal,  several `.csv` files in the results folder and a `summary.json` file in the same folder. For a 1-warehouse run with 10 terminals, the histogram might look like this:

```shell
[INFO ] 2024-11-21 19:10:23,042 [main]  com.oltpbenchmark.DBWorkload runWorkload - ======================================================================
[INFO ] 2024-11-21 19:10:23,043 [main]  com.oltpbenchmark.DBWorkload runWorkload - Rate limited reqs/s: Results(state=EXIT, nanoSeconds=60999908412, measuredRequests=3168) = 51.93450420618642 requests/sec (throughput), 52.24598008368564 requests/sec (goodput)
[INFO ] 2024-11-21 19:10:23,049 [main]  com.oltpbenchmark.DBWorkload writeOutputs - Output Raw data into file: tpcc_2024-11-21_19-10-23.raw.csv
[INFO ] 2024-11-21 19:10:23,081 [main]  com.oltpbenchmark.DBWorkload writeOutputs - Output samples into file: tpcc_2024-11-21_19-10-23.samples.csv
[INFO ] 2024-11-21 19:10:23,088 [main]  com.oltpbenchmark.DBWorkload writeOutputs - Output summary data into file: tpcc_2024-11-21_19-10-23.summary.json
[INFO ] 2024-11-21 19:10:23,096 [main]  com.oltpbenchmark.DBWorkload writeOutputs - Output DBMS parameters into file: tpcc_2024-11-21_19-10-23.params.json
[INFO ] 2024-11-21 19:10:23,096 [main]  com.oltpbenchmark.DBWorkload writeOutputs - Output benchmark config into file: tpcc_2024-11-21_19-10-23.config.xml
[INFO ] 2024-11-21 19:10:23,131 [main]  com.oltpbenchmark.DBWorkload writeOutputs - Output results into file: tpcc_2024-11-21_19-10-23.results.csv with window size 5
[INFO ] 2024-11-21 19:10:23,149 [main]  com.oltpbenchmark.DBWorkload writeHistograms - ======================================================================
[INFO ] 2024-11-21 19:10:23,149 [main]  com.oltpbenchmark.DBWorkload writeHistograms - Workload Histograms:

Completed Transactions:
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01                         [1473]
com.oltpbenchmark.benchmarks.tpcc.procedures.Payment/02                          [1330] *
com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus/03                      [ 136] *
com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery/04                         [ 130] *
com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel/05                       [ 118] *

Aborted Transactions:
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01                         [  15]

Rejected Transactions (Server Retry):
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01                         [  91] *
com.oltpbenchmark.benchmarks.tpcc.procedures.Payment/02                          [3468]
com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery/04                         [ 173] **

Rejected Transactions (Retry Different):
<EMPTY>

Unexpected SQL Errors:
<EMPTY>

Unknown Status Transactions:
<EMPTY>


[INFO ] 2024-11-21 19:10:23,149 [main]  com.oltpbenchmark.DBWorkload writeHistograms - ======================================================================
```

This result indicates that the test ran for `60 seconds` and processed `3168 transactions`, of which `1473` were New Order transactions. The number of New Order transactions is your `tpmC`. If the test runs for longer than 60 seconds, you can calculate the tpmC by dividing the number of completed `New Order Transactions / the test duration in minutes`.

(Checkout the TPC-C documentation to understand how the Tpmc is calculated: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf)

### Step 4: Clean Up

When you are done testing your DSQL cluster.

1. Run the following commands to clean up the package:
```shell
cd ../..
./mvnw clean
```

2. Delete your cluster by following this guide: [LINK on how to delete your DSQL cluster]

---

## Description

Benchmarking is incredibly useful, yet endlessly painful. This benchmark suite is the result of a group of
PhDs/post-docs/professors getting together and combining their workloads/frameworks/experiences/efforts. We hope this
will save other people's time, and will provide an extensible platform, that can be grown in an open-source fashion.

BenchBase is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

The BenchBase framework has the following benchmarks:

* [AuctionMark](https://github.com/cmu-db/benchbase/wiki/AuctionMark)
* [CH-benCHmark](https://github.com/cmu-db/benchbase/wiki/CH-benCHmark)
* [Epinions.com](https://github.com/cmu-db/benchbase/wiki/epinions)
* hyadapt -- pending configuration files
* [NoOp](https://github.com/cmu-db/benchbase/wiki/NoOp)
* [OT-Metrics](https://github.com/cmu-db/benchbase/wiki/OT-Metrics)
* [Resource Stresser](https://github.com/cmu-db/benchbase/wiki/Resource-Stresser)
* [SEATS](https://github.com/cmu-db/benchbase/wiki/Seats)
* [SIBench](https://github.com/cmu-db/benchbase/wiki/SIBench)
* [SmallBank](https://github.com/cmu-db/benchbase/wiki/SmallBank)
* [TATP](https://github.com/cmu-db/benchbase/wiki/TATP)
* [TPC-C](https://github.com/cmu-db/benchbase/wiki/TPC-C)
* [TPC-H](https://github.com/cmu-db/benchbase/wiki/TPC-H)
* TPC-DS -- pending configuration files
* [Twitter](https://github.com/cmu-db/benchbase/wiki/Twitter)
* [Voter](https://github.com/cmu-db/benchbase/wiki/Voter)
* [Wikipedia](https://github.com/cmu-db/benchbase/wiki/Wikipedia)
* [YCSB](https://github.com/cmu-db/benchbase/wiki/YCSB)

This framework is design to allow for easy extension. We provide stub code that a contributor can use to include a new
benchmark, leveraging all the system features (logging, controlled speed, controlled mixture, etc.)

---

## Usage Guide

### How to Build
Run the following command to build the distribution for a given database specified as the profile name (`-P`).  The following profiles are currently supported: `postgres`, `mysql`, `mariadb`, `sqlite`, `cockroachdb`, `phoenix`, `spanner`, and `auroradsql`.

```bash
./mvnw clean package -P <profile name>
```

The following files will be placed in the `./target` folder:

* `benchbase-<profile name>.tgz`
* `benchbase-<profile name>.zip`

### How to Run
Once you build and unpack the distribution, you can run `benchbase` just like any other executable jar.  The following examples assume you are running from the root of the expanded `.zip` or `.tgz` distribution.  If you attempt to run `benchbase` outside of the distribution structure you may encounter a variety of errors including `java.lang.NoClassDefFoundError`.

To bring up help contents:
```bash
java -jar benchbase.jar -h
```

To execute the `tpcc` benchmark:
```bash
java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true
```

For composite benchmarks like `chbenchmark`, which require multiple schemas to be created and loaded, you can provide a comma separated list:
```bash
java -jar benchbase.jar -b tpcc,chbenchmark -c config/postgres/sample_chbenchmark_config.xml --create=true --load=true --execute=true
```

The following options are provided:

```text
usage: benchbase
 -b,--bench <arg>               [required] Benchmark class. Currently
                                supported: [tpcc, tpch, tatp, wikipedia,
                                resourcestresser, twitter, epinions, ycsb,
                                seats, auctionmark, chbenchmark, voter,
                                sibench, noop, smallbank, hyadapt,
                                otmetrics, templated]
 -c,--config <arg>              [required] Workload configuration file
    --clear <arg>               Clear all records in the database for this
                                benchmark
    --create <arg>              Initialize the database for this benchmark
 -d,--directory <arg>           Base directory for the result files,
                                default is current directory
    --dialects-export <arg>     Export benchmark SQL to a dialects file
    --execute <arg>             Execute the benchmark workload
 -h,--help                      Print this help
 -im,--interval-monitor <arg>   Throughput Monitoring Interval in
                                milliseconds
 -jh,--json-histograms <arg>    Export histograms to JSON file
    --load <arg>                Load data using the benchmark's data
                                loader
 -s,--sample <arg>              Sampling window
```

### How to Run with Maven

Instead of first building, packaging and extracting before running benchbase, it is possible to execute benchmarks directly against the source code using Maven. Once you have the project cloned you can run any benchmark from the root project directory using the Maven `exec:java` goal. For example, the following command executes the `tpcc` benchmark against `postgres`:

```
mvn clean compile exec:java -P postgres -Dexec.args="-b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true"
```

this is equivalent to the steps above but eliminates the need to first package and then extract the distribution.

### How to Enable Logging

To enable logging, e.g., for the PostgreSQL JDBC driver, add the following JVM property when starting...

```
-Djava.util.logging.config.file=src/main/resources/logging.properties
```

To modify the logging level you can update [`logging.properties`](src/main/resources/logging.properties) and/or [`log4j.properties`](src/main/resources/log4j.properties).

### How to Release

```
./mvnw -B release:prepare
./mvnw -B release:perform
```

### How use with Docker

- Build or pull a dev image to help building from source:

  ```sh
  ./docker/benchbase/build-dev-image.sh
  ./docker/benchbase/run-dev-image.sh
  ```

  or

  ```sh
  docker run -it --rm --pull \
    -v /path/to/benchbase-source:/benchbase \
    -v $HOME/.m2:/home/containeruser/.m2 \
    benchbase.azure.cr.io/benchbase-dev
  ```

- Build the full image:

  ```sh
  # build an image with all profiles
  ./docker/benchbase/build-full-image.sh

  # or if you only want to build some of them
  BENCHBASE_PROFILES='postgres mysql' ./docker/benchbase/build-full-image.sh
  ```

- Run the image for a given profile:

  ```sh
  BENCHBASE_PROFILE='postgres' ./docker/benchbase/run-full-image.sh --help # or other benchbase args as before
  ```

  or

  ```sh
  docker run -it --rm --env BENCHBASE_PROFILE='postgres' \
    -v results:/benchbase/results benchbase.azurecr.io/benchbase --help # or other benchbase args as before
  ```

> See the [docker/benchbase/README.md](./docker/benchbase/) for further details.

[Github Codespaces](https://github.com/features/codespaces) and [VSCode devcontainer](https://code.visualstudio.com/docs/remote/containers) support is also available.

---

## Credits

BenchBase is the official modernized version of the original OLTPBench.

The original OLTPBench code was largely written by the authors of the original paper, [OLTP-Bench: An Extensible Testbed for Benchmarking Relational Databases](http://www.vldb.org/pvldb/vol7/p277-difallah.pdf), D. E. Difallah, A. Pavlo, C. Curino, and P. Cudré-Mauroux. In VLDB 2014. Please see the citation guide below.

A significant portion of the modernization was contributed by [Tim Veil @ Cockroach Labs](https://github.com/timveil-cockroach), including but not limited to:

* Built with and for Java ~~17~~ 21.
* Migration from Ant to Maven.
  * Reorganized project to fit Maven structure.
  * Removed static `lib` directory and dependencies.
  * Updated required dependencies and removed unused or unwanted dependencies.
  * Moved all non `.java` files to standard Maven `resources` directory.
  * Shipped with [Maven Wrapper](https://maven.apache.org/wrapper).
* Improved packaging and versioning.
    * Moved to Calendar Versioning (https://calver.org/).
    * Project is now distributed as a `.tgz` or `.zip` with an executable `.jar`.
    * All code updated to read `resources` from inside `.jar` instead of directory.
* Moved from direct dependence on Log4J to SLF4J.
* Reorganized and renamed many files for clarity and consistency.
* Applied countless fixes based on "Static Analysis".
    * JDK migrations (boxing, un-boxing, etc.).
    * Implemented `try-with-resources` for all `java.lang.AutoCloseable` instances.
    * Removed calls to `printStackTrace()` or `System.out.println` in favor of proper logging.
* Reformatted code and cleaned up imports.
* Removed all calls to `assert`.
* Removed various forms of dead code and stale configurations.
* Removed calls to `commit()` during `Loader` operations.
* Refactored `Worker` and `Loader` usage of `Connection` objects and cleaned up transaction handling.
* Introduced [Dependabot](https://dependabot.com/) to keep Maven dependencies up to date.
* Simplified output flags by removing most of them, generally leaving the reporting functionality enabled by default.
* Provided an alternate `Catalog` that can be populated directly from the configured Benchmark database. The old catalog was proxied through `HSQLDB` -- this remains an option for DBMSes that may have incomplete catalog support.