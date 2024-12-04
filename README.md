# BenchBase Fork For Aurora DSQL

We have temporarily made this repository available for AWS customers to run TPC-C benchmarking against the newly launched [Amazon Aurora DSQL](https://aws.amazon.com/rds/aurora/dsql/). We will archive this repository once we upstream our modifications to the original [cmu-db/benchbase](https://github.com/cmu-db/benchbase) repository.

> Please note that based on the [default limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html) for Aurora DSQL clusters during preview, you can run a TPC-C workload with up to 200 warehouses. If you need to run a higher load for benchmarking, please [contact AWS Support](https://support.console.aws.amazon.com/support/home#/) to request a limit increase.

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

Replace localhost in the <url></url> tag with an Aurora DSQL cluster endpoint.

Inside this folder, edit the `config/auroradsql/sample_tpcc_config.xml` by replacing `localhost` inside the `<url></url>` field with your Auroral DSQL cluster endpoint, then run BenchBase by executing the tpcc benchmark,
```bash
java -jar benchbase.jar -b tpcc -c config/auroradsql/sample_tpcc_config.xml --create=true --load=true --execute=true
```
The default configuration will setup a TPC-C run for 200 warehouses. To learn more about the config file changes and the benchmarking results, checkout this [wiki](https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking/wiki#loading-data-and-running-tpc-c-against-an-aurora-dsql-cluster).
