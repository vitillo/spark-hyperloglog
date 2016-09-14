# spark-hyperloglog
Algebird's HyperLogLog support for Apache Spark. This package can be used in concert
with [presto-hyperloglog](https://github.com/vitillo/presto-hyperloglog) to share
HyperLogLog sets between Spark and Presto.

[![Build Status](https://travis-ci.org/vitillo/spark-hyperloglog.svg?branch=master)](https://travis-ci.org/vitillo/spark-hyperloglog)
[![codecov.io](https://codecov.io/github/vitillo/spark-hyperloglog/coverage.svg?branch=travis)](https://codecov.io/github/vitillo/spark-hyperloglog?branch=travis)

### Example usage
```scala
import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._

val hllMerge = new HyperLogLogMerge
sqlContext.udf.register("hll_merge", hllMerge)
sqlContext.udf.register("hll_create", hllCreate _)
sqlContext.udf.register("hll_cardinality", hllCardinality _)

val frame = sc.parallelize(List("a", "b", "c", "c")).toDF("id")
val count = frame
  .select(expr("hll_create(id, 12) as hll"))
  .groupBy()
  .agg(expr("hll_cardinality(hll_merge(hll)) as count"))
  .show()
```

yields:

```bash
+-----+
|count|
+-----+
|    3|
+-----+
```

### Deployment
1. Install the [sbt plugin](https://github.com/databricks/sbt-spark-package) for Spark Packages.

2. Configure your credentials for the Spark Packages repository in `~/.ivy2/.sbtcredentials`, e.g:
   ```
   realm=Spark Packages Realm
   host=spark-packages.org
   user=foo
   password=bar
   ```

3. Publish a new release with `sbt spPublish`
