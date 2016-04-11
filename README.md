# spark-hyperloglog
Algebird's HyperLogLog support for Apache Spark. This package can be used in concert
with [presto-hyperloglog](https://github.com/vitillo/presto-hyperloglog) to share
HyperLogLog sets between Spark and Presto.

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

