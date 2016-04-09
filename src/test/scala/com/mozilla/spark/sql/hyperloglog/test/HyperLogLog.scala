/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.spark.sql.hyperloglog.test

import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.function._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class HyperLogLogTest extends FlatSpec with Matchers{
 "Algebird's HyperLogLog" can "be used from Spark" in {
  val sparkConf = new SparkConf().setAppName("HyperLogLog")
  sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val hllMerge = new HyperLogLogMerge
  sqlContext.udf.register("hll_merge", hllMerge)
  sqlContext.udf.register("hll_create", hllCreate _)
  sqlContext.udf.register("hll_cardinality", hllCardinality _)

  val frame = sc.parallelize(List("a", "b", "c", "c"), 4).toDF("id")
  val count = frame
    .select(expr("hll_create(id, 12) as hll"))
    .groupBy()
    .agg(expr("hll_cardinality(hll_merge(hll)) as count"))
    .collect()
  count(0)(0) should be (3)
 }
}
