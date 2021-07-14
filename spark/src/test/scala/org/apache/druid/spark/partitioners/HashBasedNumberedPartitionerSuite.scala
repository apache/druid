/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.spark.partitioners

import org.apache.druid.spark.SparkFunSuite
import org.apache.druid.spark.v2.DruidDataSourceV2TestUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.seqAsJavaListConverter

class HashBasedNumberedPartitionerSuite extends SparkFunSuite with Matchers with DruidDataSourceV2TestUtils {
  val schemaWithHashPartitionCols: StructType = schema
    .add(_timeBucketCol, LongType, false)
    .add(_rankCol, IntegerType)
    .add(_partitionNumCol, IntegerType, false)
    .add(_partitionKeyCol, IntegerType, false)

  test("HashBasedNumberedPartitioner.partition should correctly hash partition within time buckets") {
    val partitionedDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(
        1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch, firstTimeBucket, 1, 2, 0)),
      Row.fromSeq(Seq(
        1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch, firstTimeBucket, 2, 2, 1)),
      Row.fromSeq(Seq(
        1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch, firstTimeBucket, 3, 2, 0)),
      Row.fromSeq(Seq(
        1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch, firstTimeBucket, 4, 2, 1)),
      Row.fromSeq(Seq(
        1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, secondTimeBucket,
        1, 1, 0)),
      Row.fromSeq(Seq(
        1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, secondTimeBucket, 2, 1, 0))
    ).asJava, schemaWithHashPartitionCols)
      .repartition(col(_timeBucketCol), col(_partitionKeyCol))

    val partitionDimensions = None

    val expectedRows = Set[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch)),
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
      )
    )

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("bucketId" -> "0", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "2",
        "numPartitions" -> "2"),
      Map("bucketId" -> "1", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "1", "numBuckets" -> "2",
        "numPartitions" -> "2"),
      Map("bucketId" -> "0", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "1",
        "numPartitions" -> "1")
    )

    validateRowsAndPartitionMap(
      partitionedDf,
      expectedRows,
      expectedPartitionMap,
      partitionDimensions,
      (partitioner: HashBasedNumberedPartitioner) => partitioner
        .partition(timestampColumn, timestampFormat, segmentGranularity, 2L, partitionDimensions, false)
    )
  }

  test("HashBasedNumberedPartitioner.partition should correctly group all rows with the same partition key" +
    " into the same partition even if the number of rows with the given partition key exceeds rowsPerPartition") {
    val partitionedDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(
        1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch, firstTimeBucket, 1, 2, 1)),
      Row.fromSeq(Seq(
        1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch, firstTimeBucket, 2, 2, 1)),
      Row.fromSeq(Seq(
        1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch, firstTimeBucket, 3, 2, 1)),
      Row.fromSeq(Seq(
        1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch, firstTimeBucket, 4, 2, 0)),
      Row.fromSeq(Seq(
        1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, secondTimeBucket,
        1, 1, 0)),
      Row.fromSeq(Seq(
        1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, secondTimeBucket, 2, 1, 0))
    ).asJava, schemaWithHashPartitionCols)
      .repartition(col(_timeBucketCol), col(_partitionKeyCol))

    val partitionDimensions = Some(Seq("dim2"))

    val expectedRows = Set[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch)),
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
      )
    )

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("bucketId" -> "0", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "2",
        "partitionDimension" -> "dim2", "numPartitions" -> "2"),
      Map("bucketId" -> "1", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "1", "numBuckets" -> "2",
        "partitionDimension" -> "dim2", "numPartitions" -> "2"),
      Map("bucketId" -> "0", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "1",
        "partitionDimension" -> "dim2", "numPartitions" -> "1")
    )

    validateRowsAndPartitionMap(
      partitionedDf,
      expectedRows,
      expectedPartitionMap,
      partitionDimensions.map(_.mkString(",")),
      (partitioner: HashBasedNumberedPartitioner) => partitioner
        .partition(timestampColumn, timestampFormat, segmentGranularity, 2L, partitionDimensions, false)
    )
  }

  test("HashBasedNumberedPartitioner.partition should correctly assign all rows to one partition when" +
    " rowsPerPartition exceeds the number of rows in a partition") {
    val partitionedDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(
        1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch, firstTimeBucket, 1, 1, 0)),
      Row.fromSeq(Seq(
        1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch, firstTimeBucket, 2, 1, 0)),
      Row.fromSeq(Seq(
        1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch, firstTimeBucket, 3, 1, 0)),
      Row.fromSeq(Seq(
        1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch, firstTimeBucket, 4, 1, 0)),
      Row.fromSeq(Seq(
        1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, secondTimeBucket,
        1, 1, 0)),
      Row.fromSeq(Seq(
        1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, secondTimeBucket, 2, 1, 0))
    ).asJava, schemaWithHashPartitionCols)
      .repartition(col(_timeBucketCol), col(_partitionKeyCol))

    val partitionDimensions = Some(Seq("dim2", "id1"))

    val expectedRows = Set[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch)),
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch)),
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
      )
    )

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("bucketId" -> "0", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "1",
        "partitionDimension" -> "dim2,id1", "numPartitions" -> "1"),
      Map("bucketId" -> "0", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "1",
        "partitionDimension" -> "dim2,id1", "numPartitions" -> "1")
    )

    validateRowsAndPartitionMap(
      partitionedDf,
      expectedRows,
      expectedPartitionMap,
      partitionDimensions.map(_.mkString(",")),
      (partitioner: HashBasedNumberedPartitioner) => partitioner
        .partition(timestampColumn, timestampFormat, segmentGranularity, 5L, partitionDimensions, false)
    )
  }

  /**
    * What's happening here is that each row should in theory be in its own partition, but some rows hash to the same
    * key and so we end up with some physical partitions with more rows than rowsPerPartition and some partitions with
    * no rows assigned. In this case, we should "pack" the partitions that do have rows
    */
  test("DruidDataFrame.hashPartition should correctly pack partitions when not all buckets are filled") {
    val partitionedDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(
        1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch, firstTimeBucket, 1, 4, 0)),
      Row.fromSeq(Seq(
        1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch, firstTimeBucket, 2, 4, 1)),
      Row.fromSeq(Seq(
        1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch, firstTimeBucket, 3, 4, 0)),
      Row.fromSeq(Seq(
        1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch, firstTimeBucket, 4, 4, 1)),
      Row.fromSeq(Seq(
        1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, secondTimeBucket,
        1, 2, 1)),
      Row.fromSeq(Seq(
        1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, secondTimeBucket, 2, 2, 1))
    ).asJava, schemaWithHashPartitionCols)
      .repartition(col(_timeBucketCol), col(_partitionKeyCol))

    val partitionDimensions = None

    val expectedRows = Set[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch)),
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
      )
    )

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("bucketId" -> "0", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "4",
        "numPartitions" -> "2"),
      Map("bucketId" -> "1", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "1", "numBuckets" -> "4",
        "numPartitions" -> "2"),
      Map("bucketId" -> "1", "hashPartitionFunction" -> "murmur3_32_abs", "partitionId" -> "0", "numBuckets" -> "2",
        "numPartitions" -> "1")
    )

    validateRowsAndPartitionMap(
      partitionedDf,
      expectedRows,
      expectedPartitionMap,
      partitionDimensions,
      (partitioner: HashBasedNumberedPartitioner) => partitioner
        .partition(timestampColumn, timestampFormat, segmentGranularity, 1L, None, false)
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "25")
  }

  private def validateRowsAndPartitionMap(
                                           df: DataFrame,
                                           expectedRows: Set[Seq[Row]],
                                           expectedPartitionMap: Seq[Map[String, String]],
                                           partitionDimensions: Option[String],
                                           partitionFunc: HashBasedNumberedPartitioner => DataFrame
                                         ): Unit = {
    val sourceDf = df.select(schema.fieldNames.map(name => col(name)): _*)
    val partitioner = new HashBasedNumberedPartitioner(sourceDf)
    val result = partitionFunc(partitioner)
    val resultPartitions = getDataFramePartitions(result)

    expectedRows should contain theSameElementsAs resultPartitions

    val partitionMap = HashBasedNumberedPartitioner.generatePartitionMap(df, partitionDimensions)
    expectedPartitionMap should contain theSameElementsAs partitionMap.values
  }
}
