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

class SingleDimensionPartitionerSuite extends SparkFunSuite with Matchers with DruidDataSourceV2TestUtils {
  val schemaWithRangePartitionCols: StructType = schema
    .add(_timeBucketCol, LongType, false)
    .add(_rankCol, IntegerType)
    .add(_partitionNumCol, IntegerType)

  test("SingleDimensionPartitioner.partition should partition by ordered dimension value") {
    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "25")
    val partitionedDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(
        1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch, firstTimeBucket, 3, 1)),
      Row.fromSeq(Seq(
        1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch, firstTimeBucket, 4, 2)),
      Row.fromSeq(Seq(
        1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, secondTimeBucket,
        2, 1)),
      Row.fromSeq(Seq(
        1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, secondTimeBucket, 1, 0))
    ).asJava, schemaWithRangePartitionCols)
      .repartition(col(_timeBucketCol), col(_partitionNumCol))

    // In reality, you can't partition Druid segments on a metric but we're just borrowing the schema here and the
    // sum_metric1 has the best distribution for testing range partitioning. We should rewrite these tests to use a
    // partitioning-tailored dataframe rather than the writing-tailored dataframe we're using now.
    val partitionCol = "sum_metric1"

    val expectedRows = Seq[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
      ),
        Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch))
      )
    )

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("partitionId" -> "0", "dimension" -> "sum_metric1", "numPartitions" -> "3", "end" -> "3"),
      Map("partitionId" -> "1", "dimension" -> "sum_metric1", "numPartitions" -> "3", "start" -> "3", "end" -> "4"),
      Map("partitionId" -> "2", "dimension" -> "sum_metric1", "numPartitions" -> "3", "start" -> "4"),
      Map("partitionId" -> "0", "dimension" -> "sum_metric1", "numPartitions" -> "2", "end" -> "2"),
      Map("partitionId" -> "1", "dimension" -> "sum_metric1", "numPartitions" -> "2", "start" -> "2")
    )

    validateRowsAndPartitionMap(
      partitionedDf,
      expectedRows,
      expectedPartitionMap,
      partitionCol,
      (partitioner: SingleDimensionPartitioner) => partitioner
        .partition(timestampColumn, timestampFormat, segmentGranularity, 2L, partitionCol, false)
    )
  }

  test("SingleDimensionPartitioner.partition should keep rows with the same partition value in the same" +
    " partition even if the resulting partition exceeds rowsPerPartition") {
    val partitionedDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(
        1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, secondTimeBucket,
        2, 1)),
      Row.fromSeq(Seq(
        1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, secondTimeBucket, 1, 0))
    ).asJava, schemaWithRangePartitionCols)
      .repartition(col(_timeBucketCol), col(_partitionNumCol))

    val partitionCol = "id1"

    val expectedRows = Seq[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch)),
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch))
      )
    )

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("partitionId" -> "0", "dimension" -> "id1", "numPartitions" -> "1"),
      Map("partitionId" -> "1", "dimension" -> "id1", "numPartitions" -> "2", "start" -> "3"),
      Map("partitionId" -> "0", "dimension" -> "id1", "numPartitions" -> "2", "end" -> "3")
    )

    validateRowsAndPartitionMap(
      partitionedDf,
      expectedRows,
      expectedPartitionMap,
      partitionCol,
      (partitioner: SingleDimensionPartitioner) => partitioner
        .partition(timestampColumn, timestampFormat, segmentGranularity, 2L, partitionCol, false)
    )
  }

  test("SingleDimensionPartitioner.partition should create a single partition when rowsPerPartition is" +
    " greater than the number of rows in a bucket.") {
    val partitionedDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(
        1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch, firstTimeBucket, 1, 0)),
      Row.fromSeq(Seq(
        1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch, firstTimeBucket, 3, 0)),
      Row.fromSeq(Seq(
        1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch, firstTimeBucket, 3, 0)),
      Row.fromSeq(Seq(
        1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, secondTimeBucket,
        1, 0)),
      Row.fromSeq(Seq(
        1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, secondTimeBucket, 2, 0))
    ).asJava, schemaWithRangePartitionCols)
      .repartition(col(_timeBucketCol), col(_partitionNumCol))

    val partitionCol = "dim1"

    val expectedRows = Seq[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch)),
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch)),
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch))
      )
    )

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("partitionId" -> "0", "dimension" -> "dim1", "numPartitions" -> "1"),
      Map("partitionId" -> "0", "dimension" -> "dim1", "numPartitions" -> "1")
    )

    validateRowsAndPartitionMap(
      partitionedDf,
      expectedRows,
      expectedPartitionMap,
      partitionCol,
      (partitioner: SingleDimensionPartitioner) => partitioner
        .partition(timestampColumn, timestampFormat, segmentGranularity, 5L, partitionCol, false)
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "25")
  }

  private def validateRowsAndPartitionMap(
                                           df: DataFrame,
                                           expectedRows: Seq[Seq[Row]],
                                           expectedPartitionMap: Seq[Map[String, String]],
                                           partitionDimension: String,
                                           partitionFunc: SingleDimensionPartitioner => DataFrame
                                         ): Unit = {
    val sourceDf = df.select(schema.fieldNames.map(name => col(name)): _*)
    val partitioner = new SingleDimensionPartitioner(sourceDf)
    val result = partitionFunc(partitioner)
    val resultPartitions = getDataFramePartitions(result).sortBy(_.length)

    expectedRows.size should equal(resultPartitions.size)
    expectedRows.indices.foreach { i =>
      expectedRows(i) should contain theSameElementsAs resultPartitions(i)
    }

    val partitionMap = SingleDimensionPartitioner.generatePartitionMap(df, partitionDimension)
    expectedPartitionMap should contain theSameElementsAs partitionMap.values
  }
}
