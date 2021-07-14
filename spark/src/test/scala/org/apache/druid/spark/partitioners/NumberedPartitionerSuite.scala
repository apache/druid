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
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.scalatest.matchers.should.Matchers

import java.util.{List => JList}
import scala.collection.JavaConverters.seqAsJavaListConverter

class NumberedPartitionerSuite extends SparkFunSuite with Matchers with DruidDataSourceV2TestUtils {
  val schemaWithPartitionCols: StructType = schema
    .add(_timeBucketCol, LongType, false)
    .add(_rankCol, IntegerType)
    .add(_partitionNumCol, IntegerType)

  test("NumberedPartitioner should partition dataframes") {
    val rows: JList[Row] = Seq(
      Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch,
        firstTimeBucket, 1, 2)),
      Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch,
        firstTimeBucket, 2, 2)),
      Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch,
        firstTimeBucket, 3, 2)),
      Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch,
        firstTimeBucket, 4, 2)),
      Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch,
        secondTimeBucket, 1, 1)),
      Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch,
        secondTimeBucket, 2, 1))
    ).asJava

    val expectedRows = Set[Seq[Row]](
      Seq(
        Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
        Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch)),
        Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch))
      ),
      Seq(
        Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch)),
        Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
      )
    )

    val sourceDf = sparkSession.createDataFrame(rows, schemaWithPartitionCols)
      .repartition(col(_timeBucketCol), col(_rankCol).divide(lit(2)))

    val partitioner = new NumberedPartitioner(sourceDf.select(schema.fieldNames.map(name => col(name)): _*))
    val result = partitioner.partition(timestampColumn, timestampFormat, segmentGranularity, 2L, false)
    val resultPartitions = getDataFramePartitions(result)

    expectedRows should contain theSameElementsAs resultPartitions

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("partitionId" -> "0", "numPartitions" -> "2"),
      Map("partitionId" -> "1", "numPartitions" -> "2"),
      Map("partitionId" -> "0", "numPartitions" -> "1")
    )

    val partitionMap = partitioner.getPartitionMap
    expectedPartitionMap should contain theSameElementsAs partitionMap.values
  }

  test("NumberedPartitioner should partition dataframes with rollup") {
    val rows: JList[Row] = Seq(
      Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch,
        firstTimeBucket, 1, 1)),
      Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch,
        firstTimeBucket, 1, 1)),
      Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch,
        firstTimeBucket, 1, 1)),
      Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch,
        firstTimeBucket, 2, 1)),
      Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch,
        secondTimeBucket, 1, 1)),
      Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch,
        secondTimeBucket, 2, 1))
    ).asJava


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

    val sourceDf = sparkSession.createDataFrame(rows, schemaWithPartitionCols)
      .repartition(col(_timeBucketCol), col(_rankCol).divide(lit(2)))

    val partitioner = new NumberedPartitioner(sourceDf.select(schema.fieldNames.map(name => col(name)): _*))
    val result =
      partitioner.partition(timestampColumn, timestampFormat, segmentGranularity, 2L, true, Some(Seq("dim2")))
    val resultPartitions = getDataFramePartitions(result)

    expectedRows should contain theSameElementsAs resultPartitions

    val expectedPartitionMap = Seq[Map[String, String]](
      Map("partitionId" -> "0", "numPartitions" -> "1"),
      Map("partitionId" -> "0", "numPartitions" -> "1")
    )

    val partitionMap = partitioner.getPartitionMap
    expectedPartitionMap should contain theSameElementsAs partitionMap.values
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "25")
  }
}
