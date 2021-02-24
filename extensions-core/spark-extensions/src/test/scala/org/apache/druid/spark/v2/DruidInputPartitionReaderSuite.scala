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

package org.apache.druid.spark.v2

import org.apache.druid.common.config.NullHandling
import org.apache.druid.data.input.MapBasedInputRow
import org.apache.druid.spark.SparkFunSuite
import org.apache.druid.spark.utils.SerializableHadoopConfiguration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, LessThan}
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructField,
  StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

class DruidInputPartitionReaderSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with DruidDataSourceV2TestUtils {
  test("DruidInputPartitionReader should read the specified segment") {
    val expected = Seq(
      InternalRow.fromSeq(Seq(1577836800000L, List("dim1"), 1, 1, 2, 1, 1, 3, 4.2, 1.7, idOneSketch)),
      InternalRow.fromSeq(Seq(1577836800000L, List("dim2"), 1, 1, 2, 1, 4, 2, 5.1, 8.9, idOneSketch))
    )
    val conf =
      sparkContext.broadcast(new SerializableHadoopConfiguration(sparkContext.hadoopConfiguration))
    val partitionReader =
      new DruidInputPartitionReader(firstSegmentString, schema, Array.empty[Filter], columnTypes, conf)

    val actual = partitionReaderToSeq(partitionReader)

    actual.zipAll(expected, InternalRow.empty, InternalRow.empty).forall{
      case (left, right) => compareInternalRows(left, right, schema)
    } shouldBe true
  }

  test("DruidInputPartitionReader should apply filters to string columns") {
    val expected = Seq(
      InternalRow.fromSeq(Seq(1577836800000L, List("dim1"), 1, 1, 2, 1, 3, 1, 0.2, 0.0, idOneSketch))
    )
    val conf =
      sparkContext.broadcast(new SerializableHadoopConfiguration(sparkContext.hadoopConfiguration))
    val filter = LessThan("dim2", 2)
    val partitionReader =
      new DruidInputPartitionReader(secondSegmentString, schema, Array[Filter](filter), columnTypes, conf)

    val actual = partitionReaderToSeq(partitionReader)

    actual.zipAll(expected, InternalRow.empty, InternalRow.empty).forall{
      case (left, right) => compareInternalRows(left, right, schema)
    } shouldBe true
  }

  test("DruidInputPartitionReader should apply filters to numeric columns") {
    val expected = Seq(
      InternalRow.fromSeq(Seq(1577836800000L, List("dim2"), 2, 1, 2, 1, 1, 5, 8.0, 4.15, idOneSketch))
    )
    val conf =
      sparkContext.broadcast(new SerializableHadoopConfiguration(sparkContext.hadoopConfiguration))
    val filter = GreaterThanOrEqual("sum_metric4", 2)
    val partitionReader =
      new DruidInputPartitionReader(secondSegmentString, schema, Array[Filter](filter), columnTypes, conf)

    val actual = partitionReaderToSeq(partitionReader)

    actual.zipAll(expected, InternalRow.empty, InternalRow.empty).forall{
      case (left, right) => compareInternalRows(left, right, schema)
    } shouldBe true
  }

  test("DruidInputPartitionReader.convertInputRowToSparkRow should convert an " +
    "InputRow to an InternalRow") {
    val timestamp = 0L
    val dimensions = List[String]("dim1", "dim2", "dim3", "dim4", "dim5", "dim6", "dim7", "dim8")
    val event = Map[String, Any](
      "dim1" -> 1L,
      "dim2" -> "false",
      "dim3" -> "str",
      "dim4" -> List[String]("val1", "val2").asJava,
      "dim5" -> 4.2,
      "dim6" -> List[Long](12L, 26L).asJava,
      "dim7" -> "12345",
      "dim8" -> "12"
    ).map{case(k, v) =>
      // Gotta do our own auto-boxing in Scala
      k -> v.asInstanceOf[AnyRef]
    }
    val inputRow: MapBasedInputRow =
      new MapBasedInputRow(timestamp, dimensions.asJava, event.asJava)
    val schema = StructType(Seq(
      StructField("__time", TimestampType),
      StructField("dim1", LongType),
      StructField("dim2", StringType),
      StructField("dim3", StringType),
      StructField("dim4", ArrayType(StringType, false)),
      StructField("dim5", DoubleType),
      StructField("dim6", ArrayType(LongType, false)),
      StructField("dim7", LongType),
      StructField("dim8", ArrayType(LongType, false))
    ))
    val res = DruidInputPartitionReader.convertInputRowToSparkRow(inputRow, schema)
    val expected = InternalRow.fromSeq(
      Seq(
        0,
        1L,
        UTF8String.fromString("false"),
        UTF8String.fromString("str"),
        ArrayData.toArrayData(Array(UTF8String.fromString("val1"), UTF8String.fromString("val2"))),
        4.2,
        ArrayData.toArrayData(Seq(12L, 26L)),
        12345,
        ArrayData.toArrayData(Seq(12L))
      )
    )
    for (i <- 0 until schema.length) {
      res.get(i, schema(i).dataType) should equal(expected.get(i, schema(i).dataType))
    }
  }

  test("DruidInputPartitionReader.convertInputRowToSparkRow should return null for " +
    "missing dimensions") {
    val timestamp = 0L
    val dimensions = List[String]("dim1", "dim2", "dim3", "dim4")
    val event = Map[String, Any](
      "dim1" -> 1L,
      "dim2" -> "false",
      "dim3" -> "str"
    ).map{case(k, v) =>
      // Gotta do our own auto-boxing in Scala
      k -> v.asInstanceOf[AnyRef]
    }
    val inputRow: MapBasedInputRow =
      new MapBasedInputRow(timestamp, dimensions.asJava, event.asJava)
    val schema = StructType(Seq(
      StructField("__time", TimestampType),
      StructField("dim1", LongType),
      StructField("dim2", StringType),
      StructField("dim3", StringType),
      StructField("dim4", ArrayType(StringType, false))
    ))
    val res = DruidInputPartitionReader.convertInputRowToSparkRow(inputRow, schema)
    val expected = InternalRow.fromSeq(
      Seq(
        0,
        1L,
        UTF8String.fromString("false"),
        UTF8String.fromString("str"),
        null // scalastyle:ignore null
      )
    )
    for (i <- 0 until schema.length) {
      res.get(i, schema(i).dataType) should equal(expected.get(i, schema(i).dataType))
    }
  }

  override def beforeEach(): Unit = {
    NullHandling.initializeForTests()
    super.beforeEach()
  }

}
