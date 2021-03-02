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

import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.SparkFunSuite
import org.apache.druid.spark.utils.DruidConfigurationKeys
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.{Filter, GreaterThan, LessThan, LessThanOrEqual}
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter,
  seqAsJavaListConverter}

class DruidDataSourceReaderSuite extends SparkFunSuite with Matchers
  with DruidDataSourceV2TestUtils {

  test("DruidDataSourceReader should correctly read directly specified segments") {
    val expected = Seq(
      InternalRow.fromSeq(Seq(1577836800000L, List("dim1"), 1, 1, 2, 1, 1, 3, 4.2, 1.7, idOneSketch)),
      InternalRow.fromSeq(Seq(1577836800000L, List("dim2"), 1, 1, 2, 1, 4, 2, 5.1, 8.9, idOneSketch)),
      InternalRow.fromSeq(Seq(1577836800000L, List("dim2"), 2, 1, 2, 1, 1, 5, 8.0, 4.15, idOneSketch)),
      InternalRow.fromSeq(Seq(1577836800000L, List("dim1"), 1, 1, 2, 1, 3, 1, 0.2, 0.0, idOneSketch)),
      InternalRow.fromSeq(Seq(1577923200000L, List("dim2"), 3, 2, 1, 1, 1, 7, 0.0, 19.0, idTwoSketch)),
      InternalRow.fromSeq(Seq(1577923200000L, List("dim1", "dim3"), 2, 3, 7, 1, 2, 4, 11.17, 3.7, idThreeSketch))
    )

    val segmentsString = MAPPER.writeValueAsString(
      List[DataSegment](firstSegment, secondSegment, thirdSegment).asJava
    )
    val dsoMap = Map(s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString)
    val dso = new DataSourceOptions(dsoMap.asJava)
    val reader = DruidDataSourceReader(schema, dso)
    val actual =
      reader.planInputPartitions().asScala
        .flatMap(r => partitionReaderToSeq(r.createPartitionReader()))

    actual.zipAll(expected, InternalRow.empty, InternalRow.empty).forall{
      case (left, right) => compareInternalRows(left, right, schema)
    } shouldBe true
  }

  test("getTimeFilterBounds should handle upper and lower bounds") {
    val expected = (Some(2501L), Some(5000L))
    val reader = DruidDataSourceReader(schema, DataSourceOptions.empty())
    val filters = Array[Filter](LessThanOrEqual("__time", 5000L), GreaterThan("__time", 2500L))
    reader.pushFilters(filters)
    val actual = reader.getTimeFilterBounds
    actual should equal(expected)
  }

  test("getTimeFilterBounds should handle empty or multiple filters for a bound") {
    val expected = (None, Some(2499L))
    val reader = DruidDataSourceReader(schema, DataSourceOptions.empty())
    val filters = Array[Filter](LessThanOrEqual("__time", 5000L), LessThan("__time", 2500L))
    reader.pushFilters(filters)
    val actual = reader.getTimeFilterBounds
    actual should equal(expected)
  }

  test("DruidFrameReader.convertDruidSchemaToSparkSchema should convert a Druid schema") {
    val columnMap = Map[String, (String, Boolean)](
      "__time" -> ("LONG", false),
      "dim1" -> ("STRING", true),
      "dim2" -> ("STRING", false),
      "id1" -> ("STRING", false),
      "id2" -> ("STRING", false),
      "count" -> ("LONG", false),
      "sum_metric1" -> ("LONG", false),
      "sum_metric2" -> ("LONG", false),
      "sum_metric3" -> ("DOUBLE", false),
      "sum_metric4" -> ("FLOAT", false),
      "uniq_id1" -> ("thetaSketch", false)
    )

    val actualSchema = DruidDataSourceReader.convertDruidSchemaToSparkSchema(columnMap)
    actualSchema.fields should contain theSameElementsAs schema.fields
  }
}
