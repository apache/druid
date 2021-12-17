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

package org.apache.druid.spark.v2.reader

import org.apache.druid.spark.configuration.DruidConfigurationKeys
import org.apache.druid.spark.v2.DruidDataSourceV2TestUtils
import org.apache.druid.spark.{MAPPER, SparkFunSuite}
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, LessThan}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter, seqAsJavaListConverter}

class DruidDataSourceReaderSuite extends SparkFunSuite with Matchers
  with DruidDataSourceV2TestUtils {
  private val segmentsString = MAPPER.writeValueAsString(
    List[DataSegment](firstSegment, secondSegment, thirdSegment).asJava
  )

  private val defaultExpected = Seq(
    Seq(1577836800000L, List("dim1"), 1, 1, 2, 1L, 1L, 3L, 4.2, 1.7F, idOneSketch),
    Seq(1577862000000L, List("dim2"), 1, 1, 2, 1L, 4L, 2L, 5.1, 8.9F, idOneSketch),
    Seq(1577851200000L, List("dim1"), 1, 1, 2, 1L, 3L, 1L, 0.2, 0.0F, idOneSketch),
    Seq(1577876400000L, List("dim2"), 2, 1, 2, 1L, 1L, 5L, 8.0, 4.15F, idOneSketch),
    Seq(1577962800000L, List("dim1", "dim3"), 2, 3, 7, 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch),
    Seq(1577988000000L, List("dim2"), 3, 2, 1, 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch)
  ).map(wrapSeqToInternalRow(_, schema))

  test("DruidDataSourceReader should correctly read directly specified segments") {
    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString
    )

    readSpecifiedSegments(dsoMap, false)
  }

  test("DruidDataSourceReader should correctly read directly specified segments with a filter") {
    val expected = Seq(
      Seq(1577862000000L, List("dim2"), 1, 1, 2, 1L, 4L, 2L, 5.1, 8.9F, idOneSketch),
      Seq(1577876400000L, List("dim2"), 2, 1, 2, 1L, 1L, 5L, 8.0, 4.15F, idOneSketch),
      Seq(1577962800000L, List("dim1", "dim3"), 2, 3, 7, 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch),
      Seq(1577988000000L, List("dim2"), 3, 2, 1, 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch)
    ).map(wrapSeqToInternalRow(_, schema))

    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString
    )

    readSpecifiedSegments(dsoMap, false, expected, Option(Array[Filter](GreaterThanOrEqual("sum_metric4", 2L))))
  }

  test("DruidDataSourceReader should correctly read directly specified segments with vectorize = true, " +
    "batch size 1") {
    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString,
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.vectorizeKey}" -> "true",
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.batchSizeKey}" -> "1"
    )

    readSpecifiedSegments(dsoMap, true)
  }

  test("DruidDataSourceReader should correctly read directly specified segments with vectorize = true, " +
    "batch size 2") {
    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString,
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.vectorizeKey}" -> "true",
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.batchSizeKey}" -> "2"
    )

    readSpecifiedSegments(dsoMap, true)
  }

  test("DruidDataSourceReader should correctly read directly specified segments with vectorize = true, " +
    "default batch size") {
    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString,
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.vectorizeKey}" -> "true"
    )

    readSpecifiedSegments(dsoMap, true)
  }

  test("DruidDataSourceReader should correctly read directly specified segments with vectorize = true " +
    "and a filter") {
    val expected = Seq(
      Seq(1577836800000L, List("dim1"), 1, 1, 2, 1L, 1L, 3L, 4.2, 1.7F, idOneSketch),
      Seq(1577862000000L, List("dim2"), 1, 1, 2, 1L, 4L, 2L, 5.1, 8.9F, idOneSketch),
      Seq(1577851200000L, List("dim1"), 1, 1, 2, 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)
    ).map(wrapSeqToInternalRow(_, schema))

    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString,
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.vectorizeKey}" -> "true"
    )

    readSpecifiedSegments(dsoMap, true, expected, Option(Array[Filter](LessThan("dim2", 2))))
  }

  test("DruidDataSourceReader should correctly read directly specified segments with " +
    "useSparkConfForDeepStorage = true") {
    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString,
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.useSparkConfForDeepStorageKey}" -> "true"
    )

    readSpecifiedSegments(dsoMap, false)
  }

  test("DruidDataSourceReader should correctly report which filters it does not support") {
    val dsoMap = Map(
      s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString
    )

    // DruidDataSourceReader doesn't support pushing down filters on complex types
    val filters = Array[Filter](GreaterThanOrEqual("count", 5), LessThan("uniq_id1", 4))
    val reader = DruidDataSourceReader(schema, new DataSourceOptions(dsoMap.asJava))
    val result = reader.pushFilters(filters)
    Array[Filter](LessThan("uniq_id1", 4)) should equal(result)
    Array[Filter](GreaterThanOrEqual("count", 5)) should equal(reader.pushedFilters())
  }

  def readSpecifiedSegments(
                             optionsMap: Map[String, String],
                             useVectorizedReads: Boolean,
                             expected: Seq[InternalRow] = defaultExpected,
                             filterOpt: Option[Array[Filter]] = None
                           ): Unit = {
    val dso = new DataSourceOptions(optionsMap.asJava)
    val reader = DruidDataSourceReader(schema, dso)
    if (filterOpt.isDefined) {
      reader.pushFilters(filterOpt.get)
    }
    useVectorizedReads should equal(reader.enableBatchRead())
    val actual = if (useVectorizedReads) {
      reader.planBatchInputPartitions().asScala
        .flatMap(r => columnarPartitionReaderToSeq(r.createPartitionReader()))
    } else {
      reader.planInputPartitions().asScala
        .flatMap(r => partitionReaderToSeq(r.createPartitionReader()))
    }

    actual should equal(expected)
  }
}
