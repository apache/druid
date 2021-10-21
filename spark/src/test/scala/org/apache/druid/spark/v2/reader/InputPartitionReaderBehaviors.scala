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

import org.apache.druid.query.filter.DimFilter
import org.apache.druid.spark.configuration.{Configuration, SerializableHadoopConfiguration}
import org.apache.druid.spark.utils.FilterUtils
import org.apache.druid.spark.v2.DruidDataSourceV2TestUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, LessThan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait InputPartitionReaderBehaviors[T] extends DruidDataSourceV2TestUtils with Matchers { self: AnyFunSuite =>
  def inputPartitionReader( // scalastyle:ignore method.length (Need to wrap the tests in a method to call as one)
                            name: String,
                            hadoopConf: () => Broadcast[SerializableHadoopConfiguration],
                            inputPartitionReaderConstructor: (
                                String,
                                StructType,
                                Option[DimFilter],
                                Option[Set[String]],
                                Broadcast[SerializableHadoopConfiguration],
                                Configuration,
                                Boolean,
                                Boolean,
                                Boolean
                              ) => InputPartitionReader[T],
                            internalRowFetcher: InputPartitionReader[T] => Seq[InternalRow]): Unit = {
    test(s"$name should read the specified segment") {
      val expected = Seq(
        Seq(1577836800000L, List("dim1"), 1, 1, 2, 1L, 1L, 3L, 4.2, 1.7F, idOneSketch),
        Seq(1577862000000L, List("dim2"), 1, 1, 2, 1L, 4L, 2L, 5.1, 8.9F, idOneSketch)
      ).map(wrapSeqToInternalRow(_, schema))
      val partitionReader =
        inputPartitionReaderConstructor(
          firstSegmentString,
          schema,
          None,
          columnTypes,
          hadoopConf(),
          Configuration.EMPTY_CONF,
          false,
          false,
          false
        )

      val actual = internalRowFetcher(partitionReader)
      actual should equal(expected)
    }

    test(s"$name should apply filters to string columns") {
      val expected = Seq(
        Seq(1577851200000L, List("dim1"), 1, 1, 2, 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)
      ).map(wrapSeqToInternalRow(_, schema))
      val filter = FilterUtils.mapFilters(Array[Filter](LessThan("dim2", 2)), schema)
      val partitionReader =
        inputPartitionReaderConstructor(
          secondSegmentString,
          schema,
          filter,
          columnTypes,
          hadoopConf(),
          Configuration.EMPTY_CONF,
          false,
          false,
          false
        )

      val actual = internalRowFetcher(partitionReader)
      actual should equal(expected)
    }

    test(s"$name should apply filters to numeric columns") {
      val expected = Seq(
        Seq(1577876400000L, List("dim2"), 2, 1, 2, 1L, 1L, 5L, 8.0, 4.15F, idOneSketch)
      ).map(wrapSeqToInternalRow(_, schema))
      val filter = FilterUtils.mapFilters(Array[Filter](GreaterThanOrEqual("sum_metric4", 2L)), schema)
      val partitionReader =
        inputPartitionReaderConstructor(
          secondSegmentString,
          schema,
          filter,
          columnTypes,
          hadoopConf(),
          Configuration.EMPTY_CONF,
          false,
          false,
          false
        )

      val actual = internalRowFetcher(partitionReader)
      actual should equal(expected)
    }

    test(s"$name should handle multi-valued dimensions") {
      val expected = Seq(
        Seq(1577962800000L, List("dim1", "dim3"), 2, 3, 7, 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch),
        Seq(1577988000000L, List("dim2"), 3, 2, 1, 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch)
      ).map(wrapSeqToInternalRow(_, schema))
      val partitionReader =
        inputPartitionReaderConstructor(
          thirdSegmentString,
          schema,
          None,
          columnTypes,
          hadoopConf(),
          Configuration.EMPTY_CONF,
          false,
          false,
          false
        )

      val actual = internalRowFetcher(partitionReader)

      actual should equal(expected)
    }

    test(s"$name should handle missing columns using default values for nulls") {
      val extendedSchema =
        new StructType(schema.fields ++ Seq(StructField("newCol", LongType), StructField("newStringCol", StringType)))

      val expected = Seq(
        Seq(1577962800000L, List("dim1", "dim3"), 2, 3, 7, 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, 0L, ""),
        Seq(1577988000000L, List("dim2"), 3, 2, 1, 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, 0L, "")
      ).map(wrapSeqToInternalRow(_, extendedSchema))

      val partitionReader =
        inputPartitionReaderConstructor(
          thirdSegmentString,
          extendedSchema,
          None,
          columnTypes,
          hadoopConf(),
          Configuration.EMPTY_CONF,
          false,
          false,
          true
        )

      val actual = internalRowFetcher(partitionReader)

      actual should equal(expected)
    }

    test(s"$name should handle missing columns using SQL-compatible null handling") {
      val extendedSchema = new StructType(schema.fields :+ StructField("newCol", LongType))

      // scalastyle:off null
      val expected = Seq(
        Seq(1577962800000L, List("dim1", "dim3"), 2, 3, 7, 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch, null),
        Seq(1577988000000L, List("dim2"), 3, 2, 1, 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch, null)
      ).map(wrapSeqToInternalRow(_, extendedSchema))
      // scalastyle:on null

      val partitionReader =
        inputPartitionReaderConstructor(
          thirdSegmentString,
          extendedSchema,
          None,
          columnTypes,
          hadoopConf(),
          Configuration.EMPTY_CONF,
          false,
          false,
          false
        )

      val actual = internalRowFetcher(partitionReader)

      actual should equal(expected)
    }
  }
}
