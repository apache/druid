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

import org.apache.druid.java.util.common.StringUtils
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.{MAPPER, SparkFunSuite}
import org.apache.druid.spark.mixins.TryWithResources
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.seqAsJavaListConverter

class DruidDataSourceV2Suite extends SparkFunSuite with Matchers
  with DruidDataSourceV2TestUtils with TryWithResources {
  test("sparkSession.read(\"druid\") should correctly read segments into a dataFrame") {
    val expected = sparkSession.createDataFrame(Seq(
      // Reading from segments will not sort the resulting dataframe by time across segments, only within it
      Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
      Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch)),
      Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
      Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch)),
      Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch)),
      Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
    ).asJava, schema)

    val segmentsString = MAPPER.writeValueAsString(
      List[DataSegment](firstSegment, secondSegment, thirdSegment).asJava
    )

    val df = sparkSession
      .read
      .format("druid")
      .options(Map(
        s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString,
        s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.useSparkConfForDeepStorageKey}" -> "true"
      ))
      .schema(schema)
      .load()

    matchDfs(df, expected)
  }

  /**
    * Match two DataFrames, DF and EXPECTED.
    *
    * @param df The result DataFrame to match against EXPECTED.
    * @param expected The expected DataFrame.
    */
  private def matchDfs(df: DataFrame, expected: DataFrame): Unit = {
    df.schema should equal(expected.schema)

    df.collect().map{row =>
      row.toSeq.map {
        case v: Array[Byte] => StringUtils.encodeBase64String(v)
        case x: Any => x
      }
    }.zip(expected.collect().map{row =>
      row.toSeq.map {
        case v: Array[Byte] => StringUtils.encodeBase64String(v)
        case x: Any => x
      }
    }).map(row => row._1 should contain theSameElementsAs row._2)
  }
}
