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
import org.apache.druid.spark.configuration.DruidConfigurationKeys
import org.apache.druid.spark.mixins.TryWithResources
import org.apache.druid.spark.{MAPPER, SparkFunSuite}
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter,
  seqAsJavaListConverter}

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
      .schema(schema)
      .options(Map(s"${DruidConfigurationKeys.readerPrefix}.${DruidConfigurationKeys.segmentsKey}" -> segmentsString))
      .load()

    matchDfs(df, expected)
  }

  test("round trip test") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      Row.fromSeq(Seq(1577836800000L, List("dim1"), "1", "1", "2", 1L, 1L, 3L, 4.2, 1.7F, idOneSketch)),
      Row.fromSeq(Seq(1577851200000L, List("dim1"), "1", "1", "2", 1L, 3L, 1L, 0.2, 0.0F, idOneSketch)),
      Row.fromSeq(Seq(1577862000000L, List("dim2"), "1", "1", "2", 1L, 4L, 2L, 5.1, 8.9F, idOneSketch)),
      Row.fromSeq(Seq(1577876400000L, List("dim2"), "2", "1", "2", 1L, 1L, 5L, 8.0, 4.15F, idOneSketch)),
      Row.fromSeq(Seq(1577962800000L, List("dim1", "dim3"), "2", "3", "7", 1L, 2L, 4L, 11.17, 3.7F, idThreeSketch)),
      Row.fromSeq(Seq(1577988000000L, List("dim2"), "3", "2", "1", 1L, 1L, 7L, 0.0, 19.0F, idTwoSketch))
    ).asJava, schema)

    val uri = generateUniqueTestUri()
    createTestDb(uri)
    registerEmbeddedDerbySQLConnector()

    sourceDf.write
      .format(DruidDataSourceV2ShortName)
      .mode(SaveMode.Overwrite)
      .options((writerProps ++ metadataClientProps(uri)).asJava)
      .save()

    tryWithResources(openDbiToTestDb(uri)) {
      handle =>
        val res =
          handle.createQuery("SELECT DATASOURCE, START, \"end\", PARTITIONED, VERSION, USED FROM druid_segments")
            .list().asScala.toSeq.map(m => m.values().asScala.map(_.toString).toSeq)
        val expected = Seq[Seq[String]](
          Seq(
            dataSource,
            "2020-01-01T00:00:00.000Z",
            "2020-01-02T00:00:00.000Z",
            "true",
            version,
            "true"
          ), Seq(
            dataSource,
            "2020-01-02T00:00:00.000Z",
            "2020-01-03T00:00:00.000Z",
            "true",
            version,
            "true"
          )
        )
        expected.size should equal(res.size)
        expected.zipWithIndex.foreach{
          // The results from the query are stored in an unordered map, so we can't rely on a simple should equal
          case (s, index) => s should contain theSameElementsAs res(index)
        }
    }

    val readerProps = Map[String, String](
      DataSourceOptions.TABLE_KEY -> dataSource
    )
    val readDf = sparkSession
      .read
      .format(DruidDataSourceV2ShortName)
      .schema(schema)
      .options((readerProps ++ metadataClientProps(uri)).asJava)
      .load()

    matchDfs(readDf, sourceDf)

    tearDownTestDb(uri)
    cleanUpWorkingDirectory()
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
