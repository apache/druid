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

package org.apache.druid.spark.clients

import org.apache.druid.java.util.common.StringUtils
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.Configuration
import org.apache.druid.spark.mixins.TryWithResources
import org.apache.druid.spark.v2.DruidDataSourceV2TestUtils
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.NumberedShardSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{asScalaBufferConverter, collectionAsScalaIterableConverter,
  mapAsJavaMapConverter, seqAsJavaListConverter}

class DruidMetadataClientSuite extends AnyFunSuite with Matchers with DruidDataSourceV2TestUtils
  with BeforeAndAfterEach with TryWithResources {
  private var uri: String = _

  private val differentDataSourceSegment: DataSegment = new DataSegment(
    "differentDataSource",
    interval,
    version,
    loadSpec(makePath(segmentsDir.getCanonicalPath, firstSegmentPath)),
    dimensions,
    metrics,
    new NumberedShardSpec(0, 0),
    binaryVersion,
    3278L
  )
  private val differentDataSourceSegmentString: String = MAPPER.writeValueAsString(differentDataSourceSegment)

  test("getSegmentPayloads should retrieve selected DataSegment payloads from the metadata store") {
    val metadataClient = DruidMetadataClient(Configuration(metadataClientProps(uri)))
    // Need to exercise the underlying connector to create the metadata SQL tables in the test DB
    metadataClient.checkIfDataSourceExists(dataSource)

    tryWithResources(openDbiToTestDb(uri)) {
      handle =>
        val updateSql = """
        |INSERT INTO druid_segments(
        |  id, datasource, created_date, start, \"end\", partitioned, version, used, payload
        |) VALUES
        |(:id, :datasource, :created_date, :start, :end, :partitioned, :version, :used, :payload)
        |""".stripMargin

        val argsMap = Seq[Map[String, Any]](
          Map[String, Any](
            "id" -> firstSegment.getId.toString,
            "datasource" -> dataSource,
            "created_date" -> "2020-01-01T00:00:000Z",
            "start" -> "2020-01-01T00:00:00.000Z",
            "end" -> "2020-01-02T00:00:00.000Z",
            "partitioned" -> true,
            "version" -> version,
            "used" -> true,
            "payload" -> firstSegmentString.getBytes(StringUtils.UTF8_STRING)
          ),
          Map[String, Any](
            "id" -> firstSegment.withVersion("test").getId.toString,
            "datasource" -> dataSource,
            "created_date" -> "2020-01-01T00:00:000Z",
            "start" -> "2020-01-01T00:00:00.000Z",
            "end" -> "2020-01-02T00:00:00.000Z",
            "partitioned" -> true,
            "version" -> version,
            "used" -> false,
            "payload" -> firstSegmentString.getBytes(StringUtils.UTF8_STRING)
          ),
          Map[String, Any](
            "id" -> differentDataSourceSegment.getId.toString,
            "datasource" -> "differentDataSource",
            "created_date" -> "2020-01-01T00:00:000Z",
            "start" -> "2020-01-01T00:00:00.000Z",
            "end" -> "2020-01-02T00:00:00.000Z",
            "partitioned" -> true,
            "version" -> version,
            "used" -> true,
            "payload" -> differentDataSourceSegmentString.getBytes(StringUtils.UTF8_STRING)
          ),
          Map[String, Any](
            "id" -> thirdSegment.getId.toString,
            "datasource" -> dataSource,
            "created_date" -> "2020-01-01T00:00:000Z",
            "start" -> "2020-01-02T00:00:00.000Z",
            "end" -> "2020-01-03T00:00:00.000Z",
            "partitioned" -> true,
            "version" -> version,
            "used" -> true,
            "payload" -> thirdSegmentString.getBytes(StringUtils.UTF8_STRING)
          )
        )

        argsMap.foreach{argMap =>
          val statement = handle.createStatement(updateSql).bindFromMap(argMap.asJava)
          statement.execute()
        }
    }
    val usedSegments = metadataClient.getSegmentPayloads(dataSource, None, None)
    // Interval is 2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z
    val segmentsByDate =
      metadataClient.getSegmentPayloads(dataSource, Some(1577836800000L), Some(1577923200000L))

    val expectedUsedSegments = Seq[DataSegment](firstSegment, thirdSegment)
    val expectedSegmentsByDate = Seq[DataSegment](firstSegment)

    usedSegments should contain theSameElementsInOrderAs expectedUsedSegments
    segmentsByDate should contain theSameElementsInOrderAs expectedSegmentsByDate
  }

  test("checkIfDataSourceExists should return true iff the specified dataSource exists") {
    val metadataClient = DruidMetadataClient(Configuration(metadataClientProps(uri)))
    // Need to exercise the underlying connector to create the metadata SQL tables in the test DB
    metadataClient.checkIfDataSourceExists(dataSource)

    tryWithResources(openDbiToTestDb(uri)) {
      handle =>
        val updateSql = """
        |INSERT INTO druid_segments(
        |  id, datasource, created_date, start, \"end\", partitioned, version, used, payload
        |) VALUES
        |(:id, :datasource, :created_date, :start, :end, :partitioned, :version, :used, :payload)
        |""".stripMargin

        val argsMap = Map[String, Any](
          "id" -> firstSegment.getId.toString,
          "datasource" -> dataSource,
          "created_date" -> "2020-01-01T00:00:000Z",
          "start" -> "2020-01-01T00:00:00.000Z",
          "end" -> "2020-01-02T00:00:00.000Z",
          "partitioned" -> true,
          "version" -> version,
          "used" -> true,
          "payload" -> firstSegmentString.getBytes(StringUtils.UTF8_STRING)
        )
        val statement = handle.createStatement(updateSql).bindFromMap(argsMap.asJava)
        statement.execute()
    }
    metadataClient.checkIfDataSourceExists(dataSource) should be(true)
    metadataClient.checkIfDataSourceExists("differentDataSource") should be(false)
  }

  test("publishSegments") {
    val metadataClient = DruidMetadataClient(Configuration(metadataClientProps(uri)))
    metadataClient.publishSegments(List(firstSegment, thirdSegment).asJava)

    tryWithResources(openDbiToTestDb(uri)) {
      handle =>
        val res =
          handle.createQuery("SELECT DATASOURCE, START, \"end\", PARTITIONED, VERSION, USED FROM druid_segments")
            .list().asScala.map(m => m.values().asScala.map(_.toString).toSeq)
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
  }

  override def beforeEach(): Unit = {
    uri = generateUniqueTestUri()
    createTestDb(uri)
    registerEmbeddedDerbySQLConnector()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    tearDownTestDb(uri)
    cleanUpWorkingDirectory()
    super.afterEach()
  }
}
