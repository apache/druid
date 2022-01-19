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

package org.apache.druid.spark.v2.writer

import org.apache.druid.java.util.common.{FileUtils, StringUtils}
import org.apache.druid.spark.clients.DruidMetadataClient
import org.apache.druid.spark.configuration.Configuration
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.v2.DruidDataSourceV2TestUtils
import org.apache.druid.spark.{MAPPER, SparkFunSuite}
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.NumberedShardSpec
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import java.io.File

class DruidDataSourceWriterSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with DruidDataSourceV2TestUtils with Logging {
  var uri: String = _

  test("commit should correctly record segment data in the metadata database") {
    val druidDataSourceWriter = DruidDataSourceWriter(
      schema,
      SaveMode.Overwrite,
      Configuration(writerProps ++ metadataClientProps(uri))
    ).get()
    val metadataClient = DruidMetadataClient(Configuration(metadataClientProps(uri)))
    val commitMessages =
      DruidWriterCommitMessage(Seq(firstSegmentString, secondSegmentString, thirdSegmentString))

    druidDataSourceWriter.commit(Array(commitMessages.asInstanceOf[WriterCommitMessage]))
    val committedSegments = metadataClient.getSegmentPayloads(dataSource, None, None)
    Seq(firstSegment, secondSegment, thirdSegment) should contain theSameElementsInOrderAs committedSegments
  }

  // The point of this test isn't to test segment rationalization directly (that's handled in SegmentRationalizerSuite)
  // but instead is to ensure that commit() remains able to handle discontinuous & reordered segments.
  test("commit should correctly record rationalized segment data in the metadata database") {
    val druidDataSourceWriter = DruidDataSourceWriter(
      schema,
      SaveMode.Overwrite,
      Configuration(writerProps ++ metadataClientProps(uri))
    ).get()
    val metadataClient = DruidMetadataClient(Configuration(metadataClientProps(uri)))

    val firstSpreadSegment: DataSegment = new DataSegment(
      dataSource,
      interval,
      version,
      loadSpec(makePath(segmentsDir.getCanonicalPath, firstSegmentPath)),
      dimensions,
      metrics,
      new NumberedShardSpec(1, 1),
      binaryVersion,
      3278L
    )
    val secondSpreadSegment: DataSegment = new DataSegment(
      dataSource,
      interval,
      version,
      loadSpec(makePath(segmentsDir.getCanonicalPath, secondSegmentPath)),
      dimensions,
      metrics,
      new NumberedShardSpec(2, 4),
      binaryVersion,
      3299L
    )
    val thirdSpreadSegment: DataSegment = new DataSegment(
      dataSource,
      secondInterval,
      version,
      loadSpec(makePath(segmentsDir.getCanonicalPath, thirdSegmentPath)),
      dimensions,
      metrics,
      new NumberedShardSpec(4, 4),
      binaryVersion,
      3409L
    )


    val commitMessages =
      DruidWriterCommitMessage(Seq(
        MAPPER.writeValueAsString(firstSpreadSegment),
        MAPPER.writeValueAsString(thirdSpreadSegment),
        MAPPER.writeValueAsString(secondSpreadSegment)
      ))

    druidDataSourceWriter.commit(Array(commitMessages.asInstanceOf[WriterCommitMessage]))
    val committedSegments = metadataClient.getSegmentPayloads(dataSource, None, None)
    Seq(firstSegment, secondSegment, thirdSegment) should contain theSameElementsInOrderAs committedSegments
  }

  test("abort should delete segments") {
    val tempDir = FileUtils.createTempDir()
    org.apache.commons.io.FileUtils.copyDirectory(segmentsDir, tempDir, false)
    logInfo(tempDir.getAbsolutePath)
    logInfo(tempDir.list().toSeq.mkString(", "))

    // Segments should have been copied, starting from the root directory named for the data source
    tempDir.list() should contain theSameElementsInOrderAs Seq(dataSource)

    val druidDataSourceWriter = DruidDataSourceWriter(
      schema,
      SaveMode.Overwrite,
      Configuration(writerProps ++ metadataClientProps(uri))
    ).get()

    // TODO: Make helper functions to create DataSegments from base paths
    val locations = Seq(firstSegmentString, secondSegmentString, thirdSegmentString)
      .map(path => StringUtils.replace(path, segmentsDir.getCanonicalPath, tempDir.getCanonicalPath))
    val commitMessages = DruidWriterCommitMessage(locations)
    druidDataSourceWriter.abort(Array(commitMessages.asInstanceOf[WriterCommitMessage]))

    // Having killed all segments, we should have deleted the directory structure up to the data source directory
    logInfo(walkDir(tempDir))
    tempDir.list().toSeq shouldBe 'isEmpty

    FileUtils.deleteDirectory(tempDir)
  }

  private def walkDir(file: File): String = {
    val files = Option(file.listFiles())
    s"${file.getAbsolutePath}: ${files.getOrElse(Array.empty[File]).toSeq.map(walkDir).mkString(", ")}"
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
