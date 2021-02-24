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

import org.apache.druid.java.util.common.{FileUtils, StringUtils}
import org.apache.druid.spark.utils.DruidMetadataClient
import org.apache.druid.spark.SparkFunSuite
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.mapAsJavaMapConverter

class DruidDataSourceWriterSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with DruidDataSourceV2TestUtils {
  var uri: String = _

  // TODO: Test that segment rationalization is handled correctly (rationalization logic should be tested in
  //  SegmentRationalizerSuite, but we still need to verify that commit still works as expected)
  test("commit should correctly record segment data in the metadata database") {
    val druidDataSourceWriter = DruidDataSourceWriter(
      schema,
      SaveMode.Overwrite,
      new DataSourceOptions((writerProps ++ metadataClientProps(uri)).asJava)
    ).get()
    val metadataClient = DruidMetadataClient(new DataSourceOptions(metadataClientProps(uri).asJava))
    val commitMessages =
      DruidWriterCommitMessage(Seq(firstSegmentString, secondSegmentString, thirdSegmentString))

    druidDataSourceWriter.commit(Array(commitMessages.asInstanceOf[WriterCommitMessage]))
    val committedSegments = metadataClient.getSegmentPayloads(dataSource, None, None)
    Seq(firstSegment, secondSegment, thirdSegment) should contain theSameElementsInOrderAs committedSegments
  }

  test("abort should delete segments") {
    val tempDir = FileUtils.createTempDir()
    org.apache.commons.io.FileUtils.copyDirectory(segmentsDir, tempDir, false)

    // Segments should have been copied, starting from the root directory named for the data source
    tempDir.list() should contain theSameElementsInOrderAs Seq(dataSource)

    val druidDataSourceWriter = DruidDataSourceWriter(
      schema,
      SaveMode.Overwrite,
      new DataSourceOptions((writerProps ++ metadataClientProps(uri)).asJava)
    ).get()

    // TODO: Make helper functions to create DataSegments from base paths
    val locations = Seq(firstSegmentString, secondSegmentString, thirdSegmentString)
      .map(path => StringUtils.replace(path, segmentsDir.getCanonicalPath, tempDir.getCanonicalPath))
    val commitMessages = DruidWriterCommitMessage(locations)
    druidDataSourceWriter.abort(Array(commitMessages.asInstanceOf[WriterCommitMessage]))

    // Having killed all segments, we should have deleted the directory structure up to the data source directory
    tempDir.list().toSeq shouldBe 'isEmpty

    FileUtils.deleteDirectory(tempDir)
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
