/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.cli.convert;

import org.apache.commons.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;

import io.airlift.command.Cli;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ConvertPropertiesTest
{
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Test
  public void testConvertProperties() throws IOException
  {
    File inputFile = tmpFolder.newFile();
    File outputFile = tmpFolder.newFile();
    String oldVersionPropertiesString = "druid.database.rules.defaultDatasource 1"
        + System.lineSeparator()
        + "druid.indexer.chathandler.publishDiscovery true"
        + System.lineSeparator()
        + "druid.database.segmentTable table"
        + System.lineSeparator()
        + "druid.pusher.local false"
        + System.lineSeparator()
        + "druid.paths.indexCache hdfs://path"
        + System.lineSeparator()
        + "notHandled";
    String newVersionPropertiesString;
    FileUtils.writeStringToFile(inputFile, oldVersionPropertiesString, StandardCharsets.UTF_8.toString());
    Cli<?> parser = Cli.builder("convertProps")
        .withCommand(ConvertProperties.class)
        .build();
    Object command = parser.parse("convertProps","-f", inputFile.getAbsolutePath(),"-o", outputFile.getAbsolutePath());
    Assert.assertNotNull(command);
    ConvertProperties convertProperties = (ConvertProperties) command;
    convertProperties.run();

    newVersionPropertiesString = FileUtils.readFileToString(outputFile, StandardCharsets.UTF_8.toString());
    System.out.printf(newVersionPropertiesString);
    Assert.assertTrue(newVersionPropertiesString.contains("druid.manager.rules.defaultTier=1"));
    Assert.assertTrue(newVersionPropertiesString.contains("druid.db.tables.segments=table"));
    Assert.assertTrue(newVersionPropertiesString.contains("druid.indexer.task.chathandler.type=curator"));
    Assert.assertTrue(newVersionPropertiesString.contains("druid.storage.local=false"));
    Assert.assertTrue(newVersionPropertiesString.contains("druid.segmentCache.locations=[{\"path\": \"hdfs://path\", \"maxSize\": null}]"));
    Assert.assertTrue(newVersionPropertiesString.contains("notHandled"));
  }

  @After
  public void tearDown()
  {
    tmpFolder.delete();
  }
}
