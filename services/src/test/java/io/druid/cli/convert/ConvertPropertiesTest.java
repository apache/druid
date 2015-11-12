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

import io.airlift.airline.Cli;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConvertPropertiesTest
{
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testConvertProperties() throws IOException
  {
    String oldPropertiesFile = this.getClass().getResource("/convertProps/old.properties").getFile();

    File convertedPropertiesFile = tmpFolder.newFile();

    Cli<?> parser = Cli.builder("convertProps")
        .withCommand(ConvertProperties.class)
        .build();
    Object command = parser.parse("convertProps","-f", oldPropertiesFile,"-o", convertedPropertiesFile.getAbsolutePath());
    Assert.assertNotNull(command);
    ConvertProperties convertProperties = (ConvertProperties) command;
    convertProperties.run();

    Properties actualConvertedProperties = new Properties();
    actualConvertedProperties.load(new FileInputStream(convertedPropertiesFile));

    Properties expectedConvertedProperties = new Properties();
    expectedConvertedProperties.load(this.getClass().getResourceAsStream("/convertProps/new.properties"));

    Assert.assertEquals(expectedConvertedProperties, actualConvertedProperties);
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpFolder.getRoot());
  }
}
