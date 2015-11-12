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

package io.druid.cli.validate;

import io.airlift.airline.Cli;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class DruidJsonValidatorTest
{
  private File inputFile;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    inputFile = temporaryFolder.newFile();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testExceptionCase()
  {
    String type = "";
    Cli<?> parser = Cli.builder("validator")
        .withCommand(DruidJsonValidator.class)
        .build();
    Object command = parser.parse("validator","-f", inputFile.getAbsolutePath(), "-t", type);
    Assert.assertNotNull(command);
    DruidJsonValidator druidJsonValidator = (DruidJsonValidator) command;
    druidJsonValidator.run();
  }

  @Test(expected = RuntimeException.class)
  public void testExceptionCaseNoFile()
  {
    String type = "query";
    Cli<?> parser = Cli.builder("validator")
        .withCommand(DruidJsonValidator.class)
        .build();
    Object command = parser.parse("validator","-f", "", "-t", type);
    Assert.assertNotNull(command);
    DruidJsonValidator druidJsonValidator = (DruidJsonValidator) command;
    druidJsonValidator.run();
  }

  @After public void tearDown()
  {
    temporaryFolder.delete();
  }
}
