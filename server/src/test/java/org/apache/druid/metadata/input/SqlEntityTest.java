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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SqlEntityTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper mapper = TestHelper.makeSmileMapper();
  private TestDerbyConnector derbyConnector;
  String TABLE_NAME_1 = "FOOS_TABLE";

  String VALID_SQL = "SELECT timestamp,a,b FROM FOOS_TABLE";
  String INVALID_SQL = "DONT SELECT timestamp,a,b FROM FOOS_TABLE";
  String resultJson = "[{\"a\":\"0\","
                      + "\"b\":\"0\","
                      + "\"timestamp\":\"2011-01-12T00:00:00.000Z\""
                      + "}]";

  @Before
  public void setUp()
  {
    for (Module jacksonModule : new InputSourceModule().getJacksonModules()) {
      mapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testExecuteQuery() throws IOException
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    testUtils.createAndUpdateTable(TABLE_NAME_1, 1);
    File tmpFile = File.createTempFile(
        "testQueryResults",
        ""
    );
    InputEntity.CleanableFile queryResult = SqlEntity.openCleanableFile(
        VALID_SQL,
        testUtils.getDerbyFirehoseConnector(),
        mapper,
        true,
        tmpFile
    );
    InputStream queryInputStream = new FileInputStream(queryResult.file());
    String actualJson = IOUtils.toString(queryInputStream, StandardCharsets.UTF_8);

    Assert.assertEquals(actualJson, resultJson);
    testUtils.dropTable(TABLE_NAME_1);
  }

  @Test(expected = IOException.class)
  public void testFailOnInvalidQuery() throws IOException
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    testUtils.createAndUpdateTable(TABLE_NAME_1, 1);
    File tmpFile = File.createTempFile(
        "testQueryResults",
        ""
    );
    InputEntity.CleanableFile queryResult = SqlEntity.openCleanableFile(
        INVALID_SQL,
        testUtils.getDerbyFirehoseConnector(),
        mapper,
        true,
        tmpFile
    );

    Assert.assertTrue(tmpFile.exists());
  }

  @Test
  public void testFileDeleteOnInvalidQuery() throws IOException
  {
    //The test parameters here are same as those used for testFailOnInvalidQuery().
    //The only difference is that this test checks if the temporary file is deleted upon failure.
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    testUtils.createAndUpdateTable(TABLE_NAME_1, 1);
    File tmpFile = File.createTempFile(
        "testQueryResults",
        ""
    );
    try {
      SqlEntity.openCleanableFile(
          INVALID_SQL,
          testUtils.getDerbyFirehoseConnector(),
          mapper,
          true,
          tmpFile
      );
    }
    // Lets catch the exception so as to test temporary file deletion.
    catch (IOException e) {
      Assert.assertFalse(tmpFile.exists());
    }
  }
}
