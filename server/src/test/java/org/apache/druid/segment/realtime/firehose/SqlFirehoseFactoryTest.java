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

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.input.SqlTestUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlFirehoseFactoryTest
{
  private static final List<File> FIREHOSE_TMP_DIRS = new ArrayList<>();
  private static File TEST_DIR;
  private static final String TABLE_NAME_1 = "FOOS_TABLE_1";
  private static final String TABLE_NAME_2 = "FOOS_TABLE_2";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private final ObjectMapper mapper = TestHelper.makeSmileMapper();

  private final InputRowParser parser = TransformSpec.NONE.decorate(
      new MapInputRowParser(
        new TimeAndDimsParseSpec(
            new TimestampSpec("timestamp", "auto", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(Arrays.asList("timestamp", "a", "b"))
            )
        )
      )
  );
  private TestDerbyConnector derbyConnector;

  @BeforeClass
  public static void setup() throws IOException
  {
    TEST_DIR = File.createTempFile(SqlFirehoseFactoryTest.class.getSimpleName(), "testDir");
    org.apache.commons.io.FileUtils.forceDelete(TEST_DIR);
    FileUtils.mkdirp(TEST_DIR);
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    org.apache.commons.io.FileUtils.forceDelete(TEST_DIR);
    for (File dir : FIREHOSE_TMP_DIRS) {
      org.apache.commons.io.FileUtils.forceDelete(dir);
    }
  }

  private void assertNumRemainingCacheFiles(File firehoseTmpDir, int expectedNumFiles)
  {
    final String[] files = firehoseTmpDir.list();
    Assert.assertNotNull(files);
    Assert.assertEquals(expectedNumFiles, files.length);
  }

  private File createFirehoseTmpDir(String dirSuffix) throws IOException
  {
    final File firehoseTempDir = File.createTempFile(
        SqlFirehoseFactoryTest.class.getSimpleName(),
        dirSuffix
    );
    org.apache.commons.io.FileUtils.forceDelete(firehoseTempDir);
    FileUtils.mkdirp(firehoseTempDir);
    FIREHOSE_TMP_DIRS.add(firehoseTempDir);
    return firehoseTempDir;
  }

  @Test
  public void testWithoutCacheAndFetch() throws Exception
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    final List<InputRow> expectedRows = testUtils.createTableWithRows(TABLE_NAME_1, 10);
    final SqlFirehoseFactory factory =
        new SqlFirehoseFactory(
            SqlTestUtils.selectFrom(TABLE_NAME_1),
            0L,
            0L,
            0L,
            0L,
            true,
            testUtils.getDerbyFirehoseConnector(),
            mapper
        );

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithoutCacheAndFetch");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    Assert.assertEquals(expectedRows, rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 0);
    testUtils.dropTable(TABLE_NAME_1);
  }


  @Test
  public void testWithoutCache() throws IOException
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    final List<InputRow> expectedRows = testUtils.createTableWithRows(TABLE_NAME_1, 10);
    final SqlFirehoseFactory factory =
        new SqlFirehoseFactory(
            SqlTestUtils.selectFrom(TABLE_NAME_1),
            0L,
            null,
            null,
            null,
            true,
            testUtils.getDerbyFirehoseConnector(),
            mapper
        );


    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithoutCache");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    Assert.assertEquals(expectedRows, rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 0);
    testUtils.dropTable(TABLE_NAME_1);
  }


  @Test
  public void testWithCacheAndFetch() throws IOException
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    final List<InputRow> expectedRowsTable1 = testUtils.createTableWithRows(TABLE_NAME_1, 10);
    final List<InputRow> expectedRowsTable2 = testUtils.createTableWithRows(TABLE_NAME_2, 10);

    final SqlFirehoseFactory factory = new
        SqlFirehoseFactory(
        SqlTestUtils.selectFrom(TABLE_NAME_1, TABLE_NAME_2),
        null,
        null,
        0L,
        null,
        true,
        testUtils.getDerbyFirehoseConnector(),
        mapper
    );

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithCacheAndFetch");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    Assert.assertEquals(expectedRowsTable1, rows.subList(0, 10));
    Assert.assertEquals(expectedRowsTable2, rows.subList(10, 20));
    assertNumRemainingCacheFiles(firehoseTmpDir, 2);
    testUtils.dropTable(TABLE_NAME_1);
    testUtils.dropTable(TABLE_NAME_2);

  }
}
