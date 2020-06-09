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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlInputSourceTest
{
  private static final List<File> FIREHOSE_TMP_DIRS = new ArrayList<>();
  private final String TABLE_NAME_1 = "FOOS_TABLE_1";
  private final String TABLE_NAME_2 = "FOOS_TABLE_2";

  private final List<String> SQLLIST1 = ImmutableList.of("SELECT timestamp,a,b FROM FOOS_TABLE_1");
  private final List<String> SQLLIST2 = ImmutableList.of(
      "SELECT timestamp,a,b FROM FOOS_TABLE_1",
      "SELECT timestamp,a,b FROM FOOS_TABLE_2"
  );

  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("timestamp", "auto", null),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("timestamp", "a", "b")),
          new ArrayList<>(),
          new ArrayList<>()
      ),
      Collections.emptyList()
  );
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private final ObjectMapper mapper = TestHelper.makeSmileMapper();
  private TestDerbyConnector derbyConnector;

  @Before
  public void setUp()
  {
    for (Module jacksonModule : new InputSourceModule().getJacksonModules()) {
      mapper.registerModule(jacksonModule);
    }
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    for (File dir : FIREHOSE_TMP_DIRS) {
      FileUtils.forceDelete(dir);
    }
  }

  private void assertResult(List<Row> rows, List<String> sqls)
  {
    Assert.assertEquals(10 * sqls.size(), rows.size());
    rows.sort(Comparator.comparing(Row::getTimestamp)
                        .thenComparingInt(r -> Integer.valueOf(r.getDimension("a").get(0)))
                        .thenComparingInt(r -> Integer.valueOf(r.getDimension("b").get(0))));
    int rowCount = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < sqls.size(); j++) {
        final Row row = rows.get(rowCount);
        String timestampSt = StringUtils.format("2011-01-12T00:0%s:00.000Z", i);
        Assert.assertEquals(timestampSt, row.getTimestamp().toString());
        Assert.assertEquals(i, Integer.valueOf(row.getDimension("a").get(0)).intValue());
        Assert.assertEquals(i, Integer.valueOf(row.getDimension("b").get(0)).intValue());
        rowCount++;
      }
    }
  }

  private File createFirehoseTmpDir(String dirSuffix) throws IOException
  {
    final File firehoseTempDir = File.createTempFile(
        SqlInputSourceTest.class.getSimpleName(),
        dirSuffix
    );
    FileUtils.forceDelete(firehoseTempDir);
    FileUtils.forceMkdir(firehoseTempDir);
    FIREHOSE_TMP_DIRS.add(firehoseTempDir);
    return firehoseTempDir;
  }

  @Test
  public void testSerde() throws IOException
  {
    mapper.registerSubtypes(TestSerdeFirehoseConnector.class);
    final SqlInputSourceTest.TestSerdeFirehoseConnector testSerdeFirehoseConnector = new SqlInputSourceTest.TestSerdeFirehoseConnector(
        new MetadataStorageConnectorConfig());
    final SqlInputSource sqlInputSource = new SqlInputSource(SQLLIST1, true, testSerdeFirehoseConnector, mapper);
    final String valueString = mapper.writeValueAsString(sqlInputSource);
    final SqlInputSource inputSourceFromJson = mapper.readValue(valueString, SqlInputSource.class);
    Assert.assertEquals(sqlInputSource, inputSourceFromJson);
  }

  @Test
  public void testSingleSplit() throws Exception
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    testUtils.createAndUpdateTable(TABLE_NAME_1, 10);
    final File tempDir = createFirehoseTmpDir("testSingleSplit");
    SqlInputSource sqlInputSource = new SqlInputSource(SQLLIST1, true, testUtils.getDerbyFirehoseConnector(), mapper);
    InputSourceReader sqlReader = sqlInputSource.fixedFormatReader(INPUT_ROW_SCHEMA, tempDir);
    CloseableIterator<InputRow> resultIterator = sqlReader.read();
    final List<Row> rows = new ArrayList<>();
    while (resultIterator.hasNext()) {
      rows.add(resultIterator.next());
    }
    assertResult(rows, SQLLIST1);
    testUtils.dropTable(TABLE_NAME_1);
  }


  @Test
  public void testMultipleSplits() throws Exception
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    testUtils.createAndUpdateTable(TABLE_NAME_1, 10);
    testUtils.createAndUpdateTable(TABLE_NAME_2, 10);
    final File tempDir = createFirehoseTmpDir("testMultipleSplit");
    SqlInputSource sqlInputSource = new SqlInputSource(SQLLIST2, true, testUtils.getDerbyFirehoseConnector(), mapper);
    InputSourceReader sqlReader = sqlInputSource.fixedFormatReader(INPUT_ROW_SCHEMA, tempDir);
    CloseableIterator<InputRow> resultIterator = sqlReader.read();
    final List<Row> rows = new ArrayList<>();
    while (resultIterator.hasNext()) {
      rows.add(resultIterator.next());
    }
    assertResult(rows, SQLLIST2);
    testUtils.dropTable(TABLE_NAME_1);
    testUtils.dropTable(TABLE_NAME_2);
  }

  @Test
  public void testNumSplits()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    SqlTestUtils testUtils = new SqlTestUtils(derbyConnector);
    SqlInputSource sqlInputSource = new SqlInputSource(SQLLIST2, true, testUtils.getDerbyFirehoseConnector(), mapper);
    InputFormat inputFormat = EasyMock.createMock(InputFormat.class);
    Stream<InputSplit<String>> sqlSplits = sqlInputSource.createSplits(inputFormat, null);
    Assert.assertEquals(SQLLIST2, sqlSplits.map(InputSplit::get).collect(Collectors.toList()));
    Assert.assertEquals(2, sqlInputSource.estimateNumSplits(inputFormat, null));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SqlInputSource.class)
                  .withPrefabValues(
                      ObjectMapper.class,
                      new ObjectMapper(),
                      new ObjectMapper()
                  )
                  .withIgnoredFields("objectMapper")
                  .withNonnullFields("sqls", "sqlFirehoseDatabaseConnector")
                  .usingGetClass()
                  .verify();
  }

  @JsonTypeName("test")
  private static class TestSerdeFirehoseConnector extends SQLFirehoseDatabaseConnector
  {
    private final DBI dbi;
    private final MetadataStorageConnectorConfig metadataStorageConnectorConfig;

    private TestSerdeFirehoseConnector(
        @JsonProperty("connectorConfig") MetadataStorageConnectorConfig metadataStorageConnectorConfig
    )
    {
      final BasicDataSource datasource = getDatasource(metadataStorageConnectorConfig);
      datasource.setDriverClassLoader(getClass().getClassLoader());
      datasource.setDriverClassName("org.apache.derby.jdbc.ClientDriver");
      this.dbi = new DBI(datasource);
      this.metadataStorageConnectorConfig = metadataStorageConnectorConfig;
    }

    @JsonProperty("connectorConfig")
    public MetadataStorageConnectorConfig getConnectorConfig()
    {
      return metadataStorageConnectorConfig;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestSerdeFirehoseConnector that = (TestSerdeFirehoseConnector) o;
      return metadataStorageConnectorConfig.equals(that.metadataStorageConnectorConfig);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(metadataStorageConnectorConfig);
    }

    @Override
    public DBI getDBI()
    {
      return dbi;
    }
  }
}
