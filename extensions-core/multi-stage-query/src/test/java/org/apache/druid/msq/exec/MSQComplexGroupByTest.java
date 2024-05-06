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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;

public class MSQComplexGroupByTest extends MSQTestBase
{
  static {
    NestedDataModule.registerHandlersAndSerde();
  }

  private String dataFileNameJsonString;
  private String dataFileSignatureJsonString;
  private DataSource dataFileExternalDataSource;


  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {PARALLEL_MERGE, PARALLEL_MERGE_MSQ_CONTEXT}
    };
    return Arrays.asList(data);
  }

  @BeforeEach
  public void setup() throws IOException
  {
    File dataFile = newTempFile("dataFile");
    final InputStream resourceStream = this.getClass().getClassLoader()
                                                                .getResourceAsStream(NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE);
    final InputStream decompressing = CompressionUtils.decompress(
        resourceStream,
        "nested-all-types-test-data.json"
    );
    Files.copy(decompressing, dataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    decompressing.close();

    dataFileNameJsonString = queryFramework().queryJsonMapper().writeValueAsString(dataFile);

    RowSignature dataFileSignature = RowSignature.builder()
                                                 .add("timestamp", ColumnType.STRING)
                                                 .add("obj", ColumnType.NESTED_DATA)
                                                 .add("arrayObject", ColumnType.STRING_ARRAY)
                                                 .add("arrayNestedLong", ColumnType.LONG_ARRAY)
                                                 .build();
    dataFileSignatureJsonString = queryFramework().queryJsonMapper().writeValueAsString(dataFileSignature);

    dataFileExternalDataSource = new ExternalDataSource(
        new LocalInputSource(null, null, ImmutableList.of(dataFile), SystemFields.none()),
        new JsonInputFormat(null, null, null, null, null),
        dataFileSignature
    );

    objectMapper.registerModules(NestedDataModule.getJacksonModulesList());
  }

  @Test
  public void testInsertWithRollupOnNestedData()
  {
    testIngestQuery().setSql("INSERT INTO foo1 SELECT\n"
                             + " obj,\n"
                             + " COUNT(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"obj\", \"type\": \"COMPLEX<json>\"}]'\n"
                             + "   )\n"
                             + " )\n"
                             + " GROUP BY 1\n"
                             + " PARTITIONED BY ALL")
                     .setQueryContext(ImmutableMap.of())
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(RowSignature.builder()
                                                          .add("obj", ColumnType.NESTED_DATA)
                                                          .add("cnt", ColumnType.LONG)
                                                          .build())
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();

  }

  @Test
  public void testSortingOnNestedData()
  {
    testSelectQuery().setSql("SELECT\n"
                             + " obj\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"obj\", \"type\": \"COMPLEX<json>\"}]'\n"

                             + "   )\n"
                             + " )\n"
                             + " ORDER BY 1")
                     .setQueryContext(ImmutableMap.of())
                     .setExpectedRowSignature(RowSignature.builder()
                                                          .add("obj", ColumnType.NESTED_DATA)
                                                          .build())
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();
  }

}
