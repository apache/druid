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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MSQExportTest extends MSQTestBase
{
  @Test
  public void testExport() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    File exportDir = newTempFolder("export");
    final String sql = StringUtils.format("insert into extern(local(exportPath=>'%s')) as csv select cnt, dim1 as dim from foo", exportDir.getAbsolutePath());

    testIngestQuery().setSql(sql)
                     .setExpectedDataSource("foo1")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of())
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();

    Assert.assertEquals(
        1,
        Objects.requireNonNull(new File(exportDir.getAbsolutePath()).listFiles()).length
    );

    File resultFile = new File(exportDir, "query-test-query-worker0-partition0.csv");
    List<String> results = readResultsFromFile(resultFile);
    Assert.assertEquals(
        expectedFooFileContents(true),
        results
    );
  }

  @Test
  public void testExport2() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    File exportDir = newTempFolder("export");
    final String sql = StringUtils.format("insert into extern(local(exportPath=>'%s')) as csv select dim1 as table_dim, count(*) as table_count from foo where dim1 = 'abc' group by 1", exportDir.getAbsolutePath());

    testIngestQuery().setSql(sql)
                     .setExpectedDataSource("foo1")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of())
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();

    Assert.assertEquals(
        1,
        Objects.requireNonNull(new File(exportDir.getAbsolutePath()).listFiles()).length
    );

    File resultFile = new File(exportDir, "query-test-query-worker0-partition0.csv");
    List<String> results = readResultsFromFile(resultFile);
    Assert.assertEquals(
        expectedFoo2FileContents(true),
        results
    );
  }

  @Test
  public void testNumberOfRowsPerFile()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    File exportDir = newTempFolder("export");

    Map<String, Object> queryContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    queryContext.put(MultiStageQueryContext.CTX_ROWS_PER_PAGE, 1);

    final String sql = StringUtils.format("insert into extern(local(exportPath=>'%s')) as csv select cnt, dim1 from foo", exportDir.getAbsolutePath());

    testIngestQuery().setSql(sql)
                     .setExpectedDataSource("foo1")
                     .setQueryContext(queryContext)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of())
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();

    Assert.assertEquals(
        expectedFooFileContents(false).size(),
        Objects.requireNonNull(new File(exportDir.getAbsolutePath()).listFiles()).length
    );
  }

  private List<String> expectedFooFileContents(boolean withHeader)
  {
    ArrayList<String> expectedResults = new ArrayList<>();
    if (withHeader) {
      expectedResults.add("cnt,dim");
    }
    expectedResults.addAll(ImmutableList.of(
                               "1,",
                               "1,10.1",
                               "1,2",
                               "1,1",
                               "1,def",
                               "1,abc"
                           )
    );
    return expectedResults;
  }

  private List<String> expectedFoo2FileContents(boolean withHeader)
  {
    ArrayList<String> expectedResults = new ArrayList<>();
    if (withHeader) {
      expectedResults.add("table_dim,table_count");
    }
    expectedResults.addAll(ImmutableList.of("abc,1"));
    return expectedResults;
  }

  private List<String> readResultsFromFile(File resultFile) throws IOException
  {
    List<String> results = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(resultFile.toPath()), StringUtils.UTF8_STRING))) {
      String line;
      while (!(line = br.readLine()).isEmpty()) {
        results.add(line);
      }
      return results;
    }
  }
}
