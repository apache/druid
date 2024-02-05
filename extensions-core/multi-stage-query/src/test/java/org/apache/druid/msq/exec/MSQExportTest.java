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
import org.apache.druid.sql.calcite.export.TestExportStorageConnector;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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
    TestExportStorageConnector storageConnector = (TestExportStorageConnector) exportStorageConnectorProvider.get();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    final String sql = StringUtils.format("insert into extern(%s()) as csv select cnt, dim1 from foo", TestExportStorageConnector.TYPE_NAME);

    testIngestQuery().setSql(sql)
                     .setExpectedDataSource("foo1")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of())
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();

    List<Object[]> objects = expectedFooFileContents();

    Assert.assertEquals(
        convertResultsToString(objects),
        new String(storageConnector.getByteArrayOutputStream().toByteArray(), Charset.defaultCharset())
    );
  }

  @Test
  public void testNumberOfRowsPerFile() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    File exportDir = temporaryFolder.newFolder("export/");

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
        expectedFooFileContents().size(),
        Objects.requireNonNull(new File(exportDir.getAbsolutePath()).listFiles()).length
    );
  }

  private List<Object[]> expectedFooFileContents()
  {
    return new ArrayList<>(ImmutableList.of(
        new Object[]{"1", null},
        new Object[]{"1", 10.1},
        new Object[]{"1", 2},
        new Object[]{"1", 1},
        new Object[]{"1", "def"},
        new Object[]{"1", "abc"}
    ));
  }

  private String convertResultsToString(List<Object[]> expectedRows) throws IOException
  {
    ByteArrayOutputStream expectedResult = new ByteArrayOutputStream();
    ResultFormat.Writer formatter = ResultFormat.CSV.createFormatter(expectedResult, objectMapper);
    formatter.writeResponseStart();
    for (Object[] row : expectedRows) {
      formatter.writeRowStart();
      for (Object object : row) {
        formatter.writeRowField("", object);
      }
      formatter.writeRowEnd();
    }
    formatter.writeResponseEnd();
    return new String(expectedResult.toByteArray(), Charset.defaultCharset());
  }
}
