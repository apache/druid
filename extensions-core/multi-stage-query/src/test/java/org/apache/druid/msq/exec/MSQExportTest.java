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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.export.TestExportStorageConnector;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.http.ResultFormat;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class MSQExportTest extends MSQTestBase
{
  @Test
  public void testExport() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into extern(" + TestExportStorageConnector.TYPE + "()) as csv select cnt, dim1 from foo")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of())
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();

    Assert.assertEquals(
        expectedFooFileContents(),
        new String(testExportStorageConnector.getByteArrayOutputStream().toByteArray(), Charset.defaultCharset())
    );
  }

  @Test
  public void testWithUnsupportedStorageConnector()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into extern(hdfs(basePath = '/var')) as csv select  __time, dim1 from foo")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of())
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(ISE.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "No storage connector found for storage connector type:[hdfs]."
                         )))
                     ).verifyExecutionError();
  }

  private String expectedFooFileContents() throws IOException
  {
    List<Object[]> expectedRows = new ArrayList<>(ImmutableList.of(
        new Object[]{0, "1", null},
        new Object[]{1, "1", 10.1},
        new Object[]{2, "1", 2},
        new Object[]{3, "1", 1},
        new Object[]{4, "1", "def"},
        new Object[]{5, "1", "abc"}
    ));

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
