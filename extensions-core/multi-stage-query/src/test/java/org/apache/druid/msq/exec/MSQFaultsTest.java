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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.error.TooManyClusteredByColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MSQFaultsTest extends MSQTestBase
{

  @Test
  public void testInsertWithUnsupportedColumnType()
  {
    RowSignature dummyRowSignature = RowSignature.builder().add("__time", ColumnType.LONG).build();

    testIngestQuery()
        .setSql(StringUtils.format(
            " insert into foo1 SELECT\n"
            + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
            + " col1\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{ \"files\": [\"ignored\"],\"type\":\"local\"}',\n"
            + "    '{\"type\": \"json\"}',\n"
            + "    '[{\"name\": \"timestamp\", \"type\": \"string\"},{\"name\": \"col1\", \"type\": \"long_array\"} ]'\n"
            + "  )\n"
            + ") PARTITIONED by day"
        ))
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(dummyRowSignature)
        .setExpectedMSQFault(UnknownFault.forMessage(
            "org.apache.druid.java.util.common.ISE: Cannot create dimension for type [ARRAY<LONG>]"))
        .verifyResults();
  }

  @Test
  public void testInsertWithManyColumns()
  {
    RowSignature dummyRowSignature = RowSignature.builder().add("__time", ColumnType.LONG).build();

    final int numColumns = 2000;

    String columnNames = IntStream.range(1, numColumns)
                                  .mapToObj(i -> "col" + i).collect(Collectors.joining(", "));

    String externSignature = IntStream.range(1, numColumns)
                                      .mapToObj(i -> StringUtils.format(
                                          "{\"name\": \"col%d\", \"type\": \"string\"}",
                                          i
                                      ))
                                      .collect(Collectors.joining(", "));

    testIngestQuery()
        .setSql(StringUtils.format(
            " insert into foo1 SELECT\n"
            + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
            + " %s\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{ \"files\": [\"ignored\"],\"type\":\"local\"}',\n"
            + "    '{\"type\": \"json\"}',\n"
            + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, %s]'\n"
            + "  )\n"
            + ") PARTITIONED by day",
            columnNames,
            externSignature
        ))
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(dummyRowSignature)
        .setExpectedMSQFault(new TooManyColumnsFault(numColumns + 2, 2000))
        .verifyResults();
  }

  @Test
  public void testInsertWithHugeClusteringKeys()
  {
    RowSignature dummyRowSignature = RowSignature.builder().add("__time", ColumnType.LONG).build();

    final int numColumns = 1700;

    String columnNames = IntStream.range(1, numColumns)
                                  .mapToObj(i -> "col" + i).collect(Collectors.joining(", "));

    String clusteredByClause = IntStream.range(1, numColumns + 1)
                                        .mapToObj(String::valueOf)
                                        .collect(Collectors.joining(", "));

    String externSignature = IntStream.range(1, numColumns)
                                      .mapToObj(i -> StringUtils.format(
                                          "{\"name\": \"col%d\", \"type\": \"string\"}",
                                          i
                                      ))
                                      .collect(Collectors.joining(", "));

    testIngestQuery()
        .setSql(StringUtils.format(
            " insert into foo1 SELECT\n"
            + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
            + " %s\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{ \"files\": [\"ignored\"],\"type\":\"local\"}',\n"
            + "    '{\"type\": \"json\"}',\n"
            + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, %s]'\n"
            + "  )\n"
            + ") PARTITIONED by day CLUSTERED BY %s",
            columnNames,
            externSignature,
            clusteredByClause
        ))
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(dummyRowSignature)
        .setExpectedMSQFault(new TooManyClusteredByColumnsFault(numColumns + 2, 1500, 0))
        .verifyResults();
  }

}
