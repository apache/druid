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

package org.apache.druid.data.input.parquet;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Duplicate of {@link TimestampsParquetInputTest} but for {@link DruidParquetReader} instead of Hadoop
 */
public class TimestampsParquetReaderTest extends BaseParquetReaderTest
{
  @Test
  public void testDateHandling() throws IOException
  {
    InputRowSchema schemaAsString = new InputRowSchema(
        new TimestampSpec("date_as_string", "Y-M-d", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        Collections.emptyList()
    );
    InputRowSchema schemaAsDate = new InputRowSchema(
        new TimestampSpec("date_as_date", null, null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        Collections.emptyList()
    );
    InputEntityReader readerAsString = createReader(
        "example/timestamps/test_date_data.snappy.parquet",
        schemaAsString,
        JSONPathSpec.DEFAULT
    );
    InputEntityReader readerAsDate = createReader(
        "example/timestamps/test_date_data.snappy.parquet",
        schemaAsDate,
        JSONPathSpec.DEFAULT
    );

    List<InputRow> rowsWithString = readAllRows(readerAsString);
    List<InputRow> rowsWithDate = readAllRows(readerAsDate);
    Assert.assertEquals(rowsWithDate.size(), rowsWithString.size());

    for (int i = 0; i < rowsWithDate.size(); i++) {
      Assert.assertEquals(rowsWithString.get(i).getTimestamp(), rowsWithDate.get(i).getTimestamp());
    }
  }

  @Test
  public void testParseInt96Timestamp() throws IOException
  {
    // the source parquet file was found in apache spark sql repo tests, where it is known as impala_timestamp.parq
    // it has a single column, "ts" which is an int96 timestamp
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("ts", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        Collections.emptyList()
    );
    InputEntityReader reader = createReader("example/timestamps/int96_timestamp.parquet", schema, JSONPathSpec.DEFAULT);

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("2001-01-01T01:01:01.000Z", rows.get(0).getTimestamp().toString());
  }

  @Test
  public void testTimeMillisInInt64() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        Collections.emptyList()
    );
    InputEntityReader reader = createReader(
        "example/timestamps/timemillis-in-i64.parquet",
        schema,
        JSONPathSpec.DEFAULT
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("1970-01-01T00:00:00.010Z", rows.get(0).getTimestamp().toString());
  }
}
