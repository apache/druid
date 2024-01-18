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
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * Duplicate of {@link DecimalParquetInputTest} but for {@link ParquetReader} instead of Hadoop
 */
public class DecimalParquetReaderTest extends BaseParquetReaderTest
{
  @Test
  public void testReadParquetDecimalFixedLen() throws IOException
  {
    final String file = "example/decimals/dec-in-fixed-len.parquet";
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("fixed_len_dec"))),
        ColumnsFilter.all()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "fixed_len_dec", "fixed_len_dec"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "metric1", "$.fixed_len_dec")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        file,
        schema,
        flattenSpec
    );

    /*
    The raw data in the parquet file has the following columns:
    ############ Column(fixed_len_dec) ############
    name: fixed_len_dec
    path: fixed_len_dec
    max_definition_level: 1
    max_repetition_level: 0
    physical_type: FIXED_LEN_BYTE_ARRAY
    logical_type: Decimal(precision=10, scale=2)
    converted_type (legacy): DECIMAL

    The raw data in the parquet file has the following rows:
    0.0
    1.0
    2.0
    3.0
    4.0
    5.0
    6.0
    7.0
    8.0
    9.0
    0.0
    1.0
    2.0
    3.0
    4.0
    5.0
     */

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(1).getTimestamp().toString());
    Assert.assertEquals("1.0", rows.get(1).getDimension("fixed_len_dec").get(0));
    Assert.assertEquals(new BigDecimal("1.0"), rows.get(1).getMetric("metric1"));

    reader = createReader(
        file,
        schema,
        flattenSpec
    );
    List<InputRowListPlusRawValues> sampled = sampleAllRows(reader);
    final String expectedJson = "{\n"
                                + "  \"fixed_len_dec\" : 1.0\n"
                                + "}";
    Assert.assertEquals(expectedJson, DEFAULT_JSON_WRITER.writeValueAsString(sampled.get(1).getRawValues()));
  }

  @Test
  public void testReadParquetDecimali32() throws IOException
  {
    final String file = "example/decimals/dec-in-i32.parquet";
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("i32_dec"))),
        ColumnsFilter.all()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "i32_dec", "i32_dec"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "metric1", "$.i32_dec")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        file,
        schema,
        flattenSpec
    );

    /*
    The raw data in the parquet file has the following columns:
    ############ Column(i32_dec) ############
    name: i32_dec
    path: i32_dec
    max_definition_level: 1
    max_repetition_level: 0
    physical_type: INT32
    logical_type: Decimal(precision=5, scale=2)
    converted_type (legacy): DECIMAL

    The raw data in the parquet file has the following rows:
    0
    1.00
    2.00
    3.00
    4.00
    5.00
    6.00
    7.00
    8.00
    9.00
    0
    1.00
    2.00
    3.00
    4.00
    5.00
     */

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(1).getTimestamp().toString());
    Assert.assertEquals("1.00", rows.get(1).getDimension("i32_dec").get(0));
    Assert.assertEquals(BigDecimal.valueOf(100L, 2), rows.get(1).getMetric("metric1"));

    reader = createReader(
        file,
        schema,
        flattenSpec
    );
    List<InputRowListPlusRawValues> sampled = sampleAllRows(reader);
    final String expectedJson = "{\n"
                                + "  \"i32_dec\" : 1.00\n"
                                + "}";
    Assert.assertEquals(expectedJson, DEFAULT_JSON_WRITER.writeValueAsString(sampled.get(1).getRawValues()));
  }

  @Test
  public void testReadParquetDecimali64() throws IOException
  {
    final String file = "example/decimals/dec-in-i64.parquet";
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("i64_dec"))),
        ColumnsFilter.all()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "i32_dec", "i64_dec"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "metric1", "$.i64_dec")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        file,
        schema,
        flattenSpec
    );

    /*
    The raw data in the parquet file has the following columns:
    ############ Column(i64_dec) ############
    name: i64_dec
    path: i64_dec
    max_definition_level: 1
    max_repetition_level: 0
    physical_type: INT64
    logical_type: Decimal(precision=10, scale=2)
    converted_type (legacy): DECIMAL

    The raw data in the parquet file has the following rows:
    0
    1.00
    2.00
    3.00
    4.00
    5.00
    6.00
    7.00
    8.00
    9.00
    0
    1.00
    2.00
    3.00
    4.00
    5.00
     */

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(1).getTimestamp().toString());
    Assert.assertEquals("1.00", rows.get(1).getDimension("i64_dec").get(0));
    Assert.assertEquals(BigDecimal.valueOf(100L, 2), rows.get(1).getMetric("metric1"));

    reader = createReader(
        file,
        schema,
        flattenSpec
    );
    List<InputRowListPlusRawValues> sampled = sampleAllRows(reader);
    final String expectedJson = "{\n"
                                + "  \"i64_dec\" : 1.00\n"
                                + "}";
    Assert.assertEquals(expectedJson, DEFAULT_JSON_WRITER.writeValueAsString(sampled.get(1).getRawValues()));
  }
}
