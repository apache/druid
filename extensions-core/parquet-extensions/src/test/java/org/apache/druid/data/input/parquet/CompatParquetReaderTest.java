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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Duplicate of {@link CompatParquetInputTest} but for {@link DruidParquetReader} instead of Hadoop
 */
public class CompatParquetReaderTest extends BaseParquetReaderTest
{
  @Test
  public void testBinaryAsString() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("ts", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("field"))),
        ImmutableList.of()
    );
    InputEntityReader reader = createReader(
        "example/compat/284a0e001476716b-56d5676f53bd6e85_115466471_data.0.parq",
        schema,
        JSONPathSpec.DEFAULT,
        true
    );

    List<InputRow> rows = readAllRows(reader);

    // without binaryAsString: true, the value would be "aGV5IHRoaXMgaXMgJsOpKC3DqF/Dp8OgKT1eJMO5KiEgzqleXg=="
    Assert.assertEquals("hey this is &é(-è_çà)=^$ù*! Ω^^", rows.get(0).getDimension("field").get(0));
    Assert.assertEquals(1471800234, rows.get(0).getTimestampFromEpoch());
  }


  @Test
  public void testParquet1217() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        ImmutableList.of("metric1")
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "col", "col"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "metric1", "$.col")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        "example/compat/parquet-1217.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);

    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("-1", rows.get(0).getDimension("col").get(0));
    Assert.assertEquals(-1, rows.get(0).getMetric("metric1"));
    Assert.assertTrue(rows.get(4).getDimension("col").isEmpty());
  }

  @Test
  public void testParquetThriftCompat() throws IOException
  {
    /*
      message ParquetSchema {
        required boolean boolColumn;
        required int32 byteColumn;
        required int32 shortColumn;
        required int32 intColumn;
        required int64 longColumn;
        required double doubleColumn;
        required binary binaryColumn (UTF8);
        required binary stringColumn (UTF8);
        required binary enumColumn (ENUM);
        optional boolean maybeBoolColumn;
        optional int32 maybeByteColumn;
        optional int32 maybeShortColumn;
        optional int32 maybeIntColumn;
        optional int64 maybeLongColumn;
        optional double maybeDoubleColumn;
        optional binary maybeBinaryColumn (UTF8);
        optional binary maybeStringColumn (UTF8);
        optional binary maybeEnumColumn (ENUM);
        required group stringsColumn (LIST) {
          repeated binary stringsColumn_tuple (UTF8);
        }
        required group intSetColumn (LIST) {
          repeated int32 intSetColumn_tuple;
        }
        required group intToStringColumn (MAP) {
          repeated group map (MAP_KEY_VALUE) {
            required int32 key;
            optional binary value (UTF8);
          }
        }
        required group complexColumn (MAP) {
          repeated group map (MAP_KEY_VALUE) {
            required int32 key;
            optional group value (LIST) {
              repeated group value_tuple {
                required group nestedIntsColumn (LIST) {
                  repeated int32 nestedIntsColumn_tuple;
                }
                required binary nestedStringColumn (UTF8);
              }
            }
          }
        }
      }
     */

    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        Collections.emptyList()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "extractByLogicalMap", "$.intToStringColumn.1"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "extractByComplexLogicalMap", "$.complexColumn.1[0].nestedIntsColumn[1]")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        "example/compat/parquet-thrift-compat.snappy.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);

    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("true", rows.get(0).getDimension("boolColumn").get(0));
    Assert.assertEquals("0", rows.get(0).getDimension("byteColumn").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("shortColumn").get(0));
    Assert.assertEquals("2", rows.get(0).getDimension("intColumn").get(0));
    Assert.assertEquals("0", rows.get(0).getDimension("longColumn").get(0));
    Assert.assertEquals("0.2", rows.get(0).getDimension("doubleColumn").get(0));
    Assert.assertEquals("val_0", rows.get(0).getDimension("binaryColumn").get(0));
    Assert.assertEquals("val_0", rows.get(0).getDimension("stringColumn").get(0));
    Assert.assertEquals("SPADES", rows.get(0).getDimension("enumColumn").get(0));
    Assert.assertTrue(rows.get(0).getDimension("maybeBoolColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeByteColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeShortColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeIntColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeLongColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeDoubleColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeBinaryColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeStringColumn").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("maybeEnumColumn").isEmpty());
    Assert.assertEquals("arr_0", rows.get(0).getDimension("stringsColumn").get(0));
    Assert.assertEquals("arr_1", rows.get(0).getDimension("stringsColumn").get(1));
    Assert.assertEquals("0", rows.get(0).getDimension("intSetColumn").get(0));
    Assert.assertEquals("val_1", rows.get(0).getDimension("extractByLogicalMap").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("extractByComplexLogicalMap").get(0));
  }

  @Test
  public void testOldRepeatedInt() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("repeatedInt"))),
        Collections.emptyList()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "repeatedInt", "repeatedInt")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        "example/compat/old-repeated-int.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("1", rows.get(0).getDimension("repeatedInt").get(0));
    Assert.assertEquals("2", rows.get(0).getDimension("repeatedInt").get(1));
    Assert.assertEquals("3", rows.get(0).getDimension("repeatedInt").get(2));
  }


  @Test
  public void testReadNestedArrayStruct() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("i32_dec", "extracted1", "extracted2"))),
        Collections.emptyList()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "extracted1", "$.myComplex[0].id"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "extracted2", "$.myComplex[0].repeatedMessage[*].someId")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        "example/compat/nested-array-struct.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(1).getTimestamp().toString());
    Assert.assertEquals("5", rows.get(1).getDimension("primitive").get(0));
    Assert.assertEquals("4", rows.get(1).getDimension("extracted1").get(0));
    Assert.assertEquals("6", rows.get(1).getDimension("extracted2").get(0));
  }

  @Test
  public void testProtoStructWithArray() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", DateTimes.of("2018-09-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        Collections.emptyList()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "extractedOptional", "$.optionalMessage.someId"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "extractedRequired", "$.requiredMessage.someId"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "extractedRepeated", "$.repeatedMessage[*]")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        "example/compat/proto-struct-with-array.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("10", rows.get(0).getDimension("optionalPrimitive").get(0));
    Assert.assertEquals("9", rows.get(0).getDimension("requiredPrimitive").get(0));
    Assert.assertTrue(rows.get(0).getDimension("repeatedPrimitive").isEmpty());
    Assert.assertTrue(rows.get(0).getDimension("extractedOptional").isEmpty());
    Assert.assertEquals("9", rows.get(0).getDimension("extractedRequired").get(0));
    Assert.assertEquals("9", rows.get(0).getDimension("extractedRepeated").get(0));
    Assert.assertEquals("10", rows.get(0).getDimension("extractedRepeated").get(1));
  }
}
