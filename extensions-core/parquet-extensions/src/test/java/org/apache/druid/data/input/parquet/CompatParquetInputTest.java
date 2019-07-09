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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.path.StaticPathSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

@RunWith(Parameterized.class)
public class CompatParquetInputTest extends BaseParquetInputTest
{
  @Parameterized.Parameters(name = "type = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE},
        new Object[]{ParquetExtensionsModule.PARQUET_SIMPLE_INPUT_PARSER_TYPE}
    );
  }

  private final String parserType;
  private final Job job;

  public CompatParquetInputTest(String parserType) throws IOException
  {
    this.parserType = parserType;
    this.job = Job.getInstance(new Configuration());
  }

  @Test
  public void testBinaryAsString() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/compat/impala_hadoop_parquet_job.json",
        parserType,
        false
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(data)).get(0);

    // without binaryAsString: true, the value would something like "[104, 101, 121, 32, 116, 104, 105, 115, 32, 105, 115, 3.... ]"
    Assert.assertEquals("hey this is &é(-è_çà)=^$ù*! Ω^^", row.getDimension("field").get(0));
    Assert.assertEquals(1471800234, row.getTimestampFromEpoch());
  }


  @Test
  public void testParquet1217() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/compat/parquet_1217.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    List<InputRow> rows2 = getAllRows(parserType, config);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("-1", rows.get(0).getDimension("col").get(0));
    Assert.assertEquals(-1, rows.get(0).getMetric("metric1"));
    Assert.assertTrue(rows2.get(2).getDimension("col").isEmpty());
  }

  @Test
  public void testParquetThriftCompat() throws IOException, InterruptedException
  {
    // parquet-avro does not support this conversion:
    // Map key type must be binary (UTF8): required int32 key
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }
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

    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/compat/parquet_thrift_compat.json",
        parserType,
        true
    );

    config.intoConfiguration(job);
    Object data = getFirstRow(job, parserType, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
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
  public void testOldRepeatedInt() throws IOException, InterruptedException
  {
    // parquet-avro does not support this conversion:
    // REPEATED not supported outside LIST or MAP. Type: repeated int32 repeatedInt
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/compat/old_repeated_int.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    List<InputRow> rows = getAllRows(parserType, config);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("1", rows.get(0).getDimension("repeatedInt").get(0));
    Assert.assertEquals("2", rows.get(0).getDimension("repeatedInt").get(1));
    Assert.assertEquals("3", rows.get(0).getDimension("repeatedInt").get(2));
  }

  @Test
  public void testReadNestedArrayStruct() throws IOException, InterruptedException
  {
    // parquet-avro does not support this conversion
    // REPEATED not supported outside LIST or MAP. Type: repeated group repeatedMessage {
    //  optional int32 someId;
    // }
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }
    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/compat/nested_array_struct.json",
        parserType,
        true
    );

    config.intoConfiguration(job);
    List<InputRow> rows = getAllRows(parserType, config);
    Assert.assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals("5", rows.get(0).getDimension("primitive").get(0));
    Assert.assertEquals("4", rows.get(0).getDimension("extracted1").get(0));
    Assert.assertEquals("6", rows.get(0).getDimension("extracted2").get(0));
  }

  @Test
  public void testProtoStructWithArray() throws IOException, InterruptedException
  {
    // parquet-avro does not support this conversion:
    // "REPEATED not supported outside LIST or MAP. Type: repeated int32 repeatedPrimitive"
    if (parserType.equals(ParquetExtensionsModule.PARQUET_AVRO_INPUT_PARSER_TYPE)) {
      return;
    }

    HadoopDruidIndexerConfig config = transformHadoopDruidIndexerConfig(
        "example/compat/proto_struct_with_array.json",
        parserType,
        true
    );
    config.intoConfiguration(job);
    List<InputRow> rows = getAllRows(parserType, config);
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
