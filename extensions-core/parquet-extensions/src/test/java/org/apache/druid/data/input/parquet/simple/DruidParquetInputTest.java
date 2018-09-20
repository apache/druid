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
package org.apache.druid.data.input.parquet.simple;

import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.path.StaticPathSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.example.data.Group;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DruidParquetInputTest
{
  private static String TS1 = "2018-09-18T00:18:00.023Z";

  @Test
  public void testReadParquetFile() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/wikipedia_hadoop_parquet_job.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(rows.get(0).getDimension("page").get(0), "Gypsy Danger");
    String s1 = rows.get(0).getDimension("language").get(0);
    String s2 = rows.get(0).getDimension("language").get(1);
    assertEquals("en", s1);
    assertEquals("zh", s2);
  }

  @Test
  public void testBinaryAsString() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/impala_hadoop_parquet_job.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(data)).get(0);

    // without binaryAsString: true, the value would something like "[104, 101, 121, 32, 116, 104, 105, 115, 32, 105, 115, 3.... ]"
    assertEquals(row.getDimension("field").get(0), "hey this is &é(-è_çà)=^$ù*! Ω^^");
    assertEquals(row.getTimestampFromEpoch(), 1471800234);
  }

  @Test
  public void testDateHandling() throws IOException, InterruptedException
  {
    List<InputRow> rowsWithString = getAllRows("example/simple/date_test_data_job_string.json");
    List<InputRow> rowsWithDate = getAllRows("example/simple/date_test_data_job_date.json");
    assertEquals(rowsWithDate.size(), rowsWithString.size());

    for (int i = 0; i < rowsWithDate.size(); i++) {
      assertEquals(rowsWithString.get(i).getTimestamp(), rowsWithDate.get(i).getTimestamp());
    }
  }

  @Test
  public void testParseInt96Timestamp() throws IOException, InterruptedException
  {
    // the source parquet file was found in apache spark sql repo tests, where it is known as impala_timestamp.parq
    // it has a single column, "ts" which is an int96 timestamp
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/int96_timestamp.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals("2001-01-01T01:01:01.000Z", rows.get(0).getTimestamp().toString());
  }

  @Test
  public void testReadParquetDecimalFixedLen() throws IOException, InterruptedException
  {
    List<InputRow> rows = getAllRows("example/simple/dec_in_fix_len.json");
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("1.0", rows.get(0).getDimension("fixed_len_dec").get(0));
    assertEquals(new BigDecimal("1.0"), rows.get(0).getMetric("metric1"));
  }

  @Test
  public void testReadParquetDecimali32() throws IOException, InterruptedException
  {
    List<InputRow> rows = getAllRows("example/simple/dec_in_i32.json");
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("100", rows.get(0).getDimension("i32_dec").get(0));
    assertEquals(new BigDecimal(100), rows.get(0).getMetric("metric1"));
  }

  @Test
  public void testReadParquetDecimali64() throws IOException, InterruptedException
  {
    List<InputRow> rows = getAllRows("example/simple/dec_in_i64.json");
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("100", rows.get(0).getDimension("i64_dec").get(0));
    assertEquals(new BigDecimal(100), rows.get(0).getMetric("metric1"));
  }

  @Test
  public void testParquet1217() throws IOException, InterruptedException
  {
    String path = "example/simple/parquet_1217.json";
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        path)
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    List<InputRow> rows2 = getAllRows(path);
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("-1", rows.get(0).getDimension("col").get(0));
    assertEquals(-1, rows.get(0).getMetric("metric1"));
    assertTrue(rows2.get(2).getDimension("col").isEmpty());
  }

  @Test
  public void testParquetThriftCompat() throws IOException, InterruptedException
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
    String path = "example/simple/parquet_thrift_compat.json";
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        path)
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("true", rows.get(0).getDimension("boolColumn").get(0));
    assertEquals("0", rows.get(0).getDimension("byteColumn").get(0));
    assertEquals("1", rows.get(0).getDimension("shortColumn").get(0));
    assertEquals("2", rows.get(0).getDimension("intColumn").get(0));
    assertEquals("0", rows.get(0).getDimension("longColumn").get(0));
    assertEquals("0.2", rows.get(0).getDimension("doubleColumn").get(0));
    assertEquals("val_0", rows.get(0).getDimension("binaryColumn").get(0));
    assertEquals("val_0", rows.get(0).getDimension("stringColumn").get(0));
    assertEquals("SPADES", rows.get(0).getDimension("enumColumn").get(0));
    assertTrue(rows.get(0).getDimension("maybeBoolColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeByteColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeShortColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeIntColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeLongColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeDoubleColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeBinaryColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeStringColumn").isEmpty());
    assertTrue(rows.get(0).getDimension("maybeEnumColumn").isEmpty());
    assertEquals("arr_0", rows.get(0).getDimension("stringsColumn").get(0));
    assertEquals("arr_1", rows.get(0).getDimension("stringsColumn").get(1));
    assertEquals("0", rows.get(0).getDimension("intSetColumn").get(0));
    assertEquals("val_1", rows.get(0).getDimension("extractByLogicalMap").get(0));
    assertEquals("1", rows.get(0).getDimension("extractByComplexLogicalMap").get(0));
  }

  @Test
  public void testOldRepeatedInt() throws IOException, InterruptedException
  {
    String path = "example/simple/old_repeated_int.json";
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        path)
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = getAllRows(path);
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("1", rows.get(0).getDimension("repeatedInt").get(0));
    assertEquals("2", rows.get(0).getDimension("repeatedInt").get(1));
    assertEquals("3", rows.get(0).getDimension("repeatedInt").get(2));
  }

  @Test
  public void testReadNestedArrayStruct() throws IOException, InterruptedException
  {
    String path = "example/simple/nested_array_struct.json";
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        path)
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = getAllRows(path);
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("5", rows.get(0).getDimension("primitive").get(0));
    assertEquals("4", rows.get(0).getDimension("extracted1").get(0));
    assertEquals("6", rows.get(0).getDimension("extracted2").get(0));
  }

  @Test
  public void testProtoStructWithArray() throws IOException, InterruptedException
  {
    String path = "example/simple/proto_struct_with_array.json";
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        path)
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = getAllRows(path);
    assertEquals("2018-09-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    assertEquals("10", rows.get(0).getDimension("optionalPrimitive").get(0));
    assertEquals("9", rows.get(0).getDimension("requiredPrimitive").get(0));
    assertTrue(rows.get(0).getDimension("repeatedPrimitive").isEmpty());
    assertTrue(rows.get(0).getDimension("extractedOptional").isEmpty());
    assertEquals("9", rows.get(0).getDimension("extractedRequired").get(0));
    assertEquals("9", rows.get(0).getDimension("extractedRepeated").get(0));
    assertEquals("10", rows.get(0).getDimension("extractedRepeated").get(1));
  }

  @Test
  public void testTimeMillisInInt64() throws IOException, InterruptedException
  {
    String path = "example/simple/timemillis_in_i64.json";
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        path)
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    List<InputRow> rows = getAllRows(path);
    assertEquals("1970-01-01T00:00:00.010Z", rows.get(0).getTimestamp().toString());
  }

  @Test
  public void testFlat1NoFlattenSpec() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/flat_1.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1Autodiscover() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/flat_1_autodiscover_fields.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1Flatten() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/flat_1_flatten.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    assertEquals("listDim1v1", rows.get(0).getDimension("list").get(0));
    assertEquals("listDim1v2", rows.get(0).getDimension("list").get(1));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1FlattenSelectListItem() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/flat_1_list_index.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    assertEquals("listDim1v2", rows.get(0).getDimension("listextracted").get(0));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }


  @Test
  public void testNested1NoFlattenSpec() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/nested_1.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    List<String> dims = rows.get(0).getDimensions();
    Assert.assertFalse(dims.contains("dim2"));
    Assert.assertFalse(dims.contains("dim3"));
    Assert.assertFalse(dims.contains("listDim"));
    Assert.assertFalse(dims.contains("nestedData"));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testNested1Autodiscover() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/nested_1_autodiscover_fields.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    List<String> dims = rows.get(0).getDimensions();
    Assert.assertFalse(dims.contains("dim2"));
    Assert.assertFalse(dims.contains("dim3"));
    Assert.assertFalse(dims.contains("listDim"));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testNested1Flatten() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/nested_1_flatten.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
    assertEquals(2, rows.get(0).getMetric("metric2").longValue());
  }

  @Test
  public void testNested1FlattenSelectListItem() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/simple/nested_1_list_index.json")
    );
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    Group data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());

    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(TS1, rows.get(0).getTimestamp().toString());
    assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    assertEquals("listDim1v2", rows.get(0).getDimension("listextracted").get(0));
    assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  private Group getFirstRecord(Job job, String parquetPath) throws IOException, InterruptedException
  {
    File testFile = new File(parquetPath);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    DruidParquetInputFormat inputFormat = ReflectionUtils.newInstance(
        DruidParquetInputFormat.class,
        job.getConfiguration()
    );
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());

    try (RecordReader reader = inputFormat.createRecordReader(split, context)) {

      reader.initialize(split, context);
      reader.nextKeyValue();
      return (Group) reader.getCurrentValue();
    }
  }

  private List<InputRow> getAllRows(String configPath) throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(configPath));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    File testFile = new File(((StaticPathSpec) config.getPathSpec()).getPaths());
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    DruidParquetInputFormat inputFormat = ReflectionUtils.newInstance(
        DruidParquetInputFormat.class,
        job.getConfiguration()
    );
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());

    try (RecordReader reader = inputFormat.createRecordReader(split, context)) {
      List<InputRow> records = Lists.newArrayList();
      InputRowParser parser = config.getParser();

      reader.initialize(split, context);
      while (reader.nextKeyValue()) {
        reader.nextKeyValue();
        Group data = (Group) reader.getCurrentValue();
        records.add(((List<InputRow>) parser.parseBatch(data)).get(0));
      }

      return records;
    }
  }
}
