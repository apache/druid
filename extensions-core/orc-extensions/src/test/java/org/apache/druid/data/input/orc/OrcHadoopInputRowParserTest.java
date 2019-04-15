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

package org.apache.druid.data.input.orc;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.path.StaticPathSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OrcHadoopInputRowParserTest
{
  @Test
  public void testTest1() throws IOException, InterruptedException
  {
    // total auto-discover fields (no flattenSpec, no dimensionSpec)
    HadoopDruidIndexerConfig config = loadHadoopDruidIndexerConfig("example/test_1_hadoop_job.json");
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    /*
      test_1.orc
      struct<timestamp:string,col1:string,col2:array<string>,val1:float>
      {2016-01-01T00:00:00.000Z, bar, [dat1, dat2, dat3], 1.1}
     */
    OrcStruct data = getFirstRow(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(3, rows.get(0).getDimensions().size());
    assertEquals("bar", rows.get(0).getDimension("col1").get(0));
    String s1 = rows.get(0).getDimension("col2").get(0);
    String s2 = rows.get(0).getDimension("col2").get(1);
    String s3 = rows.get(0).getDimension("col2").get(2);
    assertEquals("dat1", s1);
    assertEquals("dat2", s2);
    assertEquals("dat3", s3);
  }

  @Test
  public void testTest2() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = loadHadoopDruidIndexerConfig("example/test_2_hadoop_job.json");
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    /*
      test_2.orc
      struct<timestamp:string,col1:string,col2:array<string>,col3:float,col4:bigint,col5:decimal,col6:array<string>,col7:map<string,string>>
      {2016-01-01, bar, [dat1, dat2, dat3], 1.1, 2, 3.5, [], {subcol7=subval7}}
     */
    OrcStruct data = getFirstRow(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(7, rows.get(0).getDimensions().size());
    assertEquals("bar", rows.get(0).getDimension("col1").get(0));
    assertEquals("dat1", rows.get(0).getDimension("col2").get(0));
    assertEquals("dat2", rows.get(0).getDimension("col2").get(1));
    assertEquals("dat3", rows.get(0).getDimension("col2").get(2));
    assertEquals(1.1f, rows.get(0).getRaw("col3"));
    assertEquals(2L, rows.get(0).getRaw("col4"));
    assertEquals(3.5d, rows.get(0).getRaw("col5"));
    assertEquals(ImmutableList.of(), rows.get(0).getRaw("col6"));
    assertEquals("subval7", rows.get(0).getRaw("col7-subcol7"));
  }

  @Test
  public void testOrcFile11Format() throws IOException, InterruptedException
  {
    // not sure what file 11 format means, but we'll test it!

    /*
      orc-file-11-format.orc
      struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int,long1:bigint,float1:float,double1:double,bytes1:binary,string1:string,middle:struct<list:array<struct<int1:int,string1:string>>>,list:array<struct<int1:int,string1:string>>,map:map<string,struct<int1:int,string1:string>>,ts:timestamp,decimal1:decimal(38,10)>
      {false, 1, 1024, 65536, 9223372036854775807, 1.0, -15.0, 00 01 02 03 04, hi, {[{1, bye}, {2, sigh}]}, [{3, good}, {4, bad}], {}, 2000-03-12 15:00:00.0, 12345678.6547456}
     */
    HadoopDruidIndexerConfig config =
        loadHadoopDruidIndexerConfig("example/orc-file-11-format-hadoop-job.json");

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    OrcStruct data = getFirstRow(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(14, rows.get(0).getDimensions().size());
    assertEquals("false", rows.get(0).getDimension("boolean1").get(0));
    assertEquals("1", rows.get(0).getDimension("byte1").get(0));
    assertEquals("1024", rows.get(0).getDimension("short1").get(0));
    assertEquals("65536", rows.get(0).getDimension("int1").get(0));
    assertEquals("9223372036854775807", rows.get(0).getDimension("long1").get(0));
    assertEquals("1.0", rows.get(0).getDimension("float1").get(0));
    assertEquals("-15.0", rows.get(0).getDimension("double1").get(0));
    assertEquals("AAECAwQAAA==", rows.get(0).getDimension("bytes1").get(0));
    assertEquals("hi", rows.get(0).getDimension("string1").get(0));
    assertEquals("1.23456786547456E7", rows.get(0).getDimension("decimal1").get(0));
    assertEquals("2", rows.get(0).getDimension("struct_list_struct_int").get(0));
    assertEquals("1", rows.get(0).getDimension("struct_list_struct_intlist").get(0));
    assertEquals("2", rows.get(0).getDimension("struct_list_struct_intlist").get(1));
    assertEquals("good", rows.get(0).getDimension("list_struct_string").get(0));
    assertEquals(DateTimes.of("2000-03-12T15:00:00.0Z"), rows.get(0).getTimestamp());

    // first row has empty 'map' column, so lets read another!
    List<InputRow> allRows = getAllRows(config);
    InputRow anotherRow = allRows.get(0);
    assertEquals(14, rows.get(0).getDimensions().size());
    assertEquals("true", anotherRow.getDimension("boolean1").get(0));
    assertEquals("100", anotherRow.getDimension("byte1").get(0));
    assertEquals("2048", anotherRow.getDimension("short1").get(0));
    assertEquals("65536", anotherRow.getDimension("int1").get(0));
    assertEquals("9223372036854775807", anotherRow.getDimension("long1").get(0));
    assertEquals("2.0", anotherRow.getDimension("float1").get(0));
    assertEquals("-5.0", anotherRow.getDimension("double1").get(0));
    assertEquals("AAECAwQAAA==", rows.get(0).getDimension("bytes1").get(0));
    assertEquals("bye", anotherRow.getDimension("string1").get(0));
    assertEquals("1.23456786547457E7", anotherRow.getDimension("decimal1").get(0));
    assertEquals("2", anotherRow.getDimension("struct_list_struct_int").get(0));
    assertEquals("cat", anotherRow.getDimension("list_struct_string").get(0));
    assertEquals("5", anotherRow.getDimension("map_struct_int").get(0));
  }

  @Test
  public void testOrcSplitElim() throws IOException, InterruptedException
  {
    // not sure what SplitElim means, but we'll test it!

    /*
      orc_split_elim.orc
      struct<userid:bigint,string1:string,subtype:double,decimal1:decimal(38,10),ts:timestamp>
      {2, foo, 0.8, 1.2, 1969-12-31 16:00:00.0}
     */
    HadoopDruidIndexerConfig config = loadHadoopDruidIndexerConfig("example/orc_split_elim_hadoop_job.json");
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    OrcStruct data = getFirstRow(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(4, rows.get(0).getDimensions().size());
    assertEquals("2", rows.get(0).getDimension("userid").get(0));
    assertEquals("foo", rows.get(0).getDimension("string1").get(0));
    assertEquals("0.8", rows.get(0).getDimension("subtype").get(0));
    assertEquals("1.2", rows.get(0).getDimension("decimal1").get(0));
    assertEquals(DateTimes.of("1969-12-31T16:00:00.0Z"), rows.get(0).getTimestamp());
  }

  @Test
  public void testDate1900() throws IOException, InterruptedException
  {
    /*
      TestOrcFile.testDate1900.orc
      struct<time:timestamp,date:date>
      {1900-05-05 12:34:56.1, 1900-12-25}
     */
    HadoopDruidIndexerConfig config = loadHadoopDruidIndexerConfig("example/testDate1900_hadoop_job.json");
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    OrcStruct data = getFirstRow(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(1, rows.get(0).getDimensions().size());
    assertEquals("1900-12-25T00:00:00.000Z", rows.get(0).getDimension("date").get(0));
    assertEquals(DateTimes.of("1900-05-05T12:34:56.1Z"), rows.get(0).getTimestamp());
  }

  @Test
  public void testDate2038() throws IOException, InterruptedException
  {
    /*
      TestOrcFile.testDate2038.orc
      struct<time:timestamp,date:date>
      {2038-05-05 12:34:56.1, 2038-12-25}
     */
    HadoopDruidIndexerConfig config = loadHadoopDruidIndexerConfig("example/testDate2038_hadoop_job.json");
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    OrcStruct data = getFirstRow(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    List<InputRow> rows = (List<InputRow>) config.getParser().parseBatch(data);
    assertEquals(1, rows.get(0).getDimensions().size());
    assertEquals("2038-12-25T00:00:00.000Z", rows.get(0).getDimension("date").get(0));
    assertEquals(DateTimes.of("2038-05-05T12:34:56.1Z"), rows.get(0).getTimestamp());
  }

  private static HadoopDruidIndexerConfig loadHadoopDruidIndexerConfig(String configPath)
  {
    return HadoopDruidIndexerConfig.fromFile(new File(configPath));
  }

  private static OrcStruct getFirstRow(Job job, String orcPath) throws IOException, InterruptedException
  {
    File testFile = new File(orcPath);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(
        OrcInputFormat.class,
        job.getConfiguration()
    );
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());

    try (RecordReader reader = inputFormat.createRecordReader(split, context)) {

      reader.initialize(split, context);
      reader.nextKeyValue();
      return (OrcStruct) reader.getCurrentValue();
    }
  }

  private static List<InputRow> getAllRows(HadoopDruidIndexerConfig config)
      throws IOException, InterruptedException
  {
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    File testFile = new File(((StaticPathSpec) config.getPathSpec()).getPaths());
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(
        OrcInputFormat.class,
        job.getConfiguration()
    );
    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());

    try (RecordReader reader = inputFormat.createRecordReader(split, context)) {
      List<InputRow> records = new ArrayList<>();
      InputRowParser parser = config.getParser();

      reader.initialize(split, context);
      while (reader.nextKeyValue()) {
        reader.nextKeyValue();
        Object data = reader.getCurrentValue();
        records.add(((List<InputRow>) parser.parseBatch(data)).get(0));
      }

      return records;
    }
  }
}
