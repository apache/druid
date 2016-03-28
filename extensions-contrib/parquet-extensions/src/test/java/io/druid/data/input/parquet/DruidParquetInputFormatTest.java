/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input.parquet;

import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
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
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class DruidParquetInputFormatTest
{
  @Test
  public void test() throws IOException, InterruptedException
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/wikipedia_hadoop_parquet_job.json"));

    config.intoConfiguration(job);

    File testFile = new File("example/wikipedia_list.parquet");
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(DruidParquetInputFormat.class, job.getConfiguration());

    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    RecordReader reader = inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    reader.nextKeyValue();

    GenericRecord data = (GenericRecord) reader.getCurrentValue();

    // field not read, should return null
    assertEquals(data.get("added"), null);

    assertEquals(data.get("page"), new Utf8("Gypsy Danger"));

    reader.close();
  }
}
