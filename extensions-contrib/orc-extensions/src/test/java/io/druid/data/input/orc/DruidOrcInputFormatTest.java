/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.orc;

import io.druid.java.util.common.StringUtils;
import io.druid.data.input.MapBasedInputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class DruidOrcInputFormatTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  String timestamp = "2016-01-01T00:00:00.000Z";
  String col1 = "bar";
  String[] col2 = {"dat1", "dat2", "dat3"};
  double val1 = 1.1;
  Job job;
  HadoopDruidIndexerConfig config;
  File testFile;
  Path path;
  FileSplit split;

  @Before
  public void setUp() throws IOException
  {
    Configuration conf = new Configuration();
    job = Job.getInstance(conf);

    config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/hadoop_orc_job.json"));

    config.intoConfiguration(job);

    testFile = makeOrcFile();
    path = new Path(testFile.getAbsoluteFile().toURI());
    split = new FileSplit(path, 0, testFile.length(), null);

  }

  @Test
  public void testRead() throws IOException, InterruptedException
  {
    InputFormat inputFormat = ReflectionUtils.newInstance(OrcNewInputFormat.class, job.getConfiguration());

    TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    RecordReader reader = inputFormat.createRecordReader(split, context);
    OrcHadoopInputRowParser parser = (OrcHadoopInputRowParser)config.getParser();

    reader.initialize(split, context);

    reader.nextKeyValue();

    OrcStruct data = (OrcStruct) reader.getCurrentValue();

    MapBasedInputRow row = (MapBasedInputRow)parser.parse(data);

    Assert.assertTrue(row.getEvent().keySet().size() == 4);
    Assert.assertEquals(new DateTime(timestamp), row.getTimestamp());
    Assert.assertEquals(parser.getParseSpec().getDimensionsSpec().getDimensionNames(), row.getDimensions());
    Assert.assertEquals(col1, row.getEvent().get("col1"));
    Assert.assertEquals(Arrays.asList(col2), row.getDimension("col2"));

    reader.close();
  }

  private File makeOrcFile() throws IOException
  {
    final File dir = temporaryFolder.newFolder();
    final File testOrc = new File(dir, "test.orc");
    TypeDescription schema = TypeDescription.createStruct()
        .addField("timestamp", TypeDescription.createString())
        .addField("col1", TypeDescription.createString())
        .addField("col2", TypeDescription.createList(TypeDescription.createString()))
        .addField("val1", TypeDescription.createFloat());
    Configuration conf = new Configuration();
    Writer writer = OrcFile.createWriter(
        new Path(testOrc.getPath()),
        OrcFile.writerOptions(conf)
            .setSchema(schema)
        .stripeSize(100000)
        .bufferSize(10000)
        .compress(CompressionKind.ZLIB)
        .version(OrcFile.Version.CURRENT)
    );
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1;
    ((BytesColumnVector) batch.cols[0]).setRef(
        0,
        StringUtils.toUtf8(timestamp),
        0,
        timestamp.length()
    );
    ((BytesColumnVector) batch.cols[1]).setRef(0, StringUtils.toUtf8(col1), 0, col1.length());

    ListColumnVector listColumnVector = (ListColumnVector) batch.cols[2];
    listColumnVector.childCount = col2.length;
    listColumnVector.lengths[0] = 3;
    for (int idx = 0; idx < col2.length; idx++) {
      ((BytesColumnVector) listColumnVector.child).setRef(
          idx,
          StringUtils.toUtf8(col2[idx]),
          0,
          col2[idx].length()
      );
    }

    ((DoubleColumnVector) batch.cols[3]).vector[0] = val1;
    writer.addRowBatch(batch);
    writer.close();

    return testOrc;
  }
}
