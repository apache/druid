package io.druid.indexer.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 */
public class QueryBasedInputFormatTest
{
  @Test
  public void test() throws IOException, InterruptedException
  {
    Configuration conf = new Configuration();
    conf.set(QueryBasedInputFormat.CONF_DRUID_BROKER_ADDRESS, "http://localhost:8082");
    conf.set(QueryBasedInputFormat.CONF_DRUID_DATASOURCE, "wikipedia");
    conf.set(QueryBasedInputFormat.CONF_DRUID_INTERVALS, "2010-12-02T03:00:00.000Z/2015-12-02T04:00:00.000Z");
    conf.set(QueryBasedInputFormat.CONF_SELECT_COLUMNS, "__time, page, language, count, added, delta, deleted");

    JobContext context = EasyMock.createMock(JobContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(conf);
    EasyMock.replay(context);

    TaskAttemptContext attemptContext = EasyMock.createMock(TaskAttemptContext.class);
    EasyMock.expect(attemptContext.getConfiguration()).andReturn(conf);
    EasyMock.replay(attemptContext);

    QueryBasedInputFormat format = new QueryBasedInputFormat();
    Assert.assertTrue(format instanceof InputFormat);
    Assert.assertTrue(format instanceof org.apache.hadoop.mapred.InputFormat);
    for (InputSplit split : format.getSplits(context)) {
      QueryBasedInputFormat.DruidInputSplit dsplit = (QueryBasedInputFormat.DruidInputSplit) split;

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      dsplit.write(new DataOutputStream(bout));

      dsplit = new QueryBasedInputFormat.DruidInputSplit();
      dsplit.readFields(new DataInputStream(new ByteArrayInputStream(bout.toByteArray())));
      System.out.println("[DruidInputFormatTest/test] " + split);
      RecordReader<NullWritable, MapWritable> reader = format.createRecordReader(dsplit, null);
      reader.initialize(split, attemptContext);

      while (reader.nextKeyValue()) {
        System.out.println("[DruidInputFormatTest/test] " + reader.getCurrentValue());
      }
      reader.close();

      org.apache.hadoop.mapred.RecordReader<NullWritable, MapWritable> reader2 =
          format.getRecordReader(dsplit, new JobConf(conf), null);

      NullWritable key = reader2.createKey();
      MapWritable value = reader2.createValue();
      while (reader2.next(key, value)) {
        System.out.println("[DruidInputFormatTest/test] " + value);
      }
      reader2.close();
    }
  }
}
