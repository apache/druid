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

package io.druid.hive;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import io.druid.indexer.hadoop.QueryBasedInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.joda.time.Interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class HiveDruidInputFormat extends QueryBasedInputFormat implements HiveOutputFormat
{
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
  {
    DruidInputSplit[] splits = getInputSplits(job);

    String input = job.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, "");
    String[] dirs = org.apache.hadoop.util.StringUtils.split(input);
    if (dirs.length == 0) {
      throw new IllegalStateException("input dir is null");
    }
    Path path = new Path(dirs[0]);
    InputSplit[] converted = new InputSplit[splits.length];
    for (int i = 0; i < converted.length; i++) {
      converted[i] = new InputSplitWrapper(path, splits[i]);
    }
    return converted;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException
  {
    DruidRecordReader reader = new DruidRecordReader();
    reader.initialize(((InputSplitWrapper) split).druidSplit, job);
    return reader;
  }

  @Override
  protected final Configuration configure(Configuration configuration)
  {
    List<Interval> intervals = ExpressionConverter.convert(configuration);
    if (intervals == null || intervals.isEmpty()) {
      throw new IllegalArgumentException("failed to extract intervals from predicate");
    }
    configuration.set(
        CONF_DRUID_INTERVALS,
        StringUtils.join(Lists.transform(intervals, Functions.toStringFunction()), ",")
    );
    return configuration;
  }

  @Override
  public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc,
      Path finalOutPath,
      Class valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress
  ) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  public static class InputSplitWrapper extends FileSplit
  {
    private DruidInputSplit druidSplit;

    public InputSplitWrapper() {}

    public InputSplitWrapper(Path path, DruidInputSplit druidSplit)
    {
      super(path, 0, 0, druidSplit.getLocations());
      this.druidSplit = druidSplit;
    }

    public void write(DataOutput out) throws IOException
    {
      super.write(out);
      druidSplit.write(out);
    }

    public void readFields(DataInput in) throws IOException
    {
      super.readFields(in);
      this.druidSplit = new DruidInputSplit();
      this.druidSplit.readFields(in);
    }
  }
}