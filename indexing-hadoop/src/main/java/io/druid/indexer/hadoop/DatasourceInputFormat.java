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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class DatasourceInputFormat extends InputFormat<NullWritable, InputRow>
{
  private static final Logger logger = new Logger(DatasourceInputFormat.class);

  public static final String CONF_INPUT_SEGMENTS = "druid.segments";
  public static final String CONF_DRUID_SCHEMA = "druid.datasource.schema";
  public static final String CONF_MAX_SPLIT_SIZE = "druid.datasource.split.max.size";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();

    String segmentsStr = Preconditions.checkNotNull(conf.get(CONF_INPUT_SEGMENTS), "No segments found to read");
    List<WindowedDataSegment> segments = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        segmentsStr,
        new TypeReference<List<WindowedDataSegment>>()
        {
        }
    );
    if (segments == null || segments.size() == 0) {
      throw new ISE("No segments found to read");
    }

    logger.info("segments to read [%s]", segmentsStr);

    long maxSize = conf.getLong(CONF_MAX_SPLIT_SIZE, 0);

    if (maxSize > 0) {
      //combining is to happen, let us sort the segments list by size so that they
      //are combined appropriately
      Collections.sort(
          segments,
          new Comparator<WindowedDataSegment>()
          {
            @Override
            public int compare(WindowedDataSegment s1, WindowedDataSegment s2)
            {
              return Long.compare(s1.getSegment().getSize(), s2.getSegment().getSize());
            }
          }
      );
    }

    List<InputSplit> splits = Lists.newArrayList();

    List<WindowedDataSegment> list = new ArrayList<>();
    long size = 0;

    for (WindowedDataSegment segment : segments) {
      if (size + segment.getSegment().getSize() > maxSize && size > 0) {
        splits.add(new DatasourceInputSplit(list));
        list = Lists.newArrayList();
        size = 0;
      }

      list.add(segment);
      size += segment.getSegment().getSize();
    }

    if (list.size() > 0) {
      splits.add(new DatasourceInputSplit(list));
    }

    logger.info("Number of splits [%d]", splits.size());
    return splits;
  }

  @Override
  public RecordReader<NullWritable, InputRow> createRecordReader(
      InputSplit split,
      TaskAttemptContext context
  ) throws IOException, InterruptedException
  {
    return new DatasourceRecordReader();
  }
}
