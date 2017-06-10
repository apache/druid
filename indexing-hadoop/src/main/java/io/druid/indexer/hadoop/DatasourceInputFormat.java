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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.JobHelper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    String segmentsStr = Preconditions.checkNotNull(
        conf.get(CONF_INPUT_SEGMENTS),
        "No segments found to read"
    );
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
    if (maxSize < 0) {
      long totalSize = 0;
      for (WindowedDataSegment segment : segments) {
        totalSize += segment.getSegment().getSize();
      }
      int mapTask = ((JobConf) conf).getNumMapTasks();
      if (mapTask > 0) {
        maxSize = totalSize / mapTask;
      }
    }

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

    JobConf dummyConf = new JobConf();
    org.apache.hadoop.mapred.InputFormat fio = supplier.get();
    for (WindowedDataSegment segment : segments) {
      if (size + segment.getSegment().getSize() > maxSize && size > 0) {
        splits.add(toDataSourceSplit(list, fio, dummyConf));
        list = Lists.newArrayList();
        size = 0;
      }

      list.add(segment);
      size += segment.getSegment().getSize();
    }

    if (list.size() > 0) {
      splits.add(toDataSourceSplit(list, fio, dummyConf));
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

  private Supplier<org.apache.hadoop.mapred.InputFormat> supplier = new Supplier<org.apache.hadoop.mapred.InputFormat>()
  {
    @Override
    public org.apache.hadoop.mapred.InputFormat get()
    {
      return new TextInputFormat()
      {
        //Always consider non-splittable as we only want to get location of blocks for the segment
        //and not consider the splitting.
        //also without this, isSplitable(..) fails with NPE because compressionCodecs is not properly setup.
        @Override
        protected boolean isSplitable(FileSystem fs, Path file)
        {
          return false;
        }

        @Override
        protected FileStatus[] listStatus(JobConf job) throws IOException
        {
          // to avoid globbing which needs input path should be hadoop-compatible (':' is not acceptable in path, etc.)
          List<FileStatus> statusList = Lists.newArrayList();
          for (Path path : FileInputFormat.getInputPaths(job)) {
            // load spec in segment points specifically zip file itself
            statusList.add(path.getFileSystem(job).getFileStatus(path));
          }
          return statusList.toArray(new FileStatus[statusList.size()]);
        }
      };
    }
  };

  @VisibleForTesting
  DatasourceInputFormat setSupplier(Supplier<org.apache.hadoop.mapred.InputFormat> supplier)
  {
    this.supplier = supplier;
    return this;
  }

  private DatasourceInputSplit toDataSourceSplit(
      List<WindowedDataSegment> segments,
      org.apache.hadoop.mapred.InputFormat fio,
      JobConf conf
  )
  {
    String[] locations = getFrequentLocations(getLocations(segments, fio, conf));

    return new DatasourceInputSplit(segments, locations);
  }

  @VisibleForTesting
  static Stream<String> getLocations(
      final List<WindowedDataSegment> segments,
      final org.apache.hadoop.mapred.InputFormat fio,
      final JobConf conf
  )
  {
    return segments.stream().sequential().flatMap(
        (final WindowedDataSegment segment) -> {
          FileInputFormat.setInputPaths(
              conf,
              new Path(JobHelper.getURIFromSegment(segment.getSegment()))
          );
          try {
            return Arrays.stream(fio.getSplits(conf, 1)).flatMap(
                (final org.apache.hadoop.mapred.InputSplit split) -> {
                  try {
                    return Arrays.stream(split.getLocations());
                  }
                  catch (final IOException e) {
                    logger.error(e, "Exception getting locations");
                    return Stream.empty();
                  }
                }
            );
          }
          catch (final IOException e) {
            logger.error(e, "Exception getting splits");
            return Stream.empty();
          }
        }
    );
  }

  @VisibleForTesting
  static String[] getFrequentLocations(final Stream<String> locations)
  {
    final Map<String, Long> locationCountMap = locations.collect(
        Collectors.groupingBy(location -> location, Collectors.counting())
    );

    final Comparator<Map.Entry<String, Long>> valueComparator =
        Map.Entry.comparingByValue(Comparator.reverseOrder());

    final Comparator<Map.Entry<String, Long>> keyComparator =
        Map.Entry.comparingByKey();

    return locationCountMap
        .entrySet().stream()
        .sorted(valueComparator.thenComparing(keyComparator))
        .limit(3)
        .map(Map.Entry::getKey)
        .toArray(String[]::new);
  }
}
