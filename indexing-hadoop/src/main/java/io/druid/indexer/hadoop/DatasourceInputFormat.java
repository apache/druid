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
import com.google.common.base.Supplier;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.JobHelper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
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

  private static final String CONF_DATASOURCES = "druid.datasource.input.datasources";
  private static final String CONF_SCHEMA = "druid.datasource.input.schema";
  private static final String CONF_SEGMENTS = "druid.datasource.input.segments";
  private static final String CONF_MAX_SPLIT_SIZE = "druid.datasource.input.split.max.size";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {
    JobConf conf = new JobConf(context.getConfiguration());

    List<String> dataSources = getDataSources(conf);
    List<InputSplit> splits = new ArrayList<>();

    for (String dataSource : dataSources) {
      List<WindowedDataSegment> segments = getSegments(conf, dataSource);
      if (segments == null || segments.size() == 0) {
        throw new ISE("No segments found to read for dataSource[%s]", dataSource);
      }

      // Note: Each segment is logged separately to avoid creating a huge String if we are loading lots of segments.
      for (int i = 0; i < segments.size(); i++) {
        final WindowedDataSegment segment = segments.get(i);
        logger.info(
            "Segment %,d/%,d for dataSource[%s] has identifier[%s], interval[%s]",
            i,
            segments.size(),
            dataSource,
            segment.getSegment().getIdentifier(),
            segment.getInterval()
        );
      }

      long maxSize = getMaxSplitSize(conf, dataSource);
      if (maxSize < 0) {
        long totalSize = 0;
        for (WindowedDataSegment segment : segments) {
          totalSize += segment.getSegment().getSize();
        }
        int mapTask = conf.getNumMapTasks();
        if (mapTask > 0) {
          maxSize = totalSize / mapTask;
        }
      }

      if (maxSize > 0) {
        //combining is to happen, let us sort the segments list by size so that they
        //are combined appropriately
        segments.sort(Comparator.comparingLong(s -> s.getSegment().getSize()));
      }

      List<WindowedDataSegment> list = new ArrayList<>();
      long size = 0;

      org.apache.hadoop.mapred.InputFormat fio = supplier.get();
      for (WindowedDataSegment segment : segments) {
        if (size + segment.getSegment().getSize() > maxSize && size > 0) {
          splits.add(toDataSourceSplit(list, fio, conf));
          list = new ArrayList<>();
          size = 0;
        }

        list.add(segment);
        size += segment.getSegment().getSize();
      }

      if (list.size() > 0) {
        splits.add(toDataSourceSplit(list, fio, conf));
      }
    }

    logger.info("Number of splits [%d]", splits.size());
    return splits;
  }

  @Override
  public RecordReader<NullWritable, InputRow> createRecordReader(
      InputSplit split,
      TaskAttemptContext context
  )
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
          List<FileStatus> statusList = new ArrayList<>();
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
                  catch (final Exception e) {
                    logger.error(e, "Exception getting locations");
                    return Stream.empty();
                  }
                }
            );
          }
          catch (final Exception e) {
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

  public static List<String> getDataSources(final Configuration conf) throws IOException
  {
    final String currentDatasources = conf.get(CONF_DATASOURCES);

    if (currentDatasources == null) {
      return Collections.emptyList();
    }

    return HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        currentDatasources,
        new TypeReference<List<String>>() {}
    );
  }

  public static DatasourceIngestionSpec getIngestionSpec(final Configuration conf, final String dataSource)
      throws IOException
  {
    final String specString = conf.get(StringUtils.format("%s.%s", CONF_SCHEMA, dataSource));
    if (specString == null) {
      throw new NullPointerException(StringUtils.format("null spec for dataSource[%s]", dataSource));
    }

    final DatasourceIngestionSpec spec = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        specString,
        DatasourceIngestionSpec.class
    );

    if (spec.getDimensions() == null || spec.getDimensions().size() == 0) {
      throw new ISE("load schema does not have dimensions");
    }

    if (spec.getMetrics() == null || spec.getMetrics().size() == 0) {
      throw new ISE("load schema does not have metrics");
    }

    return spec;
  }

  public static List<WindowedDataSegment> getSegments(final Configuration conf, final String dataSource)
      throws IOException
  {
    return HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        conf.get(StringUtils.format("%s.%s", CONF_SEGMENTS, dataSource)),
        new TypeReference<List<WindowedDataSegment>>() {}
    );
  }

  public static long getMaxSplitSize(final Configuration conf, final String dataSource)
  {
    return conf.getLong(StringUtils.format("%s.%s", CONF_MAX_SPLIT_SIZE, dataSource), 0L);
  }

  public static void addDataSource(
      final Configuration conf,
      final DatasourceIngestionSpec spec,
      final List<WindowedDataSegment> segments,
      final long maxSplitSize
  ) throws IOException
  {
    final List<String> dataSources = getDataSources(conf);

    if (dataSources.contains(spec.getDataSource())) {
      throw new ISE("Oops, cannot load the same dataSource twice!");
    }

    final List<String> newDataSources = new ArrayList<>(dataSources);
    newDataSources.add(spec.getDataSource());

    conf.set(CONF_DATASOURCES, HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(newDataSources));

    conf.set(
        StringUtils.format("%s.%s", CONF_SCHEMA, spec.getDataSource()),
        HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(spec)
    );

    conf.set(
        StringUtils.format("%s.%s", CONF_SEGMENTS, spec.getDataSource()),
        HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(segments)
    );

    conf.set(
        StringUtils.format("%s.%s", CONF_MAX_SPLIT_SIZE, spec.getDataSource()),
        String.valueOf(maxSplitSize)
    );
  }
}
