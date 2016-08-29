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

package io.druid.indexer.partitions;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopyShardSpec;
import io.druid.indexer.Jobby;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DetermineSizeBasedPartitionsJob implements Jobby
{
  private static final Logger log = new Logger(DetermineSizeBasedPartitionsJob.class);
  private final HadoopDruidIndexerConfig config;

  public DetermineSizeBasedPartitionsJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
  }

  @Override
  public boolean run()
  {
    long startTime = System.currentTimeMillis();
    long targetSize = config.getTargetPartitionSize();

    Optional<Set<Interval>> intervals = config.getSegmentGranularIntervals();
    if (!intervals.isPresent() || intervals.get().size() != 1) {
      throw new IllegalArgumentException("Invalid interval.. only single granular interval is allowed");
    }
    Interval interval = intervals.get().iterator().next();
    try {
      final Job job = Job.getInstance(
          new Configuration(),
          String.format("%s-determine_partitions_filesized-%s", config.getDataSource(), config.getIntervals())
      );

      config.addInputPaths(job);

      Configuration configuration = job.getConfiguration();
      boolean recursive = FileInputFormat.getInputDirRecursive(job);

      PathFilter inputFilter = getPathFilter(job);

      long total = 0;
      for (String mapping : configuration.get(MultipleInputs.DIR_FORMATS).split(",")) {
        Path path = new Path(mapping.split(";")[0]);
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        FileStatus[] matches = fs.globStatus(path, inputFilter);
        if (matches == null || matches.length == 0) {
          throw new IllegalArgumentException("input path is not valid");
        }
        for (FileStatus globStat : matches) {
          total += iterate(fs, globStat, inputFilter, recursive);
        }
      }
      Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());

      DateTime bucket = interval.getStart();

      final int numberOfShards = (int) Math.ceil((double) total / targetSize);

      log.info(
            "Creating [%,d] shards from total length [%,d] with target size [%,d] for interval [%s]",
            numberOfShards,
            total,
            targetSize,
            interval
        );

      List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithCapacity(numberOfShards);
      if (numberOfShards == 1) {
        actualSpecs.add(new HadoopyShardSpec(new NoneShardSpec(), 0));
      } else {
        int shardCount = 0;
        for (int i = 0; i < numberOfShards; ++i) {
          HadoopyShardSpec shardSpec = new HadoopyShardSpec(
              new HashBasedNumberedShardSpec(i, numberOfShards, null, HadoopDruidIndexerConfig.JSON_MAPPER),
              shardCount++
          );
          log.info("DateTime[%s], partition[%d], spec[%s]", bucket, i, shardSpec);
          actualSpecs.add(shardSpec);
        }
      }
      shardSpecs.put(bucket, actualSpecs);

      config.setShardSpecs(shardSpecs);
      log.info("DetermineHashedPartitionsJob took %d millis", (System.currentTimeMillis() - startTime));

      return true;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private long iterate(FileSystem fs, FileStatus status, PathFilter inputFilter, boolean recursive) throws IOException
  {
    if (status.isDirectory()) {
      long total = 0;
      for (FileStatus child : fs.listStatus(status.getPath())) {
        if (inputFilter.accept(child.getPath())) {
          if (recursive && child.isDirectory()) {
            total += iterate(fs, child, inputFilter, recursive);
          } else {
            total += child.getLen();
          }
        }
      }
      return total;
    } else if (recursive && status.isSymlink()) {
      return iterate(fs, fs.getFileStatus(status.getSymlink()), inputFilter, recursive);
    }
    return status.getLen();
  }

  private PathFilter getPathFilter(Job job)
  {
    // creates a MultiPathFilter with the hiddenFileFilter and the user provided one (if any).
    PathFilter hiddenFilter = new PathFilter()
    {
      public boolean accept(Path p)
      {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    };
    PathFilter jobFilter = FileInputFormat.getInputPathFilter(job);
    if (jobFilter == null) {
      return hiddenFilter;
    }
    final List<PathFilter> filters = Arrays.asList(hiddenFilter, jobFilter);
    return new PathFilter()
    {
      @Override
      public boolean accept(Path path)
      {
        for (PathFilter filter : filters) {
          if (!filter.accept(path)) {
            return false;
          }
        }
        return true;
      }
    };
  }
}
