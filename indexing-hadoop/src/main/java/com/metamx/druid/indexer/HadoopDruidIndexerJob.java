/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexer;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.shard.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class HadoopDruidIndexerJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidIndexerJob.class);

  private final HadoopDruidIndexerConfig config;
  private final DbUpdaterJob dbUpdaterJob;
  private IndexGeneratorJob indexJob;
  private volatile List<DataSegment> publishedSegments = null;

  public HadoopDruidIndexerJob(
      HadoopDruidIndexerConfig config
  )
  {
    config.verify();
    this.config = config;

    if (config.isUpdaterJobSpecSet()) {
      dbUpdaterJob = new DbUpdaterJob(config);
    } else {
      dbUpdaterJob = null;
    }
  }

  @Override
  public boolean run()
  {
    List<Jobby> jobs = Lists.newArrayList();

    ensurePaths();

    if (config.partitionByDimension()) {
      jobs.add(new DeterminePartitionsJob(config));
    }
    else {
      Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals()) {
        DateTime bucket = segmentGranularity.getStart();
        final HadoopyShardSpec spec = new HadoopyShardSpec(new NoneShardSpec(), shardCount++);
        shardSpecs.put(bucket, Lists.newArrayList(spec));
        log.info("DateTime[%s], spec[%s]", bucket, spec);
      }
      config.setShardSpecs(shardSpecs);
    }

    indexJob = new IndexGeneratorJob(config);
    jobs.add(indexJob);

    if (dbUpdaterJob != null) {
      jobs.add(dbUpdaterJob);
    } else {
      log.info("No updaterJobSpec set, not uploading to database");
    }

    String failedMessage = null;
    for (Jobby job : jobs) {
      if (failedMessage == null) {
        if (!job.run()) {
          failedMessage = String.format("Job[%s] failed!", job.getClass());
        }
      }
    }

    if (failedMessage == null) {
      publishedSegments = IndexGeneratorJob.getPublishedSegments(config);
    }

    if (!config.isLeaveIntermediate()) {
      if (failedMessage == null || config.isCleanupOnFailure()) {
        Path workingPath = config.makeIntermediatePath();
        log.info("Deleting path[%s]", workingPath);
        try {
          workingPath.getFileSystem(new Configuration()).delete(workingPath, true);
        }
        catch (IOException e) {
          log.error(e, "Failed to cleanup path[%s]", workingPath);
        }
      }
    }

    if (failedMessage != null) {
      throw new ISE(failedMessage);
    }

    return true;
  }

  private void ensurePaths()
  {
    // config.addInputPaths() can have side-effects ( boo! :( ), so this stuff needs to be done before anything else
    try {
      Job job = new Job(
          new Configuration(),
          String.format("%s-determine_partitions-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.19");
      for (String propName : System.getProperties().stringPropertyNames()) {
        Configuration conf = job.getConfiguration();
        if (propName.startsWith("hadoop.")) {
          conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
        }
      }

      config.addInputPaths(job);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public List<DataSegment> getPublishedSegments() {
    if(publishedSegments == null) {
      throw new IllegalStateException("Job hasn't run yet. No segments have been published yet.");
    }
    return publishedSegments;
  }

  public IndexGeneratorJob.IndexGeneratorStats getIndexJobStats()
  {
    return indexJob.getJobStats();
  }
}
