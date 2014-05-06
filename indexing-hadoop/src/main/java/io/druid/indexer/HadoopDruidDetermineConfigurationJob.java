/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class HadoopDruidDetermineConfigurationJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidDetermineConfigurationJob.class);
  private final HadoopDruidIndexerConfig config;

  @Inject
  public HadoopDruidDetermineConfigurationJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
  }

  @Override
  public boolean run()
  {
    List<Jobby> jobs = Lists.newArrayList();

    JobHelper.ensurePaths(config);

    if (config.isDeterminingPartitions()) {
      jobs.add(config.getPartitionsSpec().getPartitionJob(config));
    } else {
      int shardsPerInterval = config.getPartitionsSpec().getNumShards();
      Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
        DateTime bucket = segmentGranularity.getStart();
        if (shardsPerInterval > 0) {
          List<HadoopyShardSpec> specs = Lists.newArrayListWithCapacity(shardsPerInterval);
          for (int i = 0; i < shardsPerInterval; i++) {
            specs.add(
                new HadoopyShardSpec(
                    new HashBasedNumberedShardSpec(i, shardsPerInterval),
                    shardCount++
                )
            );
          }
          shardSpecs.put(bucket, specs);
          log.info("DateTime[%s], spec[%s]", bucket, specs);
        } else {
          final HadoopyShardSpec spec = new HadoopyShardSpec(new NoneShardSpec(), shardCount++);
          shardSpecs.put(bucket, Lists.newArrayList(spec));
          log.info("DateTime[%s], spec[%s]", bucket, spec);
        }
      }
      config.setShardSpecs(shardSpecs);
    }

    return JobHelper.runJobs(jobs, config);

  }

}
