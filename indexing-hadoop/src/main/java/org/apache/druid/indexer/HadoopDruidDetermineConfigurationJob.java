/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.indexer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class HadoopDruidDetermineConfigurationJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidDetermineConfigurationJob.class);
  private final HadoopDruidIndexerConfig config;
  private Jobby job;

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
    JobHelper.ensurePaths(config);

    if (config.isDeterminingPartitions()) {
      job = config.getPartitionsSpec().getPartitionJob(config);
      return JobHelper.runSingleJob(job, config);
    } else {
      int shardsPerInterval = config.getPartitionsSpec().getNumShards();
      Map<Long, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap();
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
        DateTime bucket = segmentGranularity.getStart();
        if (shardsPerInterval > 0) {
          List<HadoopyShardSpec> specs = Lists.newArrayListWithCapacity(shardsPerInterval);
          for (int i = 0; i < shardsPerInterval; i++) {
            specs.add(
                new HadoopyShardSpec(
                    new HashBasedNumberedShardSpec(
                        i,
                        shardsPerInterval,
                        config.getPartitionsSpec().getPartitionDimensions(),
                        HadoopDruidIndexerConfig.JSON_MAPPER
                    ),
                    shardCount++
                )
            );
          }
          shardSpecs.put(bucket.getMillis(), specs);
          log.info("DateTime[%s], spec[%s]", bucket, specs);
        } else {
          final HadoopyShardSpec spec = new HadoopyShardSpec(NoneShardSpec.instance(), shardCount++);
          shardSpecs.put(bucket.getMillis(), Collections.singletonList(spec));
          log.info("DateTime[%s], spec[%s]", bucket, spec);
        }
      }
      config.setShardSpecs(shardSpecs);
      return true;
    }
  }

  @Override
  public Map<String, Object> getStats()
  {
    if (job == null) {
      return null;
    }

    return job.getStats();
  }

  @Override
  public String getErrorMessage()
  {
    if (job == null) {
      return null;
    }

    return job.getErrorMessage();
  }
}
