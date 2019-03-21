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
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class HadoopDruidDetermineConfigurationJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidDetermineConfigurationJob.class);
  private final HadoopDruidIndexerConfig config;
  private Jobby job;
  private String hadoopJobIdFile;

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
      config.setHadoopJobIdFileName(hadoopJobIdFile);
      return JobHelper.runSingleJob(job, config);
    } else {
      int shardsPerInterval = config.getPartitionsSpec().getNumShards();
      Map<Long, List<HadoopyShardSpec>> shardSpecs = new TreeMap<>();
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
        DateTime bucket = segmentGranularity.getStart();
        // negative shardsPerInterval means a single shard
        final int realShardsPerInterval = shardsPerInterval < 0 ? 1 : shardsPerInterval;
        List<HadoopyShardSpec> specs = Lists.newArrayListWithCapacity(realShardsPerInterval);
        for (int i = 0; i < realShardsPerInterval; i++) {
          specs.add(
              new HadoopyShardSpec(
                  new HashBasedNumberedShardSpec(
                      i,
                      realShardsPerInterval,
                      config.getPartitionsSpec().getPartitionDimensions(),
                      HadoopDruidIndexerConfig.JSON_MAPPER
                  ),
                  shardCount++
              )
          );
        }
        shardSpecs.put(bucket.getMillis(), specs);
        log.info("DateTime[%s], spec[%s]", bucket, specs);
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

  public void setHadoopJobIdFile(String hadoopJobIdFile)
  {
    this.hadoopJobIdFile = hadoopJobIdFile;
  }
}
