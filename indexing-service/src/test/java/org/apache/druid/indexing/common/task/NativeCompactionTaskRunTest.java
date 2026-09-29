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

package org.apache.druid.indexing.common.task;

import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.task.CompactionTaskRunTestCases.Configuration;
import org.apache.druid.indexing.common.task.CompactionTaskRunTestCases.ConfigurationProvider;
import org.apache.druid.indexing.common.task.CompactionTaskRunTestCases.ConfigurationSource;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@ConfigurationSource(NativeCompactionTaskRunTest.NativeConfigurations.class)
public class NativeCompactionTaskRunTest extends CompactionTaskRunBase
{
  public static class NativeConfigurations implements ConfigurationProvider
  {
    @Override
    public Stream<Configuration> configurations()
    {
      final List<Configuration> configurations = new ArrayList<>();

      for (final LockGranularity lockGranularity
          : new LockGranularity[]{LockGranularity.TIME_CHUNK, LockGranularity.SEGMENT}) {
        for (final boolean useCentralizedDatasourceSchema : new boolean[]{true}) {
          for (final boolean batchSegmentAllocation : new boolean[]{false, true}) {
            for (final boolean useSegmentMetadataCache : new boolean[]{false, true}) {
              for (final boolean useConcurrentLocks : new boolean[]{false, true}) {
                for (final Interval inputInterval : new Interval[]{TEST_INTERVAL, TEST_INTERVAL_DAY}) {
                  for (final Granularity segmentGranularity
                      : new Granularity[]{null, Granularities.HOUR, Granularities.SIX_HOUR}) {
                    configurations.add(
                        new Configuration(
                            lockGranularity,
                            useCentralizedDatasourceSchema,
                            batchSegmentAllocation,
                            useSegmentMetadataCache,
                            useConcurrentLocks,
                            inputInterval,
                            segmentGranularity
                        )
                    );
                  }
                }
              }
            }
          }
        }
      }
      return configurations.stream();
    }
  }

  @Override
  protected CompactionTask.Builder compactionTaskBuilder(ClientCompactionTaskGranularitySpec granularitySpec)
  {
    return new CompactionTask.Builder(DATA_SOURCE, segmentCacheManagerFactory)
        .compactionRunner(null) // default to native compaction runner
        .granularitySpec(granularitySpec);
  }
}
