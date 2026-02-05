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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class NativeCompactionTaskRunTest extends CompactionTaskRunBase
{
  @Parameterized.Parameters(name = "name={0}, inputInterval={5}, segmentGran={6}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (LockGranularity lockGranularity : new LockGranularity[]{LockGranularity.TIME_CHUNK, LockGranularity.SEGMENT}) {
      for (boolean useCentralizedDatasourceSchema : new boolean[]{true}) {
        for (boolean batchSegmentAllocation : new boolean[]{true}) {
          for (boolean useSegmentMetadataCache : new boolean[]{false, true}) {
            for (boolean useConcurrentLocks : new boolean[]{false, true}) {
              for (Interval inputInterval : new Interval[]{TEST_INTERVAL, TEST_INTERVAL_DAY}) {
                for (Granularity segmentGran : new Granularity[]{null, Granularities.HOUR, Granularities.SIX_HOUR}) {
                  String name = StringUtils.format(
                      "lockGranularity=%s, useCentralizedDatasourceSchema=%s, batchSegmentAllocation=%s, useSegmentMetadataCache=%s, useConcurrentLocks=%s",
                      lockGranularity,
                      useCentralizedDatasourceSchema,
                      batchSegmentAllocation,
                      useSegmentMetadataCache,
                      useConcurrentLocks
                  );
                  constructors.add(new Object[]{
                      name,
                      lockGranularity,
                      useCentralizedDatasourceSchema,
                      batchSegmentAllocation,
                      useSegmentMetadataCache,
                      useConcurrentLocks,
                      inputInterval,
                      segmentGran
                  });
                }
              }
            }
          }
        }
      }

    }
    return constructors;
  }

  public NativeCompactionTaskRunTest(
      String name,
      LockGranularity lockGranularity,
      boolean useCentralizedDatasourceSchema,
      boolean batchSegmentAllocation,
      boolean useSegmentMetadataCache,
      boolean useConcurrentLocks,
      Interval inputInterval,
      Granularity compactionGranularity
  ) throws IOException
  {
    super(
        name,
        lockGranularity,
        useCentralizedDatasourceSchema,
        batchSegmentAllocation,
        useSegmentMetadataCache,
        useConcurrentLocks,
        inputInterval,
        compactionGranularity
    );
  }

  @Override
  protected CompactionTask.Builder compactionTaskBuilder(ClientCompactionTaskGranularitySpec granularitySpec)
  {
    return new CompactionTask.Builder(DATA_SOURCE, segmentCacheManagerFactory)
        .compactionRunner(null) // default to native compaction runner
        .granularitySpec(granularitySpec);
  }
}
