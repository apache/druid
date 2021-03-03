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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class HadoopDruidDetermineConfigurationJobTest
{
  @Test
  public void testRunWithHashedPartitionsSpecCreateHashBasedNumberedShardSpecWithHashPartitionFunction()
  {
    final Set<Interval> intervals = ImmutableSet.of(
        Intervals.of("2020-01-01/P1D"),
        Intervals.of("2020-01-02/P1D"),
        Intervals.of("2020-01-03/P1D")
    );
    final HashedPartitionsSpec partitionsSpec = new HashedPartitionsSpec(
        null,
        2,
        null,
        HashPartitionFunction.MURMUR3_32_ABS,
        null,
        null
    );
    final HadoopDruidIndexerConfig config = Mockito.mock(HadoopDruidIndexerConfig.class);
    Mockito.when(config.isDeterminingPartitions()).thenReturn(false);
    Mockito.when(config.getPartitionsSpec()).thenReturn(partitionsSpec);
    Mockito.when(config.getSegmentGranularIntervals()).thenReturn(intervals);
    final ArgumentCaptor<Map<Long, List<HadoopyShardSpec>>> resultCaptor = ArgumentCaptor.forClass(Map.class);
    Mockito.doNothing().when(config).setShardSpecs(resultCaptor.capture());

    final HadoopDruidDetermineConfigurationJob job = new HadoopDruidDetermineConfigurationJob(config);
    Assert.assertTrue(job.run());
    final Map<Long, List<HadoopyShardSpec>> shardSpecs = resultCaptor.getValue();
    Assert.assertEquals(3, shardSpecs.size());
    for (Interval interval : intervals) {
      final List<HadoopyShardSpec> shardSpecsPerInterval = shardSpecs.get(interval.getStartMillis());
      Assert.assertEquals(2, shardSpecsPerInterval.size());
      for (int i = 0; i < shardSpecsPerInterval.size(); i++) {
        Assert.assertEquals(
            new HashBasedNumberedShardSpec(
                i,
                shardSpecsPerInterval.size(),
                i,
                shardSpecsPerInterval.size(),
                null,
                HashPartitionFunction.MURMUR3_32_ABS,
                new ObjectMapper()
            ),
            shardSpecsPerInterval.get(i).getActualSpec()
        );
      }
    }
  }

  @Test
  public void testRunWithSingleDimensionPartitionsSpecCreateHashBasedNumberedShardSpecWithoutHashPartitionFunction()
  {
    final Set<Interval> intervals = ImmutableSet.of(
        Intervals.of("2020-01-01/P1D"),
        Intervals.of("2020-01-02/P1D"),
        Intervals.of("2020-01-03/P1D")
    );
    final SingleDimensionPartitionsSpec partitionsSpec = new SingleDimensionPartitionsSpec(1000, null, "dim", false);
    final HadoopDruidIndexerConfig config = Mockito.mock(HadoopDruidIndexerConfig.class);
    Mockito.when(config.isDeterminingPartitions()).thenReturn(false);
    Mockito.when(config.getPartitionsSpec()).thenReturn(partitionsSpec);
    Mockito.when(config.getSegmentGranularIntervals()).thenReturn(intervals);
    final ArgumentCaptor<Map<Long, List<HadoopyShardSpec>>> resultCaptor = ArgumentCaptor.forClass(Map.class);
    Mockito.doNothing().when(config).setShardSpecs(resultCaptor.capture());

    final HadoopDruidDetermineConfigurationJob job = new HadoopDruidDetermineConfigurationJob(config);
    Assert.assertTrue(job.run());
    final Map<Long, List<HadoopyShardSpec>> shardSpecs = resultCaptor.getValue();
    Assert.assertEquals(3, shardSpecs.size());
    for (Interval interval : intervals) {
      final List<HadoopyShardSpec> shardSpecsPerInterval = shardSpecs.get(interval.getStartMillis());
      Assert.assertEquals(1, shardSpecsPerInterval.size());
      Assert.assertEquals(
          new HashBasedNumberedShardSpec(
              0,
              shardSpecsPerInterval.size(),
              0,
              shardSpecsPerInterval.size(),
              ImmutableList.of("dim"),
              null,
              new ObjectMapper()
          ),
          shardSpecsPerInterval.get(0).getActualSpec()
      );
    }
  }
}
