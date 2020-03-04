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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.hamcrest.Matchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Enclosed.class)
public class ParallelIndexSupervisorTaskTest
{
  @RunWith(Parameterized.class)
  public static class CreateMergeIoConfigsTest
  {
    private static final int TOTAL_NUM_MERGE_TASKS = 10;
    private static final Function<List<HashPartitionLocation>, PartialHashSegmentMergeIOConfig>
        CREATE_PARTIAL_SEGMENT_MERGE_IO_CONFIG = PartialHashSegmentMergeIOConfig::new;

    @Parameterized.Parameters(name = "count = {0}")
    public static Iterable<? extends Object> data()
    {
      // different scenarios for last (index = 10 - 1 = 9) partition:
      return Arrays.asList(
          20,  // even partitions per task: round(20 / 10) * (10 - 1) = 2 * 9 = 18 < 20
          24,  // round down:               round(24 / 10) * (10 - 1) = 2 * 9 = 18 < 24
          25,  // round up to greater:      round(25 / 10) * (10 - 1) = 3 * 9 = 27 > 25 (index out of bounds)
          27   // round up to equal:        round(27 / 10) * (10 - 1) = 3 * 9 = 27 == 27 (empty partition)
      );
    }

    @Parameterized.Parameter
    public int count;

    @Test
    public void handlesLastPartitionCorrectly()
    {
      List<PartialHashSegmentMergeIOConfig> assignedPartitionLocation = createMergeIOConfigs();
      assertNoMissingPartitions(count, assignedPartitionLocation);
    }

    @Test
    public void sizesPartitionsEvenly()
    {
      List<PartialHashSegmentMergeIOConfig> assignedPartitionLocation = createMergeIOConfigs();
      List<Integer> actualPartitionSizes = assignedPartitionLocation.stream()
                                                                    .map(i -> i.getPartitionLocations().size())
                                                                    .collect(Collectors.toList());
      List<Integer> sortedPartitionSizes = Ordering.natural().sortedCopy(actualPartitionSizes);
      int minPartitionSize = sortedPartitionSizes.get(0);
      int maxPartitionSize = sortedPartitionSizes.get(sortedPartitionSizes.size() - 1);
      int partitionSizeRange = maxPartitionSize - minPartitionSize;

      Assert.assertThat(
          "partition sizes = " + actualPartitionSizes,
          partitionSizeRange,
          Matchers.is(Matchers.both(Matchers.greaterThanOrEqualTo(0)).and(Matchers.lessThanOrEqualTo(1)))
      );
    }

    private List<PartialHashSegmentMergeIOConfig> createMergeIOConfigs()
    {
      return ParallelIndexSupervisorTask.createMergeIOConfigs(
          TOTAL_NUM_MERGE_TASKS,
          createPartitionToLocations(count),
          CREATE_PARTIAL_SEGMENT_MERGE_IO_CONFIG
      );
    }

    private static Map<Pair<Interval, Integer>, List<HashPartitionLocation>> createPartitionToLocations(int count)
    {
      return IntStream.range(0, count).boxed().collect(
          Collectors.toMap(
              i -> Pair.of(createInterval(i), i),
              i -> Collections.singletonList(createPartitionLocation(i))
          )
      );
    }

    private static HashPartitionLocation createPartitionLocation(int id)
    {
      return new HashPartitionLocation(
          "host",
          0,
          false,
          "subTaskId",
          createInterval(id),
          id
      );
    }

    private static Interval createInterval(int id)
    {
      return Intervals.utc(id, id + 1);
    }

    private static void assertNoMissingPartitions(
        int count,
        List<PartialHashSegmentMergeIOConfig> assignedPartitionLocation
    )
    {
      List<Integer> expectedIds = IntStream.range(0, count).boxed().collect(Collectors.toList());

      List<Integer> actualIds = assignedPartitionLocation.stream()
                                                         .flatMap(
                                                             i -> i.getPartitionLocations()
                                                                   .stream()
                                                                   .map(HashPartitionLocation::getPartitionId)
                                                         )
                                                         .sorted()
                                                         .collect(Collectors.toList());

      Assert.assertEquals(expectedIds, actualIds);
    }
  }
}
