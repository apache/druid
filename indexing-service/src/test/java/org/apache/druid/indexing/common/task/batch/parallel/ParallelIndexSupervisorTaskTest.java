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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.partition.BuildingHashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;

@RunWith(Enclosed.class)
public class ParallelIndexSupervisorTaskTest
{
  @RunWith(Parameterized.class)
  public static class CreateMergeIoConfigsTest
  {
    private static final int TOTAL_NUM_MERGE_TASKS = 10;
    private static final Function<List<PartitionLocation>, PartialSegmentMergeIOConfig>
        CREATE_PARTIAL_SEGMENT_MERGE_IO_CONFIG = PartialSegmentMergeIOConfig::new;

    @Parameterized.Parameters(name = "count = {0}, partitionLocationType = {1}")
    public static Iterable<? extends Object[]> data()
    {
      // different scenarios for last (index = 10 - 1 = 9) partition:
      return Arrays.asList(
          new Object[][]{
              {20, GenericPartitionStat.TYPE},  // even partitions per task: round(20 / 10) * (10 - 1) = 2 * 9 = 18 < 20
              {24, DeepStoragePartitionStat.TYPE},  // round down:               round(24 / 10) * (10 - 1) = 2 * 9 = 18 < 24
              {25, GenericPartitionStat.TYPE},  // round up to greater:      round(25 / 10) * (10 - 1) = 3 * 9 = 27 > 25 (index out of bounds)
              {27, DeepStoragePartitionStat.TYPE} // round up to equal:        round(27 / 10) * (10 - 1) = 3 * 9 = 27 == 27 (empty partition)
          }
      );
    }

    public CreateMergeIoConfigsTest(int count, String partitionLocationType)
    {
      this.count = count;
      this.partitionLocationType = partitionLocationType;
    }

    public int count;
    public String partitionLocationType;

    @Test
    public void handlesLastPartitionCorrectly()
    {
      List<PartialSegmentMergeIOConfig> assignedPartitionLocation = createMergeIOConfigs();
      assertNoMissingPartitions(count, assignedPartitionLocation);
    }

    @Test
    public void sizesPartitionsEvenly()
    {
      List<PartialSegmentMergeIOConfig> assignedPartitionLocation = createMergeIOConfigs();
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

    private List<PartialSegmentMergeIOConfig> createMergeIOConfigs()
    {
      return ParallelIndexSupervisorTask.createMergeIOConfigs(
          TOTAL_NUM_MERGE_TASKS,
          createPartitionToLocations(count, partitionLocationType),
          CREATE_PARTIAL_SEGMENT_MERGE_IO_CONFIG
      );
    }

    private static Map<Pair<Interval, Integer>, List<PartitionLocation>> createPartitionToLocations(
        int count,
        String partitionLocationType
    )
    {
      return IntStream.range(0, count).boxed().collect(
          Collectors.toMap(
              i -> Pair.of(createInterval(i), i),
              i -> Collections.singletonList(createPartitionLocation(i, partitionLocationType))
          )
      );
    }

    private static PartitionLocation createPartitionLocation(int id, String partitionLocationType)
    {
      if (DeepStoragePartitionStat.TYPE.equals(partitionLocationType)) {
        return new DeepStoragePartitionLocation("", Intervals.of("2000/2099"), new BuildingHashBasedNumberedShardSpec(
            id,
            id,
            id + 1,
            null,
            HashPartitionFunction.MURMUR3_32_ABS,
            new ObjectMapper()
        ), ImmutableMap.of());
      } else {
        return new GenericPartitionLocation(
            "host",
            0,
            false,
            "subTaskId",
            createInterval(id),
            new BuildingHashBasedNumberedShardSpec(
                id,
                id,
                id + 1,
                null,
                HashPartitionFunction.MURMUR3_32_ABS,
                new ObjectMapper()
            )
        );
      }
    }

    private static Interval createInterval(int id)
    {
      return Intervals.utc(id, id + 1);
    }

    private static void assertNoMissingPartitions(
        int count,
        List<PartialSegmentMergeIOConfig> assignedPartitionLocation
    )
    {
      List<Integer> expectedIds = IntStream.range(0, count).boxed().collect(Collectors.toList());

      List<Integer> actualIds = assignedPartitionLocation.stream()
                                                         .flatMap(
                                                             i -> i.getPartitionLocations()
                                                                   .stream()
                                                                   .map(PartitionLocation::getBucketId)
                                                         )
                                                         .sorted()
                                                         .collect(Collectors.toList());

      Assert.assertEquals(expectedIds, actualIds);
    }
  }

  public static class ConstructorTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testFailToConstructWhenBothAppendToExistingAndForceGuaranteedRollupAreSet()
    {
      final boolean appendToExisting = true;
      final boolean forceGuaranteedRollup = true;
      final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          null,
          new InlineInputSource("test"),
          new JsonInputFormat(null, null, null),
          appendToExisting,
          null
      );
      final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
          null,
          null,
          null,
          10,
          1000L,
          null,
          null,
          null,
          null,
          new HashedPartitionsSpec(null, 10, null),
          new IndexSpec(
              new RoaringBitmapSerdeFactory(true),
              CompressionStrategy.UNCOMPRESSED,
              CompressionStrategy.LZF,
              LongEncodingStrategy.LONGS
          ),
          new IndexSpec(),
          1,
          forceGuaranteedRollup,
          true,
          10000L,
          OffHeapMemorySegmentWriteOutMediumFactory.instance(),
          null,
          10,
          100,
          20L,
          new Duration(3600),
          128,
          null,
          null,
          false,
          null,
          null,
          null,
          null,
          null
      );
      final ParallelIndexIngestionSpec indexIngestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              "datasource",
              new TimestampSpec(null, null, null),
              DimensionsSpec.EMPTY,
              null,
              null,
              null
          ),
          ioConfig,
          tuningConfig
      );
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Perfect rollup cannot be guaranteed when appending to existing dataSources");
      new ParallelIndexSupervisorTask(
          null,
          null,
          null,
          indexIngestionSpec,
          null
      );
    }
  }

  public static class StaticUtilsTest
  {
    @Test
    public void testIsParallelModeFalse_nullTuningConfig()
    {
      InputSource inputSource = mock(InputSource.class);
      Assert.assertFalse(ParallelIndexSupervisorTask.isParallelMode(inputSource, null));
    }

    @Test
    public void testIsParallelModeFalse_rangePartition()
    {
      InputSource inputSource = mock(InputSource.class);
      expect(inputSource.isSplittable()).andReturn(true).anyTimes();

      ParallelIndexTuningConfig tuningConfig = mock(ParallelIndexTuningConfig.class);
      expect(tuningConfig.getGivenOrDefaultPartitionsSpec()).andReturn(mock(SingleDimensionPartitionsSpec.class))
                                                            .anyTimes();
      expect(tuningConfig.getMaxNumConcurrentSubTasks()).andReturn(0).andReturn(1).andReturn(2);
      EasyMock.replay(inputSource, tuningConfig);

      Assert.assertFalse(ParallelIndexSupervisorTask.isParallelMode(inputSource, tuningConfig));
      Assert.assertTrue(ParallelIndexSupervisorTask.isParallelMode(inputSource, tuningConfig));
      Assert.assertTrue(ParallelIndexSupervisorTask.isParallelMode(inputSource, tuningConfig));
    }

    @Test
    public void testIsParallelModeFalse_notRangePartition()
    {
      InputSource inputSource = mock(InputSource.class);
      expect(inputSource.isSplittable()).andReturn(true).anyTimes();

      ParallelIndexTuningConfig tuningConfig = mock(ParallelIndexTuningConfig.class);
      expect(tuningConfig.getGivenOrDefaultPartitionsSpec()).andReturn(mock(PartitionsSpec.class))
                                                            .anyTimes();
      expect(tuningConfig.getMaxNumConcurrentSubTasks()).andReturn(1).andReturn(2).andReturn(3);
      EasyMock.replay(inputSource, tuningConfig);

      Assert.assertFalse(ParallelIndexSupervisorTask.isParallelMode(inputSource, tuningConfig));
      Assert.assertTrue(ParallelIndexSupervisorTask.isParallelMode(inputSource, tuningConfig));
      Assert.assertTrue(ParallelIndexSupervisorTask.isParallelMode(inputSource, tuningConfig));
    }

    @Test
    public void testIsParallelModeFalse_inputSourceNotSplittable()
    {
      InputSource inputSource = mock(InputSource.class);
      expect(inputSource.isSplittable()).andReturn(false).anyTimes();

      ParallelIndexTuningConfig tuningConfig = mock(ParallelIndexTuningConfig.class);
      expect(tuningConfig.getGivenOrDefaultPartitionsSpec()).andReturn(mock(SingleDimensionPartitionsSpec.class))
                                                            .anyTimes();
      expect(tuningConfig.getMaxNumConcurrentSubTasks()).andReturn(3);
      EasyMock.replay(inputSource, tuningConfig);

      Assert.assertFalse(ParallelIndexSupervisorTask.isParallelMode(inputSource, tuningConfig));
    }
  }

}
