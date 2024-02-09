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
import com.google.common.util.concurrent.Futures;
import org.apache.commons.codec.Charsets;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.partition.BuildingHashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.DimensionRangeBucketShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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

    private static Map<ParallelIndexSupervisorTask.Partition, List<PartitionLocation>> createPartitionToLocations(
        int count,
        String partitionLocationType
    )
    {
      return IntStream.range(0, count).boxed().collect(
          Collectors.toMap(
              i -> new ParallelIndexSupervisorTask.Partition(createInterval(i), i),
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
          new JsonInputFormat(null, null, null, null, null),
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
          IndexSpec.builder()
                   .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                   .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                   .withMetricCompression(CompressionStrategy.LZF)
                   .withLongEncoding(LongEncodingStrategy.LONGS)
                   .build(),
          IndexSpec.DEFAULT,
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

    @Test
    public void testFailToConstructWhenBothInputSourceAndParserAreSet()
    {
      final ObjectMapper mapper = new DefaultObjectMapper();
      final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          null,
          new InlineInputSource("test"),
          null,
          false,
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
          IndexSpec.builder()
                   .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                   .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                   .withMetricCompression(CompressionStrategy.LZF)
                   .withLongEncoding(LongEncodingStrategy.LONGS)
                   .build(),
          IndexSpec.DEFAULT,
          1,
          true,
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
          null,
          null
      );

      expectedException.expect(IAE.class);
      expectedException.expectMessage("Cannot use parser and inputSource together. Try using inputFormat instead of parser.");
      new ParallelIndexIngestionSpec(
          new DataSchema(
              "datasource",
              mapper.convertValue(
                  new StringInputRowParser(
                      new JSONParseSpec(
                          new TimestampSpec(null, null, null),
                          DimensionsSpec.EMPTY,
                          null,
                          null,
                          null
                      )
                  ),
                  Map.class
              ),
              null,
              null,
              null,
              mapper
          ),
          ioConfig,
          tuningConfig
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

    @Test
    public void test_getPartitionToLocations_ordersPartitionsCorrectly()
    {
      final Interval day1 = Intervals.of("2022-01-01/2022-01-02");
      final Interval day2 = Intervals.of("2022-01-02/2022-01-03");

      final String task1 = "task1";
      final String task2 = "task2";

      // Create task reports
      Map<String, GeneratedPartitionsReport> taskIdToReport = new HashMap<>();
      taskIdToReport.put(task1, new GeneratedPartitionsReport(task1, Arrays.asList(
          createRangePartitionStat(day1, 1),
          createRangePartitionStat(day2, 7),
          createRangePartitionStat(day1, 0),
          createRangePartitionStat(day2, 1)
      ), null));
      taskIdToReport.put(task2, new GeneratedPartitionsReport(task2, Arrays.asList(
          createRangePartitionStat(day1, 4),
          createRangePartitionStat(day1, 6),
          createRangePartitionStat(day2, 1),
          createRangePartitionStat(day1, 1)
      ), null));

      Map<ParallelIndexSupervisorTask.Partition, List<PartitionLocation>> partitionToLocations
          = ParallelIndexSupervisorTask.getPartitionToLocations(taskIdToReport);
      Assert.assertEquals(6, partitionToLocations.size());

      // Verify that partitionIds are packed and in the same order as bucketIds
      verifyPartitionIdAndLocations(day1, 0, partitionToLocations,
                                    0, task1);
      verifyPartitionIdAndLocations(day1, 1, partitionToLocations,
                                    1, task1, task2);
      verifyPartitionIdAndLocations(day1, 4, partitionToLocations,
                                    2, task2);
      verifyPartitionIdAndLocations(day1, 6, partitionToLocations,
                                    3, task2);

      verifyPartitionIdAndLocations(day2, 1, partitionToLocations,
                                    0, task1, task2);
      verifyPartitionIdAndLocations(day2, 7, partitionToLocations,
                                    1, task1);
    }

    @Test
    public void testGetTaskReportOk() throws Exception
    {
      final String taskId = "task";
      final Map<String, Object> report = ImmutableMap.of("foo", "bar");

      final OverlordClient client = mock(OverlordClient.class);
      expect(client.taskReportAsMap(taskId)).andReturn(Futures.immediateFuture(report));
      EasyMock.replay(client);

      Assert.assertEquals(report, ParallelIndexSupervisorTask.getTaskReport(client, taskId));
      EasyMock.verify(client);
    }

    @Test
    public void testGetTaskReport404() throws Exception
    {
      final String taskId = "task";

      final OverlordClient client = mock(OverlordClient.class);
      final HttpResponse response = mock(HttpResponse.class);
      expect(response.getContent()).andReturn(ChannelBuffers.buffer(0));
      expect(response.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).anyTimes();
      EasyMock.replay(response);

      expect(client.taskReportAsMap(taskId)).andReturn(
          Futures.immediateFailedFuture(
              new HttpResponseException(new StringFullResponseHolder(response, Charsets.UTF_8))
          )
      );
      EasyMock.replay(client);

      Assert.assertNull(ParallelIndexSupervisorTask.getTaskReport(client, taskId));
      EasyMock.verify(client, response);
    }

    @Test
    public void testGetTaskReport403()
    {
      final String taskId = "task";

      final OverlordClient client = mock(OverlordClient.class);
      final HttpResponse response = mock(HttpResponse.class);
      expect(response.getContent()).andReturn(ChannelBuffers.buffer(0));
      expect(response.getStatus()).andReturn(HttpResponseStatus.FORBIDDEN).anyTimes();
      EasyMock.replay(response);

      expect(client.taskReportAsMap(taskId)).andReturn(
          Futures.immediateFailedFuture(
              new HttpResponseException(new StringFullResponseHolder(response, Charsets.UTF_8))
          )
      );
      EasyMock.replay(client);

      final ExecutionException e = Assert.assertThrows(
          ExecutionException.class,
          () -> ParallelIndexSupervisorTask.getTaskReport(client, taskId)
      );

      MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(HttpResponseException.class));
      MatcherAssert.assertThat(
          e.getCause(),
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Server error [403 Forbidden]"))
      );

      EasyMock.verify(client, response);
    }

    private PartitionStat createRangePartitionStat(Interval interval, int bucketId)
    {
      return new DeepStoragePartitionStat(
          interval,
          new DimensionRangeBucketShardSpec(bucketId, Arrays.asList("dim1", "dim2"), null, null),
          new HashMap<>()
      );
    }

    private void verifyPartitionIdAndLocations(
        Interval interval,
        int bucketId,
        Map<ParallelIndexSupervisorTask.Partition, List<PartitionLocation>> partitionToLocations,
        int expectedPartitionId,
        String... expectedTaskIds
    )
    {
      final ParallelIndexSupervisorTask.Partition partition
          = new ParallelIndexSupervisorTask.Partition(interval, bucketId);
      List<PartitionLocation> locations = partitionToLocations.get(partition);
      Assert.assertEquals(expectedTaskIds.length, locations.size());

      final Set<String> observedTaskIds = new HashSet<>();
      for (PartitionLocation location : locations) {
        Assert.assertEquals(bucketId, location.getBucketId());
        Assert.assertEquals(interval, location.getInterval());
        Assert.assertEquals(expectedPartitionId, location.getShardSpec().getPartitionNum());

        observedTaskIds.add(location.getSubTaskId());
      }

      // Verify the taskIds of the locations
      Assert.assertEquals(
          new HashSet<>(Arrays.asList(expectedTaskIds)),
          observedTaskIds
      );
    }
  }

}
