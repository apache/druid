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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingWorker;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.druid.utils.Streams;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class CompactSegmentsTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final String DATA_SOURCE_PREFIX = "dataSource_";
  // Each dataSource starts with 440 byte, 44 segments, and 11 intervals needing compaction
  int TOTAL_BYTE_PER_DATASOURCE = 440;
  int TOTAL_SEGMENT_PER_DATASOURCE = 44;
  int TOTAL_INTERVAL_PER_DATASOURCE = 11;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    final MutableInt nextRangePartitionBoundary = new MutableInt(0);
    return ImmutableList.of(
        new Object[]{
            new DynamicPartitionsSpec(300000, null),
            (BiFunction<Integer, Integer, ShardSpec>) NumberedShardSpec::new
        },
        new Object[]{
            new HashedPartitionsSpec(null, 2, ImmutableList.of("dim")),
            (BiFunction<Integer, Integer, ShardSpec>) (bucketId, numBuckets) -> new HashBasedNumberedShardSpec(
                bucketId,
                numBuckets,
                bucketId,
                numBuckets,
                ImmutableList.of("dim"),
                JSON_MAPPER
            )
        },
        new Object[]{
            new SingleDimensionPartitionsSpec(300000, null, "dim", false),
            (BiFunction<Integer, Integer, ShardSpec>) (bucketId, numBuckets) -> new SingleDimensionShardSpec(
                "dim",
                bucketId == 0 ? null : String.valueOf(nextRangePartitionBoundary.getAndIncrement()),
                bucketId.equals(numBuckets) ? null : String.valueOf(nextRangePartitionBoundary.getAndIncrement()),
                bucketId,
                numBuckets
            )
        }
    );
  }

  private final PartitionsSpec partitionsSpec;
  private final BiFunction<Integer, Integer, ShardSpec> shardSpecFactory;

  private Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;

  public CompactSegmentsTest(PartitionsSpec partitionsSpec, BiFunction<Integer, Integer, ShardSpec> shardSpecFactory)
  {
    this.partitionsSpec = partitionsSpec;
    this.shardSpecFactory = shardSpecFactory;
  }

  @Before
  public void setup()
  {
    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      for (int j : new int[]{0, 1, 2, 3, 7, 8}) {
        for (int k = 0; k < 4; k++) {
          segments.add(createSegment(dataSource, j, true, k));
          segments.add(createSegment(dataSource, j, false, k));
        }
      }
    }
    dataSources = DataSourcesSnapshot
        .fromUsedSegments(segments, ImmutableMap.of())
        .getUsedSegmentsTimelinesPerDataSource();
  }

  private DataSegment createSegment(String dataSource, int startDay, boolean beforeNoon, int partition)
  {
    final ShardSpec shardSpec = shardSpecFactory.apply(partition, 2);
    final Interval interval = beforeNoon ?
                              Intervals.of(
                                  StringUtils.format(
                                      "2017-01-%02dT00:00:00/2017-01-%02dT12:00:00",
                                      startDay + 1,
                                      startDay + 1
                                  )
                              ) :
                              Intervals.of(
                                  StringUtils.format(
                                      "2017-01-%02dT12:00:00/2017-01-%02dT00:00:00",
                                      startDay + 1,
                                      startDay + 2
                                  )
                              );
    return new DataSegment(
        dataSource,
        interval,
        "version",
        null,
        ImmutableList.of(),
        ImmutableList.of(),
        shardSpec,
        0,
        10L
    );
  }

  @Test
  public void testRun()
  {
    final TestDruidLeaderClient leaderClient = new TestDruidLeaderClient(JSON_MAPPER);
    leaderClient.start();
    final HttpIndexingServiceClient indexingServiceClient = new HttpIndexingServiceClient(JSON_MAPPER, leaderClient);
    final CompactSegments compactSegments = new CompactSegments(JSON_MAPPER, indexingServiceClient);

    final Supplier<String> expectedVersionSupplier = new Supplier<String>()
    {
      private int i = 0;

      @Override
      public String get()
      {
        return "newVersion_" + i++;
      }
    };
    int expectedCompactTaskCount = 1;
    int expectedRemainingSegments = 400;

    // compact for 2017-01-08T12:00:00.000Z/2017-01-09T12:00:00.000Z
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", 9, 9),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", 8, 9),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );

    // compact for 2017-01-07T12:00:00.000Z/2017-01-08T12:00:00.000Z
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", 8, 8),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );
    expectedRemainingSegments -= 40;
    assertCompactSegments(
        compactSegments,
        Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", 4, 5),
        expectedRemainingSegments,
        expectedCompactTaskCount,
        expectedVersionSupplier
    );

    for (int endDay = 4; endDay > 1; endDay -= 1) {
      expectedRemainingSegments -= 40;
      assertCompactSegments(
          compactSegments,
          Intervals.of("2017-01-%02dT00:00:00/2017-01-%02dT12:00:00", endDay, endDay),
          expectedRemainingSegments,
          expectedCompactTaskCount,
          expectedVersionSupplier
      );
      expectedRemainingSegments -= 40;
      assertCompactSegments(
          compactSegments,
          Intervals.of("2017-01-%02dT12:00:00/2017-01-%02dT00:00:00", endDay - 1, endDay),
          expectedRemainingSegments,
          expectedCompactTaskCount,
          expectedVersionSupplier
      );
    }

    assertLastSegmentNotCompacted(compactSegments);
  }

  @Test
  public void testMakeStats()
  {
    final TestDruidLeaderClient leaderClient = new TestDruidLeaderClient(JSON_MAPPER);
    leaderClient.start();
    final HttpIndexingServiceClient indexingServiceClient = new HttpIndexingServiceClient(JSON_MAPPER, leaderClient);
    final CompactSegments compactSegments = new CompactSegments(JSON_MAPPER, indexingServiceClient);

    // Before any compaction, we do not have any snapshot of compactions
    Map<String, AutoCompactionSnapshot> autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
    Assert.assertEquals(0, autoCompactionSnapshots.size());
    for (int compaction_run_count = 0; compaction_run_count < 11; compaction_run_count++) {
      assertCompactSegmentStatistics(compactSegments, compaction_run_count);
    }
    // Test that stats does not change (and is still correct) when auto compaction runs everything is fully compacted
    final CoordinatorStats stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        0,
        stats.getGlobalStat(CompactSegments.COMPACTION_TASK_COUNT)
    );
    autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
    for (int i = 0; i < 3; i++) {
      AutoCompactionSnapshot snapshot = autoCompactionSnapshots.get(DATA_SOURCE_PREFIX + i);
      Assert.assertEquals(DATA_SOURCE_PREFIX + i, snapshot.getDataSource());
      Assert.assertEquals(AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING, snapshot.getScheduleStatus());
      Assert.assertEquals(0, snapshot.getByteAwaitingCompaction());
      Assert.assertEquals(TOTAL_BYTE_PER_DATASOURCE, snapshot.getByteProcessed());
      Assert.assertEquals(0, snapshot.getIntervalCountAwaitingCompaction());
      Assert.assertEquals(TOTAL_INTERVAL_PER_DATASOURCE, snapshot.getIntervalCountProcessed());
      Assert.assertEquals(0, snapshot.getSegmentCountAwaitingCompaction());
      Assert.assertEquals(TOTAL_SEGMENT_PER_DATASOURCE / 2, snapshot.getSegmentCountProcessed());
    }

    assertLastSegmentNotCompacted(compactSegments);
  }

  private void assertCompactSegmentStatistics(CompactSegments compactSegments, int compaction_run_count)
  {
    for (int dataSourceIndex = 0; dataSourceIndex < 3; dataSourceIndex++) {
      // One compaction task triggered
      final CoordinatorStats stats = doCompactSegments(compactSegments);
      Assert.assertEquals(
          1,
          stats.getGlobalStat(CompactSegments.COMPACTION_TASK_COUNT)
      );
      Map<String, AutoCompactionSnapshot> autoCompactionSnapshots = compactSegments.getAutoCompactionSnapshot();
      // Note: Subsequent compaction run after the dataSource was compacted will show different numbers than
      // on the run it was compacted. For example, in a compaction run, if a dataSource had 4 segments compacted,
      // on the same compaction run the segment processed count will be 4 but on subsequent run it might be 2
      // (assuming the 4 segments was compacted into 2 segments).
      for (int i = 0; i <= dataSourceIndex; i++) {
        // dataSource up to dataSourceIndex now compacted. Check that the stats match the expectedAfterCompaction values
        // This verify that dataSource which got slot to compact has correct statistics
        if (i != dataSourceIndex) {
          AutoCompactionSnapshot snapshot = autoCompactionSnapshots.get(DATA_SOURCE_PREFIX + i);
          Assert.assertEquals(DATA_SOURCE_PREFIX + i, snapshot.getDataSource());
          Assert.assertEquals(AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING, snapshot.getScheduleStatus());
          Assert.assertEquals(TOTAL_BYTE_PER_DATASOURCE - 40 * (compaction_run_count + 1), snapshot.getByteAwaitingCompaction());
          Assert.assertEquals(40 * (compaction_run_count + 1), snapshot.getByteProcessed());
          Assert.assertEquals(TOTAL_INTERVAL_PER_DATASOURCE - (compaction_run_count + 1), snapshot.getIntervalCountAwaitingCompaction());
          Assert.assertEquals((compaction_run_count + 1), snapshot.getIntervalCountProcessed());
          Assert.assertEquals(TOTAL_SEGMENT_PER_DATASOURCE - 4 * (compaction_run_count + 1), snapshot.getSegmentCountAwaitingCompaction());
          Assert.assertEquals(2 * (compaction_run_count + 1), snapshot.getSegmentCountProcessed());
        } else {
          AutoCompactionSnapshot snapshot = autoCompactionSnapshots.get(DATA_SOURCE_PREFIX + i);
          Assert.assertEquals(DATA_SOURCE_PREFIX + i, snapshot.getDataSource());
          Assert.assertEquals(AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING, snapshot.getScheduleStatus());
          Assert.assertEquals(TOTAL_BYTE_PER_DATASOURCE - 40 * (compaction_run_count + 1), snapshot.getByteAwaitingCompaction());
          Assert.assertEquals(40 * (compaction_run_count + 1), snapshot.getByteProcessed());
          Assert.assertEquals(TOTAL_INTERVAL_PER_DATASOURCE - (compaction_run_count + 1), snapshot.getIntervalCountAwaitingCompaction());
          Assert.assertEquals((compaction_run_count + 1), snapshot.getIntervalCountProcessed());
          Assert.assertEquals(TOTAL_SEGMENT_PER_DATASOURCE - 4 * (compaction_run_count + 1), snapshot.getSegmentCountAwaitingCompaction());
          Assert.assertEquals(2 * compaction_run_count + 4, snapshot.getSegmentCountProcessed());
        }
      }
      for (int i = dataSourceIndex + 1; i < 3; i++) {
        // dataSource after dataSourceIndex is not yet compacted. Check that the stats match the expectedBeforeCompaction values
        // This verify that dataSource that ran out of slot has correct statistics
        AutoCompactionSnapshot snapshot = autoCompactionSnapshots.get(DATA_SOURCE_PREFIX + i);
        Assert.assertEquals(DATA_SOURCE_PREFIX + i, snapshot.getDataSource());
        Assert.assertEquals(AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING, snapshot.getScheduleStatus());
        Assert.assertEquals(TOTAL_BYTE_PER_DATASOURCE - 40 * compaction_run_count, snapshot.getByteAwaitingCompaction());
        Assert.assertEquals(40 * compaction_run_count, snapshot.getByteProcessed());
        Assert.assertEquals(TOTAL_INTERVAL_PER_DATASOURCE - compaction_run_count, snapshot.getIntervalCountAwaitingCompaction());
        Assert.assertEquals(compaction_run_count, snapshot.getIntervalCountProcessed());
        Assert.assertEquals(TOTAL_SEGMENT_PER_DATASOURCE - 4 * compaction_run_count, snapshot.getSegmentCountAwaitingCompaction());
        Assert.assertEquals(2 * compaction_run_count, snapshot.getSegmentCountProcessed());
      }
    }
  }

  private CoordinatorStats doCompactSegments(CompactSegments compactSegments)
  {
    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withUsedSegmentsTimelinesPerDataSourceInTest(dataSources)
        .withCompactionConfig(CoordinatorCompactionConfig.from(createCompactionConfigs()))
        .build();
    return compactSegments.run(params).getCoordinatorStats();
  }

  private void assertCompactSegments(
      CompactSegments compactSegments,
      Interval expectedInterval,
      int expectedRemainingSegments,
      int expectedCompactTaskCount,
      Supplier<String> expectedVersionSupplier
  )
  {
    for (int i = 0; i < 3; i++) {
      final CoordinatorStats stats = doCompactSegments(compactSegments);
      Assert.assertEquals(
          expectedCompactTaskCount,
          stats.getGlobalStat(CompactSegments.COMPACTION_TASK_COUNT)
      );

      // One of dataSource is compacted
      if (expectedRemainingSegments > 0) {
        // If expectedRemainingSegments is positive, we check how many dataSources have the segments waiting for
        // compaction.
        long numDataSourceOfExpectedRemainingSegments = stats
            .getDataSources(CompactSegments.TOTAL_SIZE_OF_SEGMENTS_AWAITING_COMPACTION)
            .stream()
            .mapToLong(ds -> stats.getDataSourceStat(CompactSegments.TOTAL_SIZE_OF_SEGMENTS_AWAITING_COMPACTION, ds))
            .filter(stat -> stat == expectedRemainingSegments)
            .count();
        Assert.assertEquals(i + 1, numDataSourceOfExpectedRemainingSegments);
      } else {
        // Otherwise, we check how many dataSources are in the coordinator stats.
        Assert.assertEquals(
            2 - i,
            stats.getDataSources(CompactSegments.TOTAL_SIZE_OF_SEGMENTS_AWAITING_COMPACTION).size()
        );
      }
    }

    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      List<TimelineObjectHolder<String, DataSegment>> holders = dataSources.get(dataSource).lookup(expectedInterval);
      Assert.assertEquals(1, holders.size());
      List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holders.get(0).getObject());
      Assert.assertEquals(2, chunks.size());
      final String expectedVersion = expectedVersionSupplier.get();
      for (PartitionChunk<DataSegment> chunk : chunks) {
        Assert.assertEquals(expectedInterval, chunk.getObject().getInterval());
        Assert.assertEquals(expectedVersion, chunk.getObject().getVersion());
      }
    }
  }

  private void assertLastSegmentNotCompacted(CompactSegments compactSegments)
  {
    // Segments of the latest interval should not be compacted
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      final Interval interval = Intervals.of(StringUtils.format("2017-01-09T12:00:00/2017-01-10"));
      List<TimelineObjectHolder<String, DataSegment>> holders = dataSources.get(dataSource).lookup(interval);
      Assert.assertEquals(1, holders.size());
      for (TimelineObjectHolder<String, DataSegment> holder : holders) {
        List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holder.getObject());
        Assert.assertEquals(4, chunks.size());
        for (PartitionChunk<DataSegment> chunk : chunks) {
          DataSegment segment = chunk.getObject();
          Assert.assertEquals(interval, segment.getInterval());
          Assert.assertEquals("version", segment.getVersion());
        }
      }
    }

    // Emulating realtime dataSource
    final String dataSource = DATA_SOURCE_PREFIX + 0;
    addMoreData(dataSource, 9);

    CoordinatorStats stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        1,
        stats.getGlobalStat(CompactSegments.COMPACTION_TASK_COUNT)
    );

    addMoreData(dataSource, 10);

    stats = doCompactSegments(compactSegments);
    Assert.assertEquals(
        1,
        stats.getGlobalStat(CompactSegments.COMPACTION_TASK_COUNT)
    );
  }

  private void addMoreData(String dataSource, int day)
  {
    for (int i = 0; i < 2; i++) {
      DataSegment newSegment = createSegment(dataSource, day, true, i);
      dataSources.get(dataSource).add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
      newSegment = createSegment(dataSource, day, false, i);
      dataSources.get(dataSource).add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
    }
  }

  private List<DataSourceCompactionConfig> createCompactionConfigs()
  {
    final List<DataSourceCompactionConfig> compactionConfigs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final String dataSource = DATA_SOURCE_PREFIX + i;
      compactionConfigs.add(
          new DataSourceCompactionConfig(
              dataSource,
              0,
              50L,
              null,
              new Period("PT1H"), // smaller than segment interval
              new UserCompactionTaskQueryTuningConfig(
                  null,
                  null,
                  null,
                  null,
                  partitionsSpec,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null
              ),
              null
          )
      );
    }
    return compactionConfigs;
  }

  private class TestDruidLeaderClient extends DruidLeaderClient
  {
    private final ObjectMapper jsonMapper;

    private int compactVersionSuffix = 0;

    private TestDruidLeaderClient(ObjectMapper jsonMapper)
    {
      super(null, new TestNodeDiscoveryProvider(), null, null, null);
      this.jsonMapper = jsonMapper;
    }

    @Override
    public Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException
    {
      return new Request(httpMethod, new URL("http", "host", 8090, urlPath));
    }

    @Override
    public StringFullResponseHolder go(Request request) throws IOException
    {
      final String urlString = request.getUrl().toString();
      if (urlString.contains("/druid/indexer/v1/task")) {
        return handleTask(request);
      } else if (urlString.contains("/druid/indexer/v1/workers")) {
        return handleWorkers();
      } else if (urlString.contains("/druid/indexer/v1/waitingTasks")
                 || urlString.contains("/druid/indexer/v1/pendingTasks")
                 || urlString.contains("/druid/indexer/v1/runningTasks")) {
        return createStringFullResponseHolder(jsonMapper.writeValueAsString(Collections.emptyList()));
      } else {
        throw new IAE("Cannot handle request for url[%s]", request.getUrl());
      }
    }

    private StringFullResponseHolder createStringFullResponseHolder(String content)
    {
      final HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      final StringFullResponseHolder holder = new StringFullResponseHolder(
          HttpResponseStatus.OK,
          httpResponse,
          StandardCharsets.UTF_8
      );
      holder.addChunk(content);
      return holder;
    }

    private StringFullResponseHolder handleWorkers() throws JsonProcessingException
    {
      final List<IndexingWorkerInfo> workerInfos = new ArrayList<>();
      // There are 10 workers available in this test
      for (int i = 0; i < 10; i++) {
        workerInfos.add(
            new IndexingWorkerInfo(
                new IndexingWorker("http", "host", "8091", 1, "version"),
                0,
                Collections.emptySet(),
                Collections.emptyList(),
                DateTimes.EPOCH,
                null
            )
        );
      }
      return createStringFullResponseHolder(jsonMapper.writeValueAsString(workerInfos));
    }

    private StringFullResponseHolder handleTask(Request request) throws IOException
    {
      final ClientTaskQuery taskQuery = jsonMapper.readValue(request.getContent().array(), ClientTaskQuery.class);
      if (!(taskQuery instanceof ClientCompactionTaskQuery)) {
        throw new IAE("Cannot run non-compaction task");
      }
      final ClientCompactionTaskQuery compactionTaskQuery = (ClientCompactionTaskQuery) taskQuery;
      final Interval intervalToCompact = compactionTaskQuery.getIoConfig().getInputSpec().getInterval();
      final VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(
          compactionTaskQuery.getDataSource()
      );
      final List<DataSegment> segments = timeline.lookup(intervalToCompact)
                                                 .stream()
                                                 .flatMap(holder -> Streams.sequentialStreamFrom(holder.getObject()))
                                                 .map(PartitionChunk::getObject)
                                                 .collect(Collectors.toList());
      compactSegments(
          timeline,
          segments,
          compactionTaskQuery.getTuningConfig()
      );
      return createStringFullResponseHolder(jsonMapper.writeValueAsString(ImmutableMap.of("task", taskQuery.getId())));
    }

    private void compactSegments(
        VersionedIntervalTimeline<String, DataSegment> timeline,
        List<DataSegment> segments,
        ClientCompactionTaskQueryTuningConfig tuningConfig
    )
    {
      Preconditions.checkArgument(segments.size() > 1);
      DateTime minStart = DateTimes.MAX, maxEnd = DateTimes.MIN;
      for (DataSegment segment : segments) {
        if (segment.getInterval().getStart().compareTo(minStart) < 0) {
          minStart = segment.getInterval().getStart();
        }
        if (segment.getInterval().getEnd().compareTo(maxEnd) > 0) {
          maxEnd = segment.getInterval().getEnd();
        }
      }
      Interval compactInterval = new Interval(minStart, maxEnd);
      segments.forEach(
          segment -> timeline.remove(
              segment.getInterval(),
              segment.getVersion(),
              segment.getShardSpec().createChunk(segment)
          )
      );
      final String version = "newVersion_" + compactVersionSuffix++;
      final long segmentSize = segments.stream().mapToLong(DataSegment::getSize).sum() / 2;
      final PartitionsSpec compactionPartitionsSpec;
      if (tuningConfig.getPartitionsSpec() instanceof DynamicPartitionsSpec) {
        compactionPartitionsSpec = new DynamicPartitionsSpec(
            tuningConfig.getPartitionsSpec().getMaxRowsPerSegment(),
            ((DynamicPartitionsSpec) tuningConfig.getPartitionsSpec()).getMaxTotalRowsOr(Long.MAX_VALUE)
        );
      } else {
        compactionPartitionsSpec = tuningConfig.getPartitionsSpec();
      }

      for (int i = 0; i < 2; i++) {
        DataSegment compactSegment = new DataSegment(
            segments.get(0).getDataSource(),
            compactInterval,
            version,
            null,
            segments.get(0).getDimensions(),
            segments.get(0).getMetrics(),
            shardSpecFactory.apply(i, 2),
            new CompactionState(
                compactionPartitionsSpec,
                ImmutableMap.of(
                    "bitmap",
                    ImmutableMap.of("type", "roaring", "compressRunOnSerialization", true),
                    "dimensionCompression",
                    "lz4",
                    "metricCompression",
                    "lz4",
                    "longEncoding",
                    "longs"
                )
            ),
            1,
            segmentSize
        );

        timeline.add(
            compactInterval,
            compactSegment.getVersion(),
            compactSegment.getShardSpec().createChunk(compactSegment)
        );
      }
    }
  }

  private static class TestNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
  {
    @Override
    public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
    {
      return EasyMock.niceMock(DruidNodeDiscovery.class);
    }
  }
}
