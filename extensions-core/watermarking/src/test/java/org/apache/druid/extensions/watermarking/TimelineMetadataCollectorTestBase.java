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

package org.apache.druid.extensions.watermarking;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.client.BatchServerInventoryView;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollectorServerView;
import org.apache.druid.extensions.watermarking.storage.WatermarkStore;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCache;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCacheConfig;
import org.apache.druid.extensions.watermarking.storage.memory.MemoryWatermarkStore;
import org.apache.druid.extensions.watermarking.watermarks.BatchCompletenessLowWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.BatchDataHighWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.MaxtimeWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.MintimeWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.StableDataHighWatermarkFactory;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

public class TimelineMetadataCollectorTestBase extends CuratorTestBase
{
  static final int DELAY_MILLIS = 1000;

  static final String serviceName = "watermark-test";
  static final String testDatasource = "test_watermark_collector";

  final ObjectMapper jsonMapper;
  final ZkPathsConfig zkPathsConfig;

  final List<Pair<String, String>> initSegment = ImmutableList.of(
      Pair.of("2011-03-31T00:00:00Z/2011-04-01T00:00:00Z", "v1")
  );

  final List<Pair<String, String>> initBatchSegments = ImmutableList.of(
      Pair.of("2011-04-01T00:00:00Z/2011-04-03T00:00:00Z", "v1"),
      Pair.of("2011-04-03T00:00:00Z/2011-04-06T00:00:00Z", "v1"),
      Pair.of("2011-04-01T00:00:00Z/2011-04-05T00:00:00Z", "v2"),
      Pair.of("2011-04-05T00:00:00Z/2011-04-09T00:00:00Z", "v2"),
      Pair.of("2011-04-07T00:00:00Z/2011-04-09T00:00:00Z", "v3"),
      Pair.of("2011-04-01T00:00:00Z/2011-04-02T00:00:00Z", "v3")
  );

  final List<Pair<String, String>> initBatchSegmentsWithGap = ImmutableList.of(
      Pair.of("2011-04-01T00:00:00Z/2011-04-03T00:00:00Z", "v1"),
      Pair.of("2011-04-03T00:00:00Z/2011-04-06T00:00:00Z", "v1"),
      Pair.of("2011-04-01T00:00:00Z/2011-04-04T00:00:00Z", "v2"),
      Pair.of("2011-04-07T00:00:00Z/2011-04-08T00:00:00Z", "v2"),
      Pair.of("2011-04-07T00:00:00Z/2011-04-09T00:00:00Z", "v3"),
      Pair.of("2011-04-01T00:00:00Z/2011-04-02T00:00:00Z", "v3")
  );

  final List<String> initGaplessRealtimeSegments = ImmutableList.of(
      "2011-04-09T00:00:00Z/2011-04-10T00:00:00Z",
      "2011-04-10T00:00:00Z/2011-04-11T00:00:00Z",
      "2011-04-11T00:00:00Z/2011-04-12T00:00:00Z",
      "2011-04-12T00:00:00Z/2011-04-13T00:00:00Z"
  );


  final List<String> initGapRealtimeSegments = ImmutableList.of(
      "2011-04-09T00:00:00Z/2011-04-10T00:00:00Z",
      "2011-04-11T00:00:00Z/2011-04-12T00:00:00Z",
      "2011-04-12T00:00:00Z/2011-04-13T00:00:00Z"
  );

  final List<String> initStartGapRealtimeSegments = ImmutableList.of(
      "2011-04-11T00:00:00Z/2011-04-12T00:00:00Z",
      "2011-04-12T00:00:00Z/2011-04-13T00:00:00Z"
  );


  CountDownLatch segmentViewInitLatch;
  CountDownLatch segmentAddedLatch;
  CountDownLatch segmentRemovedLatch;
  Map<String, Pair<CountDownLatch, Predicate<DateTime>>> conditionLatches = new HashMap<>();

  BatchServerInventoryView baseView;
  TimelineMetadataCollectorServerView timelineMetadataCollectorServerView;

  ServiceAnnouncer serviceAnnouncer;

  WatermarkCollector watermarkCollector;
  WatermarkStore timelineStore;
  WatermarkCache cache;

  TimelineMetadataCollectorTestBase()
  {
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerModules((Iterable<Module>) new ServerModule().getJacksonModules());
    zkPathsConfig = new ZkPathsConfig();
  }


  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    serviceAnnouncer = EasyMock.createStrictMock(ServiceAnnouncer.class);
    curator.start();
    curator.blockUntilConnected();
  }

  public int getZkPort()
  {
    return this.server.getPort();
  }

  @After
  public void tearDown() throws Exception
  {
    tearDownViews();
    tearDownServerAndCurator();
  }

  void setupStore() throws Exception
  {
    timelineStore = new MemoryWatermarkStore(new ConcurrentHashMap<>(), new WatermarkCollectorConfig())
    {
      @Override
      public void update(String datasource, String type, DateTime timestamp)
      {
        super.update(datasource, type, timestamp);
        if (datasource == testDatasource && conditionLatches.containsKey(type)) {
          Pair<CountDownLatch, Predicate<DateTime>> conditionCountdown = conditionLatches.get(type);
          if (conditionCountdown.rhs.test(timestamp)) {
            conditionCountdown.lhs.countDown();
          }
        }
      }
    };

    timelineStore.initialize();
    cache = new WatermarkCache(timelineStore, new WatermarkCacheConfig());
  }

  void setupViews() throws Exception
  {
    setupViews(true);
  }

  void setupViews(boolean shouldStart) throws Exception
  {
    setupStore();
    WatermarkCollectorConfig moduleConfig = new WatermarkCollectorConfig();

    baseView = new BatchServerInventoryView(
        zkPathsConfig,
        curator,
        jsonMapper,
        Predicates.alwaysTrue()
    );

    timelineMetadataCollectorServerView = new TimelineMetadataCollectorServerView(
        EasyMock.createMock(QueryToolChestWarehouse.class),
        EasyMock.createMock(QueryWatcher.class),
        getSmileMapper(),
        EasyMock.createMock(HttpClient.class),
        baseView,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig()
    )
    {
      @Override
      public void registerTimelineCallback(Executor exec, final TimelineCallback callback)
      {
        super.registerTimelineCallback(
            exec,
            new TimelineCallback()
            {
              @Override
              public CallbackAction timelineInitialized()
              {
                CallbackAction res = callback.timelineInitialized();
                segmentViewInitLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentAdded(server, segment);
                segmentAddedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentRemoved(DataSegment dataSegment)
              {
                CallbackAction res = callback.segmentRemoved(dataSegment);
                segmentRemovedLatch.countDown();
                return res;
              }
            }
        );
      }
    };


    watermarkCollector = new WatermarkCollector(
        moduleConfig,
        serviceAnnouncer,
        new NoopServiceEmitter(),
        timelineMetadataCollectorServerView,
        baseView,
        timelineStore,
        timelineStore,
        ImmutableList.of(
            new BatchCompletenessLowWatermarkFactory(cache, timelineStore),
            new DataCompletenessLowWatermarkFactory(cache, timelineStore),
            new MaxtimeWatermarkFactory(cache, timelineStore),
            new MintimeWatermarkFactory(cache, timelineStore),
            new StableDataHighWatermarkFactory(cache, timelineStore),
            new BatchDataHighWatermarkFactory(cache, timelineStore)
        ),
        new DruidNode(
            serviceName,
            "localhost",
            true,
            8888,
            null,
            true,
            false
        )
    );

    baseView.start();
    watermarkCollector.start();
  }

  void setupMockView()
  {
    timelineMetadataCollectorServerView = new TimelineMetadataCollectorServerView(
        EasyMock.createMock(QueryToolChestWarehouse.class),
        EasyMock.createMock(QueryWatcher.class),
        EasyMock.createMock(ObjectMapper.class),
        EasyMock.createMock(HttpClient.class),
        EasyMock.createMock(BatchServerInventoryView.class),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig()
    )
    {
      @Override
      public boolean isInitialized()
      {
        return true;
      }
    };

  }

  private void tearDownViews() throws Exception
  {
    if (baseView != null) {
      baseView.stop();
    }

    if (watermarkCollector != null) {
      watermarkCollector.stop();
    }
  }


  List<DruidServer> initializeDruidServers(int count, int offset)
  {
    return initializeDruidServers(count, offset, false);
  }

  List<DruidServer> initializeDruidServers(int count, int offset, boolean realtime)
  {
    List<String> seeds = new ArrayList<>();
    for (int i = 0; i < count; ++i) {
      seeds.add("localhost:" + (i + offset));
    }
    final List<DruidServer> druidServers = Lists.transform(
        seeds,
        input -> new DruidServer(
            input,
            input,
            null,
            10000000L,
            realtime ? ServerType.REALTIME : ServerType.HISTORICAL,
            "default_tier",
            0
        )
    );

    for (DruidServer druidServer : druidServers) {
      setupZNodeForServer(druidServer, zkPathsConfig, jsonMapper);
    }

    return druidServers;
  }

  List<Pair<DruidServer, DataSegment>> addSegments(List<Pair<String, String>> seeds)
  {
    return addSegments(seeds, 0);
  }

  List<Pair<DruidServer, DataSegment>> addSegments(List<Pair<String, String>> seeds, int offset)
  {
    final List<DruidServer> druidServers = initializeDruidServers(seeds.size(), offset);

    final List<DataSegment> segments = Lists.transform(
        seeds,
        input -> dataSegmentWithIntervalAndVersion(input.lhs, input.rhs)
    );

    final List<Pair<DruidServer, DataSegment>> pairs = new ArrayList<>();
    for (int i = 0; i < segments.size(); ++i) {
      announceSegmentForServer(druidServers.get(i), segments.get(i), zkPathsConfig, jsonMapper);
      pairs.add(new Pair<>(druidServers.get(i), segments.get(i)));
    }

    return pairs;
  }

  DataSegment dataSegmentWithIntervalAndVersion(String intervalStr, String version)
  {
    return DataSegment.builder()
                      .dataSource(testDatasource)
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version(version)
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }

  List<Pair<DruidServer, DataSegment>> addRealtimeSegments(List<String> seeds, boolean indexing)
  {
    return addRealtimeSegments(seeds, indexing, 0);
  }

  List<Pair<DruidServer, DataSegment>> addRealtimeSegments(List<String> seeds, boolean indexing, int offset)
  {
    final List<DruidServer> druidServers = initializeDruidServers(seeds.size(), offset, indexing);

    final List<DataSegment> segments = Lists.transform(
        seeds,
        input -> realtimeDataSegmentWithIntervalAndVersion(input)
    );

    final List<Pair<DruidServer, DataSegment>> pairs = new ArrayList<>();
    for (int i = 0; i < segments.size(); ++i) {
      announceSegmentForServer(druidServers.get(i), segments.get(i), zkPathsConfig, jsonMapper);
      pairs.add(new Pair<>(druidServers.get(i), segments.get(i)));
    }

    return pairs;
  }

  DataSegment realtimeDataSegmentWithIntervalAndVersion(String intervalStr)
  {
    return DataSegment.builder()
                      .dataSource(testDatasource)
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version("v0")
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(new LinearShardSpec(0))
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }

  ObjectMapper getSmileMapper()
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper retVal = new DefaultObjectMapper(smileFactory);
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }


  Pair<Interval, Pair<String, Pair<DruidServer, DataSegment>>> createExpected(
      String intervalStr,
      String version,
      DruidServer druidServer,
      DataSegment segment
  )
  {
    return Pair.of(Intervals.of(intervalStr), Pair.of(version, Pair.of(druidServer, segment)));
  }

  void assertValues(
      List<Pair<Interval, Pair<String, Pair<DruidServer, DataSegment>>>> expected, List<TimelineObjectHolder> actual
  )
  {
    Assert.assertEquals(expected.size(), actual.size());

    for (int i = 0; i < expected.size(); ++i) {
      Pair<Interval, Pair<String, Pair<DruidServer, DataSegment>>> expectedPair = expected.get(i);
      TimelineObjectHolder<String, ServerSelector> actualTimelineObjectHolder = actual.get(i);

      Assert.assertEquals(expectedPair.lhs, actualTimelineObjectHolder.getInterval());
      Assert.assertEquals(expectedPair.rhs.lhs, actualTimelineObjectHolder.getVersion());

      PartitionHolder<ServerSelector> actualPartitionHolder = actualTimelineObjectHolder.getObject();
      Assert.assertTrue(actualPartitionHolder.isComplete());
      Assert.assertEquals(1, Iterables.size(actualPartitionHolder));

      ServerSelector selector = actualPartitionHolder.iterator()
                                                     .next().getObject();
      Assert.assertFalse(selector.isEmpty());
      Assert.assertEquals(expectedPair.rhs.rhs.lhs, selector.pick().getServer());
      Assert.assertEquals(expectedPair.rhs.rhs.rhs, selector.getSegment());
    }
  }

  void addConditionLatch(String type, DateTime expected)
  {
    Predicate<DateTime> condition = dateTime -> dateTime.equals(expected);
    conditionLatches.put(type, new Pair<>(new CountDownLatch(1), condition));
  }

  CountDownLatch getConditionLatch(String type)
  {
    return conditionLatches.get(type).lhs;
  }
}
