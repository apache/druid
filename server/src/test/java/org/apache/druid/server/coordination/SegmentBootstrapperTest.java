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

package org.apache.druid.server.coordination;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.druid.server.TestSegmentUtils.makeSegment;

public class SegmentBootstrapperTest
{
  private static final int COUNT = 50;

  private TestDataSegmentAnnouncer segmentAnnouncer;
  private TestDataServerAnnouncer serverAnnouncer;
  private SegmentLoaderConfig segmentLoaderConfig;
  private TestCoordinatorClient coordinatorClient;
  private StubServiceEmitter serviceEmitter;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    final File segmentCacheDir = temporaryFolder.newFolder();

    segmentAnnouncer = new TestDataSegmentAnnouncer();
    serverAnnouncer = new TestDataServerAnnouncer();
    segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public File getInfoDir()
      {
        return segmentCacheDir;
      }

      @Override
      public int getNumLoadingThreads()
      {
        return 5;
      }

      @Override
      public int getAnnounceIntervalMillis()
      {
        return 50;
      }

      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Collections.singletonList(
            new StorageLocationConfig(segmentCacheDir, null, null)
        );
      }
    };

    coordinatorClient = new TestCoordinatorClient();
    serviceEmitter = new StubServiceEmitter();
    EmittingLogger.registerEmitter(serviceEmitter);
  }


  @Test
  public void testStartStop() throws Exception
  {
    final Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager(segments);
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = new SegmentLoadDropHandler(
        segmentLoaderConfig,
        segmentAnnouncer,
        segmentManager
    );
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter,
        new DataSourceTaskIdHolder()
    );

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());
    bootstrapper.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());

    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(3L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }

    Assert.assertEquals(ImmutableList.copyOf(segments), segmentAnnouncer.getObservedSegments());

    final ImmutableList<DataSegment> expectedBootstrapSegments = ImmutableList.copyOf(segments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedSegmentsLoadedIntoPageCache());

    bootstrapper.stop();

    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
    Assert.assertEquals(1, cacheManager.getObservedShutdownBootstrapCount().get());
  }

  @Test
  public void testLoadCachedSegments() throws Exception
  {
    final Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-03")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-04")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-05")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T01")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T03")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T05")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T06")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager(segments);
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = new SegmentLoadDropHandler(segmentLoaderConfig, segmentAnnouncer, segmentManager);
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter,
        new DataSourceTaskIdHolder()
    );

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    bootstrapper.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());

    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(11L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }

    Assert.assertEquals(ImmutableList.copyOf(segments), segmentAnnouncer.getObservedSegments());

    final ImmutableList<DataSegment> expectedBootstrapSegments = ImmutableList.copyOf(segments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedSegmentsLoadedIntoPageCache());

    bootstrapper.stop();

    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
    Assert.assertEquals(1, cacheManager.getObservedShutdownBootstrapCount().get());
  }

  @Test
  public void testLoadBootstrapSegments() throws Exception
  {
    final Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestCoordinatorClient coordinatorClient = new TestCoordinatorClient(segments);
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = new SegmentLoadDropHandler(
        segmentLoaderConfig,
        segmentAnnouncer,
        segmentManager
    );
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter,
        new DataSourceTaskIdHolder()
    );

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    bootstrapper.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());

    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }

    final ImmutableList<DataSegment> expectedBootstrapSegments = ImmutableList.copyOf(segments);

    Assert.assertEquals(expectedBootstrapSegments, segmentAnnouncer.getObservedSegments());

    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());
    serviceEmitter.verifyValue("segment/bootstrap/count", expectedBootstrapSegments.size());
    serviceEmitter.verifyEmitted("segment/bootstrap/time", 1);

    bootstrapper.stop();
  }

  @Test
  public void testLoadNoBootstrapSegments() throws Exception
  {
    final Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    Injector injector = Guice.createInjector(
        new JacksonModule(),
        new LifecycleModule(),
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          final BroadcastDatasourceLoadingSpec broadcastMode = BroadcastDatasourceLoadingSpec.NONE;
          binder.bind(Key.get(BroadcastDatasourceLoadingSpec.class, Names.named(DataSourceTaskIdHolder.BROADCAST_DATASOURCES_TO_LOAD_FOR_TASK)))
                .toInstance(broadcastMode);
        }
    );

    final TestCoordinatorClient coordinatorClient = new TestCoordinatorClient(segments);
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = new SegmentLoadDropHandler(
        segmentLoaderConfig,
        segmentAnnouncer,
        segmentManager
    );
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter,
        injector.getInstance(DataSourceTaskIdHolder.class)
    );

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    bootstrapper.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    final ImmutableList<DataSegment> expectedBootstrapSegments = ImmutableList.of();

    Assert.assertEquals(expectedBootstrapSegments, segmentAnnouncer.getObservedSegments());

    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());

    bootstrapper.stop();
  }

  @Test
  public void testLoadOnlyRequiredBootstrapSegments() throws Exception
  {
    final Set<DataSegment> segments = new HashSet<>();
    final DataSegment ds1Segment1 = makeSegment("test1", "1", Intervals.of("P1D/2011-04-01"));
    final DataSegment ds1Segment2 = makeSegment("test1", "1", Intervals.of("P1D/2012-04-01"));
    final DataSegment ds2Segment1 = makeSegment("test2", "1", Intervals.of("P1d/2011-04-01"));
    final DataSegment ds2Segment2 = makeSegment("test2", "1", Intervals.of("P1d/2012-04-01"));
    segments.add(ds1Segment1);
    segments.add(ds1Segment2);
    segments.add(ds2Segment1);
    segments.add(ds2Segment2);

    Injector injector = Guice.createInjector(
        new JacksonModule(),
        new LifecycleModule(),
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          final BroadcastDatasourceLoadingSpec broadcastMode = BroadcastDatasourceLoadingSpec.loadOnly(ImmutableSet.of("test1"));
          binder.bind(Key.get(BroadcastDatasourceLoadingSpec.class, Names.named(DataSourceTaskIdHolder.BROADCAST_DATASOURCES_TO_LOAD_FOR_TASK)))
                .toInstance(broadcastMode);
        }
    );

    final TestCoordinatorClient coordinatorClient = new TestCoordinatorClient(segments);
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = new SegmentLoadDropHandler(
        segmentLoaderConfig,
        segmentAnnouncer,
        segmentManager
    );
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter,
        injector.getInstance(DataSourceTaskIdHolder.class)
    );

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    bootstrapper.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertFalse(segmentManager.getDataSourceCounts().isEmpty());
    Assert.assertEquals(ImmutableSet.of("test1"), segmentManager.getDataSourceNames());

    final ImmutableList<DataSegment> expectedBootstrapSegments = ImmutableList.of(ds1Segment2, ds1Segment1);

    Assert.assertEquals(expectedBootstrapSegments, segmentAnnouncer.getObservedSegments());

    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());
    serviceEmitter.verifyValue("segment/bootstrap/count", expectedBootstrapSegments.size());
    serviceEmitter.verifyEmitted("segment/bootstrap/time", 1);

    bootstrapper.stop();
  }

  @Test
  public void testLoadBootstrapSegmentsWhenExceptionThrown() throws Exception
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = new SegmentLoadDropHandler(
        segmentLoaderConfig,
        segmentAnnouncer,
        segmentManager
    );
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter,
        new DataSourceTaskIdHolder()
    );

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    bootstrapper.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    Assert.assertEquals(ImmutableList.of(), segmentAnnouncer.getObservedSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.getObservedBootstrapSegmentsLoadedIntoPageCache());
    serviceEmitter.verifyValue("segment/bootstrap/count", 0);
    serviceEmitter.verifyEmitted("segment/bootstrap/time", 1);

    bootstrapper.stop();
  }
}
