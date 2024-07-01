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
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.loading.TombstoneSegmentizerFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.TestSegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentBootstrapperTest
{
  private static final int COUNT = 50;

  private TestDataSegmentAnnouncer segmentAnnouncer;
  private TestDataServerAnnouncer serverAnnouncer;
  private SegmentLoaderConfig segmentLoaderConfig;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private TestCoordinatorClient coordinatorClient;
  private StubServiceEmitter serviceEmitter;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

      @Override
      public int getDropSegmentDelayMillis()
      {
        return 0;
      }
    };

    scheduledExecutorFactory = (corePoolSize, nameFormat) -> {
      // Override normal behavior by adding the runnable to a list so that you can make sure
      // all the shceduled runnables are executed by explicitly calling run() on each item in the list
      return new ScheduledThreadPoolExecutor(corePoolSize, Execs.makeThreadFactory(nameFormat))
      {
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
          return null;
        }
      };
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
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(TestSegmentUtils.makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(TestSegmentUtils.makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager(segments);
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter
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
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegmentsLoadedIntoPageCache);

    bootstrapper.stop();

    // Assert.assertEquals(0, serverAnnouncer.getObservedCount());
    Assert.assertEquals(1, cacheManager.observedShutdownBootstrapCount.get());
  }

  public void testLoadCache() throws Exception
  {
    Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-03")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-04")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-05")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T01")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T02")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T03")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T05")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T06")));
      segments.add(TestSegmentUtils.makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(TestSegmentUtils.makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager(segments);
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager);
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter
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
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedSegmentsLoadedIntoPageCache);

    bootstrapper.stop();

    Assert.assertEquals(0, serverAnnouncer.getObservedCount());
    Assert.assertEquals(1, cacheManager.observedShutdownBootstrapCount.get());
  }

  @Test
  public void testLoadBootstrapSegments() throws Exception
  {
    final Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(TestSegmentUtils.makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(TestSegmentUtils.makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(TestSegmentUtils.makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }

    final TestCoordinatorClient coordinatorClient = new TestCoordinatorClient(segments);
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);
    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager, coordinatorClient);
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter
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

    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegments);
    Assert.assertEquals(expectedBootstrapSegments, cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);
    serviceEmitter.verifyValue("segment/bootstrap/count", expectedBootstrapSegments.size());
    serviceEmitter.verifyEmitted("segment/bootstrap/time", 1);

    bootstrapper.stop();
  }

  @Test
  public void testLoadBootstrapSegmentsWhenExceptionThrown() throws Exception
  {
    final TestSegmentCacheManager cacheManager = new TestSegmentCacheManager();
    final SegmentManager segmentManager = new SegmentManager(cacheManager);

    final SegmentLoadDropHandler handler = initSegmentLoadDropHandler(segmentManager, new NoopCoordinatorClient());
    final SegmentBootstrapper bootstrapper = new SegmentBootstrapper(
        handler,
        segmentLoaderConfig,
        segmentAnnouncer,
        serverAnnouncer,
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL),
        coordinatorClient,
        serviceEmitter
    );

    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    bootstrapper.start();

    Assert.assertEquals(1, serverAnnouncer.getObservedCount());
    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    Assert.assertEquals(ImmutableList.of(), segmentAnnouncer.getObservedSegments());
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegments);
    Assert.assertEquals(ImmutableList.of(), cacheManager.observedBootstrapSegmentsLoadedIntoPageCache);
    serviceEmitter.verifyValue("segment/bootstrap/count", 0);
    serviceEmitter.verifyEmitted("segment/bootstrap/time", 1);

    bootstrapper.stop();
  }

  private SegmentLoadDropHandler initSegmentLoadDropHandler(
      SegmentManager segmentManager,
      CoordinatorClient coordinatorClient
  )
  {
    return initSegmentLoadDropHandler(segmentLoaderConfig, segmentManager, coordinatorClient);
  }

  private SegmentLoadDropHandler initSegmentLoadDropHandler(SegmentManager segmentManager)
  {
    return initSegmentLoadDropHandler(segmentLoaderConfig, segmentManager, coordinatorClient);
  }

  private SegmentLoadDropHandler initSegmentLoadDropHandler(
      SegmentLoaderConfig config,
      SegmentManager segmentManager,
      CoordinatorClient coordinatorClient
  )
  {
    return new SegmentLoadDropHandler(
        config,
        segmentAnnouncer,
        segmentManager,
        scheduledExecutorFactory.create(5, "SegmentLoadDropHandlerTest-[%d]")
    );
  }

  /**
   * A local cache manager to test the bootstrapping and segment add/remove operations. It stubs only the necessary
   * methods to support these operations; any other method invoked will throw an exception from the base class,
   * {@link NoopSegmentCacheManager}.
   */
  private static class TestSegmentCacheManager extends NoopSegmentCacheManager
  {
    private final List<DataSegment> cachedSegments;

    private final List<DataSegment> observedBootstrapSegments;
    private final List<DataSegment> observedBootstrapSegmentsLoadedIntoPageCache;
    private final List<DataSegment> observedSegments;
    private final List<DataSegment> observedSegmentsLoadedIntoPageCache;
    private final List<DataSegment> observedSegmentsRemovedFromCache;
    private final AtomicInteger observedShutdownBootstrapCount;

    TestSegmentCacheManager()
    {
      this(ImmutableSet.of());
    }

    TestSegmentCacheManager(final Set<DataSegment> segmentsToCache)
    {
      this.cachedSegments = ImmutableList.copyOf(segmentsToCache);
      this.observedBootstrapSegments = new ArrayList<>();
      this.observedBootstrapSegmentsLoadedIntoPageCache = new ArrayList<>();
      this.observedSegments = new ArrayList<>();
      this.observedSegmentsLoadedIntoPageCache = new ArrayList<>();
      this.observedSegmentsRemovedFromCache = new ArrayList<>();
      this.observedShutdownBootstrapCount = new AtomicInteger(0);
    }

    @Override
    public boolean canHandleSegments()
    {
      return true;
    }

    @Override
    public List<DataSegment> getCachedSegments()
    {
      return cachedSegments;
    }

    @Override
    public ReferenceCountingSegment getBootstrapSegment(DataSegment segment, SegmentLazyLoadFailCallback loadFailed)
    {
      observedBootstrapSegments.add(segment);
      return getSegmentInternal(segment);
    }

    @Override
    public ReferenceCountingSegment getSegment(final DataSegment segment)
    {
      observedSegments.add(segment);
      return getSegmentInternal(segment);
    }

    private ReferenceCountingSegment getSegmentInternal(final DataSegment segment)
    {
      if (segment.isTombstone()) {
        return ReferenceCountingSegment
            .wrapSegment(TombstoneSegmentizerFactory.segmentForTombstone(segment), segment.getShardSpec());
      } else {
        return ReferenceCountingSegment.wrapSegment(
            new TestSegmentUtils.SegmentForTesting(
                segment.getDataSource(),
                (Interval) segment.getLoadSpec().get("interval"),
                MapUtils.getString(segment.getLoadSpec(), "version")
            ), segment.getShardSpec()
        );
      }
    }

    @Override
    public void loadSegmentIntoPageCache(DataSegment segment)
    {
      observedSegmentsLoadedIntoPageCache.add(segment);
    }

    @Override
    public void loadSegmentIntoPageCacheOnBootstrap(DataSegment segment)
    {
      observedBootstrapSegmentsLoadedIntoPageCache.add(segment);
    }

    @Override
    public void shutdownBootstrap()
    {
      observedShutdownBootstrapCount.incrementAndGet();
    }

    @Override
    public void storeInfoFile(DataSegment segment)
    {
    }

    @Override
    public void removeInfoFile(DataSegment segment)
    {
    }

    @Override
    public void cleanup(DataSegment segment)
    {
      observedSegmentsRemovedFromCache.add(segment);
    }
  }

}
