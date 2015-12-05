/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.coordination;

import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.LocalCacheProvider;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.announcement.Announcer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.NoopQueryRunnerFactoryConglomerate;
import io.druid.segment.IndexIO;
import io.druid.segment.loading.CacheTestSegmentLoader;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;

/**
 */
public class ZkCoordinatorTest extends CuratorTestBase
{
  private static final Logger log = new Logger(ZkCoordinatorTest.class);
  public static final int COUNT = 50;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private ZkCoordinator zkCoordinator;
  private ServerManager serverManager;
  private DataSegmentAnnouncer announcer;
  private File infoDir;
  private AtomicInteger announceCount;

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    curator.start();
    try {
      infoDir = new File(File.createTempFile("blah", "blah2").getParent(), "ZkCoordinatorTest");
      infoDir.mkdirs();
      for (File file : infoDir.listFiles()) {
        file.delete();
      }
      log.info("Creating tmp test files in [%s]", infoDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    serverManager = new ServerManager(
        new CacheTestSegmentLoader(),
        new NoopQueryRunnerFactoryConglomerate(),
        new NoopServiceEmitter(),
        MoreExecutors.sameThreadExecutor(),
        MoreExecutors.sameThreadExecutor(),
        new DefaultObjectMapper(),
        new LocalCacheProvider().get(),
        new CacheConfig()
    );

    final DruidServerMetadata me = new DruidServerMetadata("dummyServer", "dummyHost", 0, "dummyType", "normal", 0);

    final ZkPathsConfig zkPaths = new ZkPathsConfig()
    {
      @Override
      public String getBase()
      {
        return "/druid";
      }
    };

    announceCount = new AtomicInteger(0);
    announcer = new DataSegmentAnnouncer()
    {
      private final DataSegmentAnnouncer delegate = new SingleDataSegmentAnnouncer(
          me, zkPaths, new Announcer(curator, Execs.singleThreaded("blah")), jsonMapper
      );

      @Override
      public void announceSegment(DataSegment segment) throws IOException
      {
        announceCount.incrementAndGet();
        delegate.announceSegment(segment);
      }

      @Override
      public void unannounceSegment(DataSegment segment) throws IOException
      {
        announceCount.decrementAndGet();
        delegate.unannounceSegment(segment);
      }

      @Override
      public void announceSegments(Iterable<DataSegment> segments) throws IOException
      {
        announceCount.addAndGet(Iterables.size(segments));
        delegate.announceSegments(segments);
      }

      @Override
      public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
      {
        announceCount.addAndGet(-Iterables.size(segments));
        delegate.unannounceSegments(segments);
      }
    };

    zkCoordinator = new ZkCoordinator(
        jsonMapper,
        new SegmentLoaderConfig()
        {
          @Override
          public File getInfoDir()
          {
            return infoDir;
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
        },
        zkPaths,
        me,
        announcer,
        curator,
        serverManager,
        ScheduledExecutors.createFactory(new Lifecycle())
    );
  }

  @After
  public void tearDown() throws Exception
  {
    tearDownServerAndCurator();
  }

  @Test
  public void testLoadCache() throws Exception
  {
    List<DataSegment> segments = Lists.newLinkedList();
    for(int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", new Interval("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", new Interval("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", new Interval("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "1", new Interval("P1d/2011-04-03")));
      segments.add(makeSegment("test" + i, "1", new Interval("P1d/2011-04-04")));
      segments.add(makeSegment("test" + i, "1", new Interval("P1d/2011-04-05")));
      segments.add(makeSegment("test" + i, "2", new Interval("PT1h/2011-04-04T01")));
      segments.add(makeSegment("test" + i, "2", new Interval("PT1h/2011-04-04T02")));
      segments.add(makeSegment("test" + i, "2", new Interval("PT1h/2011-04-04T03")));
      segments.add(makeSegment("test" + i, "2", new Interval("PT1h/2011-04-04T05")));
      segments.add(makeSegment("test" + i, "2", new Interval("PT1h/2011-04-04T06")));
      segments.add(makeSegment("test_two" + i, "1", new Interval("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", new Interval("P1d/2011-04-02")));
    }
    Collections.sort(segments);

    for (DataSegment segment : segments) {
      writeSegmentToCache(segment);
    }

    checkCache(segments);
    Assert.assertTrue(serverManager.getDataSourceCounts().isEmpty());
    zkCoordinator.start();
    Assert.assertTrue(!serverManager.getDataSourceCounts().isEmpty());
    for(int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(11L, serverManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, serverManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(13 * COUNT, announceCount.get());
    zkCoordinator.stop();

    for (DataSegment segment : segments) {
      deleteSegmentFromCache(segment);
    }

    Assert.assertEquals(0, infoDir.listFiles().length);
    Assert.assertTrue(infoDir.delete());
  }

  private DataSegment makeSegment(String dataSource, String version, Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        ImmutableMap.<String, Object>of("version", version, "interval", interval, "cacheDir", infoDir),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        new NoneShardSpec(),
        IndexIO.CURRENT_VERSION_ID,
        123L
    );
  }

  private void writeSegmentToCache(final DataSegment segment) throws IOException
  {
    if (!infoDir.exists()) {
      infoDir.mkdir();
    }

    File segmentInfoCacheFile = new File(
        infoDir,
        segment.getIdentifier()
    );
    try {
      jsonMapper.writeValue(segmentInfoCacheFile, segment);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    Assert.assertTrue(segmentInfoCacheFile.exists());
  }

  private void deleteSegmentFromCache(final DataSegment segment) throws IOException
  {
    File segmentInfoCacheFile = new File(
        infoDir,
        segment.getIdentifier()
    );
    if (segmentInfoCacheFile.exists()) {
      segmentInfoCacheFile.delete();
    }

    Assert.assertTrue(!segmentInfoCacheFile.exists());
  }

  private void checkCache(List<DataSegment> segments) throws IOException
  {
    Assert.assertTrue(infoDir.exists());
    File[] files = infoDir.listFiles();

    List<File> sortedFiles = Lists.newArrayList(files);
    Collections.sort(sortedFiles);

    Assert.assertEquals(segments.size(), sortedFiles.size());
    for (int i = 0; i < sortedFiles.size(); i++) {
      DataSegment segment = jsonMapper.readValue(sortedFiles.get(i), DataSegment.class);
      Assert.assertEquals(segments.get(i), segment);
    }
  }

  @Test
  public void testInjector() throws Exception
  {
    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(ObjectMapper.class).toInstance(jsonMapper);
            binder.bind(SegmentLoaderConfig.class).toInstance(
                new SegmentLoaderConfig()
                {
                  @Override
                  public File getInfoDir()
                  {
                    return infoDir;
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
                }
            );
            binder.bind(ZkPathsConfig.class).toInstance(
                new ZkPathsConfig()
                {
                  @Override
                  public String getBase()
                  {
                    return "/druid";
                  }
                }
            );
            binder.bind(DruidServerMetadata.class)
                  .toInstance(new DruidServerMetadata("dummyServer", "dummyHost", 0, "dummyType", "normal", 0));
            binder.bind(DataSegmentAnnouncer.class).toInstance(announcer);
            binder.bind(CuratorFramework.class).toInstance(curator);
            binder.bind(ServerManager.class).toInstance(serverManager);
            binder.bind(ScheduledExecutorFactory.class).toInstance(ScheduledExecutors.createFactory(new Lifecycle()));
          }

        }
    );

    ZkCoordinator zkCoordinator = injector.getInstance(ZkCoordinator.class);

    List<DataSegment> segments = Lists.newLinkedList();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", new Interval("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", new Interval("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", new Interval("P1d/2011-04-02")));
      segments.add(makeSegment("test_two" + i, "1", new Interval("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", new Interval("P1d/2011-04-02")));
    }
    Collections.sort(segments);

    for (DataSegment segment : segments) {
      writeSegmentToCache(segment);
    }

    checkCache(segments);
    Assert.assertTrue(serverManager.getDataSourceCounts().isEmpty());

    zkCoordinator.start();
    Assert.assertTrue(!serverManager.getDataSourceCounts().isEmpty());
    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(3L, serverManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, serverManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(5 * COUNT, announceCount.get());
    zkCoordinator.stop();

    for (DataSegment segment : segments) {
      deleteSegmentFromCache(segment);
    }

    Assert.assertEquals(0, infoDir.listFiles().length);
    Assert.assertTrue(infoDir.delete());
  }
}
