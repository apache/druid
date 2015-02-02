/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
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
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
        123l
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
}
