/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.coordination;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.DruidServerConfig;
import com.metamx.druid.client.ZKPhoneBook;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.NoopStorageAdapterLoader;
import com.metamx.druid.metrics.NoopServiceEmitter;
import com.metamx.druid.query.NoopQueryRunnerFactoryConglomerate;
import com.metamx.druid.shard.NoneShardSpec;

/**
 */
public class ZkCoordinatorTest
{
  private ZkCoordinator zkCoordinator;
  private ServerManager serverManager;
  private ZKPhoneBook yp;
  private File cacheDir;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private static final Logger log = new Logger(ZkCoordinatorTest.class);

  @Before
  public void setUp() throws Exception
  {
    try {
      cacheDir = new File(File.createTempFile("blah", "blah2").getParent(), "ZkCoordinatorTest");
      cacheDir.mkdirs();
      for (File file : cacheDir.listFiles()) {
        file.delete();
      }
      log.info("Creating tmp test files in [%s]", cacheDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    serverManager = new ServerManager(
        new NoopStorageAdapterLoader(),
        new NoopQueryRunnerFactoryConglomerate(),
        new NoopServiceEmitter(),
        MoreExecutors.sameThreadExecutor()
    );

    yp = EasyMock.createNiceMock(ZKPhoneBook.class);
    EasyMock.replay(yp);

    zkCoordinator = new ZkCoordinator(
        jsonMapper,
        new ZkCoordinatorConfig()
        {
          @Override
          public String getAnnounceLocation()
          {
            return null;
          }

          @Override
          public String getServedSegmentsLocation()
          {
            return null;
          }

          @Override
          public String getLoadQueueLocation()
          {
            return null;
          }

          @Override
          public File getSegmentInfoCacheDirectory()
          {
            return cacheDir;
          }
        },
        new DruidServer(
            new DruidServerConfig()
            {
              @Override
              public String getServerName()
              {
                return "dummyServer";
              }

              @Override
              public String getHost()
              {
                return "dummyHost";
              }

              @Override
              public long getMaxSize()
              {
                return 0;
              }

              @Override
              public String getType()
              {
                return "dummyType";
              }
            }
        ),
        yp,
        serverManager,
        new NoopServiceEmitter()
    );

    EasyMock.reset(yp);
  }

  @Test
  public void testLoadCache() throws Exception
  {
    EasyMock.replay(yp);

    List<DataSegment> segments = Lists.newArrayList(
        makeSegment("test", "1", new Interval("P1d/2011-04-01")),
        makeSegment("test", "1", new Interval("P1d/2011-04-02")),
        makeSegment("test", "2", new Interval("P1d/2011-04-02")),
        makeSegment("test", "1", new Interval("P1d/2011-04-03")),
        makeSegment("test", "1", new Interval("P1d/2011-04-04")),
        makeSegment("test", "1", new Interval("P1d/2011-04-05")),
        makeSegment("test", "2", new Interval("PT1h/2011-04-04T01")),
        makeSegment("test", "2", new Interval("PT1h/2011-04-04T02")),
        makeSegment("test", "2", new Interval("PT1h/2011-04-04T03")),
        makeSegment("test", "2", new Interval("PT1h/2011-04-04T05")),
        makeSegment("test", "2", new Interval("PT1h/2011-04-04T06")),
        makeSegment("test2", "1", new Interval("P1d/2011-04-01")),
        makeSegment("test2", "1", new Interval("P1d/2011-04-02"))
    );
    Collections.sort(segments);

    for (DataSegment segment : segments) {
      writeSegmentToCache(segment);
    }

    checkCache(segments);
    Assert.assertTrue(serverManager.getDataSourceCounts().isEmpty());
    zkCoordinator.start();
    Assert.assertTrue(!serverManager.getDataSourceCounts().isEmpty());
    zkCoordinator.stop();

    for (DataSegment segment : segments) {
      deleteSegmentFromCache(segment);
    }

    Assert.assertEquals(0, cacheDir.listFiles().length);
    Assert.assertTrue(cacheDir.delete());
  }

  private DataSegment makeSegment(String dataSource, String version, Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        ImmutableMap.<String, Object>of("version", version, "interval", interval),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        new NoneShardSpec(),
        123l
    );
  }

  private void writeSegmentToCache(final DataSegment segment) throws IOException
  {
    if (!cacheDir.exists()) {
      cacheDir.mkdir();
    }

    File segmentInfoCacheFile = new File(
        cacheDir,
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
        cacheDir,
        segment.getIdentifier()
    );
    if (segmentInfoCacheFile.exists()) {
      segmentInfoCacheFile.delete();
    }

    Assert.assertTrue(!segmentInfoCacheFile.exists());
  }

  private void checkCache(List<DataSegment> segments) throws IOException
  {
    Assert.assertTrue(cacheDir.exists());
    File[] files = cacheDir.listFiles();

    List<File> sortedFiles = Lists.newArrayList(files);
    Collections.sort(sortedFiles);
    
    Assert.assertEquals(segments.size(), sortedFiles.size());
    for (int i = 0; i < sortedFiles.size(); i++) {
      DataSegment segment = jsonMapper.readValue(sortedFiles.get(i), DataSegment.class);
      Assert.assertEquals(segments.get(i), segment);
    }
  }
}
