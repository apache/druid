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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class TaskDataSegmentProviderTest
{
  private static final String DATASOURCE = "foo";
  private static final int NUM_SEGMENTS = 10;
  private static final int THREADS = 8;
  private static final String LOAD_SPEC_FILE_NAME = "data";

  private List<DataSegment> segments;
  private File cacheDir;
  private SegmentLocalCacheManager cacheManager;
  private TaskDataSegmentProvider provider;
  private ListeningExecutorService exec;
  private IndexIO indexIO = EasyMock.mock(IndexIO.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    EasyMock.reset(indexIO);
    EasyMock.expect(indexIO.loadIndex(EasyMock.anyObject())).andReturn(new TestQueryableIndex()).anyTimes();
    EasyMock.replay(indexIO);

    final ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;
    jsonMapper.registerSubtypes(TestLoadSpec.class);

    segments = new ArrayList<>();

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      // Two segments per interval; helps verify that direction creation + deletion does not include races.
      final DateTime startTime = DateTimes.of("2000").plusDays(i / 2);
      final int partitionNum = i % 2;

      segments.add(
          DataSegment.builder()
                     .dataSource(DATASOURCE)
                     .interval(
                         Intervals.utc(
                             startTime.getMillis(),
                             startTime.plusDays(1).getMillis()
                         )
                     )
                     .version("0")
                     .shardSpec(new NumberedShardSpec(partitionNum, 2))
                     .loadSpec(
                         jsonMapper.convertValue(
                             new TestLoadSpec(i),
                             JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
                         )
                     )
                     .size(1)
                     .build()
      );
    }

    cacheDir = temporaryFolder.newFolder();
    cacheManager = new SegmentLocalCacheManager(
        new SegmentLoaderConfig().withLocations(
            ImmutableList.of(new StorageLocationConfig(cacheDir, 10_000_000_000L, null))
        ),
        jsonMapper
    );

    provider = new TaskDataSegmentProvider(
        new TestCoordinatorClientImpl(),
        cacheManager,
        indexIO
    );

    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(THREADS, getClass().getSimpleName() + "-%s"));
  }

  @After
  public void tearDown() throws Exception
  {
    if (indexIO != null) {
      EasyMock.verify(indexIO);
    }

    if (exec != null) {
      exec.shutdownNow();
      exec.awaitTermination(1, TimeUnit.MINUTES);
    }
  }

  @Test
  public void testConcurrency()
  {
    final int iterations = 1000;
    final List<ListenableFuture<Boolean>> testFutures = new ArrayList<>();

    for (int i = 0; i < iterations; i++) {
      final int expectedSegmentNumber = i % NUM_SEGMENTS;
      final DataSegment segment = segments.get(expectedSegmentNumber);
      final ListenableFuture<Supplier<ResourceHolder<Segment>>> f =
          exec.submit(() -> provider.fetchSegment(segment.getId(), new ChannelCounters()));

      testFutures.add(
          FutureUtils.transform(
              f,
              holderSupplier -> {
                try {
                  final ResourceHolder<Segment> holder = holderSupplier.get();
                  Assert.assertEquals(segment.getId(), holder.get().getId());

                  final String expectedStorageDir = DataSegmentPusher.getDefaultStorageDir(segment, false);
                  final File expectedFile = new File(
                      StringUtils.format(
                          "%s/%s/%s",
                          cacheDir,
                          expectedStorageDir,
                          LOAD_SPEC_FILE_NAME
                      )
                  );

                  Assert.assertTrue(expectedFile.exists());
                  Assert.assertArrayEquals(
                      Ints.toByteArray(expectedSegmentNumber),
                      Files.readAllBytes(expectedFile.toPath())
                  );

                  holder.close();

                  return true;
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
          )
      );
    }

    Assert.assertEquals(iterations, testFutures.size());
    for (int i = 0; i < iterations; i++) {
      ListenableFuture<Boolean> testFuture = testFutures.get(i);
      Assert.assertTrue("Test iteration #" + i, FutureUtils.getUnchecked(testFuture, false));
    }

    // Cache dir should exist, but be empty, since we've closed all holders.
    Assert.assertTrue(cacheDir.exists());
    Assert.assertArrayEquals(new String[]{}, cacheDir.list());
  }

  private class TestCoordinatorClientImpl extends NoopCoordinatorClient
  {
    @Override
    public ListenableFuture<DataSegment> fetchUsedSegment(String dataSource, String segmentId)
    {
      for (final DataSegment segment : segments) {
        if (segment.getDataSource().equals(dataSource) && segment.getId().toString().equals(segmentId)) {
          return Futures.immediateFuture(segment);
        }
      }

      return Futures.immediateFailedFuture(new ISE("No such segment[%s] for dataSource[%s]", segmentId, dataSource));
    }
  }

  @JsonTypeName("test")
  private static class TestLoadSpec implements LoadSpec
  {
    private final int uniqueId;

    @JsonCreator
    public TestLoadSpec(@JsonProperty("uniqueId") int uniqueId)
    {
      this.uniqueId = uniqueId;
    }

    @JsonProperty
    public int getUniqueId()
    {
      return uniqueId;
    }

    @Override
    public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
    {
      try {
        Files.write(new File(destDir, LOAD_SPEC_FILE_NAME).toPath(), Ints.toByteArray(uniqueId));
        Files.write(new File(destDir, "version.bin").toPath(), Ints.toByteArray(1));
        return new LoadSpecResult(1);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to load segment in location [%s]", destDir);
      }
    }
  }

  private static class TestQueryableIndex implements QueryableIndex
  {
    @Override
    public Interval getDataInterval()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumRows()
    {
      return 0;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
      return new ListIndexed<>();
    }

    @Override
    public BitmapFactory getBitmapFactoryForDimensions()
    {
      return RoaringBitmapFactory.INSTANCE;
    }

    @Nullable
    @Override
    public Metadata getMetadata()
    {
      return null;
    }

    @Override
    public Map<String, DimensionHandler> getDimensionHandlers()
    {
      return Collections.emptyMap();
    }

    @Override
    public List<String> getColumnNames()
    {
      return Collections.emptyList();
    }

    @Nullable
    @Override
    public ColumnHolder getColumnHolder(String columnName)
    {
      return null;
    }

    @Override
    public void close()
    {

    }
  }
}
