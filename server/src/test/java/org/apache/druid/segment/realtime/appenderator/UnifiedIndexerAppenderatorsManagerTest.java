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

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.JoinableFactoryWrapperTest;
import org.apache.druid.segment.loading.NoopDataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class UnifiedIndexerAppenderatorsManagerTest extends InitializedNullHandlingTest
{
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final UnifiedIndexerAppenderatorsManager manager = new UnifiedIndexerAppenderatorsManager(
      DirectQueryProcessingPool.INSTANCE,
      JoinableFactoryWrapperTest.NOOP_JOINABLE_FACTORY_WRAPPER,
      new WorkerConfig(),
      MapCache.create(10),
      new CacheConfig(),
      new CachePopulatorStats(),
      TestHelper.makeJsonMapper(),
      new NoopServiceEmitter(),
      () -> new DefaultQueryRunnerFactoryConglomerate(ImmutableMap.of())
  );

  private AppenderatorConfig appenderatorConfig;
  private Appenderator appenderator;

  @Before
  public void setup()
  {
    appenderatorConfig = EasyMock.createMock(AppenderatorConfig.class);
    EasyMock.expect(appenderatorConfig.getMaxPendingPersists()).andReturn(0);
    EasyMock.expect(appenderatorConfig.isSkipBytesInMemoryOverheadCheck()).andReturn(false);
    EasyMock.replay(appenderatorConfig);
    appenderator = manager.createClosedSegmentsOfflineAppenderatorForTask(
        "taskId",
        new DataSchema(
            "myDataSource",
            new TimestampSpec("__time", "millis", null),
            null,
            null,
            new UniformGranularitySpec(Granularities.HOUR, Granularities.HOUR, false, Collections.emptyList()),
            null
        ),
        appenderatorConfig,
        new FireDepartmentMetrics(),
        new NoopDataSegmentPusher(),
        TestHelper.makeJsonMapper(),
        TestHelper.getTestIndexIO(),
        TestHelper.getTestIndexMergerV9(OnHeapMemorySegmentWriteOutMediumFactory.instance()),
        new NoopRowIngestionMeters(),
        new ParseExceptionHandler(new NoopRowIngestionMeters(), false, 0, 0),
        true
    );
  }

  @Test
  public void test_getBundle_knownDataSource()
  {
    final UnifiedIndexerAppenderatorsManager.DatasourceBundle bundle = manager.getBundle(
        Druids.newScanQueryBuilder()
              .dataSource(appenderator.getDataSource())
              .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
              .build()
    );

    Assert.assertEquals("myDataSource", bundle.getWalker().getDataSource());
  }

  @Test
  public void test_getBundle_unknownDataSource()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("unknown")
                                  .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
                                  .build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Could not find segment walker for datasource");

    manager.getBundle(query);
  }

  @Test
  public void test_removeAppenderatorsForTask()
  {
    Assert.assertEquals(ImmutableSet.of("myDataSource"), manager.getDatasourceBundles().keySet());
    manager.removeAppenderatorsForTask("taskId", "myDataSource");
    Assert.assertTrue(manager.getDatasourceBundles().isEmpty());
  }

  @Test
  public void test_removeAppenderatorsForTask_withoutCreate()
  {
    // Not all tasks use Appenderators. "remove" may be called without "create", and nothing bad should happen.
    manager.removeAppenderatorsForTask("someOtherTaskId", "someOtherDataSource");
    manager.removeAppenderatorsForTask("someOtherTaskId", "myDataSource");

    // Should be no change.
    Assert.assertEquals(ImmutableSet.of("myDataSource"), manager.getDatasourceBundles().keySet());
  }

  @Test
  public void test_limitedPool_persist() throws IOException
  {
    final UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger limitedPoolIndexMerger =
        new UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger(
            new NoopIndexMerger(),
            DirectQueryProcessingPool.INSTANCE
        );

    final File file = new File("xyz");

    // Three forms of persist.

    Assert.assertEquals(file, limitedPoolIndexMerger.persist(null, null, file, null, null, null));
    Assert.assertEquals(file, limitedPoolIndexMerger.persist(null, null, file, null, null));

    // Need a mocked index for this test, since getInterval is called on it.
    final IncrementalIndex index = EasyMock.createMock(IncrementalIndex.class);
    EasyMock.expect(index.getInterval()).andReturn(null);
    EasyMock.replay(index);
    Assert.assertEquals(file, limitedPoolIndexMerger.persist(index, file, null, null));
    EasyMock.verify(index);
  }

  @Test
  public void test_limitedPool_persistFail()
  {
    final UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger limitedPoolIndexMerger =
        new UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger(
            new NoopIndexMerger(true),
            DirectQueryProcessingPool.INSTANCE
        );

    final File file = new File("xyz");

    Assert.assertThrows(
        "failed",
        RuntimeException.class, // Wrapped IOException
        () -> limitedPoolIndexMerger.persist(null, null, file, null, null, null)
    );
  }

  @Test
  public void test_limitedPool_mergeQueryableIndexFail()
  {
    final UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger limitedPoolIndexMerger =
        new UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger(
            new NoopIndexMerger(true),
            DirectQueryProcessingPool.INSTANCE
        );

    final File file = new File("xyz");

    Assert.assertThrows(
        "failed",
        RuntimeException.class, // Wrapped IOException
        () -> limitedPoolIndexMerger.mergeQueryableIndex(
            null,
            false,
            null,
            null,
            file,
            null,
            null,
            null,
            null,
            -1
        )
    );
  }

  @Test
  public void test_limitedPool_mergeQueryableIndex() throws IOException
  {
    final UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger limitedPoolIndexMerger =
        new UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger(
            new NoopIndexMerger(),
            DirectQueryProcessingPool.INSTANCE
        );

    final File file = new File("xyz");

    // Two forms of mergeQueryableIndex
    Assert.assertEquals(file, limitedPoolIndexMerger.mergeQueryableIndex(null, false, null, file, null, null, -1));
    Assert.assertEquals(
        file,
        limitedPoolIndexMerger.mergeQueryableIndex(
            null,
            false,
            null,
            null,
            file,
            null,
            null,
            null,
            null,
            -1
        )
    );
  }

  @Test
  public void test_limitedPool_merge()
  {
    final UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger limitedPoolIndexMerger =
        new UnifiedIndexerAppenderatorsManager.LimitedPoolIndexMerger(
            new NoopIndexMerger(),
            DirectQueryProcessingPool.INSTANCE
        );

    final File file = new File("xyz");

    // "merge" is neither necessary nor implemented
    expectedException.expect(UnsupportedOperationException.class);
    Assert.assertEquals(file, limitedPoolIndexMerger.merge(null, false, null, file, null, null, -1));
  }

  /**
   * An {@link IndexMerger} that does nothing, but is useful for LimitedPoolIndexMerger tests.
   */
  private static class NoopIndexMerger implements IndexMerger
  {
    private final boolean failCalls;

    public NoopIndexMerger(boolean failCalls)
    {
      this.failCalls = failCalls;
    }

    public NoopIndexMerger()
    {
      this(false);
    }

    @Override
    public File persist(
        IncrementalIndex index,
        Interval dataInterval,
        File outDir,
        IndexSpec indexSpec,
        ProgressIndicator progress,
        @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
    ) throws IOException
    {
      if (failCalls) {
        throw new IOException("failed");
      }

      return outDir;
    }

    @Override
    public File mergeQueryableIndex(
        List<QueryableIndex> indexes,
        boolean rollup,
        AggregatorFactory[] metricAggs,
        @Nullable DimensionsSpec dimensionsSpec,
        File outDir,
        IndexSpec indexSpec,
        IndexSpec indexSpecForIntermediatePersists,
        ProgressIndicator progress,
        @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        int maxColumnsToMerge
    ) throws IOException
    {
      if (failCalls) {
        throw new IOException("failed");
      }

      return outDir;
    }

    @Override
    public File merge(
        List<IndexableAdapter> indexes,
        boolean rollup,
        AggregatorFactory[] metricAggs,
        File outDir,
        DimensionsSpec dimensionsSpec,
        IndexSpec indexSpec,
        int maxColumnsToMerge
    ) throws IOException
    {
      if (failCalls) {
        throw new IOException("failed");
      }

      return outDir;
    }
  }
}
