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

package org.apache.druid.metadata.segment;

import com.google.common.base.Suppliers;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManagerTestBase;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.Metric;
import org.apache.druid.metadata.segment.cache.NoopSegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.assertj.core.util.Sets;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class SqlSegmentsMetadataManagerV2Test extends SqlSegmentsMetadataManagerTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.enabled(true));

  private SegmentsMetadataManager manager;
  private BlockingExecutorService segmentMetadataCacheExec;
  private StubServiceEmitter emitter;

  private static final DateTime JAN_1 = DateTimes.of("2025-01-01");

  private static final List<DataSegment> WIKI_SEGMENTS_1X5D
      = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(5, Granularities.DAY)
                          .startingAt(JAN_1)
                          .eachOfSize(500);

  @Before
  public void setup() throws Exception
  {
    setUp(derbyConnectorRule);
    connector.createPendingSegmentsTable();

    emitter = new StubServiceEmitter();

    WIKI_SEGMENTS_1X5D.forEach(super::publishSegment);
  }

  private void initManager(
      SegmentMetadataCache.UsageMode cacheMode
  )
  {
    segmentMetadataCacheExec = new BlockingExecutorService("test");
    SegmentMetadataCache segmentMetadataCache = new HeapMemorySegmentMetadataCache(
        jsonMapper,
        Suppliers.ofInstance(new SegmentsMetadataManagerConfig(Period.seconds(1), cacheMode)),
        Suppliers.ofInstance(storageConfig),
        Suppliers.ofInstance(CentralizedDatasourceSchemaConfig.create()),
        connector,
        (poolSize, name) -> new WrappingScheduledExecutorService(name, segmentMetadataCacheExec, false),
        emitter
    );
    segmentMetadataCache.start();
    segmentMetadataCache.becomeLeader();

    manager = new SqlSegmentsMetadataManagerV2(
        segmentMetadataCache,
        segmentSchemaCache,
        connector,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        CentralizedDatasourceSchemaConfig::create,
        emitter,
        jsonMapper
    );
    manager.start();
  }

  private void syncSegmentMetadataCache()
  {
    segmentMetadataCacheExec.finishNextPendingTasks(2);
    segmentMetadataCacheExec.finishNextPendingTasks(2);
  }

  @After
  public void tearDown()
  {
    if (manager == null) {
      return;
    }

    if (manager.isPollingDatabasePeriodically()) {
      manager.stopPollingDatabasePeriodically();
    }
    manager.stop();
  }

  @Test
  public void test_manager_usesCachedSegments_ifCacheIsEnabled()
  {
    initManager(SegmentMetadataCache.UsageMode.ALWAYS);

    manager.startPollingDatabasePeriodically();
    Assert.assertTrue(manager.isPollingDatabasePeriodically());

    syncSegmentMetadataCache();
    verifyDatasourceSnapshot();

    // isPolling returns true even after stop since cache is still polling the metadata store
    manager.stopPollingDatabasePeriodically();
    Assert.assertTrue(manager.isPollingDatabasePeriodically());

    emitter.verifyNotEmitted("segment/poll/time");
    emitter.verifyNotEmitted("segment/pollWithSchema/time");
    emitter.verifyEmitted(Metric.SYNC_DURATION_MILLIS, 2);
  }

  @Test
  public void test_manager_pollsSegments_ifCacheIsDisabled()
  {
    initManager(SegmentMetadataCache.UsageMode.NEVER);

    manager.startPollingDatabasePeriodically();
    Assert.assertTrue(manager.isPollingDatabasePeriodically());

    verifyDatasourceSnapshot();

    manager.stopPollingDatabasePeriodically();
    Assert.assertFalse(manager.isPollingDatabasePeriodically());

    emitter.verifyEmitted("segment/poll/time", 1);
    emitter.verifyNotEmitted(Metric.SYNC_DURATION_MILLIS);
  }

  @Test
  public void test_manager_throwsException_ifBothCacheAndSchemaAreEnabled()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> new SqlSegmentsMetadataManagerV2(
                new NoopSegmentMetadataCache() {
                  @Override
                  public boolean isEnabled()
                  {
                    return true;
                  }
                },
                segmentSchemaCache,
                connector,
                Suppliers.ofInstance(config),
                derbyConnectorRule.metadataTablesConfigSupplier(),
                () -> CentralizedDatasourceSchemaConfig.enabled(true),
                emitter,
                jsonMapper
            )
        ),
        ExceptionMatcher.of(IllegalArgumentException.class).expectMessageIs(
            "Segment metadata incremental cache['druid.manager.segments.useIncrementalCache']"
            + " and segment schema cache['druid.centralizedDatasourceSchema.enabled']"
            + " must not be enabled together."
        )
    );
  }

  private void verifyDatasourceSnapshot()
  {
    final DataSourcesSnapshot snapshot = manager.getRecentDataSourcesSnapshot();
    Assert.assertEquals(
        Set.copyOf(WIKI_SEGMENTS_1X5D),
        Sets.newHashSet(snapshot.iterateAllUsedSegmentsInSnapshot())
    );
    Assert.assertEquals(
        Set.copyOf(WIKI_SEGMENTS_1X5D),
        Set.copyOf(snapshot.getDataSource(TestDataSource.WIKI).getSegments())
    );
  }
}
