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

package org.apache.druid.metadata;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.assertj.core.util.Sets;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataManagerTest extends SqlSegmentsMetadataManagerTestBase
{
  private static DataSegment createSegment(
      String dataSource,
      String interval,
      String version
  )
  {
    return new DataSegment(
        dataSource,
        Intervals.of(interval),
        version,
        ImmutableMap.of(),
        ImmutableList.of(),
        ImmutableList.of(),
        NoneShardSpec.instance(),
        9,
        1234L
    );
  }

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private final DataSegment wikiSegment1 =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI).startingAt("2012-03-15").eachOfSizeInMb(500).get(0);
  private final DataSegment wikiSegment2 =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI).startingAt("2012-01-05").eachOfSizeInMb(500).get(0);

  private void publishUnusedSegments(DataSegment... segments)
  {
    for (DataSegment segment : segments) {
      publishSegment(segment);
      markSegmentsAsUnused(segment.getId());
    }
  }

  private void publishWikiSegments()
  {
    publishSegment(wikiSegment1);
    publishSegment(wikiSegment2);
  }

  @Before
  public void setUp() throws Exception
  {
    setUp(derbyConnectorRule);
  }

  @After
  public void tearDown()
  {
    teardownManager();
  }

  @Test
  public void testPollEmpty()
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertTrue(
        sqlSegmentsMetadataManager
            .getRecentDataSourcesSnapshot()
            .getDataSourcesWithAllUsedSegments()
            .isEmpty()
    );
  }

  @Test
  public void testPollPeriodically()
  {
    publishWikiSegments();
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertNull(dataSourcesSnapshot);
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    // This call make sure that the first poll is completed
    sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay();
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableList.of(TestDataSource.WIKI),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableSet.of(wikiSegment1, wikiSegment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.getDataSource(TestDataSource.WIKI).getSegments())
    );
    Assert.assertEquals(
        ImmutableSet.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testPollOnDemand()
  {
    publishWikiSegments();
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertNull(dataSourcesSnapshot);
    // This should return false and not wait/poll anything as we did not schedule periodic poll
    Assert.assertFalse(sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay());
    Assert.assertNull(dataSourcesSnapshot);
    // This call will force on demand poll
    sqlSegmentsMetadataManager.forceOrWaitOngoingDatabasePoll();
    Assert.assertFalse(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.OnDemandDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableList.of(TestDataSource.WIKI),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableSet.of(wikiSegment1, wikiSegment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.getDataSource(TestDataSource.WIKI).getSegments())
    );
    Assert.assertEquals(
        ImmutableSet.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test(timeout = 60_000)
  public void testPollPeriodicallyAndOnDemandInterleave() throws Exception
  {
    publishWikiSegments();
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertNull(dataSourcesSnapshot);
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    // This call make sure that the first poll is completed
    sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay();
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableList.of(TestDataSource.WIKI),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    publishSegment(createNewSegment1(TestDataSource.KOALA));

    // This call will force on demand poll
    sqlSegmentsMetadataManager.forceOrWaitOngoingDatabasePoll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.OnDemandDatabasePoll);
    // New datasource should now be in the snapshot since we just force on demand poll.
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableList.of(TestDataSource.KOALA, TestDataSource.WIKI),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );

    final String newDataSource3 = "wikipedia3";
    publishSegment(createNewSegment1(newDataSource3));

    // This time wait for periodic poll (not doing on demand poll so we have to wait a bit...)
    while (sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot().getDataSource(newDataSource3) == null) {
      Thread.sleep(1000);
    }
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableSet.of(TestDataSource.KOALA, "wikipedia3", TestDataSource.WIKI),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toSet())
    );
  }

  @Test
  public void testPrepareImmutableDataSourceWithUsedSegmentsAwaitsPollOnRestart()
  {
    publishWikiSegments();
    DataSegment koalaSegment = pollThenStopThenPublishKoalaSegment();
    Assert.assertEquals(
        Set.of(koalaSegment),
        Set.copyOf(sqlSegmentsMetadataManager.getRecentDataSourcesSnapshot().getDataSource(TestDataSource.KOALA).getSegments())
    );
  }

  @Test
  public void testGetDataSourceWithUsedSegmentsAwaitsPollOnRestart()
  {
    publishWikiSegments();
    DataSegment koalaSegment = pollThenStopThenPublishKoalaSegment();
    Assert.assertEquals(
        Set.of(koalaSegment),
        Set.copyOf(sqlSegmentsMetadataManager.getRecentDataSourcesSnapshot().getDataSource(TestDataSource.KOALA).getSegments())
    );
  }

  @Test
  public void testPrepareImmutableDataSourcesWithAllUsedSegmentsAwaitsPollOnRestart()
  {
    publishWikiSegments();
    DataSegment koalaSegment = pollThenStopThenPublishKoalaSegment();
    Assert.assertEquals(
        ImmutableSet.of(wikiSegment1, wikiSegment2, koalaSegment),
        ImmutableSet.copyOf(
            sqlSegmentsMetadataManager
                .getRecentDataSourcesSnapshot()
                .getDataSourcesWithAllUsedSegments()
                .stream()
                .flatMap((ImmutableDruidDataSource dataSource) -> dataSource.getSegments().stream())
                .iterator()
        )
    );
  }

  @Test
  public void testIterateAllUsedSegmentsAwaitsPollOnRestart()
  {
    publishWikiSegments();
    DataSegment koalaSegment = pollThenStopThenPublishKoalaSegment();
    Assert.assertEquals(
        ImmutableSet.of(wikiSegment1, wikiSegment2, koalaSegment),
        retrieveAllUsedSegments()
    );
  }

  private DataSegment pollThenStopThenPublishKoalaSegment()
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    sqlSegmentsMetadataManager.stopPollingDatabasePeriodically();
    Assert.assertFalse(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    final DataSegment koalaSegment = createNewSegment1(TestDataSource.KOALA);
    publishSegment(koalaSegment);
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    return koalaSegment;
  }

  /**
   * Create a corrupted segment entry in the segments table to test
   * whether the overall loading of segments from the database continues to work
   * even if one of the entries is corrupted.
   */
  @Test
  public void testPollWithCorruptedSegment()
  {
    publishWikiSegments();

    final DataSegment corruptSegment = DataSegment.builder(wikiSegment1).dataSource("corrupt-datasource").build();
    publishSegment(corruptSegment);
    updateSegmentPayload(corruptSegment, StringUtils.toUtf8("corrupt-payload"));

    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    DataSourcesSnapshot snapshot = sqlSegmentsMetadataManager.getRecentDataSourcesSnapshot();
    Assert.assertEquals(
        TestDataSource.WIKI,
        Iterables.getOnlyElement(snapshot.getDataSourcesWithAllUsedSegments()).getName()
    );
  }

  private static DataSegment createNewSegment1(String datasource)
  {
    return createSegment(
        datasource,
        "2017-10-15T00:00:00.000/2017-10-16T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );
  }

  private static DataSegment createNewSegment2(String datasource)
  {
    return createSegment(
        datasource,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );
  }

  @Test
  public void test_poll_doesNotRetrieveUnusedSegments()
  {
    publishWikiSegments();
    final DataSegment koalaSegment1 = createNewSegment1(TestDataSource.KOALA);
    final DataSegment koalaSegment2 = createNewSegment2(TestDataSource.KOALA);

    publishSegment(koalaSegment1);
    publishSegment(koalaSegment2);

    // Poll all segments
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2, koalaSegment1, koalaSegment2),
        retrieveAllUsedSegments()
    );

    // Mark the koala segments as unused
    Assert.assertEquals(2, markSegmentsAsUnused(koalaSegment1.getId(), koalaSegment2.getId()));

    // Verify that subsequent poll only retrieves the used segments
    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testStopAndStart()
  {
    // Simulate successive losing and getting the coordinator leadership
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.stopPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.stopPollingDatabasePeriodically();
  }

  @Test
  public void testIterateAllUsedNonOvershadowedSegmentsForDatasourceInterval()
  {
    publishWikiSegments();
    final Interval theInterval = Intervals.of("2012-03-15T00:00:00.000/2012-03-20T00:00:00.000");

    // Re-create SqlSegmentsMetadataManager with a higher poll duration
    final SegmentsMetadataManagerConfig config = new SegmentsMetadataManagerConfig(Period.seconds(1), null);
    sqlSegmentsMetadataManager = new SqlSegmentsMetadataManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        derbyConnectorRule.getConnector(),
        segmentSchemaCache,
        CentralizedDatasourceSchemaConfig.create(),
        NoopServiceEmitter.instance()
    );
    sqlSegmentsMetadataManager.start();

    Set<DataSegment> segments = sqlSegmentsMetadataManager
        .getRecentDataSourcesSnapshot()
        .getAllUsedNonOvershadowedSegments(TestDataSource.WIKI, theInterval);
    Assert.assertEquals(Set.of(wikiSegment1), segments);

    final DataSegment wikiSegment3 = createSegment(
        TestDataSource.WIKI,
        "2012-03-16T00:00:00.000/2012-03-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );
    publishSegment(wikiSegment3);

    // New segment is not returned since we call without force poll
    segments = sqlSegmentsMetadataManager
        .getRecentDataSourcesSnapshot()
        .getAllUsedNonOvershadowedSegments(TestDataSource.WIKI, theInterval);
    Assert.assertEquals(Set.of(wikiSegment1), segments);

    // New segment is returned since we call with force poll
    segments = sqlSegmentsMetadataManager
        .forceUpdateDataSourcesSnapshot()
        .getAllUsedNonOvershadowedSegments(TestDataSource.WIKI, theInterval);
    Assert.assertEquals(Set.of(wikiSegment1, wikiSegment3), segments);
  }

  @Test
  public void testPopulateUsedFlagLastUpdated()
  {
    allowUsedFlagLastUpdatedToBeNullable();
    final DataSegment koalaSegment = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    publishUnusedSegments(koalaSegment);
    updateUsedStatusLastUpdatedToNull(koalaSegment);

    Assert.assertEquals(1, getCountOfRowsWithLastUsedNull());
    sqlSegmentsMetadataManager.populateUsedFlagLastUpdated();
    Assert.assertEquals(0, getCountOfRowsWithLastUsedNull());
  }

  private int getCountOfRowsWithLastUsedNull()
  {
    return derbyConnectorRule.getConnector().retryWithHandle(
        handle -> handle.select(
            StringUtils.format(
                "SELECT ID FROM %1$s WHERE USED_STATUS_LAST_UPDATED IS NULL",
                derbyConnectorRule.segments().getTableName()
            )
        ).size()
    );
  }

  private void updateSegmentPayload(DataSegment segment, byte[] payload)
  {
    derbyConnectorRule.segments().update(
        "UPDATE %1$s SET PAYLOAD = ? WHERE ID = ?",
        payload,
        segment.getId().toString()
    );
  }

  private void updateUsedStatusLastUpdatedToNull(DataSegment segment)
  {
    derbyConnectorRule.segments().update(
        "UPDATE %1$s SET USED_STATUS_LAST_UPDATED = NULL WHERE ID = ?",
        segment.getId().toString()
    );
  }

  /**
   * Alters the column used_status_last_updated to be nullable. This is used to
   * test backward compatibility with versions of Druid without this column
   * present in the segments table.
   */
  private void allowUsedFlagLastUpdatedToBeNullable()
  {
    derbyConnectorRule.segments().update(
        "ALTER TABLE %1$s ALTER COLUMN USED_STATUS_LAST_UPDATED NULL"
    );
  }
  
  private Set<DataSegment> retrieveAllUsedSegments()
  {
    return Sets.newHashSet(
        sqlSegmentsMetadataManager.getRecentDataSourcesSnapshot().iterateAllUsedSegmentsInSnapshot()
    );
  }  
}
