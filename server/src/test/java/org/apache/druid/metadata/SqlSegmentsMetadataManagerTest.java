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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataManagerTest
{
  private static DataSegment createSegment(
      String dataSource,
      String interval,
      String version,
      String bucketKey,
      int binaryVersion
  )
  {
    return new DataSegment(
        dataSource,
        Intervals.of(interval),
        version,
        ImmutableMap.of(
            "type", "s3_zip",
            "bucket", "test",
            "key", dataSource + "/" + bucketKey
        ),
        ImmutableList.of("dim1", "dim2", "dim3"),
        ImmutableList.of("count", "value"),
        NoneShardSpec.instance(),
        binaryVersion,
        1234L
    );
  }

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private SQLMetadataSegmentPublisher publisher;
  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  private final DataSegment segment1 = createSegment(
      "wikipedia",
      "2012-03-15T00:00:00.000/2012-03-16T00:00:00.000",
      "2012-03-16T00:36:30.848Z",
      "index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index.zip",
      0
  );

  private final DataSegment segment2 = createSegment(
      "wikipedia",
      "2012-01-05T00:00:00.000/2012-01-06T00:00:00.000",
      "2012-01-06T22:19:12.565Z",
      "wikipedia/index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip",
      0
  );

  private void publish(DataSegment segment, boolean used) throws IOException
  {
    publish(segment, used, DateTimes.nowUtc());
  }

  private void publish(DataSegment segment, boolean used, DateTime usedFlagLastUpdated) throws IOException
  {
    boolean partitioned = !(segment.getShardSpec() instanceof NoneShardSpec);

    String usedFlagLastUpdatedStr = null;
    if (null != usedFlagLastUpdated) {
      usedFlagLastUpdatedStr = usedFlagLastUpdated.toString();
    }
    publisher.publishSegment(
        segment.getId().toString(),
        segment.getDataSource(),
        DateTimes.nowUtc().toString(),
        segment.getInterval().getStart().toString(),
        segment.getInterval().getEnd().toString(),
        partitioned,
        segment.getVersion(),
        used,
        jsonMapper.writeValueAsBytes(segment),
        usedFlagLastUpdatedStr
    );
  }

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    SegmentsMetadataManagerConfig config = new SegmentsMetadataManagerConfig();
    config.setPollDuration(Period.seconds(3));
    sqlSegmentsMetadataManager = new SqlSegmentsMetadataManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector
    );
    sqlSegmentsMetadataManager.start();

    publisher = new SQLMetadataSegmentPublisher(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        connector
    );

    connector.createSegmentTable();

    publisher.publishSegment(segment1);
    publisher.publishSegment(segment2);
  }

  @After
  public void teardown()
  {
    if (sqlSegmentsMetadataManager.isPollingDatabasePeriodically()) {
      sqlSegmentsMetadataManager.stopPollingDatabasePeriodically();
    }
    sqlSegmentsMetadataManager.stop();
  }

  @Test
  public void testPollPeriodically()
  {
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertNull(dataSourcesSnapshot);
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    // This call make sure that the first poll is completed
    sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay();
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableSet.of("wikipedia"),
        sqlSegmentsMetadataManager.retrieveAllDataSourceNames()
    );
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.getDataSource("wikipedia").getSegments())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot())
    );
  }

  @Test
  public void testPollOnDemand()
  {
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertNull(dataSourcesSnapshot);
    // This should return false and not wait/poll anything as we did not schedule periodic poll
    Assert.assertFalse(sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay());
    Assert.assertNull(dataSourcesSnapshot);
    // This call will force on demand poll
    sqlSegmentsMetadataManager.forceOrWaitOngoingDatabasePoll();
    Assert.assertFalse(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.OnDemandDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableSet.of("wikipedia"),
        sqlSegmentsMetadataManager.retrieveAllDataSourceNames()
    );
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.getDataSource("wikipedia").getSegments())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot())
    );
  }

  @Test(timeout = 60_000)
  public void testPollPeriodicallyAndOnDemandInterleave() throws Exception
  {
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertNull(dataSourcesSnapshot);
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    // This call make sure that the first poll is completed
    sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay();
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    final String newDataSource2 = "wikipedia2";
    final DataSegment newSegment2 = createNewSegment1(newDataSource2);
    publisher.publishSegment(newSegment2);

    // This call will force on demand poll
    sqlSegmentsMetadataManager.forceOrWaitOngoingDatabasePoll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.OnDemandDatabasePoll);
    // New datasource should now be in the snapshot since we just force on demand poll.
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableList.of("wikipedia2", "wikipedia"),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );

    final String newDataSource3 = "wikipedia3";
    final DataSegment newSegment3 = createNewSegment1(newDataSource3);
    publisher.publishSegment(newSegment3);

    // This time wait for periodic poll (not doing on demand poll so we have to wait a bit...)
    while (sqlSegmentsMetadataManager.getDataSourcesSnapshot().getDataSource(newDataSource3) == null) {
      Thread.sleep(1000);
    }
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableSet.of("wikipedia2", "wikipedia3", "wikipedia"),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toSet())
    );
  }

  @Test
  public void testPrepareImmutableDataSourceWithUsedSegmentsAwaitsPollOnRestart() throws IOException
  {
    DataSegment newSegment = pollThenStopThenStartIntro();
    Assert.assertEquals(
        ImmutableSet.of(newSegment),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments("wikipedia2").getSegments())
    );
  }

  @Test
  public void testGetDataSourceWithUsedSegmentsAwaitsPollOnRestart() throws IOException
  {
    DataSegment newSegment = pollThenStopThenStartIntro();
    Assert.assertEquals(
        ImmutableSet.of(newSegment),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments("wikipedia2").getSegments())
    );
  }

  @Test
  public void testPrepareImmutableDataSourcesWithAllUsedSegmentsAwaitsPollOnRestart() throws IOException
  {
    DataSegment newSegment = pollThenStopThenStartIntro();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment),
        ImmutableSet.copyOf(
            sqlSegmentsMetadataManager
                .getImmutableDataSourcesWithAllUsedSegments()
                .stream()
                .flatMap((ImmutableDruidDataSource dataSource) -> dataSource.getSegments().stream())
                .iterator()
        )
    );
  }

  @Test
  public void testIterateAllUsedSegmentsAwaitsPollOnRestart() throws IOException
  {
    DataSegment newSegment = pollThenStopThenStartIntro();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
  }

  private DataSegment pollThenStopThenStartIntro() throws IOException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    sqlSegmentsMetadataManager.stopPollingDatabasePeriodically();
    Assert.assertFalse(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    Assert.assertEquals(
        ImmutableSet.of("wikipedia"),
        sqlSegmentsMetadataManager.retrieveAllDataSourceNames()
    );
    DataSegment newSegment = createNewSegment1("wikipedia2");
    publisher.publishSegment(newSegment);
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    return newSegment;
  }

  @Test
  public void testPollWithCorruptedSegment()
  {
    //create a corrupted segment entry in segments table, which tests
    //that overall loading of segments from database continues to work
    //even in one of the entries are corrupted.
    publisher.publishSegment(
        "corrupt-segment-id",
        "corrupt-datasource",
        "corrupt-create-date",
        "corrupt-start-date",
        "corrupt-end-date",
        true,
        "corrupt-version",
        true,
        StringUtils.toUtf8("corrupt-payload"),
        "corrupt-last-used-date"
    );

    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    Assert.assertEquals(
        "wikipedia",
        Iterables.getOnlyElement(sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments()).getName()
    );
  }

  @Test
  public void testGetUnusedSegmentIntervals() throws IOException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();

    // We alter the segment table to allow nullable used_status_last_updated in order to test compatibility during druid upgrade from version without used_status_last_updated.
    derbyConnectorRule.allowUsedFlagLastUpdatedToBeNullable();

    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    int numChangedSegments = sqlSegmentsMetadataManager.markAsUnusedAllSegmentsInDataSource("wikipedia");
    Assert.assertEquals(2, numChangedSegments);

    String newDs = "newDataSource";
    final DataSegment newSegment = createSegment(
        newDs,
        "2017-10-15T00:00:00.000/2017-10-16T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "wikipedia2/index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );
    publish(newSegment, false, DateTimes.nowUtc().minus(Duration.parse("PT7200S").getMillis()));

    final DataSegment newSegment2 = createSegment(
        newDs,
        "2017-10-16T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "wikipedia2/index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );
    publish(newSegment2, false, DateTimes.nowUtc().minus(Duration.parse("PT172800S").getMillis()));

    final DataSegment newSegment3 = createSegment(
        newDs,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "wikipedia2/index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );
    publish(newSegment3, false, null);

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval()),
        sqlSegmentsMetadataManager.getUnusedSegmentIntervals("wikipedia", null, DateTimes.of("3000"), 1, DateTimes.COMPARE_DATE_AS_STRING_MAX)
    );

    // Test the DateTime maxEndTime argument of getUnusedSegmentIntervals
    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval()),
        sqlSegmentsMetadataManager.getUnusedSegmentIntervals("wikipedia", null, DateTimes.of(2012, 1, 7, 0, 0), 1, DateTimes.COMPARE_DATE_AS_STRING_MAX)
    );
    Assert.assertEquals(
        ImmutableList.of(segment1.getInterval()),
        sqlSegmentsMetadataManager.getUnusedSegmentIntervals("wikipedia", DateTimes.of(2012, 1, 7, 0, 0), DateTimes.of(2012, 4, 7, 0, 0), 1, DateTimes.COMPARE_DATE_AS_STRING_MAX)
    );
    Assert.assertEquals(
        ImmutableList.of(),
        sqlSegmentsMetadataManager.getUnusedSegmentIntervals("wikipedia", DateTimes.of(2012, 1, 7, 0, 0), DateTimes.of(2012, 1, 7, 0, 0), 1, DateTimes.COMPARE_DATE_AS_STRING_MAX)
    );

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval(), segment1.getInterval()),
        sqlSegmentsMetadataManager.getUnusedSegmentIntervals("wikipedia", null, DateTimes.of("3000"), 5, DateTimes.COMPARE_DATE_AS_STRING_MAX)
    );

    // Test a buffer period that should exclude some segments

    // The wikipedia datasource has segments generated with last used time equal to roughly the time of test run. None of these segments should be selected with a bufer period of 1 day
    Assert.assertEquals(
        ImmutableList.of(),
        sqlSegmentsMetadataManager.getUnusedSegmentIntervals("wikipedia", DateTimes.COMPARE_DATE_AS_STRING_MIN, DateTimes.of("3000"), 5, DateTimes.nowUtc().minus(Duration.parse("PT86400S")))
    );

    // One of the 3 segments in newDs has a null used_status_last_updated which should mean getUnusedSegmentIntervals never returns it
    // One of the 3 segments in newDs has a used_status_last_updated older than 1 day which means it should also be returned
    // The last of the 3 segemns in newDs has a used_status_last_updated date less than one day and should not be returned
    Assert.assertEquals(
        ImmutableList.of(newSegment2.getInterval()),
        sqlSegmentsMetadataManager.getUnusedSegmentIntervals(newDs, DateTimes.COMPARE_DATE_AS_STRING_MIN, DateTimes.of("3000"), 5, DateTimes.nowUtc().minus(Duration.parse("PT86400S")))
    );
  }

  @Test(timeout = 60_000)
  public void testMarkAsUnusedAllSegmentsInDataSource() throws IOException, InterruptedException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment = createNewSegment1(newDataSource);

    publisher.publishSegment(newSegment);

    awaitDataSourceAppeared(newDataSource);
    int numChangedSegments = sqlSegmentsMetadataManager.markAsUnusedAllSegmentsInDataSource(newDataSource);
    Assert.assertEquals(1, numChangedSegments);
    awaitDataSourceDisappeared(newDataSource);
    Assert.assertNull(sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments(newDataSource));
  }

  private static DataSegment createNewSegment1(String newDataSource)
  {
    return createSegment(
        newDataSource,
        "2017-10-15T00:00:00.000/2017-10-16T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );
  }

  private static DataSegment createNewSegment2(String newDataSource)
  {
    return createSegment(
        newDataSource,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );
  }

  @Test(timeout = 60_000)
  public void testMarkSegmentAsUnused() throws IOException, InterruptedException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment = createSegment(
        newDataSource,
        "2017-10-15T00:00:00.000/2017-10-16T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "wikipedia2/index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    publisher.publishSegment(newSegment);
    awaitDataSourceAppeared(newDataSource);
    Assert.assertNotNull(sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments(newDataSource));

    Assert.assertTrue(sqlSegmentsMetadataManager.markSegmentAsUnused(newSegment.getId()));
    awaitDataSourceDisappeared(newDataSource);
    Assert.assertNull(sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments(newDataSource));
  }

  private void awaitDataSourceAppeared(String newDataSource) throws InterruptedException
  {
    while (sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments(newDataSource) == null) {
      Thread.sleep(1000);
    }
  }

  private void awaitDataSourceDisappeared(String dataSource) throws InterruptedException
  {
    while (sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments(dataSource) != null) {
      Thread.sleep(1000);
    }
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegments() throws Exception
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createSegment(
        newDataSource,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    final DataSegment newSegment2 = createSegment(
        newDataSource,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        1
    );

    // Overshadowed by newSegment2
    final DataSegment newSegment3 = createSegment(
        newDataSource,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        1
    );

    publish(newSegment1, false);
    publish(newSegment2, false);
    publish(newSegment3, false);
    final ImmutableSet<String> segmentIds = ImmutableSet.of(
        newSegment1.getId().toString(),
        newSegment2.getId().toString(),
        newSegment3.getId().toString()
    );

    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
    Assert.assertEquals(2, sqlSegmentsMetadataManager.markAsUsedNonOvershadowedSegments(newDataSource, segmentIds));
    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment1, newSegment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
  }

  @Test(expected = UnknownSegmentIdsException.class)
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidDataSource() throws Exception
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment1(newDataSource);

    publish(newSegment1, false);
    publish(newSegment2, false);
    final ImmutableSet<String> segmentIds =
        ImmutableSet.of(newSegment1.getId().toString(), newSegment2.getId().toString());
    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
    // none of the segments are in data source
    Assert.assertEquals(0, sqlSegmentsMetadataManager.markAsUsedNonOvershadowedSegments("wrongDataSource", segmentIds));
  }

  @Test(expected = UnknownSegmentIdsException.class)
  public void testMarkAsUsedNonOvershadowedSegmentsWithInvalidSegmentIds() throws UnknownSegmentIdsException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment1(newDataSource);

    final ImmutableSet<String> segmentIds =
        ImmutableSet.of(newSegment1.getId().toString(), newSegment2.getId().toString());
    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
    // none of the segments are in data source
    Assert.assertEquals(0, sqlSegmentsMetadataManager.markAsUsedNonOvershadowedSegments(newDataSource, segmentIds));
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInInterval() throws IOException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createSegment(
        newDataSource,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        1
    );

    final DataSegment newSegment3 = createSegment(
        newDataSource,
        "2017-10-19T00:00:00.000/2017-10-20T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    // Overshadowed by newSegment2
    final DataSegment newSegment4 = createNewSegment2(newDataSource);

    publish(newSegment1, false);
    publish(newSegment2, false);
    publish(newSegment3, false);
    publish(newSegment4, false);
    final Interval theInterval = Intervals.of("2017-10-15T00:00:00.000/2017-10-18T00:00:00.000");

    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );

    // 2 out of 3 segments match the interval
    Assert.assertEquals(2, sqlSegmentsMetadataManager.markAsUsedNonOvershadowedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment1, newSegment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInIntervalWithOverlappingInterval() throws IOException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createSegment(
        newDataSource,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    final DataSegment newSegment2 = createSegment(
        newDataSource,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        1
    );

    final DataSegment newSegment3 = createSegment(
        newDataSource,
        "2017-10-19T00:00:00.000/2017-10-22T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    // Overshadowed by newSegment2
    final DataSegment newSegment4 = createNewSegment2(newDataSource);

    publish(newSegment1, false);
    publish(newSegment2, false);
    publish(newSegment3, false);
    publish(newSegment4, false);
    final Interval theInterval = Intervals.of("2017-10-16T00:00:00.000/2017-10-20T00:00:00.000");

    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );

    // 1 out of 3 segments match the interval, other 2 overlap, only the segment fully contained will be marked unused
    Assert.assertEquals(1, sqlSegmentsMetadataManager.markAsUsedNonOvershadowedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
  }

  @Test
  public void testMarkSegmentsAsUnused() throws IOException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment1(newDataSource);

    publisher.publishSegment(newSegment1);
    publisher.publishSegment(newSegment2);
    final ImmutableSet<SegmentId> segmentIds =
        ImmutableSet.of(newSegment1.getId(), newSegment1.getId());

    Assert.assertEquals(segmentIds.size(), sqlSegmentsMetadataManager.markSegmentsAsUnused(segmentIds));
    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
  }

  @Test
  public void testMarkAsUnusedSegmentsInInterval() throws IOException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment2(newDataSource);

    final DataSegment newSegment3 = createSegment(
        newDataSource,
        "2017-10-19T00:00:00.000/2017-10-20T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    publisher.publishSegment(newSegment1);
    publisher.publishSegment(newSegment2);
    publisher.publishSegment(newSegment3);
    final Interval theInterval = Intervals.of("2017-10-15T00:00:00.000/2017-10-18T00:00:00.000");

    // 2 out of 3 segments match the interval
    Assert.assertEquals(2, sqlSegmentsMetadataManager.markAsUnusedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment3),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
    );
  }

  @Test
  public void testMarkAsUnusedSegmentsInIntervalWithOverlappingInterval() throws IOException
  {
    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    sqlSegmentsMetadataManager.poll();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createSegment(
        newDataSource,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    final DataSegment newSegment2 = createNewSegment2(newDataSource);

    final DataSegment newSegment3 = createSegment(
        newDataSource,
        "2017-10-19T00:00:00.000/2017-10-22T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );

    publisher.publishSegment(newSegment1);
    publisher.publishSegment(newSegment2);
    publisher.publishSegment(newSegment3);
    final Interval theInterval = Intervals.of("2017-10-16T00:00:00.000/2017-10-20T00:00:00.000");

    // 1 out of 3 segments match the interval, other 2 overlap, only the segment fully contained will be marked unused
    Assert.assertEquals(1, sqlSegmentsMetadataManager.markAsUnusedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadataManager.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment1, newSegment3),
        ImmutableSet.copyOf(sqlSegmentsMetadataManager.iterateAllUsedSegments())
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
  public void testIterateAllUsedNonOvershadowedSegmentsForDatasourceInterval() throws Exception
  {
    final Interval theInterval = Intervals.of("2012-03-15T00:00:00.000/2012-03-20T00:00:00.000");
    Optional<Iterable<DataSegment>> segments = sqlSegmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
        "wikipedia", theInterval, true
    );
    Assert.assertTrue(segments.isPresent());
    Set<DataSegment> dataSegmentSet = ImmutableSet.copyOf(segments.get());
    Assert.assertEquals(1, dataSegmentSet.size());
    Assert.assertTrue(dataSegmentSet.contains(segment1));

    final DataSegment newSegment2 = createSegment(
        "wikipedia",
        "2012-03-16T00:00:00.000/2012-03-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );
    publisher.publishSegment(newSegment2);

    // New segment is not returned since we call without force poll
    segments = sqlSegmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
        "wikipedia", theInterval, false
    );
    Assert.assertTrue(segments.isPresent());
    dataSegmentSet = ImmutableSet.copyOf(segments.get());
    Assert.assertEquals(1, dataSegmentSet.size());
    Assert.assertTrue(dataSegmentSet.contains(segment1));

    // New segment is returned since we call with force poll
    segments = sqlSegmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
        "wikipedia", theInterval, true
    );
    Assert.assertTrue(segments.isPresent());
    dataSegmentSet = ImmutableSet.copyOf(segments.get());
    Assert.assertEquals(2, dataSegmentSet.size());
    Assert.assertTrue(dataSegmentSet.contains(segment1));
    Assert.assertTrue(dataSegmentSet.contains(newSegment2));
  }

  @Test
  public void testPopulateUsedFlagLastUpdated() throws IOException
  {
    derbyConnectorRule.allowUsedFlagLastUpdatedToBeNullable();
    final DataSegment newSegment = createSegment(
        "dummyDS",
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z",
        "wikipedia2/index/y=2017/m=10/d=15/2017-10-16T20:19:12.565Z/0/index.zip",
        0
    );
    publish(newSegment, false, null);
    Assert.assertTrue(getCountOfRowsWithLastUsedNull() > 0);
    sqlSegmentsMetadataManager.populateUsedFlagLastUpdated();
    Assert.assertTrue(getCountOfRowsWithLastUsedNull() == 0);
  }

  private int getCountOfRowsWithLastUsedNull()
  {
    return derbyConnectorRule.getConnector().retryWithHandle(
        new HandleCallback<Integer>()
        {
          @Override
          public Integer withHandle(Handle handle)
          {
            List<Map<String, Object>> lst = handle.select(
                StringUtils.format(
                    "SELECT * FROM %1$s WHERE USED_STATUS_LAST_UPDATED IS NULL",
                    derbyConnectorRule.metadataTablesConfigSupplier()
                                      .get()
                                      .getSegmentsTable()
                                      .toUpperCase(Locale.ENGLISH)
                )
            );
            return lst.size();
          }
        }
    );
  }
}
