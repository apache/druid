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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Collectors;


public class SQLMetadataSegmentManagerTest
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

  private SQLMetadataSegmentManager sqlSegmentsMetadata;
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
    boolean partitioned = !(segment.getShardSpec() instanceof NoneShardSpec);
    publisher.publishSegment(
        segment.getId().toString(),
        segment.getDataSource(),
        DateTimes.nowUtc().toString(),
        segment.getInterval().getStart().toString(),
        segment.getInterval().getEnd().toString(),
        partitioned,
        segment.getVersion(),
        used,
        jsonMapper.writeValueAsBytes(segment)
    );
  }

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    MetadataSegmentManagerConfig config = new MetadataSegmentManagerConfig();
    config.setPollDuration(Period.seconds(1));
    sqlSegmentsMetadata = new SQLMetadataSegmentManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector
    );
    sqlSegmentsMetadata.start();

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
    if (sqlSegmentsMetadata.isPollingDatabasePeriodically()) {
      sqlSegmentsMetadata.stopPollingDatabasePeriodically();
    }
    sqlSegmentsMetadata.stop();
  }

  @Test
  public void testPoll()
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        sqlSegmentsMetadata.retrieveAllDataSourceNames()
    );
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        sqlSegmentsMetadata
            .getImmutableDataSourcesWithAllUsedSegments()
            .stream()
            .map(ImmutableDruidDataSource::getName)
            .collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments("wikipedia").getSegments())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test
  public void testPrepareImmutableDataSourceWithUsedSegmentsAwaitsPollOnRestart() throws IOException
  {
    DataSegment newSegment = pollThenStopThenStartIntro();
    Assert.assertEquals(
        ImmutableSet.of(newSegment),
        ImmutableSet.copyOf(sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments("wikipedia2").getSegments())
    );
  }

  @Test
  public void testGetDataSourceWithUsedSegmentsAwaitsPollOnRestart() throws IOException
  {
    DataSegment newSegment = pollThenStopThenStartIntro();
    Assert.assertEquals(
        ImmutableSet.of(newSegment),
        ImmutableSet.copyOf(sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments("wikipedia2").getSegments())
    );
  }

  @Test
  public void testPrepareImmutableDataSourcesWithAllUsedSegmentsAwaitsPollOnRestart() throws IOException
  {
    DataSegment newSegment = pollThenStopThenStartIntro();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment),
        ImmutableSet.copyOf(
            sqlSegmentsMetadata
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
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  private DataSegment pollThenStopThenStartIntro() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    sqlSegmentsMetadata.stopPollingDatabasePeriodically();
    Assert.assertFalse(sqlSegmentsMetadata.isPollingDatabasePeriodically());
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        sqlSegmentsMetadata.retrieveAllDataSourceNames()
    );
    DataSegment newSegment = createNewSegment1("wikipedia2");
    publisher.publishSegment(newSegment);
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
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
        StringUtils.toUtf8("corrupt-payload")
    );

    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    Assert.assertEquals(
        "wikipedia",
        Iterables.getOnlyElement(sqlSegmentsMetadata.getImmutableDataSourcesWithAllUsedSegments()).getName()
    );
  }

  @Test
  public void testGetUnusedSegmentIntervals()
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());
    int numChangedSegments = sqlSegmentsMetadata.markAsUnusedAllSegmentsInDataSource("wikipedia");
    Assert.assertEquals(2, numChangedSegments);

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval()),
        sqlSegmentsMetadata.getUnusedSegmentIntervals("wikipedia", DateTimes.of("3000"), 1)
    );

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval(), segment1.getInterval()),
        sqlSegmentsMetadata.getUnusedSegmentIntervals("wikipedia", DateTimes.of("3000"), 5)
    );
  }

  @Test(timeout = 60_000)
  public void testMarkAsUnusedAllSegmentsInDataSource() throws IOException, InterruptedException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment = createNewSegment1(newDataSource);

    publisher.publishSegment(newSegment);

    awaitDataSourceAppeared(newDataSource);
    int numChangedSegments = sqlSegmentsMetadata.markAsUnusedAllSegmentsInDataSource(newDataSource);
    Assert.assertEquals(1, numChangedSegments);
    awaitDataSourceDisappeared(newDataSource);
    Assert.assertNull(sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments(newDataSource));
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
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

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
    Assert.assertNotNull(sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments(newDataSource));

    Assert.assertTrue(sqlSegmentsMetadata.markSegmentAsUnused(newSegment.getId().toString()));
    awaitDataSourceDisappeared(newDataSource);
    Assert.assertNull(sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments(newDataSource));
  }

  private void awaitDataSourceAppeared(String newDataSource) throws InterruptedException
  {
    while (sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments(newDataSource) == null) {
      Thread.sleep(1000);
    }
  }

  private void awaitDataSourceDisappeared(String dataSource) throws InterruptedException
  {
    while (sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments(dataSource) != null) {
      Thread.sleep(1000);
    }
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegments() throws Exception
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

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

    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
    Assert.assertEquals(2, sqlSegmentsMetadata.markAsUsedNonOvershadowedSegments(newDataSource, segmentIds));
    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment1, newSegment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test(expected = UnknownSegmentIdException.class)
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidDataSource() throws Exception
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment1(newDataSource);

    publish(newSegment1, false);
    publish(newSegment2, false);
    final ImmutableSet<String> segmentIds =
        ImmutableSet.of(newSegment1.getId().toString(), newSegment2.getId().toString());
    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
    // none of the segments are in data source
    Assert.assertEquals(0, sqlSegmentsMetadata.markAsUsedNonOvershadowedSegments("wrongDataSource", segmentIds));
  }

  @Test(expected = UnknownSegmentIdException.class)
  public void testMarkAsUsedNonOvershadowedSegmentsWithInvalidSegmentIds() throws UnknownSegmentIdException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment1(newDataSource);

    final ImmutableSet<String> segmentIds =
        ImmutableSet.of(newSegment1.getId().toString(), newSegment2.getId().toString());
    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
    // none of the segments are in data source
    Assert.assertEquals(0, sqlSegmentsMetadata.markAsUsedNonOvershadowedSegments(newDataSource, segmentIds));
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInInterval() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

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

    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );

    // 2 out of 3 segments match the interval
    Assert.assertEquals(2, sqlSegmentsMetadata.markAsUsedNonOvershadowedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment1, newSegment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMarkAsUsedNonOvershadowedSegmentsInIntervalWithInvalidInterval() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment2(newDataSource);

    publish(newSegment1, false);
    publish(newSegment2, false);
    // invalid interval start > end
    final Interval theInterval = Intervals.of("2017-10-22T00:00:00.000/2017-10-02T00:00:00.000");
    sqlSegmentsMetadata.markAsUsedNonOvershadowedSegmentsInInterval(newDataSource, theInterval);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInIntervalWithOverlappingInterval() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

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

    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );

    // 1 out of 3 segments match the interval, other 2 overlap, only the segment fully contained will be marked unused
    Assert.assertEquals(1, sqlSegmentsMetadata.markAsUsedNonOvershadowedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test
  public void testMarkSegmentsAsUnused() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment1(newDataSource);

    publisher.publishSegment(newSegment1);
    publisher.publishSegment(newSegment2);
    final ImmutableSet<String> segmentIds =
        ImmutableSet.of(newSegment1.getId().toString(), newSegment1.getId().toString());

    Assert.assertEquals(segmentIds.size(), sqlSegmentsMetadata.markSegmentsAsUnused(newDataSource, segmentIds));
    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test
  public void testMarkSegmentsAsUnusedInvalidDataSource() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment1(newDataSource);

    publisher.publishSegment(newSegment1);
    publisher.publishSegment(newSegment2);
    final ImmutableSet<String> segmentIds =
        ImmutableSet.of(newSegment1.getId().toString(), newSegment2.getId().toString());
    // none of the segments are in data source
    Assert.assertEquals(0, sqlSegmentsMetadata.markSegmentsAsUnused("wrongDataSource", segmentIds));
    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment1, newSegment2),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test
  public void testMarkAsUnusedSegmentsInInterval() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

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
    Assert.assertEquals(2, sqlSegmentsMetadata.markAsUnusedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment3),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMarkAsUnusedSegmentsInIntervalWithInvalidInterval() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

    final String newDataSource = "wikipedia2";
    final DataSegment newSegment1 = createNewSegment1(newDataSource);

    final DataSegment newSegment2 = createNewSegment2(newDataSource);

    publisher.publishSegment(newSegment1);
    publisher.publishSegment(newSegment2);
    // invalid interval start > end
    final Interval theInterval = Intervals.of("2017-10-22T00:00:00.000/2017-10-02T00:00:00.000");
    sqlSegmentsMetadata.markAsUnusedSegmentsInInterval(newDataSource, theInterval);
  }

  @Test
  public void testMarkAsUnusedSegmentsInIntervalWithOverlappingInterval() throws IOException
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());

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
    Assert.assertEquals(1, sqlSegmentsMetadata.markAsUnusedSegmentsInInterval(newDataSource, theInterval));

    sqlSegmentsMetadata.poll();
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2, newSegment1, newSegment3),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test
  public void testStopAndStart()
  {
    // Simulate successive losing and getting the coordinator leadership
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.stopPollingDatabasePeriodically();
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.stopPollingDatabasePeriodically();
  }
}
