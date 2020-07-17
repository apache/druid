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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedPartialShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwritePartialShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IndexerSQLMetadataStorageCoordinatorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  private final DataSegment defaultSegment = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "version",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(0),
      9,
      100
  );

  private final DataSegment defaultSegment2 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "version",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(1),
      9,
      100
  );

  private final DataSegment defaultSegment3 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
      "version",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      NoneShardSpec.instance(),
      9,
      100
  );

  // Overshadows defaultSegment, defaultSegment2
  private final DataSegment defaultSegment4 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(0),
      9,
      100
  );

  private final DataSegment numberedSegment0of0 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(0, 0),
      9,
      100
  );

  private final DataSegment numberedSegment1of0 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(1, 0),
      9,
      100
  );

  private final DataSegment numberedSegment2of0 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(2, 0),
      9,
      100
  );

  private final DataSegment numberedSegment2of1 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(2, 1),
      9,
      100
  );

  private final DataSegment numberedSegment3of1 = new DataSegment(
      "fooDataSource",
      Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(3, 1),
      9,
      100
  );

  private final Set<DataSegment> SEGMENTS = ImmutableSet.of(defaultSegment, defaultSegment2);
  private final AtomicLong metadataUpdateCounter = new AtomicLong();
  private IndexerSQLMetadataStorageCoordinator coordinator;
  private TestDerbyConnector derbyConnector;

  @Before
  public void setUp()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    mapper.registerSubtypes(LinearShardSpec.class, NumberedShardSpec.class, HashBasedNumberedShardSpec.class);
    derbyConnector.createDataSourceTable();
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentTable();
    derbyConnector.createPendingSegmentsTable();
    metadataUpdateCounter.set(0);
    coordinator = new IndexerSQLMetadataStorageCoordinator(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector
    )
    {
      @Override
      protected DataSourceMetadataUpdateResult updateDataSourceMetadataWithHandle(
          Handle handle,
          String dataSource,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        // Count number of times this method is called.
        metadataUpdateCounter.getAndIncrement();
        return super.updateDataSourceMetadataWithHandle(handle, dataSource, startMetadata, endMetadata);
      }
    };
  }

  private void markAllSegmentsUnused()
  {
    for (final DataSegment segment : SEGMENTS) {
      Assert.assertEquals(
          1,
          (int) derbyConnector.getDBI().<Integer>withHandle(
              new HandleCallback<Integer>()
              {
                @Override
                public Integer withHandle(Handle handle)
                {
                  String request = StringUtils.format(
                      "UPDATE %s SET used = false WHERE id = :id",
                      derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable()
                  );
                  return handle.createStatement(request).bind("id", segment.getId().toString()).execute();
                }
              }
          )
      );
    }
  }

  private List<String> retrieveUsedSegmentIds()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle)
          {
            return handle.createQuery("SELECT id FROM " + table + " WHERE used = true ORDER BY id")
                         .map(StringMapper.FIRST)
                         .list();
          }
        }
    );
  }

  @Test
  public void testSimpleAnnounce() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    for (DataSegment segment : SEGMENTS) {
      Assert.assertArrayEquals(
          mapper.writeValueAsString(segment).getBytes(StandardCharsets.UTF_8),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getId().toString()
          )
      );
    }

    Assert.assertEquals(
        ImmutableList.of(defaultSegment.getId().toString(), defaultSegment2.getId().toString()),
        retrieveUsedSegmentIds()
    );

    // Should not update dataSource metadata.
    Assert.assertEquals(0, metadataUpdateCounter.get());
  }

  @Test
  public void testOvershadowingAnnounce() throws IOException
  {
    final ImmutableSet<DataSegment> segments = ImmutableSet.of(defaultSegment, defaultSegment2, defaultSegment4);

    coordinator.announceHistoricalSegments(segments);

    for (DataSegment segment : segments) {
      Assert.assertArrayEquals(
          mapper.writeValueAsString(segment).getBytes(StandardCharsets.UTF_8),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getId().toString()
          )
      );
    }

    Assert.assertEquals(ImmutableList.of(defaultSegment4.getId().toString()), retrieveUsedSegmentIds());
  }

  @Test
  public void testTransactionalAnnounceSuccess() throws IOException
  {
    // Insert first segment.
    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment).getBytes(StandardCharsets.UTF_8),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment.getId().toString()
        )
    );

    // Insert second segment.
    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment2)), result2);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment2).getBytes(StandardCharsets.UTF_8),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment2.getId().toString()
        )
    );

    // Examine metadata.
    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );

    // Should only be tried once per call.
    Assert.assertEquals(2, metadataUpdateCounter.get());
  }

  @Test
  public void testTransactionalAnnounceRetryAndSuccess() throws IOException
  {
    final AtomicLong attemptCounter = new AtomicLong();

    final IndexerSQLMetadataStorageCoordinator failOnceCoordinator = new IndexerSQLMetadataStorageCoordinator(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector
    )
    {
      @Override
      protected DataSourceMetadataUpdateResult updateDataSourceMetadataWithHandle(
          Handle handle,
          String dataSource,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        metadataUpdateCounter.getAndIncrement();
        if (attemptCounter.getAndIncrement() == 0) {
          return DataSourceMetadataUpdateResult.TRY_AGAIN;
        } else {
          return super.updateDataSourceMetadataWithHandle(handle, dataSource, startMetadata, endMetadata);
        }
      }
    };

    // Insert first segment.
    final SegmentPublishResult result1 = failOnceCoordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment).getBytes(StandardCharsets.UTF_8),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment.getId().toString()
        )
    );

    // Reset attempt counter to induce another failure.
    attemptCounter.set(0);

    // Insert second segment.
    final SegmentPublishResult result2 = failOnceCoordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment2)), result2);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment2).getBytes(StandardCharsets.UTF_8),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment2.getId().toString()
        )
    );

    // Examine metadata.
    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        failOnceCoordinator.retrieveDataSourceMetadata("fooDataSource")
    );

    // Should be tried twice per call.
    Assert.assertEquals(4, metadataUpdateCounter.get());
  }

  @Test
  public void testTransactionalAnnounceFailDbNullWantNotNull() throws IOException
  {
    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Aborting transaction!"), result1);

    // Should only be tried once.
    Assert.assertEquals(1, metadataUpdateCounter.get());
  }

  @Test
  public void testTransactionalAnnounceFailDbNotNullWantNull() throws IOException
  {
    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Aborting transaction!"), result2);

    // Should only be tried once per call.
    Assert.assertEquals(2, metadataUpdateCounter.get());
  }

  @Test
  public void testTransactionalAnnounceFailDbNotNullWantDifferent() throws IOException
  {
    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(ImmutableMap.of("foo", "qux")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Aborting transaction!"), result2);

    // Should only be tried once per call.
    Assert.assertEquals(2, metadataUpdateCounter.get());
  }

  @Test
  public void testSimpleUsedList() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval(),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testMultiIntervalUsedList() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    coordinator.announceHistoricalSegments(ImmutableSet.of(defaultSegment3));

    Assertions.assertThat(
        coordinator.retrieveUsedSegmentsForIntervals(
            defaultSegment.getDataSource(),
            ImmutableList.of(defaultSegment.getInterval()),
            Segments.ONLY_VISIBLE
        )
    ).containsOnlyOnce(SEGMENTS.toArray(new DataSegment[0]));

    Assertions.assertThat(
        coordinator.retrieveUsedSegmentsForIntervals(
            defaultSegment.getDataSource(),
            ImmutableList.of(defaultSegment3.getInterval()),
            Segments.ONLY_VISIBLE
        )
    ).containsOnlyOnce(defaultSegment3);

    Assertions.assertThat(
        coordinator.retrieveUsedSegmentsForIntervals(
            defaultSegment.getDataSource(),
            ImmutableList.of(defaultSegment.getInterval(), defaultSegment3.getInterval()),
            Segments.ONLY_VISIBLE
        )
    ).containsOnlyOnce(defaultSegment, defaultSegment2, defaultSegment3);

    //case to check no duplication if two intervals overlapped with the interval of same segment.
    Assertions.assertThat(
        coordinator.retrieveUsedSegmentsForIntervals(
            defaultSegment.getDataSource(),
            ImmutableList.of(
                Intervals.of("2015-01-03T00Z/2015-01-03T05Z"),
                Intervals.of("2015-01-03T09Z/2015-01-04T00Z")
            ),
            Segments.ONLY_VISIBLE
        )
    ).containsOnlyOnce(defaultSegment3);
  }

  @Test
  public void testSimpleUnusedList() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval()
            )
        )
    );
  }


  @Test
  public void testUsedOverlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Set<DataSegment> actualSegments = ImmutableSet.copyOf(
        coordinator.retrieveUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            Intervals.of("2014-12-31T23:59:59.999Z/2015-01-01T00:00:00.001Z"), // end is exclusive
            Segments.ONLY_VISIBLE
        )
    );
    Assert.assertEquals(
        SEGMENTS,
        actualSegments
    );
  }


  @Test
  public void testUsedOverlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                Intervals.of("2015-1-1T23:59:59.999Z/2015-02-01T00Z"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testUsedOutOfBoundsLow() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertTrue(
        coordinator.retrieveUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().minus(1), defaultSegment.getInterval().getStart()),
            Segments.ONLY_VISIBLE
        ).isEmpty()
    );
  }


  @Test
  public void testUsedOutOfBoundsHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertTrue(
        coordinator.retrieveUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getEnd(), defaultSegment.getInterval().getEnd().plusDays(10)),
            Segments.ONLY_VISIBLE
        ).isEmpty()
    );
  }

  @Test
  public void testUsedWithinBoundsEnd() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().minusMillis(1)),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testUsedOverlapEnd() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plusMillis(1)),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }


  @Test
  public void testUnusedOverlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(
                defaultSegment.getInterval().getStart().minus(1),
                defaultSegment.getInterval().getStart().plus(1)
            )
        ).isEmpty()
    );
  }

  @Test
  public void testUnusedUnderlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().plus(1), defaultSegment.getInterval().getEnd())
        ).isEmpty()
    );
  }


  @Test
  public void testUnusedUnderlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart(), defaultSegment.getInterval().getEnd().minus(1))
        ).isEmpty()
    );
  }

  @Test
  public void testUnusedOverlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval().withStart(defaultSegment.getInterval().getEnd().minus(1))
        ).isEmpty()
    );
  }

  @Test
  public void testUnusedBigOverlap() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                Intervals.of("2000/2999")
            )
        )
    );
  }

  @Test
  public void testUnusedLowRange() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minus(1))
            )
        )
    );
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minusYears(1))
            )
        )
    );
  }

  @Test
  public void testUnusedHighRange() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plus(1))
            )
        )
    );
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plusYears(1))
            )
        )
    );
  }

  @Test
  public void testDeleteDataSourceMetadata() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar"))
    );

    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );

    Assert.assertFalse("deleteInvalidDataSourceMetadata", coordinator.deleteDataSourceMetadata("nonExistentDS"));
    Assert.assertTrue("deleteValidDataSourceMetadata", coordinator.deleteDataSourceMetadata("fooDataSource"));

    Assert.assertNull("getDataSourceMetadataNullAfterDelete", coordinator.retrieveDataSourceMetadata("fooDataSource"));
  }

  @Test
  public void testSingleAdditionalNumberedShardWithNoCorePartitions() throws IOException
  {
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment0of0));
  }

  @Test
  public void testMultipleAdditionalNumberedShardsWithNoCorePartitions() throws IOException
  {
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment0of0, numberedSegment1of0, numberedSegment2of0));
  }

  @Test
  public void testSingleAdditionalNumberedShardWithOneCorePartition() throws IOException
  {
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment2of1));
  }

  @Test
  public void testMultipleAdditionalNumberedShardsWithOneCorePartition() throws IOException
  {
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment2of1, numberedSegment3of1));
  }

  private void additionalNumberedShardTest(Set<DataSegment> segments) throws IOException
  {
    coordinator.announceHistoricalSegments(segments);

    for (DataSegment segment : segments) {
      Assert.assertArrayEquals(
          mapper.writeValueAsString(segment).getBytes(StandardCharsets.UTF_8),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getId().toString()
          )
      );
    }

    Assert.assertEquals(
        segments.stream().map(segment -> segment.getId().toString()).collect(Collectors.toList()),
        retrieveUsedSegmentIds()
    );

    // Should not update dataSource metadata.
    Assert.assertEquals(0, metadataUpdateCounter.get());
  }

  @Test
  public void testAllocatePendingSegment()
  {
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        "version",
        false
    );

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version", identifier.toString());

    final SegmentIdWithShardSpec identifier1 = coordinator.allocatePendingSegment(
        dataSource,
        "seq",
        identifier.toString(),
        interval,
        partialShardSpec,
        identifier.getVersion(),
        false
    );

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_1", identifier1.toString());

    final SegmentIdWithShardSpec identifier2 = coordinator.allocatePendingSegment(
        dataSource,
        "seq",
        identifier1.toString(),
        interval,
        partialShardSpec,
        identifier1.getVersion(),
        false
    );

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_2", identifier2.toString());

    final SegmentIdWithShardSpec identifier3 = coordinator.allocatePendingSegment(
        dataSource,
        "seq",
        identifier1.toString(),
        interval,
        partialShardSpec,
        identifier1.getVersion(),
        false
    );

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_2", identifier3.toString());
    Assert.assertEquals(identifier2, identifier3);

    final SegmentIdWithShardSpec identifier4 = coordinator.allocatePendingSegment(
        dataSource,
        "seq1",
        null,
        interval,
        partialShardSpec,
        "version",
        false
    );

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_3", identifier4.toString());
  }

  @Test
  public void testDeletePendingSegment() throws InterruptedException
  {
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    String prevSegmentId = null;

    final DateTime begin = DateTimes.nowUtc();

    for (int i = 0; i < 10; i++) {
      final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
          dataSource,
          "seq",
          prevSegmentId,
          interval,
          partialShardSpec,
          "version",
          false
      );
      prevSegmentId = identifier.toString();
    }
    Thread.sleep(100);

    final DateTime secondBegin = DateTimes.nowUtc();
    for (int i = 0; i < 5; i++) {
      final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
          dataSource,
          "seq",
          prevSegmentId,
          interval,
          partialShardSpec,
          "version",
          false
      );
      prevSegmentId = identifier.toString();
    }

    final int numDeleted = coordinator.deletePendingSegmentsCreatedInInterval(dataSource, new Interval(begin, secondBegin));
    Assert.assertEquals(10, numDeleted);
  }

  @Test
  public void testAllocatePendingSegmentsWithOvershadowingSegments() throws IOException
  {
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    String prevSegmentId = null;

    for (int i = 0; i < 10; i++) {
      final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
          dataSource,
          "seq",
          prevSegmentId,
          interval,
          new NumberedOverwritePartialShardSpec(0, 1, (short) (i + 1)),
          "version",
          false
      );
      Assert.assertEquals(
          StringUtils.format(
              "ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version%s",
              "_" + (i + PartitionIds.NON_ROOT_GEN_START_PARTITION_ID)
          ),
          identifier.toString()
      );
      prevSegmentId = identifier.toString();
      final Set<DataSegment> toBeAnnounced = Collections.singleton(
          new DataSegment(
              identifier.getDataSource(),
              identifier.getInterval(),
              identifier.getVersion(),
              null,
              Collections.emptyList(),
              Collections.emptyList(),
              ((NumberedOverwriteShardSpec) identifier.getShardSpec()).withAtomicUpdateGroupSize(1),
              0,
              10L
          )
      );
      final Set<DataSegment> announced = coordinator.announceHistoricalSegments(toBeAnnounced);

      Assert.assertEquals(toBeAnnounced, announced);
    }

    final Collection<DataSegment> visibleSegments =
        coordinator.retrieveUsedSegmentsForInterval(dataSource, interval, Segments.ONLY_VISIBLE);

    Assert.assertEquals(1, visibleSegments.size());
    Assert.assertEquals(
        new DataSegment(
            dataSource,
            interval,
            "version",
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            new NumberedOverwriteShardSpec(
                9 + PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
                0,
                1,
                (short) 9,
                (short) 1
            ),
            0,
            10L
        ),
        Iterables.getOnlyElement(visibleSegments)
    );
  }

  @Test
  public void testAllocatePendingSegmentsForHashBasedNumberedShardSpec() throws IOException
  {
    final PartialShardSpec partialShardSpec = new HashBasedNumberedPartialShardSpec(null, 2, 5);
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");

    SegmentIdWithShardSpec id = coordinator.allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        "version",
        true
    );

    HashBasedNumberedShardSpec shardSpec = (HashBasedNumberedShardSpec) id.getShardSpec();
    Assert.assertEquals(0, shardSpec.getPartitionNum());
    Assert.assertEquals(0, shardSpec.getNumCorePartitions());
    Assert.assertEquals(5, shardSpec.getNumBuckets());

    coordinator.announceHistoricalSegments(
        Collections.singleton(
            new DataSegment(
                id.getDataSource(),
                id.getInterval(),
                id.getVersion(),
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                id.getShardSpec(),
                0,
                10L
            )
        )
    );

    id = coordinator.allocatePendingSegment(
        dataSource,
        "seq2",
        null,
        interval,
        partialShardSpec,
        "version",
        true
    );

    shardSpec = (HashBasedNumberedShardSpec) id.getShardSpec();
    Assert.assertEquals(1, shardSpec.getPartitionNum());
    Assert.assertEquals(0, shardSpec.getNumCorePartitions());
    Assert.assertEquals(5, shardSpec.getNumBuckets());

    coordinator.announceHistoricalSegments(
        Collections.singleton(
            new DataSegment(
                id.getDataSource(),
                id.getInterval(),
                id.getVersion(),
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                id.getShardSpec(),
                0,
                10L
            )
        )
    );

    id = coordinator.allocatePendingSegment(
        dataSource,
        "seq3",
        null,
        interval,
        new HashBasedNumberedPartialShardSpec(null, 2, 3),
        "version",
        true
    );

    shardSpec = (HashBasedNumberedShardSpec) id.getShardSpec();
    Assert.assertEquals(2, shardSpec.getPartitionNum());
    Assert.assertEquals(0, shardSpec.getNumCorePartitions());
    Assert.assertEquals(3, shardSpec.getNumBuckets());
  }

  @Test
  public void testAddNumberedShardSpecAfterSingleDimensionsShardSpecWithUnknownCorePartitionSize() throws IOException
  {
    final String datasource = "datasource";
    final Interval interval = Intervals.of("2020-01-01/P1D");
    final String version = "version";
    final List<String> dimensions = ImmutableList.of("dim");
    final List<String> metrics = ImmutableList.of("met");
    final Set<DataSegment> originalSegments = new HashSet<>();
    for (int i = 0; i < 6; i++) {
      final String start = i == 0 ? null : String.valueOf(i - 1);
      final String end = i == 5 ? null : String.valueOf(i);
      originalSegments.add(
          new DataSegment(
              datasource,
              interval,
              version,
              ImmutableMap.of(),
              dimensions,
              metrics,
              new SingleDimensionShardSpec(
                  "dim",
                  start,
                  end,
                  i,
                  null // emulate shardSpecs created in older versions of Druid
              ),
              9,
              10L
          )
      );
    }
    coordinator.announceHistoricalSegments(originalSegments);
    final SegmentIdWithShardSpec id = coordinator.allocatePendingSegment(
        datasource,
        "seq",
        null,
        interval,
        NumberedPartialShardSpec.instance(),
        version,
        false
    );
    Assert.assertNull(id);
  }
}
