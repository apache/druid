/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.ObjectMetadata;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IndexerSQLMetadataStorageCoordinatorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final DataSegment defaultSegment = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "version",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(0),
      9,
      100
  );

  private final DataSegment defaultSegment2 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "version",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(1),
      9,
      100
  );

  private final DataSegment defaultSegment3 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-03T00Z/2015-01-04T00Z"),
      "version",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      NoneShardSpec.instance(),
      9,
      100
  );

  // Overshadows defaultSegment, defaultSegment2
  private final DataSegment defaultSegment4 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(0),
      9,
      100
  );

  private final DataSegment numberedSegment0of0 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(0, 0),
      9,
      100
  );

  private final DataSegment numberedSegment1of0 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(1, 0),
      9,
      100
  );

  private final DataSegment numberedSegment2of0 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(2, 0),
      9,
      100
  );

  private final DataSegment numberedSegment2of1 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(2, 1),
      9,
      100
  );

  private final DataSegment numberedSegment3of1 = new DataSegment(
      "fooDataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "zversion",
      ImmutableMap.<String, Object>of(),
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
    mapper.registerSubtypes(LinearShardSpec.class);
    derbyConnector.createDataSourceTable();
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentTable();
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

  private void unUseSegment()
  {
    for (final DataSegment segment : SEGMENTS) {
      Assert.assertEquals(
          1, (int) derbyConnector.getDBI().<Integer>withHandle(
              new HandleCallback<Integer>()
              {
                @Override
                public Integer withHandle(Handle handle) throws Exception
                {
                  return handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = false WHERE id = :id",
                          derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable()
                      )
                  ).bind("id", segment.getIdentifier()).execute();
                }
              }
          )
      );
    }
  }

  private List<String> getUsedIdentifiers()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle) throws Exception
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
          mapper.writeValueAsString(segment).getBytes("UTF-8"),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getIdentifier()
          )
      );
    }

    Assert.assertEquals(
        ImmutableList.of(defaultSegment.getIdentifier(), defaultSegment2.getIdentifier()),
        getUsedIdentifiers()
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
          mapper.writeValueAsString(segment).getBytes("UTF-8"),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getIdentifier()
          )
      );
    }

    Assert.assertEquals(ImmutableList.of(defaultSegment4.getIdentifier()), getUsedIdentifiers());
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
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(defaultSegment), true), result1);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment).getBytes("UTF-8"),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment.getIdentifier()
        )
    );

    // Insert second segment.
    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(defaultSegment2), true), result2);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment2).getBytes("UTF-8"),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment2.getIdentifier()
        )
    );

    // Examine metadata.
    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        coordinator.getDataSourceMetadata("fooDataSource")
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
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(defaultSegment), true), result1);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment).getBytes("UTF-8"),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment.getIdentifier()
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
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(defaultSegment2), true), result2);

    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment2).getBytes("UTF-8"),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment2.getIdentifier()
        )
    );

    // Examine metadata.
    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        failOnceCoordinator.getDataSourceMetadata("fooDataSource")
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
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.<DataSegment>of(), false), result1);

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
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(defaultSegment), true), result1);

    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.<DataSegment>of(), false), result2);

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
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(defaultSegment), true), result1);

    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(ImmutableMap.of("foo", "qux")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.<DataSegment>of(), false), result2);

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
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval()
            )
        )
    );
  }

  @Test
  public void testMultiIntervalUsedList() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    coordinator.announceHistoricalSegments(ImmutableSet.of(defaultSegment3));

    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.getUsedSegmentsForIntervals(
                defaultSegment.getDataSource(),
                ImmutableList.of(defaultSegment.getInterval())
            )
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(defaultSegment3),
        ImmutableSet.copyOf(
            coordinator.getUsedSegmentsForIntervals(
                defaultSegment.getDataSource(),
                ImmutableList.of(defaultSegment3.getInterval())
            )
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(defaultSegment, defaultSegment2, defaultSegment3),
        ImmutableSet.copyOf(
            coordinator.getUsedSegmentsForIntervals(
                defaultSegment.getDataSource(),
                ImmutableList.of(defaultSegment.getInterval(), defaultSegment3.getInterval())
            )
        )
    );

    //case to check no duplication if two intervals overlapped with the interval of same segment.
    Assert.assertEquals(
        ImmutableList.of(defaultSegment3),
        coordinator.getUsedSegmentsForIntervals(
            defaultSegment.getDataSource(),
            ImmutableList.of(
                Interval.parse("2015-01-03T00Z/2015-01-03T05Z"),
                Interval.parse("2015-01-03T09Z/2015-01-04T00Z")
            )
        )
    );
  }

  @Test
  public void testSimpleUnUsedList() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
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
        coordinator.getUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            Interval.parse("2014-12-31T23:59:59.999Z/2015-01-01T00:00:00.001Z") // end is exclusive
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
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                Interval.parse("2015-1-1T23:59:59.999Z/2015-02-01T00Z")
            )
        )
    );
  }

  @Test
  public void testUsedOutOfBoundsLow() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertTrue(
        coordinator.getUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().minus(1), defaultSegment.getInterval().getStart())
        ).isEmpty()
    );
  }


  @Test
  public void testUsedOutOfBoundsHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    Assert.assertTrue(
        coordinator.getUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getEnd(), defaultSegment.getInterval().getEnd().plusDays(10))
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
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().minusMillis(1))
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
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plusMillis(1))
            )
        )
    );
  }


  @Test
  public void testUnUsedOverlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(
                defaultSegment.getInterval().getStart().minus(1),
                defaultSegment.getInterval().getStart().plus(1)
            )
        ).isEmpty()
    );
  }

  @Test
  public void testUnUsedUnderlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().plus(1), defaultSegment.getInterval().getEnd())
        ).isEmpty()
    );
  }


  @Test
  public void testUnUsedUnderlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart(), defaultSegment.getInterval().getEnd().minus(1))
        ).isEmpty()
    );
  }

  @Test
  public void testUnUsedOverlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval().withStart(defaultSegment.getInterval().getEnd().minus(1))
        ).isEmpty()
    );
  }

  @Test
  public void testUnUsedBigOverlap() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                Interval.parse("2000/2999")
            )
        )
    );
  }

  @Test
  public void testUnUsedLowRange() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minus(1))
            )
        )
    );
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minusYears(1))
            )
        )
    );
  }

  @Test
  public void testUnUsedHighRange() throws IOException
  {
    coordinator.announceHistoricalSegments(SEGMENTS);
    unUseSegment();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plus(1))
            )
        )
    );
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
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
        coordinator.getDataSourceMetadata("fooDataSource")
    );

    Assert.assertFalse("deleteInvalidDataSourceMetadata", coordinator.deleteDataSourceMetadata("nonExistentDS"));
    Assert.assertTrue("deleteValidDataSourceMetadata", coordinator.deleteDataSourceMetadata("fooDataSource"));

    Assert.assertNull("getDataSourceMetadataNullAfterDelete", coordinator.getDataSourceMetadata("fooDataSource"));
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
          mapper.writeValueAsString(segment).getBytes("UTF-8"),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getIdentifier()
          )
      );
    }

    Assert.assertEquals(
        segments.stream().map(DataSegment::getIdentifier).collect(Collectors.toList()),
        getUsedIdentifiers()
    );

    // Should not update dataSource metadata.
    Assert.assertEquals(0, metadataUpdateCounter.get());
  }
}
