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
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class IndexerSQLMetadataStorageCoordinatorTest
{
  private static final int MAX_SQL_MEATADATA_RETRY_FOR_TEST = 2;

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

  private final DataSegment eternitySegment = new DataSegment(
      "fooDataSource",
      Intervals.ETERNITY,
      "version",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(0),
      9,
      100
  );


  private final DataSegment firstHalfEternityRangeSegment = new DataSegment(
      "fooDataSource",
      new Interval(DateTimes.MIN, DateTimes.of("3000")),
      "version",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(0),
      9,
      100
  );

  private final DataSegment secondHalfEternityRangeSegment = new DataSegment(
      "fooDataSource",
      new Interval(DateTimes.of("1970"), DateTimes.MAX),
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

  private final DataSegment existingSegment1 = new DataSegment(
      "fooDataSource",
      Intervals.of("1994-01-01T00Z/1994-01-02T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(1, 1),
      9,
      100
  );

  private final DataSegment existingSegment2 = new DataSegment(
      "fooDataSource",
      Intervals.of("1994-01-02T00Z/1994-01-03T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(1, 1),
      9,
      100
  );

  private final DataSegment hugeTimeRangeSegment1 = new DataSegment(
      "hugeTimeRangeDataSource",
      Intervals.of("-9994-01-02T00Z/1994-01-03T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(0, 1),
      9,
      100
  );

  private final DataSegment hugeTimeRangeSegment2 = new DataSegment(
      "hugeTimeRangeDataSource",
      Intervals.of("2994-01-02T00Z/2994-01-03T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(0, 1),
      9,
      100
  );

  private final DataSegment hugeTimeRangeSegment3 = new DataSegment(
      "hugeTimeRangeDataSource",
      Intervals.of("29940-01-02T00Z/29940-01-03T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(0, 1),
      9,
      100
  );

  private final DataSegment hugeTimeRangeSegment4 = new DataSegment(
      "hugeTimeRangeDataSource",
      Intervals.of("1990-01-01T00Z/19940-01-01T00Z"),
      "zversion",
      ImmutableMap.of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new NumberedShardSpec(0, 1),
      9,
      100
  );

  private final Set<DataSegment> SEGMENTS = ImmutableSet.of(defaultSegment, defaultSegment2);
  private final AtomicLong metadataUpdateCounter = new AtomicLong();
  private final AtomicLong segmentTableDropUpdateCounter = new AtomicLong();

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
    segmentTableDropUpdateCounter.set(0);
    coordinator = new IndexerSQLMetadataStorageCoordinator(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector
    )
    {
      @Override
      protected DataStoreMetadataUpdateResult updateDataSourceMetadataWithHandle(
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

      @Override
      protected DataStoreMetadataUpdateResult dropSegmentsWithHandle(
          final Handle handle,
          final Collection<DataSegment> segmentsToDrop,
          final String dataSource
      )
      {
        // Count number of times this method is called.
        segmentTableDropUpdateCounter.getAndIncrement();
        return super.dropSegmentsWithHandle(handle, segmentsToDrop, dataSource);
      }

      @Override
      public int getSqlMetadataMaxRetry()
      {
        return MAX_SQL_MEATADATA_RETRY_FOR_TEST;
      }
    };
  }

  private void markAllSegmentsUnused()
  {
    markAllSegmentsUnused(SEGMENTS);
  }

  private void markAllSegmentsUnused(Set<DataSegment> segments)
  {
    for (final DataSegment segment : segments) {
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

  private void markAllSegmentsUsed(Set<DataSegment> segments)
  {
    for (final DataSegment segment : segments) {
      Assert.assertEquals(
          1,
          (int) derbyConnector.getDBI().<Integer>withHandle(
              new HandleCallback<Integer>()
              {
                @Override
                public Integer withHandle(Handle handle)
                {
                  String request = StringUtils.format(
                      "UPDATE %s SET used = true WHERE id = :id",
                      derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable()
                  );
                  return handle.createStatement(request).bind("id", segment.getId().toString()).execute();
                }
              }
          )
      );
    }
  }

  private List<String> retrievePendingSegmentIds()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getPendingSegmentsTable();
    return derbyConnector.retryWithHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle)
          {
            return handle.createQuery("SELECT id FROM " + table + "  ORDER BY id")
                         .map(StringMapper.FIRST)
                         .list();
          }
        }
    );
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

  private List<String> retrieveUnusedSegmentIds()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle)
          {
            return handle.createQuery("SELECT id FROM " + table + " WHERE used = false ORDER BY id")
                         .map(StringMapper.FIRST)
                         .list();
          }
        }
    );
  }


  private Boolean insertUsedSegments(Set<DataSegment> dataSegments)
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            PreparedBatch preparedBatch = handle.prepareBatch(
                StringUtils.format(
                    "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                    table,
                    derbyConnector.getQuoteString()
                )
            );
            for (DataSegment segment : dataSegments) {
              preparedBatch.add()
                           .bind("id", segment.getId().toString())
                           .bind("dataSource", segment.getDataSource())
                           .bind("created_date", DateTimes.nowUtc().toString())
                           .bind("start", segment.getInterval().getStart().toString())
                           .bind("end", segment.getInterval().getEnd().toString())
                           .bind("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? false : true)
                           .bind("version", segment.getVersion())
                           .bind("used", true)
                           .bind("payload", mapper.writeValueAsBytes(segment));
            }

            final int[] affectedRows = preparedBatch.execute();
            final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
            if (!succeeded) {
              throw new ISE("Failed to publish segments to DB");
            }
            return true;
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
  public void testAnnounceHistoricalSegments() throws IOException
  {
    Set<DataSegment> segments = new HashSet<>();
    for (int i = 0; i < 105; i++) {
      segments.add(
          new DataSegment(
              "fooDataSource",
              Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
              "version",
              ImmutableMap.of(),
              ImmutableList.of("dim1"),
              ImmutableList.of("m1"),
              new LinearShardSpec(i),
              9,
              100
          )
      );
    }

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

    List<String> segmentIds = segments.stream().map(segment -> segment.getId().toString()).collect(Collectors.toList());
    segmentIds.sort(Comparator.naturalOrder());
    Assert.assertEquals(
        segmentIds,
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
        ImmutableSet.of(),
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
        ImmutableSet.of(),
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
      protected DataStoreMetadataUpdateResult updateDataSourceMetadataWithHandle(
          Handle handle,
          String dataSource,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        metadataUpdateCounter.getAndIncrement();
        if (attemptCounter.getAndIncrement() == 0) {
          return DataStoreMetadataUpdateResult.TRY_AGAIN;
        } else {
          return super.updateDataSourceMetadataWithHandle(handle, dataSource, startMetadata, endMetadata);
        }
      }
    };

    // Insert first segment.
    final SegmentPublishResult result1 = failOnceCoordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        ImmutableSet.of(),
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
        ImmutableSet.of(),
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
        ImmutableSet.of(),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Aborting transaction!"), result1);

    // Should only be tried once.
    Assert.assertEquals(1, metadataUpdateCounter.get());
  }

  @Test
  public void testTransactionalAnnounceFailSegmentDropFailWithoutRetry() throws IOException
  {
    insertUsedSegments(ImmutableSet.of(existingSegment1, existingSegment2));

    Assert.assertEquals(
        ImmutableList.of(existingSegment1.getId().toString(), existingSegment2.getId().toString()),
        retrieveUsedSegmentIds()
    );

    DataSegment dataSegmentBar = DataSegment.builder()
                                            .dataSource("bar")
                                            .interval(Intervals.of("2001/P1D"))
                                            .shardSpec(new LinearShardSpec(1))
                                            .version("b")
                                            .size(0)
                                            .build();
    Set<DataSegment> dropSegments = ImmutableSet.of(existingSegment1, existingSegment2, dataSegmentBar);

    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        SEGMENTS,
        dropSegments,
        null,
        null
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Aborting transaction!"), result1);

    // Should only be tried once. Since dropSegmentsWithHandle will return FAILURE (not TRY_AGAIN) as set of
    // segments to drop contains more than one datasource.
    Assert.assertEquals(1, segmentTableDropUpdateCounter.get());

    Assert.assertEquals(
        ImmutableList.of(existingSegment1.getId().toString(), existingSegment2.getId().toString()),
        retrieveUsedSegmentIds()
    );
  }

  @Test
  public void testTransactionalAnnounceSucceedWithSegmentDrop() throws IOException
  {
    insertUsedSegments(ImmutableSet.of(existingSegment1, existingSegment2));

    Assert.assertEquals(
        ImmutableList.of(existingSegment1.getId().toString(), existingSegment2.getId().toString()),
        retrieveUsedSegmentIds()
    );

    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        SEGMENTS,
        ImmutableSet.of(existingSegment1, existingSegment2),
        null,
        null
    );

    Assert.assertEquals(SegmentPublishResult.ok(SEGMENTS), result1);

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
  }

  @Test
  public void testTransactionalAnnounceFailSegmentDropFailWithRetry() throws IOException
  {
    insertUsedSegments(ImmutableSet.of(existingSegment1, existingSegment2));

    Assert.assertEquals(
        ImmutableList.of(existingSegment1.getId().toString(), existingSegment2.getId().toString()),
        retrieveUsedSegmentIds()
    );

    DataSegment nonExistingSegment = defaultSegment4;

    Set<DataSegment> dropSegments = ImmutableSet.of(existingSegment1, nonExistingSegment);

    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        SEGMENTS,
        dropSegments,
        null,
        null
    );
    Assert.assertEquals(SegmentPublishResult.fail(
        "org.apache.druid.metadata.RetryTransactionException: Aborting transaction!"), result1);

    Assert.assertEquals(MAX_SQL_MEATADATA_RETRY_FOR_TEST, segmentTableDropUpdateCounter.get());

    Assert.assertEquals(
        ImmutableList.of(existingSegment1.getId().toString(), existingSegment2.getId().toString()),
        retrieveUsedSegmentIds()
    );
  }

  @Test
  public void testTransactionalAnnounceFailDbNotNullWantNull() throws IOException
  {
    final SegmentPublishResult result1 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        ImmutableSet.of(),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        ImmutableSet.of(),
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
        ImmutableSet.of(),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment2),
        ImmutableSet.of(),
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
  public void testUsedHugeTimeRangeEternityFilter() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment1,
            hugeTimeRangeSegment2,
            hugeTimeRangeSegment3
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(hugeTimeRangeSegment1, hugeTimeRangeSegment2, hugeTimeRangeSegment3),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForIntervals(
                hugeTimeRangeSegment1.getDataSource(),
                Intervals.ONLY_ETERNITY,
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testUsedHugeTimeRangeTrickyFilter1() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment1,
            hugeTimeRangeSegment2,
            hugeTimeRangeSegment3
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(hugeTimeRangeSegment2),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                hugeTimeRangeSegment1.getDataSource(),
                Intervals.of("2900/10000"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testUsedHugeTimeRangeTrickyFilter2() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment1,
            hugeTimeRangeSegment2,
            hugeTimeRangeSegment3
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(hugeTimeRangeSegment2),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                hugeTimeRangeSegment1.getDataSource(),
                Intervals.of("2993/2995"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }


  @Test
  public void testEternitySegmentWithStringComparison() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            eternitySegment
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(eternitySegment),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                eternitySegment.getDataSource(),
                Intervals.of("2020/2021"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testEternityMultipleSegmentWithStringComparison() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            numberedSegment0of0,
            eternitySegment
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(eternitySegment, numberedSegment0of0),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                eternitySegment.getDataSource(),
                Intervals.of("2015/2016"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testFirstHalfEternitySegmentWithStringComparison() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            firstHalfEternityRangeSegment
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(firstHalfEternityRangeSegment),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                firstHalfEternityRangeSegment.getDataSource(),
                Intervals.of("2020/2021"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testFirstHalfEternityMultipleSegmentWithStringComparison() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            numberedSegment0of0,
            firstHalfEternityRangeSegment
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(numberedSegment0of0, firstHalfEternityRangeSegment),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                firstHalfEternityRangeSegment.getDataSource(),
                Intervals.of("2015/2016"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testSecondHalfEternitySegmentWithStringComparison() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            secondHalfEternityRangeSegment
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(secondHalfEternityRangeSegment),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                secondHalfEternityRangeSegment.getDataSource(),
                Intervals.of("2020/2021"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  // Known Issue: https://github.com/apache/druid/issues/12860
  @Ignore
  @Test
  public void testLargeIntervalWithStringComparison() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment4
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(hugeTimeRangeSegment4),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                hugeTimeRangeSegment4.getDataSource(),
                Intervals.of("2020/2021"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testSecondHalfEternityMultipleSegmentWithStringComparison() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(
            numberedSegment0of0,
            secondHalfEternityRangeSegment
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(numberedSegment0of0, secondHalfEternityRangeSegment),
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                secondHalfEternityRangeSegment.getDataSource(),
                Intervals.of("2015/2016"),
                Segments.ONLY_VISIBLE
            )
        )
    );
  }

  @Test
  public void testDeleteDataSourceMetadata() throws IOException
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        ImmutableSet.of(),
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
  public void testDeleteSegmentsInMetaDataStorage() throws IOException
  {
    // Published segments to MetaDataStorage
    coordinator.announceHistoricalSegments(SEGMENTS);

    // check segments Published
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
    // remove segments in MetaDataStorage
    coordinator.deleteSegments(SEGMENTS);

    // check segments removed
    Assert.assertEquals(
        0,
        ImmutableSet.copyOf(
            coordinator.retrieveUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval(),
                Segments.ONLY_VISIBLE
            )
        ).size()
    );
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

  /**
   * This test simulates an issue detected on the field consisting of the following sequence of events:
   * - A kafka stream segment was created on a given interval
   * - Later, after the above was published, another segment on same interval was created by the stream
   * - Later, after the above was published, another segment on same interval was created by the stream
   * - Later a compaction was issued for the three segments above
   * - Later, after the above was published, another segment on same interval was created by the stream
   * - Later, the compacted segment got dropped due to a drop rule
   * - Later, after the above was dropped, another segment on same interval was created by the stream but this
   *   time there was an integrity violation in the pending segments table because the
   *   {@link IndexerSQLMetadataStorageCoordinator#createNewSegment(Handle, String, Interval, PartialShardSpec, String)}
   *   method returned an segment id that already existed in the pending segments table
   */
  @Test
  public void testAllocatePendingSegmentAfterDroppingExistingSegment()
  {
    String maxVersion = "version_newer_newer";

    // simulate one load using kafka streaming
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
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version", identifier.toString());
    // Since there are no used core partitions yet
    Assert.assertEquals(0, identifier.getShardSpec().getNumCorePartitions());

    // simulate one more load using kafka streaming (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier1 = coordinator.allocatePendingSegment(
        dataSource,
        "seq2",
        identifier.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_1", identifier1.toString());
    // Since there are no used core partitions yet
    Assert.assertEquals(0, identifier1.getShardSpec().getNumCorePartitions());

    // simulate one more load using kafka streaming (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier2 = coordinator.allocatePendingSegment(
        dataSource,
        "seq3",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_2", identifier2.toString());
    // Since there are no used core partitions yet
    Assert.assertEquals(0, identifier2.getShardSpec().getNumCorePartitions());

    // now simulate that one compaction was done (batch) ingestion for same interval (like reindex of the previous three):
    DataSegment segment = new DataSegment(
        "ds",
        Intervals.of("2017-01-01T00Z/2017-02-01T00Z"),
        "version_new",
        ImmutableMap.of(),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new NumberedShardSpec(0, 1),
        9,
        100
    );
    Assert.assertTrue(insertUsedSegments(ImmutableSet.of(segment)));
    List<String> ids = retrieveUsedSegmentIds();
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_new", ids.get(0));

    // one more load on same interval:
    final SegmentIdWithShardSpec identifier3 = coordinator.allocatePendingSegment(
        dataSource,
        "seq4",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_new_1", identifier3.toString());
    // Used segment set has 1 core partition
    Assert.assertEquals(1, identifier3.getShardSpec().getNumCorePartitions());

    // now drop the used segment previously loaded:
    markAllSegmentsUnused(ImmutableSet.of(segment));

    // and final load, this reproduces an issue that could happen with multiple streaming appends,
    // followed by a reindex, followed by a drop, and more streaming data coming in for same interval
    final SegmentIdWithShardSpec identifier4 = coordinator.allocatePendingSegment(
        dataSource,
        "seq5",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_new_2", identifier4.toString());
    // Since all core partitions have been dropped
    Assert.assertEquals(0, identifier4.getShardSpec().getNumCorePartitions());

  }

  /**
   * Slightly different that the above test but that involves reverted compaction
   1) used segments of version = A, id = 0, 1, 2
   2) overwrote segments of version = B, id = 0 <= compaction
   3) marked segments unused for version = A, id = 0, 1, 2 <= overshadowing
   4) pending segment of version = B, id = 1 <= appending new data, aborted
   5) reverted compaction, mark segments used for version = A, id = 0, 1, 2, and mark compacted segments unused
   6) used segments of version = A, id = 0, 1, 2
   7) pending segment of version = B, id = 1
   */
  @Test
  public void testAnotherAllocatePendingSegmentAfterRevertingCompaction()
  {
    String maxVersion = "Z";

    // 1.0) simulate one append load
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        "A",
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A", identifier.toString());
    // Assume it publishes; create its corresponding segment
    DataSegment segment = new DataSegment(
        "ds",
        Intervals.of("2017-01-01T00Z/2017-02-01T00Z"),
        "A",
        ImmutableMap.of(),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );
    Assert.assertTrue(insertUsedSegments(ImmutableSet.of(segment)));
    List<String> ids = retrieveUsedSegmentIds();
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A", ids.get(0));


    // 1.1) simulate one more append load  (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier1 = coordinator.allocatePendingSegment(
        dataSource,
        "seq2",
        identifier.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_1", identifier1.toString());
    // Assume it publishes; create its corresponding segment
    segment = new DataSegment(
        "ds",
        Intervals.of("2017-01-01T00Z/2017-02-01T00Z"),
        "A",
        ImmutableMap.of(),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(1),
        9,
        100
    );
    Assert.assertTrue(insertUsedSegments(ImmutableSet.of(segment)));
    ids = retrieveUsedSegmentIds();
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_1", ids.get(1));


    // 1.2) simulate one more append load  (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier2 = coordinator.allocatePendingSegment(
        dataSource,
        "seq3",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_2", identifier2.toString());
    // Assume it publishes; create its corresponding segment
    segment = new DataSegment(
        "ds",
        Intervals.of("2017-01-01T00Z/2017-02-01T00Z"),
        "A",
        ImmutableMap.of(),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(2),
        9,
        100
    );
    // state so far:
    // pendings: A: 0,1,2
    // used segments A: 0,1,2
    // unused segments:
    Assert.assertTrue(insertUsedSegments(ImmutableSet.of(segment)));
    ids = retrieveUsedSegmentIds();
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_2", ids.get(2));


    // 2)
    // now simulate that one compaction was done (batch) ingestion for same interval (like reindex of the previous three):
    DataSegment compactedSegment = new DataSegment(
        "ds",
        Intervals.of("2017-01-01T00Z/2017-02-01T00Z"),
        "B",
        ImmutableMap.of(),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );
    Assert.assertTrue(insertUsedSegments(ImmutableSet.of(compactedSegment)));
    ids = retrieveUsedSegmentIds();
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_B", ids.get(3));
    // 3) When overshadowing, segments are still marked as "used" in the segments table
    // state so far:
    // pendings: A: 0,1,2
    // used segments: A: 0,1,2; B: 0 <- new compacted segment, overshadows previous version A
    // unused segment:

    // 4) pending segment of version = B, id = 1 <= appending new data, aborted
    final SegmentIdWithShardSpec identifier3 = coordinator.allocatePendingSegment(
        dataSource,
        "seq4",
        identifier2.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_B_1", identifier3.toString());
    // no corresponding segment, pending aborted
    // state so far:
    // pendings: A: 0,1,2; B:1 (note that B_1 does not make it into segments since its task aborted)
    // used segments: A: 0,1,2; B: 0 <-  compacted segment, overshadows previous version A
    // unused segment:

    // 5) reverted compaction (by marking B_0 as unused)
    // Revert compaction a manual metadata update which is basically the following two steps:
    markAllSegmentsUnused(ImmutableSet.of(compactedSegment)); // <- drop compacted segment
    //        pending: version = A, id = 0,1,2
    //                 version = B, id = 1
    //
    //        used segment: version = A, id = 0,1,2
    //        unused segment: version = B, id = 0
    List<String> pendings = retrievePendingSegmentIds();
    Assert.assertTrue(pendings.size() == 4);

    List<String> used = retrieveUsedSegmentIds();
    Assert.assertTrue(used.size() == 3);

    List<String> unused = retrieveUnusedSegmentIds();
    Assert.assertTrue(unused.size() == 1);

    // Simulate one more append load
    final SegmentIdWithShardSpec identifier4 = coordinator.allocatePendingSegment(
        dataSource,
        "seq5",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    // maxid = B_1 -> new partno = 2
    // versionofexistingchunk=A
    // ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_2
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_3", identifier4.toString());
    // Assume it publishes; create its corresponding segment
    segment = new DataSegment(
        "ds",
        Intervals.of("2017-01-01T00Z/2017-02-01T00Z"),
        "A",
        ImmutableMap.of(),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(3),
        9,
        100
    );
    //        pending: version = A, id = 0,1,2,3
    //                 version = B, id = 1
    //
    //        used segment: version = A, id = 0,1,2,3
    //        unused segment: version = B, id = 0
    Assert.assertTrue(insertUsedSegments(ImmutableSet.of(segment)));
    ids = retrieveUsedSegmentIds();
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_3", ids.get(3));

  }
  
  @Test
  public void testNoPendingSegmentsAndOneUsedSegment()
  {
    String maxVersion = "Z";

    // create one used segment
    DataSegment segment = new DataSegment(
        "ds",
        Intervals.of("2017-01-01T00Z/2017-02-01T00Z"),
        "A",
        ImmutableMap.of(),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );
    Assert.assertTrue(insertUsedSegments(ImmutableSet.of(segment)));
    List<String> ids = retrieveUsedSegmentIds();
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A", ids.get(0));


    // simulate one aborted append load
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        maxVersion,
        true
    );
    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_1", identifier.toString());
    
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

    final int numDeleted = coordinator.deletePendingSegmentsCreatedInInterval(
        dataSource,
        new Interval(begin, secondBegin)
    );
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
    final PartialShardSpec partialShardSpec = new HashBasedNumberedPartialShardSpec(null, 2, 5, null);
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
        new HashBasedNumberedPartialShardSpec(null, 2, 3, null),
        "version",
        true
    );

    shardSpec = (HashBasedNumberedShardSpec) id.getShardSpec();
    Assert.assertEquals(2, shardSpec.getPartitionNum());
    Assert.assertEquals(0, shardSpec.getNumCorePartitions());
    Assert.assertEquals(3, shardSpec.getNumBuckets());
  }

  @Test
  public void testAddNumberedShardSpecAfterMultiDimensionsShardSpecWithUnknownCorePartitionSize() throws IOException
  {
    final String datasource = "datasource";
    final Interval interval = Intervals.of("2020-01-01/P1D");
    final String version = "version";
    final List<String> dimensions = ImmutableList.of("dim");
    final List<String> metrics = ImmutableList.of("met");
    final Set<DataSegment> originalSegments = new HashSet<>();
    for (int i = 0; i < 6; i++) {
      originalSegments.add(
          new DataSegment(
              datasource,
              interval,
              version,
              ImmutableMap.of(),
              dimensions,
              metrics,
              new DimensionRangeShardSpec(
                  Collections.singletonList("dim"),
                  i == 0 ? null : StringTuple.create(String.valueOf(i - 1)),
                  i == 5 ? null : StringTuple.create(String.valueOf(i)),
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

  @Test
  public void testDropSegmentsWithHandleForSegmentThatExist()
  {
    try (Handle handle = derbyConnector.getDBI().open()) {
      Assert.assertTrue(insertUsedSegments(ImmutableSet.of(defaultSegment)));
      List<String> usedSegments = retrieveUsedSegmentIds();
      Assert.assertEquals(1, usedSegments.size());
      Assert.assertEquals(defaultSegment.getId().toString(), usedSegments.get(0));

      // Try drop segment
      IndexerSQLMetadataStorageCoordinator.DataStoreMetadataUpdateResult result = coordinator.dropSegmentsWithHandle(
          handle,
          ImmutableSet.of(defaultSegment),
          defaultSegment.getDataSource()
      );

      Assert.assertEquals(IndexerSQLMetadataStorageCoordinator.DataStoreMetadataUpdateResult.SUCCESS, result);
      usedSegments = retrieveUsedSegmentIds();
      Assert.assertEquals(0, usedSegments.size());
    }
  }

  @Test
  public void testDropSegmentsWithHandleForSegmentThatDoesNotExist()
  {
    try (Handle handle = derbyConnector.getDBI().open()) {
      // Try drop segment
      IndexerSQLMetadataStorageCoordinator.DataStoreMetadataUpdateResult result = coordinator.dropSegmentsWithHandle(
          handle,
          ImmutableSet.of(defaultSegment),
          defaultSegment.getDataSource()
      );
      Assert.assertEquals(IndexerSQLMetadataStorageCoordinator.DataStoreMetadataUpdateResult.TRY_AGAIN, result);
    }
  }

  @Test
  public void testRemoveDataSourceMetadataOlderThanDatasourceActiveShouldNotBeDeleted() throws Exception
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        ImmutableSet.of(),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar"))
    );

    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );

    // Try delete. Datasource should not be deleted as it is in excluded set
    int deletedCount = coordinator.removeDataSourceMetadataOlderThan(
        System.currentTimeMillis(),
        ImmutableSet.of("fooDataSource")
    );

    // Datasource should not be deleted
    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );
    Assert.assertEquals(0, deletedCount);
  }

  @Test
  public void testRemoveDataSourceMetadataOlderThanDatasourceNotActiveAndOlderThanTimeShouldBeDeleted() throws Exception
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        ImmutableSet.of(),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar"))
    );

    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );

    // Try delete. Datasource should be deleted as it is not in excluded set and created time older than given time
    int deletedCount = coordinator.removeDataSourceMetadataOlderThan(System.currentTimeMillis(), ImmutableSet.of());

    // Datasource should be deleted
    Assert.assertNull(
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );
    Assert.assertEquals(1, deletedCount);
  }

  @Test
  public void testRemoveDataSourceMetadataOlderThanDatasourceNotActiveButNotOlderThanTimeShouldNotBeDeleted()
      throws Exception
  {
    coordinator.announceHistoricalSegments(
        ImmutableSet.of(defaultSegment),
        ImmutableSet.of(),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar"))
    );

    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );

    // Do delete. Datasource metadata should not be deleted. Datasource is not active but it was created just now so it's
    // created timestamp will be later than the timestamp 2012-01-01T00:00:00Z
    int deletedCount = coordinator.removeDataSourceMetadataOlderThan(
        DateTimes.of("2012-01-01T00:00:00Z").getMillis(),
        ImmutableSet.of()
    );

    // Datasource should not be deleted
    Assert.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata("fooDataSource")
    );
    Assert.assertEquals(0, deletedCount);
  }

  @Test
  public void testMarkSegmentsAsUnusedWithinIntervalOneYear() throws IOException
  {
    coordinator.announceHistoricalSegments(ImmutableSet.of(existingSegment1, existingSegment2));

    // interval covers existingSegment1 and partially overlaps existingSegment2,
    // only existingSegment1 will be dropped
    coordinator.markSegmentsAsUnusedWithinInterval(
        existingSegment1.getDataSource(),
        Intervals.of("1994-01-01/1994-01-02T12Z")
    );

    Assert.assertEquals(
        ImmutableSet.of(existingSegment1),
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                existingSegment1.getDataSource(),
                existingSegment1.getInterval().withEnd(existingSegment1.getInterval().getEnd().plus(1))
            )
        )
    );
    Assert.assertEquals(
        ImmutableSet.of(),
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                existingSegment2.getDataSource(),
                existingSegment2.getInterval().withEnd(existingSegment2.getInterval().getEnd().plusYears(1))
            )
        )
    );
  }

  @Test
  public void testMarkSegmentsAsUnusedWithinIntervalTwoYears() throws IOException
  {
    coordinator.announceHistoricalSegments(ImmutableSet.of(existingSegment1, existingSegment2));

    // interval covers existingSegment1 and partially overlaps existingSegment2,
    // only existingSegment1 will be dropped
    coordinator.markSegmentsAsUnusedWithinInterval(
        existingSegment1.getDataSource(),
        Intervals.of("1993-12-31T12Z/1994-01-02T12Z")
    );

    Assert.assertEquals(
        ImmutableSet.of(existingSegment1),
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                existingSegment1.getDataSource(),
                existingSegment1.getInterval().withEnd(existingSegment1.getInterval().getEnd().plus(1))
            )
        )
    );
    Assert.assertEquals(
        ImmutableSet.of(),
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                existingSegment2.getDataSource(),
                existingSegment2.getInterval().withEnd(existingSegment2.getInterval().getEnd().plusYears(1))
            )
        )
    );
  }
}
