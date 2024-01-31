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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentCreateRequest;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
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
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IndexerSQLMetadataStorageCoordinatorTest
{
  private static final int MAX_SQL_MEATADATA_RETRY_FOR_TEST = 2;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

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

  private final DataSegment defaultSegment2WithBiggerSize = new DataSegment(
          "fooDataSource",
          Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
          "version",
          ImmutableMap.of(),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(1),
          9,
          200
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
    derbyConnector.createUpgradeSegmentsTable();
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
      public int getSqlMetadataMaxRetry()
      {
        return MAX_SQL_MEATADATA_RETRY_FOR_TEST;
      }
    };
  }

  private void markAllSegmentsUnused()
  {
    markAllSegmentsUnused(SEGMENTS, DateTimes.nowUtc());
  }

  private void markAllSegmentsUnused(Set<DataSegment> segments, DateTime usedStatusLastUpdatedTime)
  {
    for (final DataSegment segment : segments) {
      Assert.assertEquals(
          1,
          (int) derbyConnector.getDBI().<Integer>withHandle(
              handle -> {
                String request = StringUtils.format(
                    "UPDATE %s SET used = false, used_status_last_updated = :used_status_last_updated WHERE id = :id",
                    derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable()
                );
                return handle.createStatement(request)
                             .bind("id", segment.getId().toString())
                             .bind("used_status_last_updated", usedStatusLastUpdatedTime.toString()
                ).execute();
              }
          )
      );
    }
  }

  private List<String> retrievePendingSegmentIds()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getPendingSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id FROM " + table + "  ORDER BY id")
                        .map(StringMapper.FIRST)
                        .list()
    );
  }

  private List<String> retrieveUsedSegmentIds()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id FROM " + table + " WHERE used = true ORDER BY id")
                        .map(StringMapper.FIRST)
                        .list()
    );
  }

  private List<DataSegment> retrieveUsedSegments()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT payload FROM " + table + " WHERE used = true ORDER BY id")
                        .map((index, result, context) -> JacksonUtils.readValue(mapper, result.getBytes(1), DataSegment.class))
                        .list()
    );
  }

  private List<String> retrieveUnusedSegmentIds()
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id FROM " + table + " WHERE used = false ORDER BY id")
                        .map(StringMapper.FIRST)
                        .list()
    );
  }


  private Boolean insertUsedSegments(Set<DataSegment> dataSegments)
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> {
          PreparedBatch preparedBatch = handle.prepareBatch(
              StringUtils.format(
                  "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload, used_status_last_updated) "
                  + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, :used_status_last_updated)",
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
                         .bind("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
                         .bind("version", segment.getVersion())
                         .bind("used", true)
                         .bind("payload", mapper.writeValueAsBytes(segment))
                         .bind("used_status_last_updated", DateTimes.nowUtc().toString());
          }

          final int[] affectedRows = preparedBatch.execute();
          final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
          if (!succeeded) {
            throw new ISE("Failed to publish segments to DB");
          }
          return true;
        }
    );
  }

  private Boolean insertPendingSegmentAndSequenceName(Pair<SegmentIdWithShardSpec, String> pendingSegmentSequenceName)
  {
    final SegmentIdWithShardSpec pendingSegment = pendingSegmentSequenceName.lhs;
    final String sequenceName = pendingSegmentSequenceName.rhs;
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getPendingSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> {
          handle.createStatement(
                    StringUtils.format(
                        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, "
                        + "sequence_name_prev_id_sha1, payload) "
                        + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, "
                        + ":sequence_name_prev_id_sha1, :payload)",
                        table,
                        derbyConnector.getQuoteString()
                    )
                )
                .bind("id", pendingSegment.toString())
                .bind("dataSource", pendingSegment.getDataSource())
                .bind("created_date", DateTimes.nowUtc().toString())
                .bind("start", pendingSegment.getInterval().getStart().toString())
                .bind("end", pendingSegment.getInterval().getEnd().toString())
                .bind("sequence_name", sequenceName)
                .bind("sequence_prev_id", pendingSegment.toString())
                .bind("sequence_name_prev_id_sha1", BaseEncoding.base16().encode(
                    Hashing.sha1()
                           .newHasher()
                           .putLong((long) pendingSegment.hashCode() * sequenceName.hashCode())
                           .hash()
                           .asBytes()
                ))
                .bind("payload", mapper.writeValueAsBytes(pendingSegment))
                .execute();
          return true;
        }
    );
  }

  private Map<String, String> getSegmentsCommittedDuringReplaceTask(String taskId)
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getUpgradeSegmentsTable();
    return derbyConnector.retryWithHandle(handle -> {
      final String sql = StringUtils.format(
          "SELECT segment_id, lock_version FROM %1$s WHERE task_id = :task_id",
          table
      );

      ResultIterator<Pair<String, String>> resultIterator = handle
          .createQuery(sql)
          .bind("task_id", taskId)
          .map(
              (index, r, ctx) -> Pair.of(r.getString("segment_id"), r.getString("lock_version"))
          )
          .iterator();

      final Map<String, String> segmentIdToLockVersion = new HashMap<>();
      while (resultIterator.hasNext()) {
        Pair<String, String> result = resultIterator.next();
        segmentIdToLockVersion.put(result.lhs, result.rhs);
      }
      return segmentIdToLockVersion;
    });
  }

  private void insertIntoUpgradeSegmentsTable(Map<DataSegment, ReplaceTaskLock> segmentToTaskLockMap)
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getUpgradeSegmentsTable();
    derbyConnector.retryWithHandle(
        handle -> {
          PreparedBatch preparedBatch = handle.prepareBatch(
              StringUtils.format(
                  StringUtils.format(
                      "INSERT INTO %1$s (task_id, segment_id, lock_version) "
                      + "VALUES (:task_id, :segment_id, :lock_version)",
                      table
                  )
              )
          );
          for (Map.Entry<DataSegment, ReplaceTaskLock> entry : segmentToTaskLockMap.entrySet()) {
            final DataSegment segment = entry.getKey();
            final ReplaceTaskLock lock = entry.getValue();
            preparedBatch.add()
                         .bind("task_id", lock.getSupervisorTaskId())
                         .bind("segment_id", segment.getId().toString())
                         .bind("lock_version", lock.getVersion());
          }

          final int[] affectedRows = preparedBatch.execute();
          final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
          if (!succeeded) {
            throw new ISE("Failed to insert upgrade segments in DB");
          }
          return true;
        }
    );
  }

  @Test
  public void testCommitAppendSegments()
  {
    final String v1 = "2023-01-01";
    final String v2 = "2023-01-02";
    final String v3 = "2023-01-03";
    final String lockVersion = "2024-01-01";

    final String replaceTaskId = "replaceTask1";
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock(
        replaceTaskId,
        Intervals.of("2023-01-01/2023-01-03"),
        lockVersion
    );

    final Set<DataSegment> appendSegments = new HashSet<>();
    final Set<DataSegment> expectedSegmentsToUpgrade = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      final DataSegment segment = createSegment(
          Intervals.of("2023-01-01/2023-01-02"),
          v1,
          new LinearShardSpec(i)
      );
      appendSegments.add(segment);
      expectedSegmentsToUpgrade.add(segment);
    }

    for (int i = 0; i < 10; i++) {
      final DataSegment segment = createSegment(
          Intervals.of("2023-01-02/2023-01-03"),
          v2,
          new LinearShardSpec(i)
      );
      appendSegments.add(segment);
      expectedSegmentsToUpgrade.add(segment);
    }

    for (int i = 0; i < 10; i++) {
      final DataSegment segment = createSegment(
          Intervals.of("2023-01-03/2023-01-04"),
          v3,
          new LinearShardSpec(i)
      );
      appendSegments.add(segment);
    }

    final Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
        = expectedSegmentsToUpgrade.stream()
                                   .collect(Collectors.toMap(s -> s, s -> replaceLock));

    // Commit the segment and verify the results
    SegmentPublishResult commitResult
        = coordinator.commitAppendSegments(appendSegments, segmentToReplaceLock);
    Assert.assertTrue(commitResult.isSuccess());
    Assert.assertEquals(appendSegments, commitResult.getSegments());

    // Verify the segments present in the metadata store
    Assert.assertEquals(
        appendSegments,
        ImmutableSet.copyOf(retrieveUsedSegments())
    );

    // Verify entries in the segment task lock table
    final Set<String> expectedUpgradeSegmentIds
        = expectedSegmentsToUpgrade.stream()
                                   .map(s -> s.getId().toString())
                                   .collect(Collectors.toSet());
    final Map<String, String> observedSegmentToLock = getSegmentsCommittedDuringReplaceTask(replaceTaskId);
    Assert.assertEquals(expectedUpgradeSegmentIds, observedSegmentToLock.keySet());

    final Set<String> observedLockVersions = new HashSet<>(observedSegmentToLock.values());
    Assert.assertEquals(1, observedLockVersions.size());
    Assert.assertEquals(replaceLock.getVersion(), Iterables.getOnlyElement(observedLockVersions));
  }

  @Test
  public void testCommitReplaceSegments()
  {
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock("g1", Intervals.of("2023-01-01/2023-02-01"), "2023-02-01");
    final Set<DataSegment> segmentsAppendedWithReplaceLock = new HashSet<>();
    final Map<DataSegment, ReplaceTaskLock> appendedSegmentToReplaceLockMap = new HashMap<>();
    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-0" + i + "/2023-01-0" + (i + 1)),
          "2023-01-0" + i,
          ImmutableMap.of("path", "a-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(0),
          9,
          100
      );
      segmentsAppendedWithReplaceLock.add(segment);
      appendedSegmentToReplaceLockMap.put(segment, replaceLock);
    }
    insertUsedSegments(segmentsAppendedWithReplaceLock);
    insertIntoUpgradeSegmentsTable(appendedSegmentToReplaceLockMap);

    final Set<DataSegment> replacingSegments = new HashSet<>();
    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-01/2023-02-01"),
          "2023-02-01",
          ImmutableMap.of("path", "b-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new NumberedShardSpec(i, 9),
          9,
          100
      );
      replacingSegments.add(segment);
    }

    coordinator.commitReplaceSegments(replacingSegments, ImmutableSet.of(replaceLock));

    Assert.assertEquals(
        2L * segmentsAppendedWithReplaceLock.size() + replacingSegments.size(),
        retrieveUsedSegmentIds().size()
    );

    final Set<DataSegment> usedSegments = new HashSet<>(retrieveUsedSegments());

    Assert.assertTrue(usedSegments.containsAll(segmentsAppendedWithReplaceLock));
    usedSegments.removeAll(segmentsAppendedWithReplaceLock);

    Assert.assertTrue(usedSegments.containsAll(replacingSegments));
    usedSegments.removeAll(replacingSegments);

    Assert.assertEquals(segmentsAppendedWithReplaceLock.size(), usedSegments.size());
    for (DataSegment segmentReplicaWithNewVersion : usedSegments) {
      boolean hasBeenCarriedForward = false;
      for (DataSegment appendedSegment : segmentsAppendedWithReplaceLock) {
        if (appendedSegment.getLoadSpec().equals(segmentReplicaWithNewVersion.getLoadSpec())) {
          hasBeenCarriedForward = true;
          break;
        }
      }
      Assert.assertTrue(hasBeenCarriedForward);
    }
  }

  @Test
  public void testSimpleAnnounce() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
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

    coordinator.commitSegments(segments);
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

    List<String> segmentIds = segments.stream()
                                      .map(segment -> segment.getId().toString())
                                      .sorted(Comparator.naturalOrder())
                                      .collect(Collectors.toList());

    Assert.assertEquals(segmentIds, retrieveUsedSegmentIds());

    // Should not update dataSource metadata.
    Assert.assertEquals(0, metadataUpdateCounter.get());
  }

  @Test
  public void testOvershadowingAnnounce() throws IOException
  {
    final ImmutableSet<DataSegment> segments = ImmutableSet.of(defaultSegment, defaultSegment2, defaultSegment4);

    coordinator.commitSegments(segments);

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
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
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
    final SegmentPublishResult result2 = coordinator.commitSegmentsAndMetadata(
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
      protected DataStoreMetadataUpdateResult updateDataSourceMetadataWithHandle(
          Handle handle,
          String dataSource,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        metadataUpdateCounter.getAndIncrement();
        if (attemptCounter.getAndIncrement() == 0) {
          return new DataStoreMetadataUpdateResult(true, true, null);
        } else {
          return super.updateDataSourceMetadataWithHandle(handle, dataSource, startMetadata, endMetadata);
        }
      }
    };

    // Insert first segment.
    final SegmentPublishResult result1 = failOnceCoordinator.commitSegmentsAndMetadata(
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
    final SegmentPublishResult result2 = failOnceCoordinator.commitSegmentsAndMetadata(
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
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Failed to update the metadata Store. The new start metadata is ahead of last commited end state."), result1);

    // Should only be tried once.
    Assert.assertEquals(1, metadataUpdateCounter.get());
  }

  @Test
  public void testTransactionalAnnounceFailDbNotNullWantNull() throws IOException
  {
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Inconsistent metadata state. This can " +
        "happen if you update input topic in a spec without changing the supervisor name. " +
        "Stored state: [ObjectMetadata{theObject={foo=baz}}], " +
        "Target state: [ObjectMetadata{theObject=null}]."), result2);

    // Should only be tried once per call.
    Assert.assertEquals(2, metadataUpdateCounter.get());
  }

  @Test
  public void testRetrieveUsedSegmentForId()
  {
    insertUsedSegments(ImmutableSet.of(defaultSegment));
    Assert.assertEquals(defaultSegment, coordinator.retrieveSegmentForId(defaultSegment.getId().toString(), false));
  }

  @Test
  public void testRetrieveSegmentForId()
  {
    insertUsedSegments(ImmutableSet.of(defaultSegment));
    markAllSegmentsUnused(ImmutableSet.of(defaultSegment), DateTimes.nowUtc());
    Assert.assertEquals(defaultSegment, coordinator.retrieveSegmentForId(defaultSegment.getId().toString(), true));
  }

  @Test
  public void testCleanUpgradeSegmentsTableForTask()
  {
    final String taskToClean = "taskToClean";
    final ReplaceTaskLock replaceLockToClean = new ReplaceTaskLock(
        taskToClean,
        Intervals.of("2023-01-01/2023-02-01"),
        "2023-03-01"
    );
    DataSegment segmentToClean0 = createSegment(
        Intervals.of("2023-01-01/2023-02-01"),
        "2023-02-01",
        new NumberedShardSpec(0, 0)
    );
    DataSegment segmentToClean1 = createSegment(
        Intervals.of("2023-01-01/2023-01-02"),
        "2023-01-02",
        new NumberedShardSpec(0, 0)
    );
    insertIntoUpgradeSegmentsTable(
        ImmutableMap.of(segmentToClean0, replaceLockToClean, segmentToClean1, replaceLockToClean)
    );

    // Unrelated task should not result in clean up
    Assert.assertEquals(0, coordinator.deleteUpgradeSegmentsForTask("someRandomTask"));
    // The two segment entries are deleted
    Assert.assertEquals(2, coordinator.deleteUpgradeSegmentsForTask(taskToClean));
    // Nothing further to delete
    Assert.assertEquals(0, coordinator.deleteUpgradeSegmentsForTask(taskToClean));
  }

  @Test
  public void testTransactionalAnnounceFailDbNotNullWantDifferent() throws IOException
  {
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment2),
        new ObjectMetadata(ImmutableMap.of("foo", "qux")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz"))
    );
    Assert.assertEquals(SegmentPublishResult.fail("java.lang.RuntimeException: Inconsistent metadata state. This can " +
        "happen if you update input topic in a spec without changing the supervisor name. " +
        "Stored state: [ObjectMetadata{theObject={foo=baz}}], " +
        "Target state: [ObjectMetadata{theObject={foo=qux}}]."), result2);

    // Should only be tried once per call.
    Assert.assertEquals(2, metadataUpdateCounter.get());
  }

  @Test
  public void testSimpleUsedList() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
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
    coordinator.commitSegments(SEGMENTS);
    coordinator.commitSegments(ImmutableSet.of(defaultSegment3));

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
  public void testRetrieveUsedSegmentsUsingMultipleIntervals() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());

    final Collection<DataSegment> actualUsedSegments = coordinator.retrieveUsedSegmentsForIntervals(
        DS.WIKI,
        intervals,
        Segments.ONLY_VISIBLE
    );

    Assert.assertEquals(segments.size(), actualUsedSegments.size());
    Assert.assertTrue(actualUsedSegments.containsAll(segments));
  }

  @Test
  public void testRetrieveAllUsedSegmentsUsingIntervalsOutOfRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);

    final Interval outOfRangeInterval = Intervals.of("1700/1800");
    Assert.assertTrue(segments.stream()
                              .anyMatch(segment -> !segment.getInterval().overlaps(outOfRangeInterval)));

    final Collection<DataSegment> actualUsedSegments = coordinator.retrieveUsedSegmentsForIntervals(
        DS.WIKI,
        ImmutableList.of(outOfRangeInterval),
        Segments.ONLY_VISIBLE
    );

    Assert.assertEquals(0, actualUsedSegments.size());
  }

  @Test
  public void testRetrieveAllUsedSegmentsUsingNoIntervals() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);

    final Collection<DataSegment> actualUsedSegments = coordinator.retrieveAllUsedSegments(
        DS.WIKI,
        Segments.ONLY_VISIBLE
    );

    Assert.assertEquals(segments.size(), actualUsedSegments.size());
    Assert.assertTrue(actualUsedSegments.containsAll(segments));
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndNoLimit() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        DS.WIKI,
        Intervals.of("1900/3000"),
        null,
        null
    );

    Assert.assertEquals(segments.size(), actualUnusedSegments.size());
    Assert.assertTrue(actualUnusedSegments.containsAll(segments));
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndLimitAtRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final int requestedLimit = segments.size();
    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        DS.WIKI,
        Intervals.of("1900/3000"),
        requestedLimit,
        null
    );

    Assert.assertEquals(requestedLimit, actualUnusedSegments.size());
    Assert.assertTrue(actualUnusedSegments.containsAll(segments));
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndLimitInRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final int requestedLimit = segments.size() - 1;
    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        DS.WIKI,
        Intervals.of("1900/3000"),
        requestedLimit,
        null
    );

    Assert.assertEquals(requestedLimit, actualUnusedSegments.size());
    Assert.assertTrue(actualUnusedSegments.containsAll(segments.stream().limit(requestedLimit).collect(Collectors.toList())));
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndLimitOutOfRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final int limit = segments.size() + 1;
    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        DS.WIKI,
        Intervals.of("1900/3000"),
        limit,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegments.size());
    Assert.assertTrue(actualUnusedSegments.containsAll(segments));
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingSingleIntervalOutOfRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final Interval outOfRangeInterval = Intervals.of("1700/1800");
    Assert.assertTrue(segments.stream()
                              .anyMatch(segment -> !segment.getInterval().overlaps(outOfRangeInterval)));
    final int limit = segments.size() + 1;

    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        DS.WIKI,
        outOfRangeInterval,
        limit,
        null
    );
    Assert.assertEquals(0, actualUnusedSegments.size());
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsAndNoLimit() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegments.size());
    Assert.assertTrue(segments.containsAll(actualUnusedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingNoIntervalsNoLimitAndNoLastSegmentId() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(),
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegments.size());
    Assert.assertTrue(segments.containsAll(actualUnusedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingNoIntervalsAndNoLimitAndNoLastSegmentId() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(2033, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    String lastSegmentId = segments.get(9).getId().toString();
    final List<DataSegment> expectedSegmentsAscOrder = segments.stream()
        .filter(s -> s.getId().toString().compareTo(lastSegmentId) > 0)
        .collect(Collectors.toList());
    ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(),
        null,
        lastSegmentId,
        null,
        null
    );
    Assert.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegments.size());
    Assert.assertTrue(expectedSegmentsAscOrder.containsAll(actualUnusedSegments));

    ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        lastSegmentId,
        null,
        null
    );
    Assert.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(expectedSegmentsAscOrder, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);

    actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(),
        null,
        lastSegmentId,
        SortOrder.ASC,
        null
    );
    Assert.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegments.size());
    Assert.assertEquals(expectedSegmentsAscOrder, actualUnusedSegments);

    actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        lastSegmentId,
        SortOrder.ASC,
        null
    );
    Assert.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegmentsPlus.size());
    verifyEqualsAllSegmentsPlus(expectedSegmentsAscOrder, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);

    final List<DataSegment> expectedSegmentsDescOrder = segments.stream()
        .filter(s -> s.getId().toString().compareTo(lastSegmentId) < 0)
        .collect(Collectors.toList());
    Collections.reverse(expectedSegmentsDescOrder);

    actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(),
        null,
        lastSegmentId,
        SortOrder.DESC,
        null
    );
    Assert.assertEquals(expectedSegmentsDescOrder.size(), actualUnusedSegments.size());
    Assert.assertEquals(expectedSegmentsDescOrder, actualUnusedSegments);

    actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        lastSegmentId,
        SortOrder.DESC,
        null
    );
    Assert.assertEquals(expectedSegmentsDescOrder.size(), actualUnusedSegmentsPlus.size());
    verifyEqualsAllSegmentsPlus(expectedSegmentsDescOrder, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsAndLimitAtRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        segments.size(),
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegments.size());
    Assert.assertTrue(segments.containsAll(actualUnusedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        segments.size(),
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsAndLimitInRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final int requestedLimit = segments.size() - 1;
    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        requestedLimit,
        null,
        null,
        null
    );
    final List<DataSegment> expectedSegments = segments.stream().limit(requestedLimit).collect(Collectors.toList());
    Assert.assertEquals(requestedLimit, actualUnusedSegments.size());
    Assert.assertTrue(actualUnusedSegments.containsAll(expectedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        requestedLimit,
        null,
        null,
        null
    );
    Assert.assertEquals(requestedLimit, actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(expectedSegments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsInSingleBatchLimitAndLastSegmentId() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(2034, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final int requestedLimit = segments.size();
    final String lastSegmentId = segments.get(4).getId().toString();
    final List<DataSegment> expectedSegments = segments.stream()
        .filter(s -> s.getId().toString().compareTo(lastSegmentId) > 0)
        .limit(requestedLimit)
        .collect(Collectors.toList());
    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        requestedLimit,
        lastSegmentId,
        null,
        null
    );
    Assert.assertEquals(segments.size() - 5, actualUnusedSegments.size());
    Assert.assertEquals(actualUnusedSegments, expectedSegments);

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        requestedLimit,
        lastSegmentId,
        null,
        null
    );
    Assert.assertEquals(segments.size() - 5, actualUnusedSegmentsPlus.size());
    verifyEqualsAllSegmentsPlus(expectedSegments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsLimitAndLastSegmentId() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final int requestedLimit = segments.size() - 1;
    final String lastSegmentId = segments.get(4).getId().toString();
    final List<DataSegment> expectedSegments = segments.stream()
        .filter(s -> s.getId().toString().compareTo(lastSegmentId) > 0)
        .limit(requestedLimit)
        .collect(Collectors.toList());
    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        requestedLimit,
        lastSegmentId,
        null,
        null
    );
    Assert.assertEquals(requestedLimit - 4, actualUnusedSegments.size());
    Assert.assertEquals(actualUnusedSegments, expectedSegments);

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        requestedLimit,
        lastSegmentId,
        null,
        null
    );
    Assert.assertEquals(requestedLimit - 4, actualUnusedSegmentsPlus.size());
    verifyEqualsAllSegmentsPlus(expectedSegments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingMultipleIntervals() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        segments.size() + 1,
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegments.size());
    Assert.assertTrue(actualUnusedSegments.containsAll(segments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        segments.size() + 1,
        null,
        null,
        null
    );
    Assert.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @Test
  public void testRetrieveUnusedSegmentsUsingIntervalOutOfRange() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final Interval outOfRangeInterval = Intervals.of("1700/1800");
    Assert.assertTrue(segments.stream()
                              .anyMatch(segment -> !segment.getInterval().overlaps(outOfRangeInterval)));

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(outOfRangeInterval),
        null,
        null,
        null,
         null
    );
    Assert.assertEquals(0, actualUnusedSegments.size());

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(outOfRangeInterval),
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(0, actualUnusedSegmentsPlus.size());
  }

  @Test
  public void testRetrieveUnusedSegmentsWithMaxUsedStatusLastUpdatedTime() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final Interval interval = Intervals.of("1905/1920");

    final ImmutableList<DataSegment> actualUnusedSegments1 = retrieveUnusedSegments(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc()
    );
    Assert.assertEquals(5, actualUnusedSegments1.size());

    ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc()
    );
    Assert.assertEquals(5, actualUnusedSegmentsPlus.size());

    final ImmutableList<DataSegment> actualUnusedSegments2 = retrieveUnusedSegments(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc().minusHours(1)
    );
    Assert.assertEquals(0, actualUnusedSegments2.size());

    actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc().minusHours(1)
    );
    Assert.assertEquals(0, actualUnusedSegmentsPlus.size());
  }

  @Test
  public void testRetrieveUnusedSegmentsWithMaxUsedStatusLastUpdatedTime2() throws IOException
  {
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 1950);
    final List<DataSegment> evenYearSegments = new ArrayList<>();
    final List<DataSegment> oddYearSegments = new ArrayList<>();

    for (int i = 0; i < segments.size(); i++) {
      DataSegment dataSegment = segments.get(i);
      if (i % 2 == 0) {
        evenYearSegments.add(dataSegment);
      } else {
        oddYearSegments.add(dataSegment);
      }
    }

    final DateTime maxUsedStatusLastUpdatedTime1 = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(oddYearSegments), maxUsedStatusLastUpdatedTime1);

    final DateTime maxUsedStatusLastUpdatedTime2 = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(evenYearSegments), maxUsedStatusLastUpdatedTime2);

    final Interval interval = Intervals.of("1900/1950");

    final ImmutableList<DataSegment> actualUnusedSegments1 = retrieveUnusedSegments(
        ImmutableList.of(interval),
        null,
        null,
        null,
        maxUsedStatusLastUpdatedTime1
    );
    Assert.assertEquals(oddYearSegments.size(), actualUnusedSegments1.size());

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus1 = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        maxUsedStatusLastUpdatedTime1
    );
    Assert.assertEquals(oddYearSegments.size(), actualUnusedSegmentsPlus1.size());

    final ImmutableList<DataSegment> actualUnusedSegments2 = retrieveUnusedSegments(
        ImmutableList.of(interval),
        null,
        null,
        null,
        maxUsedStatusLastUpdatedTime2
    );
    Assert.assertEquals(segments.size(), actualUnusedSegments2.size());

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus2 = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        maxUsedStatusLastUpdatedTime2
    );
    Assert.assertEquals(segments.size(), actualUnusedSegmentsPlus2.size());
  }

  @Test
  public void testSimpleUnusedList() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval(),
                null,
                null
            )
        )
    );
  }

  @Test
  public void testSimpleUnusedListWithLimit() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    int limit = SEGMENTS.size() - 1;
    Set<DataSegment> retreivedUnusedSegments = ImmutableSet.copyOf(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval(),
            limit,
            null
        )
    );
    Assert.assertEquals(limit, retreivedUnusedSegments.size());
    Assert.assertTrue(SEGMENTS.containsAll(retreivedUnusedSegments));
  }

  @Test
  public void testUsedOverlapLow() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
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
    coordinator.commitSegments(SEGMENTS);
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
    coordinator.commitSegments(SEGMENTS);
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
    coordinator.commitSegments(SEGMENTS);
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
    coordinator.commitSegments(SEGMENTS);
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
    coordinator.commitSegments(SEGMENTS);
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
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(
                defaultSegment.getInterval().getStart().minus(1),
                defaultSegment.getInterval().getStart().plus(1)
            ),
            null,
            null
        ).isEmpty()
    );
  }

  @Test
  public void testUnusedUnderlapLow() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().plus(1), defaultSegment.getInterval().getEnd()),
            null,
            null
        ).isEmpty()
    );
  }


  @Test
  public void testUnusedUnderlapHigh() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart(), defaultSegment.getInterval().getEnd().minus(1)),
            null,
            null
        ).isEmpty()
    );
  }

  @Test
  public void testUnusedOverlapHigh() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval().withStart(defaultSegment.getInterval().getEnd().minus(1)),
            null,
            null
        ).isEmpty()
    );
  }

  @Test
  public void testUnusedBigOverlap() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                Intervals.of("2000/2999"),
                null,
                null
            )
        )
    );
  }

  @Test
  public void testUnusedLowRange() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minus(1)),
                null,
                null
            )
        )
    );
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minusYears(1)),
                null,
                null
            )
        )
    );
  }

  @Test
  public void testUnusedHighRange() throws IOException
  {
    coordinator.commitSegments(SEGMENTS);
    markAllSegmentsUnused();
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plus(1)),
                null,
                null
            )
        )
    );
    Assert.assertEquals(
        SEGMENTS,
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plusYears(1)),
                null,
                null
            )
        )
    );
  }

  @Test
  public void testUsedHugeTimeRangeEternityFilter() throws IOException
  {
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegments(
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
    coordinator.commitSegmentsAndMetadata(
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
  public void testDeleteSegmentsInMetaDataStorage() throws IOException
  {
    // Published segments to MetaDataStorage
    coordinator.commitSegments(SEGMENTS);

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
  public void testUpdateSegmentsInMetaDataStorage() throws IOException
  {
    // Published segments to MetaDataStorage
    coordinator.commitSegments(SEGMENTS);

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

    // update single metadata item
    coordinator.updateSegmentMetadata(Collections.singleton(defaultSegment2WithBiggerSize));

    Collection<DataSegment> updated = coordinator.retrieveUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval(),
            Segments.ONLY_VISIBLE);

    Assert.assertEquals(SEGMENTS.size(), updated.size());

    DataSegment defaultAfterUpdate = updated.stream().filter(s -> s.equals(defaultSegment)).findFirst().get();
    DataSegment default2AfterUpdate = updated.stream().filter(s -> s.equals(defaultSegment2)).findFirst().get();

    Assert.assertNotNull(defaultAfterUpdate);
    Assert.assertNotNull(default2AfterUpdate);

    // check that default did not change
    Assert.assertEquals(defaultSegment.getSize(), defaultAfterUpdate.getSize());
    // but that default 2 did change
    Assert.assertEquals(defaultSegment2WithBiggerSize.getSize(), default2AfterUpdate.getSize());
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
    coordinator.commitSegments(segments);

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
   * This test verifies the behaviour in the following sequence of events:
   * - create segment1 for an interval and publish
   * - create segment2 for same interval and publish
   * - create segment3 for same interval and publish
   * - compact all segments above and publish new segments
   * - create segment4 for the same interval
   * - drop the compacted segment
   * - create segment5 for the same interval
   * - verify that the id for segment5 is correct
   * - Later, after the above was dropped, another segment on same interval was created by the stream but this
   * time there was an integrity violation in the pending segments table because the
   * method returned a segment id that already existed in the pending segments table
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
    markAllSegmentsUnused(ImmutableSet.of(segment), DateTimes.nowUtc());

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
   * Slightly different from the above test that involves reverted compaction
   * 1) used segments of version = A, id = 0, 1, 2
   * 2) overwrote segments of version = B, id = 0 <= compaction
   * 3) marked segments unused for version = A, id = 0, 1, 2 <= overshadowing
   * 4) pending segment of version = B, id = 1 <= appending new data, aborted
   * 5) reverted compaction, mark segments used for version = A, id = 0, 1, 2, and mark compacted segments unused
   * 6) used segments of version = A, id = 0, 1, 2
   * 7) pending segment of version = B, id = 1
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
    markAllSegmentsUnused(ImmutableSet.of(compactedSegment), DateTimes.nowUtc()); // <- drop compacted segment
    //        pending: version = A, id = 0,1,2
    //                 version = B, id = 1
    //
    //        used segment: version = A, id = 0,1,2
    //        unused segment: version = B, id = 0
    List<String> pendings = retrievePendingSegmentIds();
    Assert.assertEquals(4, pendings.size());

    List<String> used = retrieveUsedSegmentIds();
    Assert.assertEquals(3, used.size());

    List<String> unused = retrieveUnusedSegmentIds();
    Assert.assertEquals(1, unused.size());

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
  public void testAllocatePendingSegments()
  {
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final String sequenceName = "seq";

    final SegmentCreateRequest request = new SegmentCreateRequest(sequenceName, null, "v1", partialShardSpec);
    final SegmentIdWithShardSpec segmentId0 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request)
    ).get(request);

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1", segmentId0.toString());

    final SegmentCreateRequest request1 =
        new SegmentCreateRequest(sequenceName, segmentId0.toString(), segmentId0.getVersion(), partialShardSpec);
    final SegmentIdWithShardSpec segmentId1 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request1)
    ).get(request1);

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_1", segmentId1.toString());

    final SegmentCreateRequest request2 =
        new SegmentCreateRequest(sequenceName, segmentId1.toString(), segmentId1.getVersion(), partialShardSpec);
    final SegmentIdWithShardSpec segmentId2 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request2)
    ).get(request2);

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_2", segmentId2.toString());

    final SegmentCreateRequest request3 =
        new SegmentCreateRequest(sequenceName, segmentId1.toString(), segmentId1.getVersion(), partialShardSpec);
    final SegmentIdWithShardSpec segmentId3 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request3)
    ).get(request3);

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_2", segmentId3.toString());
    Assert.assertEquals(segmentId2, segmentId3);

    final SegmentCreateRequest request4 =
        new SegmentCreateRequest("seq1", null, "v1", partialShardSpec);
    final SegmentIdWithShardSpec segmentId4 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request4)
    ).get(request4);

    Assert.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_3", segmentId4.toString());
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
      final Set<DataSegment> announced = coordinator.commitSegments(toBeAnnounced);

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

    coordinator.commitSegments(
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

    coordinator.commitSegments(
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
    coordinator.commitSegments(originalSegments);
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
    coordinator.commitSegments(originalSegments);
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
  public void testRemoveDataSourceMetadataOlderThanDatasourceActiveShouldNotBeDeleted() throws Exception
  {
    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
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
    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
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
    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
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
    coordinator.commitSegments(ImmutableSet.of(existingSegment1, existingSegment2));

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
                existingSegment1.getInterval().withEnd(existingSegment1.getInterval().getEnd().plus(1)),
                null,
                null
            )
        )
    );
    Assert.assertEquals(
        ImmutableSet.of(),
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                existingSegment2.getDataSource(),
                existingSegment2.getInterval().withEnd(existingSegment2.getInterval().getEnd().plusYears(1)),
                null,
                null
            )
        )
    );
  }

  @Test
  public void testMarkSegmentsAsUnusedWithinIntervalTwoYears() throws IOException
  {
    coordinator.commitSegments(ImmutableSet.of(existingSegment1, existingSegment2));

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
                existingSegment1.getInterval().withEnd(existingSegment1.getInterval().getEnd().plus(1)),
                null,
                null
            )
        )
    );
    Assert.assertEquals(
        ImmutableSet.of(),
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                existingSegment2.getDataSource(),
                existingSegment2.getInterval().withEnd(existingSegment2.getInterval().getEnd().plusYears(1)),
                null,
                null
            )
        )
    );
  }

  @Test
  public void testGetPendingSegmentsForIntervalWithSequencePrefixes()
  {
    Pair<SegmentIdWithShardSpec, String> validIntervalValidSequence = Pair.of(
        SegmentIdWithShardSpec.fromDataSegment(defaultSegment),
        "validLOL"
    );
    insertPendingSegmentAndSequenceName(validIntervalValidSequence);

    Pair<SegmentIdWithShardSpec, String> validIntervalInvalidSequence = Pair.of(
        SegmentIdWithShardSpec.fromDataSegment(defaultSegment2),
        "invalidRandom"
    );
    insertPendingSegmentAndSequenceName(validIntervalInvalidSequence);

    Pair<SegmentIdWithShardSpec, String> invalidIntervalvalidSequence = Pair.of(
        SegmentIdWithShardSpec.fromDataSegment(existingSegment1),
        "validStuff"
    );
    insertPendingSegmentAndSequenceName(invalidIntervalvalidSequence);

    Pair<SegmentIdWithShardSpec, String> twentyFifteenWithAnotherValidSequence = Pair.of(
        new SegmentIdWithShardSpec(
            existingSegment1.getDataSource(),
            Intervals.of("2015/2016"),
            "1970-01-01",
            new NumberedShardSpec(1, 0)
        ),
        "alsoValidAgain"
    );
    insertPendingSegmentAndSequenceName(twentyFifteenWithAnotherValidSequence);

    Pair<SegmentIdWithShardSpec, String> twentyFifteenWithInvalidSequence = Pair.of(
        new SegmentIdWithShardSpec(
            existingSegment1.getDataSource(),
            Intervals.of("2015/2016"),
            "1970-01-01",
            new NumberedShardSpec(2, 0)
        ),
        "definitelyInvalid"
    );
    insertPendingSegmentAndSequenceName(twentyFifteenWithInvalidSequence);


    final Map<SegmentIdWithShardSpec, String> expected = new HashMap<>();
    expected.put(validIntervalValidSequence.lhs, validIntervalValidSequence.rhs);
    expected.put(twentyFifteenWithAnotherValidSequence.lhs, twentyFifteenWithAnotherValidSequence.rhs);

    final Map<SegmentIdWithShardSpec, String> actual =
        derbyConnector.retryWithHandle(handle -> coordinator.getPendingSegmentsForIntervalWithHandle(
            handle,
            defaultSegment.getDataSource(),
            defaultSegment.getInterval(),
            ImmutableSet.of("valid", "alsoValid")
        ));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRetrieveUsedSegmentsAndCreatedDates()
  {
    insertUsedSegments(ImmutableSet.of(defaultSegment));

    List<Pair<DataSegment, String>> resultForIntervalOnTheLeft =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("2000/2001")));
    Assert.assertTrue(resultForIntervalOnTheLeft.isEmpty());

    List<Pair<DataSegment, String>> resultForIntervalOnTheRight =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("3000/3001")));
    Assert.assertTrue(resultForIntervalOnTheRight.isEmpty());

    List<Pair<DataSegment, String>> resultForExactInterval =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(defaultSegment.getInterval()));
    Assert.assertEquals(1, resultForExactInterval.size());
    Assert.assertEquals(defaultSegment, resultForExactInterval.get(0).lhs);

    List<Pair<DataSegment, String>> resultForIntervalWithLeftOverlap =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("2000/2015-01-02")));
    Assert.assertEquals(resultForExactInterval, resultForIntervalWithLeftOverlap);

    List<Pair<DataSegment, String>> resultForIntervalWithRightOverlap =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("2015-01-01/3000")));
    Assert.assertEquals(resultForExactInterval, resultForIntervalWithRightOverlap);

    List<Pair<DataSegment, String>> resultForEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.ETERNITY));
    Assert.assertEquals(resultForExactInterval, resultForEternity);

    List<Pair<DataSegment, String>> resultForFirstHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(firstHalfEternityRangeSegment.getInterval()));
    Assert.assertEquals(resultForExactInterval, resultForFirstHalfEternity);

    List<Pair<DataSegment, String>> resultForSecondHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(secondHalfEternityRangeSegment.getInterval()));
    Assert.assertEquals(resultForExactInterval, resultForSecondHalfEternity);
  }

  @Test
  public void testRetrieveUsedSegmentsAndCreatedDatesFetchesEternityForAnyInterval()
  {

    insertUsedSegments(ImmutableSet.of(eternitySegment, firstHalfEternityRangeSegment, secondHalfEternityRangeSegment));

    List<Pair<DataSegment, String>> resultForRandomInterval =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(defaultSegment.getInterval()));
    Assert.assertEquals(3, resultForRandomInterval.size());

    List<Pair<DataSegment, String>> resultForEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(eternitySegment.getInterval()));
    Assert.assertEquals(3, resultForEternity.size());

    List<Pair<DataSegment, String>> resultForFirstHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(firstHalfEternityRangeSegment.getInterval()));
    Assert.assertEquals(3, resultForFirstHalfEternity.size());

    List<Pair<DataSegment, String>> resultForSecondHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(secondHalfEternityRangeSegment.getInterval()));
    Assert.assertEquals(3, resultForSecondHalfEternity.size());
  }

  @Test
  public void testTimelineVisibilityWith0CorePartitionTombstone() throws IOException
  {
    final Interval interval = Intervals.of("2020/2021");
    // Create and commit a tombstone segment
    final DataSegment tombstoneSegment = createSegment(
        interval,
        "version",
        new TombstoneShardSpec()
    );

    final Set<DataSegment> tombstones = new HashSet<>(Collections.singleton(tombstoneSegment));
    Assert.assertTrue(coordinator.commitSegments(tombstones).containsAll(tombstones));

    // Allocate and commit a data segment by appending to the same interval
    final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
        DS.WIKI,
        "seq",
        tombstoneSegment.getVersion(),
        interval,
        NumberedPartialShardSpec.instance(),
        "version",
        false
    );

    Assert.assertEquals("wiki_2020-01-01T00:00:00.000Z_2021-01-01T00:00:00.000Z_version_1", identifier.toString());
    Assert.assertEquals(0, identifier.getShardSpec().getNumCorePartitions());

    final DataSegment dataSegment = createSegment(
        interval,
        "version",
        identifier.getShardSpec()
    );
    final Set<DataSegment> dataSegments = new HashSet<>(Collections.singleton(dataSegment));
    Assert.assertTrue(coordinator.commitSegments(dataSegments).containsAll(dataSegments));

    // Mark the tombstone as unused
    markAllSegmentsUnused(tombstones, DateTimes.nowUtc());

    final Collection<DataSegment> allUsedSegments = coordinator.retrieveAllUsedSegments(
        DS.WIKI,
        Segments.ONLY_VISIBLE
    );

    // The appended data segment will still be visible in the timeline since the
    // tombstone contains 0 core partitions
    SegmentTimeline segmentTimeline = SegmentTimeline.forSegments(allUsedSegments);
    Assert.assertEquals(1, segmentTimeline.lookup(interval).size());
    Assert.assertEquals(dataSegment, segmentTimeline.lookup(interval).get(0).getObject().getChunk(1).getObject());
  }

  @Test
  public void testTimelineWith1CorePartitionTombstone() throws IOException
  {
    // Register the old generation tombstone spec for this test.
    mapper.registerSubtypes(TombstoneShardSpecWith1CorePartition.class);

    final Interval interval = Intervals.of("2020/2021");
    // Create and commit an old generation tombstone with 1 core partition
    final DataSegment tombstoneSegment = createSegment(
        interval,
        "version",
        new TombstoneShardSpecWith1CorePartition()
    );

    final Set<DataSegment> tombstones = new HashSet<>(Collections.singleton(tombstoneSegment));
    Assert.assertTrue(coordinator.commitSegments(tombstones).containsAll(tombstones));

    // Allocate and commit a data segment by appending to the same interval
    final SegmentIdWithShardSpec identifier = coordinator.allocatePendingSegment(
        DS.WIKI,
        "seq",
        tombstoneSegment.getVersion(),
        interval,
        NumberedPartialShardSpec.instance(),
        "version",
        false
    );

    Assert.assertEquals("wiki_2020-01-01T00:00:00.000Z_2021-01-01T00:00:00.000Z_version_1", identifier.toString());
    Assert.assertEquals(1, identifier.getShardSpec().getNumCorePartitions());

    final DataSegment dataSegment = createSegment(
        interval,
        "version",
        identifier.getShardSpec()
    );
    final Set<DataSegment> dataSegments = new HashSet<>(Collections.singleton(dataSegment));
    Assert.assertTrue(coordinator.commitSegments(dataSegments).containsAll(dataSegments));

    // Mark the tombstone as unused
    markAllSegmentsUnused(tombstones, DateTimes.nowUtc());

    final Collection<DataSegment> allUsedSegments = coordinator.retrieveAllUsedSegments(
        DS.WIKI,
        Segments.ONLY_VISIBLE
    );

    // The appended data segment will not be visible in the timeline since the old generation
    // tombstone contains 1 core partition
    SegmentTimeline segmentTimeline = SegmentTimeline.forSegments(allUsedSegments);
    Assert.assertEquals(0, segmentTimeline.lookup(interval).size());
  }

  private static class DS
  {
    static final String WIKI = "wiki";
  }

  private DataSegment createSegment(Interval interval, String version, ShardSpec shardSpec)
  {
    return DataSegment.builder()
                      .dataSource(DS.WIKI)
                      .interval(interval)
                      .version(version)
                      .shardSpec(shardSpec)
                      .size(100)
                      .build();
  }

  private List<DataSegment> createAndGetUsedYearSegments(final int startYear, final int endYear) throws IOException
  {
    final List<DataSegment> segments = new ArrayList<>();

    for (int year = startYear; year < endYear; year++) {
      segments.add(createSegment(
          Intervals.of("%d/%d", year, year + 1),
          "version",
          new LinearShardSpec(0))
      );
    }
    final Set<DataSegment> segmentsSet = new HashSet<>(segments);
    final Set<DataSegment> committedSegments = coordinator.commitSegments(segmentsSet);
    Assert.assertTrue(committedSegments.containsAll(segmentsSet));

    return segments;
  }

  private ImmutableList<DataSegment> retrieveUnusedSegments(
      final List<Interval> intervals,
      final Integer limit,
      final String lastSegmentId,
      final SortOrder sortOrder,
      final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return derbyConnector.inReadOnlyTransaction(
        (handle, status) -> {
          try (final CloseableIterator<DataSegment> iterator =
                   SqlSegmentsMetadataQuery.forHandle(
                           handle,
                           derbyConnector,
                           derbyConnectorRule.metadataTablesConfigSupplier().get(),
                           mapper
                       )
                       .retrieveUnusedSegments(DS.WIKI, intervals, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)) {
            return ImmutableList.copyOf(iterator);
          }
        }
    );
  }

  private ImmutableList<DataSegmentPlus> retrieveUnusedSegmentsPlus(
      final List<Interval> intervals,
      final Integer limit,
      final String lastSegmentId,
      final SortOrder sortOrder,
      final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return derbyConnector.inReadOnlyTransaction(
        (handle, status) -> {
          try (final CloseableIterator<DataSegmentPlus> iterator =
                   SqlSegmentsMetadataQuery.forHandle(
                           handle,
                           derbyConnector,
                           derbyConnectorRule.metadataTablesConfigSupplier().get(),
                           mapper
                       )
                       .retrieveUnusedSegmentsPlus(DS.WIKI, intervals, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)) {
            return ImmutableList.copyOf(iterator);
          }
        }
    );
  }

  private void verifyContainsAllSegmentsPlus(
      List<DataSegment> expectedSegments,
      List<DataSegmentPlus> actualUnusedSegmentsPlus,
      DateTime usedStatusLastUpdatedTime)
  {
    Map<SegmentId, DataSegment> expectedIdToSegment = expectedSegments.stream().collect(Collectors.toMap(DataSegment::getId, Function.identity()));
    Map<SegmentId, DataSegmentPlus> actualIdToSegmentPlus = actualUnusedSegmentsPlus.stream()
        .collect(Collectors.toMap(d -> d.getDataSegment().getId(), Function.identity()));
    Assert.assertTrue(expectedIdToSegment.entrySet().stream().allMatch(e -> {
      DataSegmentPlus segmentPlus = actualIdToSegmentPlus.get(e.getKey());
      return segmentPlus != null
             && !segmentPlus.getCreatedDate().isAfter(usedStatusLastUpdatedTime)
             && segmentPlus.getUsedStatusLastUpdatedDate() != null
             && segmentPlus.getUsedStatusLastUpdatedDate().equals(usedStatusLastUpdatedTime);
    }));
  }

  private void verifyEqualsAllSegmentsPlus(
      List<DataSegment> expectedSegments,
      List<DataSegmentPlus> actualUnusedSegmentsPlus,
      DateTime usedStatusLastUpdatedTime
  )
  {
    Assert.assertEquals(expectedSegments.size(), actualUnusedSegmentsPlus.size());
    for (int i = 0; i < expectedSegments.size(); i++) {
      DataSegment expectedSegment = expectedSegments.get(i);
      DataSegmentPlus actualSegmentPlus = actualUnusedSegmentsPlus.get(i);
      Assert.assertEquals(expectedSegment.getId(), actualSegmentPlus.getDataSegment().getId());
      Assert.assertTrue(!actualSegmentPlus.getCreatedDate().isAfter(usedStatusLastUpdatedTime)
                        && actualSegmentPlus.getUsedStatusLastUpdatedDate() != null
                        && actualSegmentPlus.getUsedStatusLastUpdatedDate().equals(usedStatusLastUpdatedTime));
    }
  }

  /**
   * This test-only shard type is to test the behavior of "old generation" tombstones with 1 core partition.
   */
  private static class TombstoneShardSpecWith1CorePartition extends TombstoneShardSpec
  {
    @Override
    @JsonProperty("partitions")
    public int getNumCorePartitions()
    {
      return 1;
    }
  }
}
