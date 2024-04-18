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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.metadata.SegmentSchemaTestUtils;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IndexerSqlMetadataStorageCoordinatorTestBase
{
  protected static final int MAX_SQL_MEATADATA_RETRY_FOR_TEST = 2;

  protected final ObjectMapper mapper = TestHelper.makeJsonMapper();

  protected final DataSegment defaultSegment = new DataSegment(
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

  protected final DataSegment eternitySegment = new DataSegment(
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


  protected final DataSegment firstHalfEternityRangeSegment = new DataSegment(
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

  protected final DataSegment secondHalfEternityRangeSegment = new DataSegment(
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
  protected final DataSegment defaultSegment2 = new DataSegment(
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

  protected final DataSegment defaultSegment2WithBiggerSize = new DataSegment(
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

  protected final DataSegment defaultSegment3 = new DataSegment(
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
  protected final DataSegment defaultSegment4 = new DataSegment(
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

  protected final DataSegment numberedSegment0of0 = new DataSegment(
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

  protected final DataSegment numberedSegment1of0 = new DataSegment(
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

  protected final DataSegment numberedSegment2of0 = new DataSegment(
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

  protected final DataSegment numberedSegment2of1 = new DataSegment(
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

  protected final DataSegment numberedSegment3of1 = new DataSegment(
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

  protected final DataSegment existingSegment1 = new DataSegment(
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

  protected final DataSegment existingSegment2 = new DataSegment(
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

  protected final DataSegment hugeTimeRangeSegment1 = new DataSegment(
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

  protected final DataSegment hugeTimeRangeSegment2 = new DataSegment(
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

  protected final DataSegment hugeTimeRangeSegment3 = new DataSegment(
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

  protected final DataSegment hugeTimeRangeSegment4 = new DataSegment(
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

  protected final Set<DataSegment> SEGMENTS = ImmutableSet.of(defaultSegment, defaultSegment2);
  protected final AtomicLong metadataUpdateCounter = new AtomicLong();
  protected final AtomicLong segmentTableDropUpdateCounter = new AtomicLong();

  protected IndexerSQLMetadataStorageCoordinator coordinator;
  protected TestDerbyConnector derbyConnector;
  protected TestDerbyConnector.SegmentsTable segmentsTable;
  protected SegmentSchemaManager segmentSchemaManager;
  protected FingerprintGenerator fingerprintGenerator;
  protected SegmentSchemaTestUtils segmentSchemaTestUtils;

  protected static class DS
  {
    static final String WIKI = "wiki";
  }

  protected DataSegment createSegment(Interval interval, String version, ShardSpec shardSpec)
  {
    return DataSegment.builder()
                      .dataSource(DS.WIKI)
                      .interval(interval)
                      .version(version)
                      .shardSpec(shardSpec)
                      .size(100)
                      .build();
  }

  protected List<DataSegment> createAndGetUsedYearSegments(final int startYear, final int endYear) throws IOException
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
    final Set<DataSegment> committedSegments = coordinator.commitSegments(segmentsSet, new SegmentSchemaMapping(
        CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    Assert.assertTrue(committedSegments.containsAll(segmentsSet));

    return segments;
  }

  protected ImmutableList<DataSegment> retrieveUnusedSegments(
      final List<Interval> intervals,
      final Integer limit,
      final String lastSegmentId,
      final SortOrder sortOrder,
      final DateTime maxUsedStatusLastUpdatedTime,
      final MetadataStorageTablesConfig tablesConfig
  )
  {
    return derbyConnector.inReadOnlyTransaction(
        (handle, status) -> {
          try (final CloseableIterator<DataSegment> iterator =
                   SqlSegmentsMetadataQuery.forHandle(
                                               handle,
                                               derbyConnector,
                                               tablesConfig,
                                               mapper
                                           )
                                           .retrieveUnusedSegments(DS.WIKI, intervals, null, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)) {
            return ImmutableList.copyOf(iterator);
          }
        }
    );
  }

  protected ImmutableList<DataSegmentPlus> retrieveUnusedSegmentsPlus(
      final List<Interval> intervals,
      final Integer limit,
      final String lastSegmentId,
      final SortOrder sortOrder,
      final DateTime maxUsedStatusLastUpdatedTime,
      MetadataStorageTablesConfig tablesConfig
  )
  {
    return derbyConnector.inReadOnlyTransaction(
        (handle, status) -> {
          try (final CloseableIterator<DataSegmentPlus> iterator =
                   SqlSegmentsMetadataQuery.forHandle(
                                               handle,
                                               derbyConnector,
                                               tablesConfig,
                                               mapper
                                           )
                                           .retrieveUnusedSegmentsPlus(DS.WIKI, intervals, null, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)) {
            return ImmutableList.copyOf(iterator);
          }
        }
    );
  }

  protected void verifyContainsAllSegmentsPlus(
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

  protected void verifyEqualsAllSegmentsPlus(
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
  protected static class TombstoneShardSpecWith1CorePartition extends TombstoneShardSpec
  {
    @Override
    @JsonProperty("partitions")
    public int getNumCorePartitions()
    {
      return 1;
    }
  }


  protected void markAllSegmentsUnused()
  {
    markAllSegmentsUnused(SEGMENTS, DateTimes.nowUtc());
  }

  protected void markAllSegmentsUnused(Set<DataSegment> segments, DateTime usedStatusLastUpdatedTime)
  {
    for (final DataSegment segment : segments) {
      Assert.assertEquals(
          1,
          segmentsTable.update(
              "UPDATE %s SET used = false, used_status_last_updated = ? WHERE id = ?",
              usedStatusLastUpdatedTime.toString(),
              segment.getId().toString()
          )
      );
    }
  }

  protected List<String> retrievePendingSegmentIds(MetadataStorageTablesConfig tablesConfig)
  {
    final String table = tablesConfig.getPendingSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id FROM " + table + "  ORDER BY id")
                        .map(StringMapper.FIRST)
                        .list()
    );
  }

  protected List<String> retrieveUsedSegmentIds(MetadataStorageTablesConfig tablesConfig)
  {
    final String table = tablesConfig.getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id FROM " + table + " WHERE used = true ORDER BY id")
                        .map(StringMapper.FIRST)
                        .list()
    );
  }

  protected List<DataSegment> retrieveUsedSegments(MetadataStorageTablesConfig tablesConfig)
  {
    final String table = tablesConfig.getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT payload FROM " + table + " WHERE used = true ORDER BY id")
                        .map((index, result, context) -> JacksonUtils.readValue(mapper, result.getBytes(1), DataSegment.class))
                        .list()
    );
  }

  protected List<String> retrieveUnusedSegmentIds(MetadataStorageTablesConfig tablesConfig)
  {
    final String table = tablesConfig.getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id FROM " + table + " WHERE used = false ORDER BY id")
                        .map(StringMapper.FIRST)
                        .list()
    );
  }

  protected Map<String, String> getSegmentsCommittedDuringReplaceTask(String taskId, MetadataStorageTablesConfig tablesConfig)
  {
    final String table = tablesConfig.getUpgradeSegmentsTable();
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

  protected void insertIntoUpgradeSegmentsTable(Map<DataSegment, ReplaceTaskLock> segmentToTaskLockMap, MetadataStorageTablesConfig tablesConfig)
  {
    final String table = tablesConfig.getUpgradeSegmentsTable();
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
}
