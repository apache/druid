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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.metadata.SegmentSchemaTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class IndexerSqlMetadataStorageCoordinatorSchemaPersistenceTest extends
    IndexerSqlMetadataStorageCoordinatorTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.create(true));

  @Before
  public void setUp()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    mapper.registerSubtypes(LinearShardSpec.class, NumberedShardSpec.class, HashBasedNumberedShardSpec.class);
    derbyConnector.createDataSourceTable();
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();
    derbyConnector.createUpgradeSegmentsTable();
    derbyConnector.createPendingSegmentsTable();
    metadataUpdateCounter.set(0);
    segmentTableDropUpdateCounter.set(0);

    fingerprintGenerator = new FingerprintGenerator(mapper);
    segmentSchemaManager = new SegmentSchemaManager(derbyConnectorRule.metadataTablesConfigSupplier().get(), mapper, derbyConnector);
    segmentSchemaTestUtils = new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, mapper);

    CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig = new CentralizedDatasourceSchemaConfig();
    centralizedDatasourceSchemaConfig.setEnabled(true);

    coordinator = new IndexerSQLMetadataStorageCoordinator(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        centralizedDatasourceSchemaConfig
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
    final SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);
    final Set<DataSegment> expectedSegmentsToUpgrade = new HashSet<>();

    Random random = new Random(5);

    final Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();

    for (int i = 0; i < 10; i++) {
      final DataSegment segment = createSegment(
          Intervals.of("2023-01-01/2023-01-02"),
          v1,
          new LinearShardSpec(i)
      );
      appendSegments.add(segment);
      expectedSegmentsToUpgrade.add(segment);

      int randomNum = random.nextInt();
      RowSignature rowSignature = RowSignature.builder().add("c" + randomNum, ColumnType.FLOAT).build();

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
      segmentSchemaMapping.addSchema(
          segment.getId(),
          new SchemaPayloadPlus(schemaPayload, (long) randomNum),
          fingerprintGenerator.generateFingerprint(
              schemaPayload,
              segment.getDataSource(),
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
          )
      );

      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, randomNum));
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
        = coordinator.commitAppendSegments(appendSegments, segmentToReplaceLock, "append", segmentSchemaMapping);
    Assert.assertTrue(commitResult.isSuccess());
    Assert.assertEquals(appendSegments, commitResult.getSegments());

    // Verify the segments present in the metadata store
    Assert.assertEquals(
        appendSegments,
        ImmutableSet.copyOf(retrieveUsedSegments(derbyConnectorRule.metadataTablesConfigSupplier().get()))
    );

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);

    // Verify entries in the segment task lock table
    final Set<String> expectedUpgradeSegmentIds
        = expectedSegmentsToUpgrade.stream()
                                   .map(s -> s.getId().toString())
                                   .collect(Collectors.toSet());
    final Map<String, String> observedSegmentToLock = getSegmentsCommittedDuringReplaceTask(
        replaceTaskId,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    Assert.assertEquals(expectedUpgradeSegmentIds, observedSegmentToLock.keySet());

    final Set<String> observedLockVersions = new HashSet<>(observedSegmentToLock.values());
    Assert.assertEquals(1, observedLockVersions.size());
    Assert.assertEquals(replaceLock.getVersion(), Iterables.getOnlyElement(observedLockVersions));
  }

  @Test
  public void testAnnounceHistoricalSegments() throws IOException
  {
    Set<DataSegment> segments = new HashSet<>();
    SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);
    Random random = ThreadLocalRandom.current();
    Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();

    for (int i = 0; i < 105; i++) {
      DataSegment segment = new DataSegment(
          "fooDataSource",
          Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
          "version",
          ImmutableMap.of(),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(i),
          9,
          100
      );
      segments.add(segment);

      int randomNum = random.nextInt();
      RowSignature rowSignature = RowSignature.builder().add("c" + randomNum, ColumnType.FLOAT).build();

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, randomNum));
      segmentSchemaMapping.addSchema(
          segment.getId(),
          new SchemaPayloadPlus(schemaPayload, (long) randomNum),
          fingerprintGenerator.generateFingerprint(
              schemaPayload,
              segment.getDataSource(),
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
          )
      );
    }

    coordinator.commitSegments(segments, segmentSchemaMapping);
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

    Assert.assertEquals(segmentIds, retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get()));

    // Should not update dataSource metadata.
    Assert.assertEquals(0, metadataUpdateCounter.get());

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
  }

  @Test
  public void testAnnounceHistoricalSegments_schemaExists() throws IOException
  {
    Set<DataSegment> segments = new HashSet<>();
    SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);
    Random random = ThreadLocalRandom.current();
    Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();

    Map<String, SchemaPayload> schemaPayloadMapToPerist = new HashMap<>();

    for (int i = 0; i < 105; i++) {
      DataSegment segment = new DataSegment(
          "fooDataSource",
          Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
          "version",
          ImmutableMap.of(),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(i),
          9,
          100
      );
      segments.add(segment);

      int randomNum = random.nextInt();
      RowSignature rowSignature = RowSignature.builder().add("c" + randomNum, ColumnType.FLOAT).build();

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, randomNum));
      String fingerprint =
          fingerprintGenerator.generateFingerprint(
              schemaPayload,
              segment.getDataSource(),
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
          );
      segmentSchemaMapping.addSchema(
          segment.getId(),
          new SchemaPayloadPlus(schemaPayload, (long) randomNum),
          fingerprint
      );

      schemaPayloadMapToPerist.put(fingerprint, schemaPayload);
    }

    derbyConnector.retryWithHandle(handle -> {
      segmentSchemaManager.persistSegmentSchema(handle, "fooDataSource", CentralizedDatasourceSchemaConfig.SCHEMA_VERSION, schemaPayloadMapToPerist);
      return null;
    });

    coordinator.commitSegments(segments, segmentSchemaMapping);
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

    Assert.assertEquals(segmentIds, retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get()));

    // Should not update dataSource metadata.
    Assert.assertEquals(0, metadataUpdateCounter.get());

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
  }

  @Test
  public void testCommitReplaceSegments()
  {
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock("g1", Intervals.of("2023-01-01/2023-02-01"), "2023-02-01");
    final Set<DataSegment> segmentsAppendedWithReplaceLock = new HashSet<>();
    final Map<DataSegment, ReplaceTaskLock> appendedSegmentToReplaceLockMap = new HashMap<>();

    final Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();
    final Map<String, Pair<String, Long>> segmentStatsMap = new HashMap<>();
    Random random = new Random(5);

    Map<String, SchemaPayload> schemaPayloadMap = new HashMap<>();

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

      RowSignature rowSignature = RowSignature.builder().add("c6", ColumnType.FLOAT).build();

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
      schemaPayloadMap.put(
          fingerprintGenerator.generateFingerprint(
              schemaPayload,
              segment.getDataSource(),
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
          ),
          schemaPayload
      );
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, 6));

      segmentsAppendedWithReplaceLock.add(segment);
      appendedSegmentToReplaceLockMap.put(segment, replaceLock);
    }

    segmentSchemaTestUtils.insertSegmentSchema("foo", schemaPayloadMap, schemaPayloadMap.keySet());

    for (Map.Entry<String, Pair<SchemaPayload, Integer>> entry : segmentIdSchemaMap.entrySet()) {
      String segmentId = entry.getKey();
      String fingerprint = fingerprintGenerator.generateFingerprint(entry.getValue().lhs, "foo", CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);
      long numRows = entry.getValue().rhs;
      segmentStatsMap.put(segmentId, Pair.of(fingerprint, numRows));
    }

    segmentSchemaTestUtils.insertUsedSegments(segmentsAppendedWithReplaceLock, segmentStatsMap);
    insertIntoUpgradeSegmentsTable(appendedSegmentToReplaceLockMap, derbyConnectorRule.metadataTablesConfigSupplier().get());

    final SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);
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
      int randomNum = random.nextInt();
      RowSignature rowSignature = RowSignature.builder().add("c" + randomNum, ColumnType.FLOAT).build();
      SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
      segmentSchemaMapping.addSchema(
          segment.getId(),
          new SchemaPayloadPlus(schemaPayload, (long) randomNum),
          fingerprintGenerator.generateFingerprint(
              schemaPayload,
              segment.getDataSource(),
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
          )
      );
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, randomNum));
      replacingSegments.add(segment);
    }

    coordinator.commitReplaceSegments(replacingSegments, ImmutableSet.of(replaceLock), segmentSchemaMapping);

    Assert.assertEquals(
        2L * segmentsAppendedWithReplaceLock.size() + replacingSegments.size(),
        retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get()).size()
    );

    final Set<DataSegment> usedSegments = new HashSet<>(retrieveUsedSegments(derbyConnectorRule.metadataTablesConfigSupplier().get()));

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
      RowSignature rowSignature = RowSignature.builder().add("c6", ColumnType.FLOAT).build();
      SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
      segmentIdSchemaMap.put(segmentReplicaWithNewVersion.getId().toString(), Pair.of(schemaPayload, 6));
      Assert.assertTrue(hasBeenCarriedForward);
    }

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
  }
}
