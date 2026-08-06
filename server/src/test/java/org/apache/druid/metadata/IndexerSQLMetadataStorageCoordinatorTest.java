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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentCreateRequest;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.segment.SegmentMetadataTransaction;
import org.apache.druid.metadata.segment.SqlSegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.Metric;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.metadata.HeapMemoryIndexingStateStorage;
import org.apache.druid.segment.metadata.NoopIndexingStateCache;
import org.apache.druid.segment.metadata.NoopSegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.metadata.SegmentSchemaTestUtils;
import org.apache.druid.segment.metadata.SqlIndexingStateStorage;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.DimensionValueSetShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedPartialShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.LinearShardSpec;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IndexerSQLMetadataStorageCoordinatorTest extends IndexerSqlMetadataStorageCoordinatorTestBase
{
  private static final String SUPERVISOR_ID = "supervisor";
  @RegisterExtension
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDruidLeaderSelector leaderSelector;
  private SegmentMetadataCache segmentMetadataCache;
  private StubServiceEmitter emitter;
  private SqlSegmentMetadataTransactionFactory transactionFactory;
  private BlockingExecutorService cachePollExecutor;
  private SqlIndexingStateStorage indexingStateStorage;

  private SegmentMetadataCache.UsageMode cacheMode;

  public static Object[][] testParameters()
  {
    return new Object[][]{
        {SegmentMetadataCache.UsageMode.ALWAYS},
        {SegmentMetadataCache.UsageMode.NEVER},
        {SegmentMetadataCache.UsageMode.IF_SYNCED}
    };
  }

  public void initIndexerSQLMetadataStorageCoordinatorTest(SegmentMetadataCache.UsageMode cacheMode)
  {
    this.cacheMode = cacheMode;
    derbyConnector = derbyConnectorRule.getConnector();
    segmentsTable = derbyConnectorRule.segments();
    mapper.registerSubtypes(
        LinearShardSpec.class,
        NumberedShardSpec.class,
        HashBasedNumberedShardSpec.class,
        DimensionValueSetShardSpec.class
    );
    derbyConnector.createDataSourceTable();
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentTable();
    derbyConnector.createUpgradeSegmentsTable();
    derbyConnector.createPendingSegmentsTable();
    derbyConnector.createIndexingStatesTable();
    metadataUpdateCounter.set(0);
    segmentTableDropUpdateCounter.set(0);

    fingerprintGenerator = new FingerprintGenerator(mapper);
    segmentSchemaManager = new SegmentSchemaManager(derbyConnectorRule.metadataTablesConfigSupplier().get(), mapper, derbyConnector);
    segmentSchemaTestUtils = new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, mapper);
    indexingStateStorage = new SqlIndexingStateStorage(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        mapper,
        derbyConnector
    );

    emitter = new StubServiceEmitter();
    leaderSelector = new TestDruidLeaderSelector();

    cachePollExecutor = new BlockingExecutorService("test-cache-poll-exec");

    segmentMetadataCache = new HeapMemorySegmentMetadataCache(
        mapper,
        () -> new SegmentsMetadataManagerConfig(null, cacheMode, null),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        new NoopSegmentSchemaCache(),
        new NoopIndexingStateCache(),
        derbyConnector,
        (corePoolSize, nameFormat) -> new WrappingScheduledExecutorService(
            nameFormat,
            cachePollExecutor,
            false
        ),
        emitter
    );

    leaderSelector.becomeLeader();

    // Get the cache ready if required
    if (isCacheEnabled()) {
      segmentMetadataCache.start();
      segmentMetadataCache.becomeLeader();
      refreshCache();
      refreshCache();
    }

    transactionFactory = new SqlSegmentMetadataTransactionFactory(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        leaderSelector,
        segmentMetadataCache,
        emitter
    )
    {
      @Override
      public int getMaxRetries()
      {
        return MAX_SQL_MEATADATA_RETRY_FOR_TEST;
      }
    };
    coordinator = new IndexerSQLMetadataStorageCoordinator(
        transactionFactory,
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create(),
        indexingStateStorage
    )
    {
      @Override
      protected SegmentPublishResult updateDataSourceMetadataInTransaction(
          SegmentMetadataTransaction transaction,
          String supervisorId,
          String dataSource,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        // Count number of times this method is called.
        metadataUpdateCounter.getAndIncrement();
        return super.updateDataSourceMetadataInTransaction(transaction, supervisorId, dataSource, startMetadata, endMetadata);
      }
    };
  }

  @AfterEach
  public void tearDown()
  {
    segmentMetadataCache.stopBeingLeader();
    segmentMetadataCache.stop();
    leaderSelector.stopBeingLeader();
  }

  private void refreshCache()
  {
    if (isCacheEnabled()) {
      cachePollExecutor.finishNextPendingTasks(2);
    }
  }
  
  private boolean isCacheEnabled()
  {
    return cacheMode != SegmentMetadataCache.UsageMode.NEVER;
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitAppendSegments(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String v1 = "2023-01-01";
    final String v2 = "2023-01-02";
    final String v3 = "2023-01-03";
    final String alreadyUpgradedVersion = "2023-02-01";
    final String lockVersion = "2024-01-01";

    final String taskAllocatorId = "appendTask";
    final String replaceTaskId = "replaceTask1";
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock(
        replaceTaskId,
        Intervals.of("2023-01-01/2023-01-03"),
        lockVersion
    );

    final Set<DataSegment> appendSegments = new HashSet<>();
    final List<PendingSegmentRecord> pendingSegmentsForTask = new ArrayList<>();
    final Set<DataSegment> expectedSegmentsToUpgrade = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      final DataSegment segment = createSegment(
          Intervals.of("2023-01-01/2023-01-02"),
          v1,
          new LinearShardSpec(i)
      );
      appendSegments.add(segment);
      expectedSegmentsToUpgrade.add(segment);
      // Add the same segment
      pendingSegmentsForTask.add(
          PendingSegmentRecord.create(
              SegmentIdWithShardSpec.fromDataSegment(segment),
              v1,
              segment.getId().toString(),
              null,
              taskAllocatorId
          )
      );
      // Add upgraded pending segment
      pendingSegmentsForTask.add(
          PendingSegmentRecord.create(
              new SegmentIdWithShardSpec(
                  TestDataSource.WIKI,
                  Intervals.of("2023-01-01/2023-02-01"),
                  alreadyUpgradedVersion,
                  new NumberedShardSpec(i, 0)
              ),
              alreadyUpgradedVersion,
              segment.getId().toString(),
              segment.getId().toString(),
              taskAllocatorId
          )
      );
    }

    for (int i = 0; i < 10; i++) {
      final DataSegment segment = createSegment(
          Intervals.of("2023-01-02/2023-01-03"),
          v2,
          new LinearShardSpec(i)
      );
      appendSegments.add(segment);
      expectedSegmentsToUpgrade.add(segment);
      // Add the same segment
      pendingSegmentsForTask.add(
          PendingSegmentRecord.create(
              SegmentIdWithShardSpec.fromDataSegment(segment),
              v2,
              segment.getId().toString(),
              null,
              taskAllocatorId
          )
      );
      // Add upgraded pending segment
      pendingSegmentsForTask.add(
          PendingSegmentRecord.create(
              new SegmentIdWithShardSpec(
                  TestDataSource.WIKI,
                  Intervals.of("2023-01-01/2023-02-01"),
                  alreadyUpgradedVersion,
                  new NumberedShardSpec(10 + i, 0)
              ),
              alreadyUpgradedVersion,
              segment.getId().toString(),
              segment.getId().toString(),
              taskAllocatorId
          )
      );
    }

    for (int i = 0; i < 10; i++) {
      final DataSegment segment = createSegment(
          Intervals.of("2023-01-03/2023-01-04"),
          v3,
          new LinearShardSpec(i)
      );
      appendSegments.add(segment);
      // Add the same segment
      pendingSegmentsForTask.add(
          PendingSegmentRecord.create(
              SegmentIdWithShardSpec.fromDataSegment(segment),
              v3,
              segment.getId().toString(),
              null,
              taskAllocatorId
          )
      );
      // Add upgraded pending segment
      pendingSegmentsForTask.add(
          PendingSegmentRecord.create(
              new SegmentIdWithShardSpec(
                  TestDataSource.WIKI,
                  Intervals.of("2023-01-01/2023-02-01"),
                  alreadyUpgradedVersion,
                  new NumberedShardSpec(20 + i, 0)
              ),
              alreadyUpgradedVersion,
              segment.getId().toString(),
              segment.getId().toString(),
              taskAllocatorId
          )
      );
    }

    insertPendingSegments(TestDataSource.WIKI, pendingSegmentsForTask, false);

    final Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
        = expectedSegmentsToUpgrade.stream()
                                   .collect(Collectors.toMap(s -> s, s -> replaceLock));

    // Commit the segment and verify the results
    SegmentPublishResult commitResult
        = coordinator.commitAppendSegments(appendSegments, segmentToReplaceLock, taskAllocatorId, null);
    org.junit.jupiter.api.Assertions.assertTrue(commitResult.isSuccess());

    Set<DataSegment> allCommittedSegments
        = new HashSet<>(retrieveUsedSegments(derbyConnectorRule.metadataTablesConfigSupplier().get()));
    Map<String, String> upgradedFromSegmentIdMap = coordinator.retrieveUpgradedFromSegmentIds(
        TestDataSource.WIKI,
        allCommittedSegments.stream().map(DataSegment::getId).map(SegmentId::toString).collect(Collectors.toSet())
    );
    // Verify the segments present in the metadata store
    org.junit.jupiter.api.Assertions.assertTrue(allCommittedSegments.containsAll(appendSegments));
    for (DataSegment segment : appendSegments) {
      org.junit.jupiter.api.Assertions.assertNull(upgradedFromSegmentIdMap.get(segment.getId().toString()));
    }
    allCommittedSegments.removeAll(appendSegments);

    // Verify the commit of upgraded pending segments
    org.junit.jupiter.api.Assertions.assertEquals(appendSegments.size(), allCommittedSegments.size());
    Map<String, DataSegment> segmentMap = new HashMap<>();
    for (DataSegment segment : appendSegments) {
      segmentMap.put(segment.getId().toString(), segment);
    }
    for (DataSegment segment : allCommittedSegments) {
      for (PendingSegmentRecord pendingSegmentRecord : pendingSegmentsForTask) {
        if (pendingSegmentRecord.getId().asSegmentId().toString().equals(segment.getId().toString())) {
          DataSegment upgradedFromSegment = segmentMap.get(pendingSegmentRecord.getUpgradedFromSegmentId());
          org.junit.jupiter.api.Assertions.assertNotNull(upgradedFromSegment);
          org.junit.jupiter.api.Assertions.assertEquals(segment.getLoadSpec(), upgradedFromSegment.getLoadSpec());
          org.junit.jupiter.api.Assertions.assertEquals(
              pendingSegmentRecord.getUpgradedFromSegmentId(),
              upgradedFromSegmentIdMap.get(segment.getId().toString())
          );
        }
      }
    }

    // Verify entries in the segment task lock table
    final Set<String> expectedUpgradeSegmentIds
        = expectedSegmentsToUpgrade.stream()
                                   .map(s -> s.getId().toString())
                                   .collect(Collectors.toSet());
    final Map<String, String> observedSegmentToLock = getSegmentsCommittedDuringReplaceTask(
        replaceTaskId,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(expectedUpgradeSegmentIds, observedSegmentToLock.keySet());

    final Set<String> observedLockVersions = new HashSet<>(observedSegmentToLock.values());
    org.junit.jupiter.api.Assertions.assertEquals(1, observedLockVersions.size());
    org.junit.jupiter.api.Assertions.assertEquals(replaceLock.getVersion(), Iterables.getOnlyElement(observedLockVersions));
  }

  /**
   * When a concurrent REPLACE upgrades a still-appending task, the upgraded copy must take its partition number and
   * core-partition count from the (numbered) pending segment while preserving the original append segment's
   * {@link DimensionValueSetShardSpec}, so it stays prunable by the broker.
   */
  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitAppendSegments_upgradedSegmentPreservesDimensionValueSetShardSpec(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String appendVersion = "2023-01-01";
    final String upgradedVersion = "2023-02-01";

    final String taskAllocatorId = "appendTask";
    final String replaceTaskId = "replaceTask1";
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock(
        replaceTaskId,
        Intervals.of("2023-01-01/2023-02-01"),
        upgradedVersion
    );

    final Map<String, List<String>> partitionDimensionValues = ImmutableMap.of("tenant_id", ImmutableList.of("tenant_a"));
    // The published append segment carries a DimensionValueSetShardSpec, as stamped at publish time by the streaming task.
    final DataSegment appendSegment = createSegment(
        Intervals.of("2023-01-01/2023-01-02"),
        appendVersion,
        new DimensionValueSetShardSpec(0, 1, partitionDimensionValues)
    );

    final List<PendingSegmentRecord> pendingSegmentsForTask = new ArrayList<>();
    // The pending segment for the append segment itself.
    pendingSegmentsForTask.add(
        PendingSegmentRecord.create(
            SegmentIdWithShardSpec.fromDataSegment(appendSegment),
            appendVersion,
            appendSegment.getId().toString(),
            null,
            taskAllocatorId
        )
    );
    // The upgraded pending segment minted by the concurrent REPLACE — numbered, pointing back to the append segment.
    final SegmentIdWithShardSpec upgradedPendingId = new SegmentIdWithShardSpec(
        TestDataSource.WIKI,
        Intervals.of("2023-01-01/2023-02-01"),
        upgradedVersion,
        new NumberedShardSpec(5, 8)
    );
    pendingSegmentsForTask.add(
        PendingSegmentRecord.create(
            upgradedPendingId,
            upgradedVersion,
            appendSegment.getId().toString(),
            appendSegment.getId().toString(),
            taskAllocatorId
        )
    );
    insertPendingSegments(TestDataSource.WIKI, pendingSegmentsForTask, false);

    final SegmentPublishResult commitResult = coordinator.commitAppendSegments(
        Set.of(appendSegment),
        Map.of(appendSegment, replaceLock),
        taskAllocatorId,
        null
    );
    org.junit.jupiter.api.Assertions.assertTrue(commitResult.isSuccess());

    final Set<DataSegment> allCommittedSegments
        = new HashSet<>(retrieveUsedSegments(derbyConnectorRule.metadataTablesConfigSupplier().get()));
    final Map<String, String> upgradedFromSegmentIdMap = coordinator.retrieveUpgradedFromSegmentIds(
        TestDataSource.WIKI,
        allCommittedSegments.stream().map(DataSegment::getId).map(SegmentId::toString).collect(Collectors.toSet())
    );

    // The original append segment is published as-is, retaining its DimensionValueSetShardSpec.
    org.junit.jupiter.api.Assertions.assertTrue(allCommittedSegments.contains(appendSegment));
    org.junit.jupiter.api.Assertions.assertTrue(appendSegment.getShardSpec() instanceof DimensionValueSetShardSpec);

    // Find the upgraded copy (the one whose upgradedFromSegmentId points back to the append segment).
    DataSegment upgradedSegment = null;
    for (DataSegment segment : allCommittedSegments) {
      if (appendSegment.getId().toString().equals(upgradedFromSegmentIdMap.get(segment.getId().toString()))) {
        upgradedSegment = segment;
      }
    }
    org.junit.jupiter.api.Assertions.assertNotNull(upgradedSegment, "Expected an upgraded copy of the append segment");

    // The upgraded copy is published under the replace version, with the pending segment's partition number and core
    // partitions, but it preserves the original DimensionValueSetShardSpec (and partitionDimensionValues).
    org.junit.jupiter.api.Assertions.assertEquals(upgradedVersion, upgradedSegment.getVersion());
    org.junit.jupiter.api.Assertions.assertTrue(
        upgradedSegment.getShardSpec() instanceof DimensionValueSetShardSpec,
        "upgraded append segment should preserve DimensionValueSetShardSpec"
    );
    org.junit.jupiter.api.Assertions.assertEquals(
        partitionDimensionValues,
        ((DimensionValueSetShardSpec) upgradedSegment.getShardSpec()).getPartitionDimensionValues()
    );
    // Partition number and core partitions come from the (numbered) pending segment.
    org.junit.jupiter.api.Assertions.assertEquals(5, upgradedSegment.getShardSpec().getPartitionNum());
    org.junit.jupiter.api.Assertions.assertEquals(8, upgradedSegment.getShardSpec().getNumCorePartitions());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitReplaceSegments_partiallyOverlappingPendingSegmentUnsupported(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock("g1", Intervals.of("2023-01-01/2023-02-01"), "2023-02-01");
    final Set<DataSegment> segmentsAppendedWithReplaceLock = new HashSet<>();
    final Map<DataSegment, ReplaceTaskLock> appendedSegmentToReplaceLockMap = new HashMap<>();
    final PendingSegmentRecord pendingSegmentForInterval = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "foo",
            Intervals.of("2023-01-01/2024-01-01"),
            "2023-01-02",
            new NumberedShardSpec(100, 0)
        ),
        "",
        "",
        null,
        "append"
    );
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

    segmentSchemaTestUtils.insertUsedSegments(segmentsAppendedWithReplaceLock, Collections.emptyMap());
    insertPendingSegments("foo", List.of(pendingSegmentForInterval), true);
    insertIntoUpgradeSegmentsTable(appendedSegmentToReplaceLockMap, derbyConnectorRule.metadataTablesConfigSupplier().get());

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

    org.junit.jupiter.api.Assertions.assertFalse(
        coordinator.commitReplaceSegments(replacingSegments, ImmutableSet.of(replaceLock), null)
                   .isSuccess()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitReplaceSegments(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock("g1", Intervals.of("2023-01-01/2023-02-01"), "2023-02-01");
    final Set<DataSegment> segmentsAppendedWithReplaceLock = new HashSet<>();
    final Map<DataSegment, ReplaceTaskLock> appendedSegmentToReplaceLockMap = new HashMap<>();
    final PendingSegmentRecord pendingSegmentInInterval = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "foo",
            Intervals.of("2023-01-01/2023-01-02"),
            "2023-01-02",
            new NumberedShardSpec(100, 0)
        ),
        "",
        "",
        null,
        "append"
    );
    final PendingSegmentRecord pendingSegmentOutsideInterval = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "foo",
            Intervals.of("2023-04-01/2023-04-02"),
            "2023-01-02",
            new NumberedShardSpec(100, 0)
        ),
        "",
        "",
        null,
        "append"
    );
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

    segmentSchemaTestUtils.insertUsedSegments(segmentsAppendedWithReplaceLock, Collections.emptyMap());
    insertPendingSegments(
        "foo",
        List.of(pendingSegmentInInterval, pendingSegmentOutsideInterval),
        true
    );
    insertIntoUpgradeSegmentsTable(appendedSegmentToReplaceLockMap, derbyConnectorRule.metadataTablesConfigSupplier().get());

    final Set<DataSegment> replacingSegments = new HashSet<>();
    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-01/2023-02-01"),
          "2023-02-01",
          ImmutableMap.of("path", "b-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new NumberedShardSpec(i - 1, 8),
          9,
          100
      );
      replacingSegments.add(segment);
    }

    org.junit.jupiter.api.Assertions.assertTrue(coordinator.commitReplaceSegments(replacingSegments, Set.of(replaceLock), null).isSuccess());

    org.junit.jupiter.api.Assertions.assertEquals(
        2L * segmentsAppendedWithReplaceLock.size() + replacingSegments.size(),
        retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get()).size()
    );

    final Set<DataSegment> usedSegments
        = new HashSet<>(retrieveUsedSegments(derbyConnectorRule.metadataTablesConfigSupplier().get()));

    final Map<String, String> upgradedFromSegmentIdMap = coordinator.retrieveUpgradedFromSegmentIds(
        "foo",
        usedSegments.stream().map(DataSegment::getId).map(SegmentId::toString).collect(Collectors.toSet())
    );

    org.junit.jupiter.api.Assertions.assertTrue(usedSegments.containsAll(segmentsAppendedWithReplaceLock));
    for (DataSegment appendSegment : segmentsAppendedWithReplaceLock) {
      org.junit.jupiter.api.Assertions.assertNull(upgradedFromSegmentIdMap.get(appendSegment.getId().toString()));
    }
    usedSegments.removeAll(segmentsAppendedWithReplaceLock);
    org.junit.jupiter.api.Assertions.assertEquals(usedSegments, coordinator.retrieveAllUsedSegments("foo", Segments.ONLY_VISIBLE));

    org.junit.jupiter.api.Assertions.assertTrue(usedSegments.containsAll(replacingSegments));
    for (DataSegment replaceSegment : replacingSegments) {
      org.junit.jupiter.api.Assertions.assertNull(upgradedFromSegmentIdMap.get(replaceSegment.getId().toString()));
    }
    usedSegments.removeAll(replacingSegments);

    org.junit.jupiter.api.Assertions.assertEquals(segmentsAppendedWithReplaceLock.size(), usedSegments.size());
    for (DataSegment segmentReplicaWithNewVersion : usedSegments) {
      boolean hasBeenCarriedForward = false;
      for (DataSegment appendedSegment : segmentsAppendedWithReplaceLock) {
        if (appendedSegment.getLoadSpec().equals(segmentReplicaWithNewVersion.getLoadSpec())) {
          org.junit.jupiter.api.Assertions.assertEquals(
              appendedSegment.getId().toString(),
              upgradedFromSegmentIdMap.get(segmentReplicaWithNewVersion.getId().toString())
          );
          hasBeenCarriedForward = true;
          break;
        }
      }
      org.junit.jupiter.api.Assertions.assertTrue(hasBeenCarriedForward);
    }

    List<PendingSegmentRecord> pendingSegmentsInInterval =
        coordinator.getPendingSegments("foo", Intervals.of("2023-01-01/2023-02-01"));
    org.junit.jupiter.api.Assertions.assertEquals(2, pendingSegmentsInInterval.size());
    final SegmentId rootPendingSegmentId = pendingSegmentInInterval.getId().asSegmentId();
    if (pendingSegmentsInInterval.get(0).getUpgradedFromSegmentId() == null) {
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId, pendingSegmentsInInterval.get(0).getId().asSegmentId());
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId.toString(), pendingSegmentsInInterval.get(1).getUpgradedFromSegmentId());
    } else {
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId, pendingSegmentsInInterval.get(1).getId().asSegmentId());
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId.toString(), pendingSegmentsInInterval.get(0).getUpgradedFromSegmentId());
    }

    List<PendingSegmentRecord> pendingSegmentsOutsideInterval =
        coordinator.getPendingSegments("foo", Intervals.of("2023-04-01/2023-05-01"));
    org.junit.jupiter.api.Assertions.assertEquals(1, pendingSegmentsOutsideInterval.size());
    org.junit.jupiter.api.Assertions.assertEquals(
        pendingSegmentOutsideInterval.getId().asSegmentId(), pendingSegmentsOutsideInterval.get(0).getId().asSegmentId()
    );
  }

  /**
   * When a REPLACE commits over an interval with already-published APPEND segments held under a REPLACE lock, the
   * upgraded (re-versioned) copies must preserve their {@link DimensionValueSetShardSpec} so they remain prunable by
   * the broker.
   */
  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitReplaceSegments_upgradedPublishedSegmentPreservesDimensionValueSetShardSpec(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock("g1", Intervals.of("2023-01-01/2023-02-01"), "2023-02-01");

    final Map<String, List<String>> partitionDimensionValues = ImmutableMap.of("tenant_id", ImmutableList.of("tenant_a"));
    // A published APPEND segment carrying a DimensionValueSetShardSpec (as stamped at publish time by the streaming task).
    final DataSegment appendSegment = new DataSegment(
        "foo",
        Intervals.of("2023-01-01/2023-01-02"),
        "2023-01-01",
        ImmutableMap.of("path", "a-0"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new DimensionValueSetShardSpec(0, 1, partitionDimensionValues),
        9,
        100
    );
    segmentSchemaTestUtils.insertUsedSegments(Set.of(appendSegment), Collections.emptyMap());
    insertIntoUpgradeSegmentsTable(
        Map.of(appendSegment, replaceLock),
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );

    final Set<DataSegment> replacingSegments = new HashSet<>();
    for (int i = 0; i < 4; i++) {
      replacingSegments.add(
          new DataSegment(
              "foo",
              Intervals.of("2023-01-01/2023-02-01"),
              "2023-02-01",
              ImmutableMap.of("path", "b-" + i),
              ImmutableList.of("dim1"),
              ImmutableList.of("m1"),
              new NumberedShardSpec(i, 4),
              9,
              100
          )
      );
    }
    org.junit.jupiter.api.Assertions.assertTrue(coordinator.commitReplaceSegments(replacingSegments, Set.of(replaceLock), null).isSuccess());

    final Set<DataSegment> usedSegments
        = new HashSet<>(retrieveUsedSegments(derbyConnectorRule.metadataTablesConfigSupplier().get()));
    final Map<String, String> upgradedFromSegmentIdMap = coordinator.retrieveUpgradedFromSegmentIds(
        "foo",
        usedSegments.stream().map(DataSegment::getId).map(SegmentId::toString).collect(Collectors.toSet())
    );

    // Find the upgraded copy of the append segment (the one whose upgradedFromSegmentId points back to it).
    DataSegment upgradedSegment = null;
    for (DataSegment segment : usedSegments) {
      if (appendSegment.getId().toString().equals(upgradedFromSegmentIdMap.get(segment.getId().toString()))) {
        upgradedSegment = segment;
      }
    }
    org.junit.jupiter.api.Assertions.assertNotNull(upgradedSegment, "Expected an upgraded published segment");

    // The upgraded published segment is re-versioned to the replace version but keeps its DimensionValueSetShardSpec
    // (and partitionDimensionValues), so it remains prunable by the broker.
    org.junit.jupiter.api.Assertions.assertEquals("2023-02-01", upgradedSegment.getVersion());
    org.junit.jupiter.api.Assertions.assertTrue(
        upgradedSegment.getShardSpec() instanceof DimensionValueSetShardSpec,
        "upgraded published segment should preserve DimensionValueSetShardSpec"
    );
    org.junit.jupiter.api.Assertions.assertEquals(
        partitionDimensionValues,
        ((DimensionValueSetShardSpec) upgradedSegment.getShardSpec()).getPartitionDimensionValues()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitReplaceSegmentsWithUpdatedCorePartitions(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    // this test is very similar to testCommitReplaceSegments, except both append/replace segments use DimensionRangeShardSpec
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock("g1", Intervals.of("2023-01-01/2023-02-01"), "2023-02-01");
    final Set<DataSegment> segmentsAppendedWithReplaceLock = new HashSet<>();
    final Map<DataSegment, ReplaceTaskLock> appendedSegmentToReplaceLockMap = new HashMap<>();
    final PendingSegmentRecord pendingSegmentInInterval = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "foo",
            Intervals.of("2023-01-01/2023-01-02"),
            "2023-01-02",
            new NumberedShardSpec(100, 0)
        ),
        "",
        "",
        null,
        "append"
    );
    final PendingSegmentRecord pendingSegmentOutsideInterval = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(
            "foo",
            Intervals.of("2023-04-01/2023-04-02"),
            "2023-01-02",
            new NumberedShardSpec(100, 0)
        ),
        "",
        "",
        null,
        "append"
    );
    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-0" + i + "/2023-01-0" + (i + 1)),
          "2023-01-0" + i,
          ImmutableMap.of("path", "a-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new DimensionRangeShardSpec(List.of("dim1"), null, null, null, i - 1, 8),
          9,
          100
      );
      segmentsAppendedWithReplaceLock.add(segment);
      appendedSegmentToReplaceLockMap.put(segment, replaceLock);
    }

    segmentSchemaTestUtils.insertUsedSegments(segmentsAppendedWithReplaceLock, Collections.emptyMap());
    insertPendingSegments(
        "foo",
        List.of(pendingSegmentInInterval, pendingSegmentOutsideInterval),
        true
    );
    insertIntoUpgradeSegmentsTable(appendedSegmentToReplaceLockMap, derbyConnectorRule.metadataTablesConfigSupplier().get());

    final Set<DataSegment> replacingSegments = new HashSet<>();
    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-01/2023-02-01"),
          "2023-02-01",
          ImmutableMap.of("path", "b-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new DimensionRangeShardSpec(List.of("dim1"), null, null, null, i - 1, 8),
          9,
          100
      );
      replacingSegments.add(segment);
    }

    org.junit.jupiter.api.Assertions.assertTrue(coordinator.commitReplaceSegments(replacingSegments, Set.of(replaceLock), null).isSuccess());

    org.junit.jupiter.api.Assertions.assertEquals(
        2L * segmentsAppendedWithReplaceLock.size() + replacingSegments.size(),
        retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get()).size()
    );

    final Set<DataSegment> usedSegments
        = new HashSet<>(retrieveUsedSegments(derbyConnectorRule.metadataTablesConfigSupplier().get()));

    final Map<String, String> upgradedFromSegmentIdMap = coordinator.retrieveUpgradedFromSegmentIds(
        "foo",
        usedSegments.stream().map(DataSegment::getId).map(SegmentId::toString).collect(Collectors.toSet())
    );

    org.junit.jupiter.api.Assertions.assertTrue(usedSegments.containsAll(segmentsAppendedWithReplaceLock));
    for (DataSegment appendSegment : segmentsAppendedWithReplaceLock) {
      org.junit.jupiter.api.Assertions.assertNull(upgradedFromSegmentIdMap.get(appendSegment.getId().toString()));
    }
    usedSegments.removeAll(segmentsAppendedWithReplaceLock);

    Set<DataSegment> fetched = coordinator.retrieveAllUsedSegments("foo", Segments.ONLY_VISIBLE);
    org.junit.jupiter.api.Assertions.assertEquals(usedSegments, fetched);
    // all segments have the same corePartitions, exactly the size of replaced + appended
    List<ShardSpec> shardSpecs = fetched.stream().map(DataSegment::getShardSpec).toList();
    org.junit.jupiter.api.Assertions.assertTrue(shardSpecs.stream().allMatch(s -> s.getNumCorePartitions() == usedSegments.size()));
    org.junit.jupiter.api.Assertions.assertTrue(shardSpecs.stream().allMatch(s -> s instanceof DimensionRangeShardSpec));
    org.junit.jupiter.api.Assertions.assertTrue(usedSegments.containsAll(replacingSegments));
    for (DataSegment replaceSegment : replacingSegments) {
      org.junit.jupiter.api.Assertions.assertNull(upgradedFromSegmentIdMap.get(replaceSegment.getId().toString()));
    }
    usedSegments.removeAll(replacingSegments);

    org.junit.jupiter.api.Assertions.assertEquals(segmentsAppendedWithReplaceLock.size(), usedSegments.size());
    for (DataSegment segmentReplicaWithNewVersion : usedSegments) {
      boolean hasBeenCarriedForward = false;
      for (DataSegment appendedSegment : segmentsAppendedWithReplaceLock) {
        if (appendedSegment.getLoadSpec().equals(segmentReplicaWithNewVersion.getLoadSpec())) {
          org.junit.jupiter.api.Assertions.assertEquals(
              appendedSegment.getId().toString(),
              upgradedFromSegmentIdMap.get(segmentReplicaWithNewVersion.getId().toString())
          );
          hasBeenCarriedForward = true;
          break;
        }
      }
      org.junit.jupiter.api.Assertions.assertTrue(hasBeenCarriedForward);
    }

    List<PendingSegmentRecord> pendingSegmentsInInterval =
        coordinator.getPendingSegments("foo", Intervals.of("2023-01-01/2023-02-01"));
    org.junit.jupiter.api.Assertions.assertEquals(2, pendingSegmentsInInterval.size());
    final SegmentId rootPendingSegmentId = pendingSegmentInInterval.getId().asSegmentId();
    if (pendingSegmentsInInterval.get(0).getUpgradedFromSegmentId() == null) {
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId, pendingSegmentsInInterval.get(0).getId().asSegmentId());
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId.toString(), pendingSegmentsInInterval.get(1).getUpgradedFromSegmentId());
    } else {
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId, pendingSegmentsInInterval.get(1).getId().asSegmentId());
      org.junit.jupiter.api.Assertions.assertEquals(rootPendingSegmentId.toString(), pendingSegmentsInInterval.get(0).getUpgradedFromSegmentId());
    }

    List<PendingSegmentRecord> pendingSegmentsOutsideInterval =
        coordinator.getPendingSegments("foo", Intervals.of("2023-04-01/2023-05-01"));
    org.junit.jupiter.api.Assertions.assertEquals(1, pendingSegmentsOutsideInterval.size());
    org.junit.jupiter.api.Assertions.assertEquals(
        pendingSegmentOutsideInterval.getId().asSegmentId(), pendingSegmentsOutsideInterval.get(0).getId().asSegmentId()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testDuplicatePendingSegmentEntriesAreNotInserted(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final PendingSegmentRecord pendingSegment0 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec("foo", Intervals.ETERNITY, "version", new NumberedShardSpec(0, 0)),
        "sequenceName0",
        "sequencePrevId0",
        null,
        "taskAllocatorId"
    );
    final PendingSegmentRecord pendingSegment1 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec("foo", Intervals.ETERNITY, "version", new NumberedShardSpec(1, 0)),
        "sequenceName1",
        "sequencePrevId1",
        null,
        "taskAllocatorId"
    );
    final int actualInserted = insertPendingSegments(
        "foo",
        List.of(pendingSegment0, pendingSegment0, pendingSegment1, pendingSegment1, pendingSegment1),
        true
    );
    org.junit.jupiter.api.Assertions.assertEquals(2, actualInserted);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSimpleAnnounce(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    for (DataSegment segment : SEGMENTS) {
      org.junit.jupiter.api.Assertions.assertArrayEquals(
          mapper.writeValueAsString(segment).getBytes(StandardCharsets.UTF_8),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getId().toString()
          )
      );
    }

    org.junit.jupiter.api.Assertions.assertEquals(
        ImmutableList.of(defaultSegment.getId().toString(), defaultSegment2.getId().toString()),
        retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get())
    );

    // Should not update dataSource metadata.
    org.junit.jupiter.api.Assertions.assertEquals(0, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAnnounceHistoricalSegments(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Set<DataSegment> segments = new HashSet<>();

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
    }

    coordinator.commitSegments(segments, null);
    for (DataSegment segment : segments) {
      org.junit.jupiter.api.Assertions.assertArrayEquals(
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

    org.junit.jupiter.api.Assertions.assertEquals(segmentIds, retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get()));

    // Should not update dataSource metadata.
    org.junit.jupiter.api.Assertions.assertEquals(0, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testOvershadowingAnnounce(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final ImmutableSet<DataSegment> segments = ImmutableSet.of(defaultSegment, defaultSegment2, defaultSegment4);

    coordinator.commitSegments(segments, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

    for (DataSegment segment : segments) {
      org.junit.jupiter.api.Assertions.assertArrayEquals(
          mapper.writeValueAsString(segment).getBytes(StandardCharsets.UTF_8),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getId().toString()
          )
      );
    }

    org.junit.jupiter.api.Assertions.assertEquals(ImmutableList.of(defaultSegment4.getId().toString()), retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get()));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testTransactionalAnnounceSuccess(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    // Insert first segment.
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    org.junit.jupiter.api.Assertions.assertArrayEquals(
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
        SUPERVISOR_ID,
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment2)), result2);

    org.junit.jupiter.api.Assertions.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment2).getBytes(StandardCharsets.UTF_8),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment2.getId().toString()
        )
    );

    // Examine metadata.
    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );

    // Should only be tried once per call.
    org.junit.jupiter.api.Assertions.assertEquals(2, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testTransactionalAnnounceRetryAndSuccess(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final AtomicLong attemptCounter = new AtomicLong();

    final IndexerSQLMetadataStorageCoordinator failOnceCoordinator = new IndexerSQLMetadataStorageCoordinator(
        transactionFactory,
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create(),
        new HeapMemoryIndexingStateStorage()
    )
    {
      @Override
      protected SegmentPublishResult updateDataSourceMetadataInTransaction(
          SegmentMetadataTransaction transaction,
          String supervisorId,
          String dataSource,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        metadataUpdateCounter.getAndIncrement();
        if (attemptCounter.getAndIncrement() == 0) {
          return SegmentPublishResult.retryableFailure("this failure can be retried");
        } else {
          return super.updateDataSourceMetadataInTransaction(transaction, supervisorId, dataSource, startMetadata, endMetadata);
        }
      }
    };

    // Insert first segment.
    final SegmentPublishResult result1 = failOnceCoordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.retryableFailure("this failure can be retried"), result1);

    final SegmentPublishResult resultOnRetry = failOnceCoordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), resultOnRetry);

    org.junit.jupiter.api.Assertions.assertArrayEquals(
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
        SUPERVISOR_ID,
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.retryableFailure("this failure can be retried"), result2);

    final SegmentPublishResult resultOnRetry2 = failOnceCoordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment2),
        SUPERVISOR_ID,
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment2)), resultOnRetry2);

    org.junit.jupiter.api.Assertions.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment2).getBytes(StandardCharsets.UTF_8),
        derbyConnector.lookup(
            derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
            "id",
            "payload",
            defaultSegment2.getId().toString()
        )
    );

    // Examine metadata.
    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        failOnceCoordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );

    // Should be tried twice per call.
    org.junit.jupiter.api.Assertions.assertEquals(4, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testTransactionalAnnounceFailDbNullWantNotNull(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(
        SegmentPublishResult.retryableFailure(
            "The new start metadata state[ObjectMetadata{theObject={foo=bar}}] is ahead of the last committed"
            + " end state[null]. Try resetting the supervisor."
        ),
        result1
    );

    // Should only be tried once.
    org.junit.jupiter.api.Assertions.assertEquals(1, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testTransactionalAnnounceFailDbNotNullWantNull(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment2),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(
        SegmentPublishResult.fail(
            "Stored metadata state[ObjectMetadata{theObject={foo=baz}}] has already been updated by other tasks"
            + " and has diverged from the expected start metadata state[ObjectMetadata{theObject=null}]."
            + " This task will be replaced by the supervisor with a new task using updated start offsets."
            + " Try resetting the supervisor if the issue persists."
        ),
        result2
    );

    // Should only be tried once per call.
    org.junit.jupiter.api.Assertions.assertEquals(2, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_commitSegmentsAndMetadata_isAtomic(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String dataSource = defaultSegment.getDataSource();
    org.junit.jupiter.api.Assertions.assertNull(coordinator.retrieveDataSourceMetadata(dataSource));

    // Create an instance which fails to insert segments but updates metadata successfully
    final AtomicBoolean isMetadataUpdated = new AtomicBoolean(false);
    final IndexerSQLMetadataStorageCoordinator storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        transactionFactory,
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create(),
        new HeapMemoryIndexingStateStorage()
    )
    {
      @Override
      protected Set<DataSegment> insertSegments(
          SegmentMetadataTransaction transaction,
          Set<DataSegment> segments,
          SegmentSchemaMapping segmentSchemaMapping
      )
      {
        throw new RuntimeException("Fail segment insert");
      }

      @Override
      protected SegmentPublishResult updateDataSourceMetadataInTransaction(
          SegmentMetadataTransaction transaction,
          String supervisorId,
          String dataSource,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        isMetadataUpdated.set(true);
        return super.updateDataSourceMetadataInTransaction(transaction, supervisorId, dataSource, startMetadata, endMetadata);
      }
    };

    org.apache.druid.error.DruidExceptionAssertions.assertMatches(
        org.junit.jupiter.api.Assertions.assertThrows(
            RuntimeException.class,
            () -> storageCoordinator.commitSegmentsAndMetadata(
                Set.of(defaultSegment),
                SUPERVISOR_ID,
                new ObjectMetadata(null),
                new ObjectMetadata(Map.of("foo", "baz")),
                null
            )
        ),
        ExceptionMatcher.of(RuntimeException.class)
                        .expectMessageIs("java.lang.RuntimeException: Fail segment insert")
    );

    // Verify that the datasource metadata update succeeded but was rolled back
    org.junit.jupiter.api.Assertions.assertTrue(isMetadataUpdated.get());
    org.junit.jupiter.api.Assertions.assertNull(coordinator.retrieveDataSourceMetadata(dataSource));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUsedSegmentForId(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(Set.of(defaultSegment), null);
    org.junit.jupiter.api.Assertions.assertEquals(
        defaultSegment,
        coordinator.retrieveUsedSegmentForId(defaultSegment.getId())
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveSegmentForId(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(Set.of(defaultSegment), null);
    coordinator.markSegmentAsUnused(defaultSegment.getId());
    org.junit.jupiter.api.Assertions.assertEquals(
        defaultSegment,
        coordinator.retrieveSegmentForId(defaultSegment.getId())
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCleanUpgradeSegmentsTableForTask(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Assumptions.assumeFalse(isCacheEnabled());

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
        ImmutableMap.of(segmentToClean0, replaceLockToClean, segmentToClean1, replaceLockToClean),
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );

    // Unrelated task should not result in clean up
    org.junit.jupiter.api.Assertions.assertEquals(0, coordinator.deleteUpgradeSegmentsForTask("someRandomTask"));
    // The two segment entries are deleted
    org.junit.jupiter.api.Assertions.assertEquals(2, coordinator.deleteUpgradeSegmentsForTask(taskToClean));
    // Nothing further to delete
    org.junit.jupiter.api.Assertions.assertEquals(0, coordinator.deleteUpgradeSegmentsForTask(taskToClean));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testTransactionalAnnounceFailDbNotNullWantDifferent(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final SegmentPublishResult result1 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(defaultSegment)), result1);

    final SegmentPublishResult result2 = coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment2),
        SUPERVISOR_ID,
        new ObjectMetadata(ImmutableMap.of("foo", "qux")),
        new ObjectMetadata(ImmutableMap.of("foo", "baz")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );
    org.junit.jupiter.api.Assertions.assertEquals(
        SegmentPublishResult.fail(
            "Stored metadata state[ObjectMetadata{theObject={foo=baz}}] has already been updated by other tasks"
            + " and has diverged from the expected start metadata state[ObjectMetadata{theObject={foo=qux}}]."
            + " This task will be replaced by the supervisor with a new task using updated start offsets."
            + " Try resetting the supervisor if the issue persists."
        ),
        result2
    );

    // Should only be tried once per call.
    org.junit.jupiter.api.Assertions.assertEquals(2, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSimpleUsedList(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testMultiIntervalUsedList(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    coordinator.commitSegments(ImmutableSet.of(defaultSegment3), new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUsedSegmentsUsingMultipleIntervals(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());

    final Collection<DataSegment> actualUsedSegments = coordinator.retrieveUsedSegmentsForIntervals(
        TestDataSource.WIKI,
        intervals,
        Segments.ONLY_VISIBLE
    );

    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUsedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUsedSegments.containsAll(segments));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveAllUsedSegmentsUsingIntervalsOutOfRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);

    final Interval outOfRangeInterval = Intervals.of("1700/1800");
    org.junit.jupiter.api.Assertions.assertTrue(segments.stream()
                              .anyMatch(segment -> !segment.getInterval().overlaps(outOfRangeInterval)));

    final Collection<DataSegment> actualUsedSegments = coordinator.retrieveUsedSegmentsForIntervals(
        TestDataSource.WIKI,
        ImmutableList.of(outOfRangeInterval),
        Segments.ONLY_VISIBLE
    );

    org.junit.jupiter.api.Assertions.assertEquals(0, actualUsedSegments.size());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveAllUsedSegmentsUsingNoIntervals(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);

    final Collection<DataSegment> actualUsedSegments = coordinator.retrieveAllUsedSegments(
        TestDataSource.WIKI,
        Segments.ONLY_VISIBLE
    );

    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUsedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUsedSegments.containsAll(segments));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndNoLimit(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        TestDataSource.WIKI,
        Intervals.of("1900/3000"),
        null,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUnusedSegments.containsAll(segments));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndLimitAtRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final int requestedLimit = segments.size();
    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        TestDataSource.WIKI,
        Intervals.of("1900/3000"),
        requestedLimit,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(requestedLimit, actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUnusedSegments.containsAll(segments));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndLimitInRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final int requestedLimit = segments.size() - 1;
    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        TestDataSource.WIKI,
        Intervals.of("1900/3000"),
        requestedLimit,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(requestedLimit, actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUnusedSegments.containsAll(segments.stream().limit(requestedLimit).collect(Collectors.toList())));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingSingleIntervalAndLimitOutOfRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final int limit = segments.size() + 1;
    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        TestDataSource.WIKI,
        Intervals.of("1900/3000"),
        limit,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUnusedSegments.containsAll(segments));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingSingleIntervalOutOfRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final Interval outOfRangeInterval = Intervals.of("1700/1800");
    org.junit.jupiter.api.Assertions.assertTrue(segments.stream()
                              .anyMatch(segment -> !segment.getInterval().overlaps(outOfRangeInterval)));
    final int limit = segments.size() + 1;

    final List<DataSegment> actualUnusedSegments = coordinator.retrieveUnusedSegmentsForInterval(
        TestDataSource.WIKI,
        outOfRangeInterval,
        limit,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals(0, actualUnusedSegments.size());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsAndNoLimit(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        null,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(segments.containsAll(actualUnusedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        null,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingNoIntervalsNoLimitAndNoLastSegmentId(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(),
        null,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(segments.containsAll(actualUnusedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingNoIntervalsAndNoLimitAndNoLastSegmentId(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(expectedSegmentsAscOrder.containsAll(actualUnusedSegments));

    ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        lastSegmentId,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(expectedSegmentsAscOrder, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);

    actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(),
        null,
        lastSegmentId,
        SortOrder.ASC,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsAscOrder, actualUnusedSegments);

    actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        lastSegmentId,
        SortOrder.ASC,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsAscOrder.size(), actualUnusedSegmentsPlus.size());
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
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsDescOrder.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsDescOrder, actualUnusedSegments);

    actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        null,
        lastSegmentId,
        SortOrder.DESC,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(expectedSegmentsDescOrder.size(), actualUnusedSegmentsPlus.size());
    verifyEqualsAllSegmentsPlus(expectedSegmentsDescOrder, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsAndLimitAtRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        segments.size(),
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(segments.containsAll(actualUnusedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        segments.size(),
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsAndLimitInRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final int requestedLimit = segments.size() - 1;
    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        requestedLimit,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    final List<DataSegment> expectedSegments = segments.stream().limit(requestedLimit).collect(Collectors.toList());
    org.junit.jupiter.api.Assertions.assertEquals(requestedLimit, actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUnusedSegments.containsAll(expectedSegments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        requestedLimit,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(requestedLimit, actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(expectedSegments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsInSingleBatchLimitAndLastSegmentId(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size() - 5, actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertEquals(actualUnusedSegments, expectedSegments);

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(),
        requestedLimit,
        lastSegmentId,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size() - 5, actualUnusedSegmentsPlus.size());
    verifyEqualsAllSegmentsPlus(expectedSegments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingMultipleIntervalsLimitAndLastSegmentId(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(requestedLimit - 4, actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertEquals(actualUnusedSegments, expectedSegments);

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        requestedLimit,
        lastSegmentId,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(requestedLimit - 4, actualUnusedSegmentsPlus.size());
    verifyEqualsAllSegmentsPlus(expectedSegments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingMultipleIntervals(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1900, 2133);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        segments.size() + 1,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(actualUnusedSegments.containsAll(segments));

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
        segments.size() + 1,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegmentsPlus.size());
    verifyContainsAllSegmentsPlus(segments, actualUnusedSegmentsPlus, usedStatusLastUpdatedTime);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsUsingIntervalOutOfRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);
    markAllSegmentsUnused(new HashSet<>(segments), DateTimes.nowUtc());

    final Interval outOfRangeInterval = Intervals.of("1700/1800");
    org.junit.jupiter.api.Assertions.assertTrue(segments.stream()
                              .anyMatch(segment -> !segment.getInterval().overlaps(outOfRangeInterval)));

    final ImmutableList<DataSegment> actualUnusedSegments = retrieveUnusedSegments(
        ImmutableList.of(outOfRangeInterval),
        null,
        null,
        null,
         null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(0, actualUnusedSegments.size());

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(outOfRangeInterval),
        null,
        null,
        null,
        null,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(0, actualUnusedSegmentsPlus.size());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsWithMaxUsedStatusLastUpdatedTime(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final List<DataSegment> segments = createAndGetUsedYearSegments(1905, 1910);
    DateTime usedStatusLastUpdatedTime = DateTimes.nowUtc();
    markAllSegmentsUnused(new HashSet<>(segments), usedStatusLastUpdatedTime);

    final Interval interval = Intervals.of("1905/1920");

    final ImmutableList<DataSegment> actualUnusedSegments1 = retrieveUnusedSegments(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc(),
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(5, actualUnusedSegments1.size());

    ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc(),
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(5, actualUnusedSegmentsPlus.size());

    final ImmutableList<DataSegment> actualUnusedSegments2 = retrieveUnusedSegments(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc().minusHours(1),
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(0, actualUnusedSegments2.size());

    actualUnusedSegmentsPlus = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        DateTimes.nowUtc().minusHours(1),
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(0, actualUnusedSegmentsPlus.size());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsWithMaxUsedStatusLastUpdatedTime2(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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
        maxUsedStatusLastUpdatedTime1,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(oddYearSegments.size(), actualUnusedSegments1.size());

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus1 = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        maxUsedStatusLastUpdatedTime1,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(oddYearSegments.size(), actualUnusedSegmentsPlus1.size());

    final ImmutableList<DataSegment> actualUnusedSegments2 = retrieveUnusedSegments(
        ImmutableList.of(interval),
        null,
        null,
        null,
        maxUsedStatusLastUpdatedTime2,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegments2.size());

    final ImmutableList<DataSegmentPlus> actualUnusedSegmentsPlus2 = retrieveUnusedSegmentsPlus(
        ImmutableList.of(interval),
        null,
        null,
        null,
        maxUsedStatusLastUpdatedTime2,
        derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    org.junit.jupiter.api.Assertions.assertEquals(segments.size(), actualUnusedSegmentsPlus2.size());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSimpleUnusedList(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsWithVersions(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final DateTime now = DateTimes.nowUtc();
    final String v1 = now.toString();
    final String v2 = now.plusDays(2).toString();
    final String v3 = now.plusDays(3).toString();
    final String v4 = now.plusDays(4).toString();

    final DataSegment segment1 = createSegment(
        Intervals.of("2023-01-01/2023-01-02"),
        v1,
        new LinearShardSpec(0)
    );
    final DataSegment segment2 = createSegment(
        Intervals.of("2023-01-02/2023-01-03"),
        v2,
        new LinearShardSpec(0)
    );
    final DataSegment segment3 = createSegment(
        Intervals.of("2023-01-03/2023-01-04"),
        v3,
        new LinearShardSpec(0)
    );
    final DataSegment segment4 = createSegment(
        Intervals.of("2023-01-03/2023-01-04"),
        v4,
        new LinearShardSpec(0)
    );

    final ImmutableSet<DataSegment> unusedSegments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    org.junit.jupiter.api.Assertions.assertEquals(unusedSegments, coordinator.commitSegments(unusedSegments, null));
    markAllSegmentsUnused(unusedSegments, DateTimes.nowUtc());

    for (DataSegment unusedSegment : unusedSegments) {
      Assertions.assertThat(
          coordinator.retrieveUnusedSegmentsForInterval(
              TestDataSource.WIKI,
              Intervals.of("2023-01-01/2023-01-04"),
              ImmutableList.of(unusedSegment.getVersion()),
              null,
              null
          )
      ).contains(unusedSegment);
    }

    Assertions.assertThat(
        coordinator.retrieveUnusedSegmentsForInterval(
            TestDataSource.WIKI,
            Intervals.of("2023-01-01/2023-01-04"),
            ImmutableList.of(v1, v2),
            null,
            null
        )
    ).contains(segment1, segment2);

    Assertions.assertThat(
        coordinator.retrieveUnusedSegmentsForInterval(
            TestDataSource.WIKI,
            Intervals.of("2023-01-01/2023-01-04"),
            null,
            null,
            null
        )
    ).containsAll(unusedSegments);

    Assertions.assertThat(
        coordinator.retrieveUnusedSegmentsForInterval(
            TestDataSource.WIKI,
            Intervals.of("2023-01-01/2023-01-04"),
            ImmutableList.of("some-non-existent-version"),
              null,
              null
          )
    ).containsAll(ImmutableSet.of());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSimpleUnusedListWithLimit(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    int limit = SEGMENTS.size() - 1;
    Set<DataSegment> retreivedUnusedSegments = ImmutableSet.copyOf(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval(),
            null,
            limit,
            null
        )
    );
    org.junit.jupiter.api.Assertions.assertEquals(limit, retreivedUnusedSegments.size());
    org.junit.jupiter.api.Assertions.assertTrue(SEGMENTS.containsAll(retreivedUnusedSegments));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsWithExactInterval(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String dataSource = defaultSegment.getDataSource();
    coordinator.commitSegments(Set.of(defaultSegment, defaultSegment2, defaultSegment3), null);

    final DateTime now = DateTimes.nowUtc();
    markAllSegmentsUnused(Set.of(defaultSegment, defaultSegment2, defaultSegment3), now.minusHours(1));

    // Verify that query for overlapping interval does not return the segments
    org.junit.jupiter.api.Assertions.assertTrue(
        coordinator.retrieveUnusedSegmentsWithExactInterval(
            dataSource,
            Intervals.ETERNITY,
            now,
            10
        ).isEmpty()
    );

    // Verify that query for exact interval returns the segments
    org.junit.jupiter.api.Assertions.assertEquals(
        List.of(toSegmentPlusUpgradedId(defaultSegment3, null)),
        coordinator.retrieveUnusedSegmentsWithExactInterval(
            dataSource,
            defaultSegment3.getInterval(),
            now,
            10
        )
    );

    org.junit.jupiter.api.Assertions.assertEquals(defaultSegment.getInterval(), defaultSegment2.getInterval());
    org.junit.jupiter.api.Assertions.assertEquals(
        Set.of(toSegmentPlusUpgradedId(defaultSegment, null), toSegmentPlusUpgradedId(defaultSegment2, null)),
        Set.copyOf(
            coordinator.retrieveUnusedSegmentsWithExactInterval(
                dataSource,
                defaultSegment.getInterval(),
                now,
                10
            )
        )
    );

    // Verify that query with limit 1 returns only 1 result
    org.junit.jupiter.api.Assertions.assertEquals(
        1,
        coordinator.retrieveUnusedSegmentsWithExactInterval(
            dataSource,
            defaultSegment.getInterval(),
            now,
            1
        ).size()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveSomeUnusedSegmentIntervals(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String dataSource = defaultSegment.getDataSource();
    coordinator.commitSegments(Set.of(defaultSegment, defaultSegment3), null);

    org.junit.jupiter.api.Assertions.assertTrue(coordinator.retrieveSomeUnusedSegmentIntervals(dataSource, 100).isEmpty());

    markAllSegmentsUnused(Set.of(defaultSegment), DateTimes.nowUtc().minusHours(1));
    org.junit.jupiter.api.Assertions.assertEquals(
        List.of(defaultSegment.getInterval()),
        coordinator.retrieveSomeUnusedSegmentIntervals(dataSource, 100)
    );

    markAllSegmentsUnused(Set.of(defaultSegment3), DateTimes.nowUtc().minusHours(1));
    org.junit.jupiter.api.Assertions.assertEquals(
        Set.of(defaultSegment.getInterval(), defaultSegment3.getInterval()),
        Set.copyOf(coordinator.retrieveSomeUnusedSegmentIntervals(dataSource, 100))
    );

    // Verify retrieve with limit 1 returns only 1 interval
    org.junit.jupiter.api.Assertions.assertEquals(
        1,
        coordinator.retrieveSomeUnusedSegmentIntervals(dataSource, 1).size()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveAllDatasourceNames(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(Set.of(defaultSegment), null);
    coordinator.commitSegments(Set.of(hugeTimeRangeSegment1), null);
    org.junit.jupiter.api.Assertions.assertEquals(
        Set.of("fooDataSource", "hugeTimeRangeDataSource"),
        coordinator.retrieveAllDatasourceNames()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedOverlapLow(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    Set<DataSegment> actualSegments = ImmutableSet.copyOf(
        coordinator.retrieveUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            Intervals.of("2014-12-31T23:59:59.999Z/2015-01-01T00:00:00.001Z"), // end is exclusive
            Segments.ONLY_VISIBLE
        )
    );
    org.junit.jupiter.api.Assertions.assertEquals(
        SEGMENTS,
        actualSegments
    );
  }


  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedOverlapHigh(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedOutOfBoundsLow(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    org.junit.jupiter.api.Assertions.assertTrue(
        coordinator.retrieveUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().minus(1), defaultSegment.getInterval().getStart()),
            Segments.ONLY_VISIBLE
        ).isEmpty()
    );
  }


  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedOutOfBoundsHigh(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    org.junit.jupiter.api.Assertions.assertTrue(
        coordinator.retrieveUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getEnd(), defaultSegment.getInterval().getEnd().plusDays(10)),
            Segments.ONLY_VISIBLE
        ).isEmpty()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedWithinBoundsEnd(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedOverlapEnd(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUnusedOverlapLow(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertTrue(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUnusedUnderlapLow(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().plus(1), defaultSegment.getInterval().getEnd()),
            null,
            null
        ).isEmpty()
    );
  }


  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUnusedUnderlapHigh(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart(), defaultSegment.getInterval().getEnd().minus(1)),
            null,
            null
        ).isEmpty()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUnusedOverlapHigh(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertTrue(
        coordinator.retrieveUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval().withStart(defaultSegment.getInterval().getEnd().minus(1)),
            null,
            null
        ).isEmpty()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUnusedBigOverlap(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUnusedLowRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertEquals(
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
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUnusedHighRange(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    markAllSegmentsUnused();
    org.junit.jupiter.api.Assertions.assertEquals(
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
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedHugeTimeRangeEternityFilter(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment1,
            hugeTimeRangeSegment2,
            hugeTimeRangeSegment3
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedHugeTimeRangeTrickyFilter1(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment1,
            hugeTimeRangeSegment2,
            hugeTimeRangeSegment3
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUsedHugeTimeRangeTrickyFilter2(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment1,
            hugeTimeRangeSegment2,
            hugeTimeRangeSegment3
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testEternitySegmentWithStringComparison(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            eternitySegment
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testEternityMultipleSegmentWithStringComparison(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            numberedSegment0of0,
            eternitySegment
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testFirstHalfEternitySegmentWithStringComparison(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            firstHalfEternityRangeSegment
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testFirstHalfEternityMultipleSegmentWithStringComparison(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            numberedSegment0of0,
            firstHalfEternityRangeSegment
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSecondHalfEternitySegmentWithStringComparison(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            secondHalfEternityRangeSegment
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testLargeIntervalWithStringComparison(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    // Known Issue when not using cache: https://github.com/apache/druid/issues/12860
    Assumptions.assumeTrue(isCacheEnabled());

    coordinator.commitSegments(
        ImmutableSet.of(
            hugeTimeRangeSegment4
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSecondHalfEternityMultipleSegmentWithStringComparison(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(
        ImmutableSet.of(
            numberedSegment0of0,
            secondHalfEternityRangeSegment
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testDeleteDataSourceMetadata(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );

    org.junit.jupiter.api.Assertions.assertFalse(coordinator.deleteDataSourceMetadata("nonExistentSupervisor"), "deleteInvalidDataSourceMetadata");
    org.junit.jupiter.api.Assertions.assertTrue(coordinator.deleteDataSourceMetadata(SUPERVISOR_ID), "deleteValidDataSourceMetadata");

    org.junit.jupiter.api.Assertions.assertNull(coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID), "getDataSourceMetadataNullAfterDelete");
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testDeleteSegmentsInMetaDataStorage(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    // Published segments to MetaDataStorage
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

    // check segments Published
    org.junit.jupiter.api.Assertions.assertEquals(
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
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testUpdateSegmentsInMetaDataStorage(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Assumptions.assumeFalse(isCacheEnabled());

    // Published segments to MetaDataStorage
    coordinator.commitSegments(SEGMENTS, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

    // check segments Published
    org.junit.jupiter.api.Assertions.assertEquals(
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

    org.junit.jupiter.api.Assertions.assertEquals(SEGMENTS.size(), updated.size());

    DataSegment defaultAfterUpdate = updated.stream().filter(s -> s.equals(defaultSegment)).findFirst().get();
    DataSegment default2AfterUpdate = updated.stream().filter(s -> s.equals(defaultSegment2)).findFirst().get();

    org.junit.jupiter.api.Assertions.assertNotNull(defaultAfterUpdate);
    org.junit.jupiter.api.Assertions.assertNotNull(default2AfterUpdate);

    // check that default did not change
    org.junit.jupiter.api.Assertions.assertEquals(defaultSegment.getSize(), defaultAfterUpdate.getSize());
    // but that default 2 did change
    org.junit.jupiter.api.Assertions.assertEquals(defaultSegment2WithBiggerSize.getSize(), default2AfterUpdate.getSize());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSingleAdditionalNumberedShardWithNoCorePartitions(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment0of0));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testMultipleAdditionalNumberedShardsWithNoCorePartitions(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment0of0, numberedSegment1of0, numberedSegment2of0));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSingleAdditionalNumberedShardWithOneCorePartition(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment2of1));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testMultipleAdditionalNumberedShardsWithOneCorePartition(SegmentMetadataCache.UsageMode cacheMode) throws IOException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    additionalNumberedShardTest(ImmutableSet.of(numberedSegment2of1, numberedSegment3of1));
  }

  private void additionalNumberedShardTest(Set<DataSegment> segments) throws IOException
  {
    coordinator.commitSegments(segments, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

    for (DataSegment segment : segments) {
      org.junit.jupiter.api.Assertions.assertArrayEquals(
          mapper.writeValueAsString(segment).getBytes(StandardCharsets.UTF_8),
          derbyConnector.lookup(
              derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
              "id",
              "payload",
              segment.getId().toString()
          )
      );
    }

    org.junit.jupiter.api.Assertions.assertEquals(
        segments.stream().map(segment -> segment.getId().toString()).collect(Collectors.toList()),
        retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get())
    );

    // Should not update dataSource metadata.
    org.junit.jupiter.api.Assertions.assertEquals(0, metadataUpdateCounter.get());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAllocatePendingSegment(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final SegmentIdWithShardSpec identifier = allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        "version",
        false,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version", identifier.toString());

    final SegmentIdWithShardSpec identifier1 = allocatePendingSegment(
        dataSource,
        "seq",
        identifier.toString(),
        interval,
        partialShardSpec,
        identifier.getVersion(),
        false,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_1", identifier1.toString());

    final SegmentIdWithShardSpec identifier2 = allocatePendingSegment(
        dataSource,
        "seq",
        identifier1.toString(),
        interval,
        partialShardSpec,
        identifier1.getVersion(),
        false,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_2", identifier2.toString());

    final SegmentIdWithShardSpec identifier3 = allocatePendingSegment(
        dataSource,
        "seq",
        identifier1.toString(),
        interval,
        partialShardSpec,
        identifier1.getVersion(),
        false,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_2", identifier3.toString());
    org.junit.jupiter.api.Assertions.assertEquals(identifier2, identifier3);

    final SegmentIdWithShardSpec identifier4 = allocatePendingSegment(
        dataSource,
        "seq1",
        null,
        interval,
        partialShardSpec,
        "version",
        false,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_3", identifier4.toString());
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
  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAllocatePendingSegmentAfterDroppingExistingSegment(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    String maxVersion = "version_newer_newer";

    // simulate one load using kafka streaming
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final SegmentIdWithShardSpec identifier = allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        "version",
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version", identifier.toString());
    // Since there are no used core partitions yet
    org.junit.jupiter.api.Assertions.assertEquals(0, identifier.getShardSpec().getNumCorePartitions());

    // simulate one more load using kafka streaming (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier1 = allocatePendingSegment(
        dataSource,
        "seq2",
        identifier.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_1", identifier1.toString());
    // Since there are no used core partitions yet
    org.junit.jupiter.api.Assertions.assertEquals(0, identifier1.getShardSpec().getNumCorePartitions());

    // simulate one more load using kafka streaming (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier2 = allocatePendingSegment(
        dataSource,
        "seq3",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_2", identifier2.toString());
    // Since there are no used core partitions yet
    org.junit.jupiter.api.Assertions.assertEquals(0, identifier2.getShardSpec().getNumCorePartitions());

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
    coordinator.commitSegments(Set.of(segment), null);
    List<String> ids = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_new", ids.get(0));

    // one more load on same interval:
    final SegmentIdWithShardSpec identifier3 = allocatePendingSegment(
        dataSource,
        "seq4",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_new_1", identifier3.toString());
    // Used segment set has 1 core partition
    org.junit.jupiter.api.Assertions.assertEquals(1, identifier3.getShardSpec().getNumCorePartitions());

    // now drop the used segment previously loaded:
    coordinator.markSegmentAsUnused(segment.getId());

    // and final load, this reproduces an issue that could happen with multiple streaming appends,
    // followed by a reindex, followed by a drop, and more streaming data coming in for same interval
    final SegmentIdWithShardSpec identifier4 = allocatePendingSegment(
        dataSource,
        "seq5",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_version_new_2", identifier4.toString());
    // Since all core partitions have been dropped
    org.junit.jupiter.api.Assertions.assertEquals(0, identifier4.getShardSpec().getNumCorePartitions());
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
  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAnotherAllocatePendingSegmentAfterRevertingCompaction(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    String maxVersion = "Z";

    // 1.0) simulate one append load
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final SegmentIdWithShardSpec identifier = allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        "A",
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A", identifier.toString());
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
    coordinator.commitSegments(Set.of(segment), null);
    List<String> ids = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A", ids.get(0));


    // 1.1) simulate one more append load  (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier1 = allocatePendingSegment(
        dataSource,
        "seq2",
        identifier.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_1", identifier1.toString());
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
    coordinator.commitSegments(Set.of(segment), null);
    ids = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_1", ids.get(1));


    // 1.2) simulate one more append load  (as if previous segment was published, note different sequence name)
    final SegmentIdWithShardSpec identifier2 = allocatePendingSegment(
        dataSource,
        "seq3",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_2", identifier2.toString());
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
    coordinator.commitSegments(Set.of(segment), null);
    ids = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_2", ids.get(2));


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
    coordinator.commitSegments(Set.of(compactedSegment), null);
    ids = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_B", ids.get(3));
    // 3) When overshadowing, segments are still marked as "used" in the segments table
    // state so far:
    // pendings: A: 0,1,2
    // used segments: A: 0,1,2; B: 0 <- new compacted segment, overshadows previous version A
    // unused segment:

    // 4) pending segment of version = B, id = 1 <= appending new data, aborted
    final SegmentIdWithShardSpec identifier3 = allocatePendingSegment(
        dataSource,
        "seq4",
        identifier2.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_B_1", identifier3.toString());
    // no corresponding segment, pending aborted
    // state so far:
    // pendings: A: 0,1,2; B:1 (note that B_1 does not make it into segments since its task aborted)
    // used segments: A: 0,1,2; B: 0 <-  compacted segment, overshadows previous version A
    // unused segment:

    // 5) reverted compaction (by marking B_0 as unused)
    // Revert compaction a manual metadata update which is basically the following two steps:
    coordinator.markSegmentAsUnused(compactedSegment.getId());
    //        pending: version = A, id = 0,1,2
    //                 version = B, id = 1
    //
    //        used segment: version = A, id = 0,1,2
    //        unused segment: version = B, id = 0
    List<String> pendings = retrievePendingSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals(4, pendings.size());

    List<String> used = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals(3, used.size());

    List<String> unused = retrieveUnusedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals(1, unused.size());

    // Simulate one more append load
    final SegmentIdWithShardSpec identifier4 = allocatePendingSegment(
        dataSource,
        "seq5",
        identifier1.toString(),
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    // maxid = B_1 -> new partno = 2
    // versionofexistingchunk=A
    // ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_2
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_3", identifier4.toString());
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
    coordinator.commitSegments(Set.of(segment), null);
    ids = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_3", ids.get(3));

  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAllocatePendingSegmentsSkipSegmentPayloadFetch(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final String sequenceName = "seq";

    final SegmentCreateRequest request = new SegmentCreateRequest(sequenceName, null, "v1", partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId0 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request),
        true
    ).get(request);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1", segmentId0.toString());

    final SegmentCreateRequest request1 =
        new SegmentCreateRequest(sequenceName, segmentId0.toString(), segmentId0.getVersion(), partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId1 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request1),
        true
    ).get(request1);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_1", segmentId1.toString());

    final SegmentCreateRequest request2 =
        new SegmentCreateRequest(sequenceName, segmentId1.toString(), segmentId1.getVersion(), partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId2 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request2),
        true
    ).get(request2);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_2", segmentId2.toString());

    final SegmentCreateRequest request3 =
        new SegmentCreateRequest(sequenceName, segmentId1.toString(), segmentId1.getVersion(), partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId3 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request3),
        true
    ).get(request3);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_2", segmentId3.toString());
    org.junit.jupiter.api.Assertions.assertEquals(segmentId2, segmentId3);

    final SegmentCreateRequest request4 =
        new SegmentCreateRequest("seq1", null, "v1", partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId4 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request4),
        true
    ).get(request4);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_3", segmentId4.toString());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAllocatePendingSegments(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final String sequenceName = "seq";

    final SegmentCreateRequest request = new SegmentCreateRequest(sequenceName, null, "v1", partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId0 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request),
        false
    ).get(request);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1", segmentId0.toString());

    final SegmentCreateRequest request1 =
        new SegmentCreateRequest(sequenceName, segmentId0.toString(), segmentId0.getVersion(), partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId1 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request1),
        false
    ).get(request1);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_1", segmentId1.toString());

    final SegmentCreateRequest request2 =
        new SegmentCreateRequest(sequenceName, segmentId1.toString(), segmentId1.getVersion(), partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId2 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request2),
        false
    ).get(request2);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_2", segmentId2.toString());

    final SegmentCreateRequest request3 =
        new SegmentCreateRequest(sequenceName, segmentId1.toString(), segmentId1.getVersion(), partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId3 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request3),
        false
    ).get(request3);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_2", segmentId3.toString());
    org.junit.jupiter.api.Assertions.assertEquals(segmentId2, segmentId3);

    final SegmentCreateRequest request4 =
        new SegmentCreateRequest("seq1", null, "v1", partialShardSpec, null);
    final SegmentIdWithShardSpec segmentId4 = coordinator.allocatePendingSegments(
        dataSource,
        interval,
        false,
        Collections.singletonList(request4),
        false
    ).get(request4);

    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_v1_3", segmentId4.toString());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testNoPendingSegmentsAndOneUsedSegment(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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

    coordinator.commitSegments(Set.of(segment), null);
    List<String> ids = retrieveUsedSegmentIds(derbyConnectorRule.metadataTablesConfigSupplier().get());
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A", ids.get(0));

    // simulate one aborted append load
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final SegmentIdWithShardSpec identifier = allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        maxVersion,
        true,
        null
    );
    org.junit.jupiter.api.Assertions.assertEquals("ds_2017-01-01T00:00:00.000Z_2017-02-01T00:00:00.000Z_A_1", identifier.toString());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_concurrentAppend_toIntervalWithUnusedAppendSegment_createsFreshVersion(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String wiki = TestDataSource.WIKI;
    final String appendLockVersion = PendingSegmentRecord.DEFAULT_VERSION_FOR_CONCURRENT_APPEND;
    final Interval firstOfJan23 = Intervals.of("2023-01-01/P1D");

    // Allocate and commit an APPEND segment
    final String taskAllocator1 = "taskAlloc1";
    final SegmentIdWithShardSpec pendingSegment
        = allocatePendingSegmentForAppendTask(wiki, firstOfJan23, taskAllocator1);

    org.junit.jupiter.api.Assertions.assertNotNull(pendingSegment);
    org.junit.jupiter.api.Assertions.assertEquals(appendLockVersion, pendingSegment.getVersion());
    org.junit.jupiter.api.Assertions.assertEquals(0, pendingSegment.getShardSpec().getPartitionNum());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    coordinator.commitAppendSegments(Set.of(segmentV01), Map.of(), taskAllocator1, null);

    verifyIntervalHasUsedSegments(wiki, firstOfJan23, segmentV01);
    verifyIntervalHasVisibleSegments(wiki, firstOfJan23, segmentV01);

    // Mark the segment as unused with a future update time to avoid race conditions
    final DateTime markUnusedTime = DateTimes.nowUtc().plusHours(1);
    transactionFactory.inReadWriteDatasourceTransaction(
        wiki,
        t -> t.markAllSegmentsAsUnused(markUnusedTime)
    );
    verifyIntervalHasUsedSegments(wiki, firstOfJan23);

    // Allocate and commit another APPEND segment
    final String taskAllocator2 = "taskAlloc2";
    final SegmentIdWithShardSpec pendingSegment2
        = allocatePendingSegmentForAppendTask(wiki, firstOfJan23, taskAllocator2);

    // Verify that the new segment gets a different version
    org.junit.jupiter.api.Assertions.assertNotNull(pendingSegment2);
    org.junit.jupiter.api.Assertions.assertEquals(appendLockVersion + "S", pendingSegment2.getVersion());
    org.junit.jupiter.api.Assertions.assertEquals(0, pendingSegment2.getShardSpec().getPartitionNum());

    final DataSegment segmentV02 = asSegment(pendingSegment2);
    coordinator.commitAppendSegments(Set.of(segmentV02), Map.of(), taskAllocator2, null);
    org.junit.jupiter.api.Assertions.assertNotEquals(segmentV01, segmentV02);

    verifyIntervalHasUsedSegments(wiki, firstOfJan23, segmentV02);
    verifyIntervalHasVisibleSegments(wiki, firstOfJan23, segmentV02);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_allocateCommitDelete_createsFreshVersion_uptoMaxAllowedRetries(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String wiki = TestDataSource.WIKI;
    final Interval firstOfJan23 = Intervals.of("2023-01-01/P1D");

    final int maxAllowedAppends = 10;
    final int expectedParitionNum = 0;

    String expectedVersion = DateTimes.EPOCH.toString();

    // Allocate, commit, delete, repeat
    for (int i = 0; i < maxAllowedAppends; ++i, expectedVersion += "S") {
      // Allocate a segment and verify its version and partition number
      final String taskAllocatorId = IdUtils.getRandomId();
      final SegmentIdWithShardSpec pendingSegment
          = allocatePendingSegmentForAppendTask(wiki, firstOfJan23, taskAllocatorId);

      org.junit.jupiter.api.Assertions.assertNotNull(pendingSegment);
      org.junit.jupiter.api.Assertions.assertEquals(expectedVersion, pendingSegment.getVersion());
      org.junit.jupiter.api.Assertions.assertEquals(expectedParitionNum, pendingSegment.getShardSpec().getPartitionNum());

      // Commit the segment and verify its version and partition number
      final DataSegment segment = asSegment(pendingSegment);
      coordinator.commitAppendSegments(Set.of(segment), Map.of(), taskAllocatorId, null);

      org.junit.jupiter.api.Assertions.assertEquals(expectedVersion, segment.getVersion());
      org.junit.jupiter.api.Assertions.assertEquals(expectedParitionNum, segment.getShardSpec().getPartitionNum());

      verifyIntervalHasUsedSegments(wiki, firstOfJan23, segment);
      verifyIntervalHasVisibleSegments(wiki, firstOfJan23, segment);

      // Mark the segment as unused with a future update time to avoid race conditions
      final DateTime markUnusedTime = DateTimes.nowUtc().plusHours(1);
      transactionFactory.inReadWriteDatasourceTransaction(
          wiki,
          t -> t.markAllSegmentsAsUnused(markUnusedTime)
      );
      verifyIntervalHasUsedSegments(wiki, firstOfJan23);
    }

    // Verify that the next attempt fails
    org.apache.druid.error.DruidExceptionAssertions.assertMatches(
        org.junit.jupiter.api.Assertions.assertThrows(
            CallbackFailedException.class,
            () -> allocatePendingSegmentForAppendTask(wiki, firstOfJan23, IdUtils.getRandomId())
        ),
        ExceptionMatcher.of(CallbackFailedException.class).expectRootCause(
            DruidExceptionMatcher.internalServerError().expectMessageIs(
                "Could not allocate segment"
                + "[wiki_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_1970-01-01T00:00:00.000Z]"
                + " as there are too many clashing unused versions(upto [1970-01-01T00:00:00.000ZSSSSSSSSSS])"
                + " in the interval. Kill the old unused versions to proceed."
            )
        )
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testDeletePendingSegment(SegmentMetadataCache.UsageMode cacheMode) throws InterruptedException
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final PartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    String prevSegmentId = null;

    final DateTime begin = DateTimes.nowUtc();

    for (int i = 0; i < 10; i++) {
      final SegmentIdWithShardSpec identifier = allocatePendingSegment(
          dataSource,
          "seq",
          prevSegmentId,
          interval,
          partialShardSpec,
          "version",
          false,
          null
      );
      prevSegmentId = identifier.toString();
    }
    Thread.sleep(100);

    final DateTime secondBegin = DateTimes.nowUtc();
    for (int i = 0; i < 5; i++) {
      final SegmentIdWithShardSpec identifier = allocatePendingSegment(
          dataSource,
          "seq",
          prevSegmentId,
          interval,
          partialShardSpec,
          "version",
          false,
          null
      );
      prevSegmentId = identifier.toString();
    }

    final int numDeleted = coordinator.deletePendingSegmentsCreatedInInterval(
        dataSource,
        new Interval(begin, secondBegin)
    );
    org.junit.jupiter.api.Assertions.assertEquals(10, numDeleted);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAllocatePendingSegmentsWithOvershadowingSegments(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    String prevSegmentId = null;

    for (int i = 0; i < 10; i++) {
      final SegmentIdWithShardSpec identifier = allocatePendingSegment(
          dataSource,
          "seq",
          prevSegmentId,
          interval,
          new NumberedOverwritePartialShardSpec(0, 1, (short) (i + 1)),
          "version",
          false,
          null
      );
      org.junit.jupiter.api.Assertions.assertEquals(
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
      final Set<DataSegment> announced = coordinator.commitSegments(toBeAnnounced, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

      org.junit.jupiter.api.Assertions.assertEquals(toBeAnnounced, announced);
    }

    final Collection<DataSegment> visibleSegments =
        coordinator.retrieveUsedSegmentsForInterval(dataSource, interval, Segments.ONLY_VISIBLE);

    org.junit.jupiter.api.Assertions.assertEquals(1, visibleSegments.size());
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAllocatePendingSegmentsForHashBasedNumberedShardSpec(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final PartialShardSpec partialShardSpec = new HashBasedNumberedPartialShardSpec(null, 2, 5, null);
    final String dataSource = "ds";
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");

    SegmentIdWithShardSpec id = allocatePendingSegment(
        dataSource,
        "seq",
        null,
        interval,
        partialShardSpec,
        "version",
        true,
        null
    );

    HashBasedNumberedShardSpec shardSpec = (HashBasedNumberedShardSpec) id.getShardSpec();
    org.junit.jupiter.api.Assertions.assertEquals(0, shardSpec.getPartitionNum());
    org.junit.jupiter.api.Assertions.assertEquals(0, shardSpec.getNumCorePartitions());
    org.junit.jupiter.api.Assertions.assertEquals(5, shardSpec.getNumBuckets());

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
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    id = allocatePendingSegment(
        dataSource,
        "seq2",
        null,
        interval,
        partialShardSpec,
        "version",
        true,
        null
    );

    shardSpec = (HashBasedNumberedShardSpec) id.getShardSpec();
    org.junit.jupiter.api.Assertions.assertEquals(1, shardSpec.getPartitionNum());
    org.junit.jupiter.api.Assertions.assertEquals(0, shardSpec.getNumCorePartitions());
    org.junit.jupiter.api.Assertions.assertEquals(5, shardSpec.getNumBuckets());

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
        ),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    id = allocatePendingSegment(
        dataSource,
        "seq3",
        null,
        interval,
        new HashBasedNumberedPartialShardSpec(null, 2, 3, null),
        "version",
        true,
        null
    );

    shardSpec = (HashBasedNumberedShardSpec) id.getShardSpec();
    org.junit.jupiter.api.Assertions.assertEquals(2, shardSpec.getPartitionNum());
    org.junit.jupiter.api.Assertions.assertEquals(0, shardSpec.getNumCorePartitions());
    org.junit.jupiter.api.Assertions.assertEquals(3, shardSpec.getNumBuckets());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAddNumberedShardSpecAfterMultiDimensionsShardSpecWithUnknownCorePartitionSize(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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
                  VirtualColumns.EMPTY,
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
    coordinator.commitSegments(originalSegments, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    final SegmentIdWithShardSpec id = allocatePendingSegment(
        datasource,
        "seq",
        null,
        interval,
        NumberedPartialShardSpec.instance(),
        version,
        false,
        null
    );
    org.junit.jupiter.api.Assertions.assertNull(id);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testAddNumberedShardSpecAfterSingleDimensionsShardSpecWithUnknownCorePartitionSize(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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
    coordinator.commitSegments(originalSegments, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));
    final SegmentIdWithShardSpec id = allocatePendingSegment(
        datasource,
        "seq",
        null,
        interval,
        NumberedPartialShardSpec.instance(),
        version,
        false,
        null
    );
    org.junit.jupiter.api.Assertions.assertNull(id);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRemoveDataSourceMetadataOlderThanDatasourceActiveShouldNotBeDeleted(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );

    // Try delete. Datasource should not be deleted as it is in excluded set
    int deletedCount = coordinator.removeDataSourceMetadataOlderThan(
        System.currentTimeMillis(),
        ImmutableSet.of(SUPERVISOR_ID)
    );

    // Datasource should not be deleted
    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );
    org.junit.jupiter.api.Assertions.assertEquals(0, deletedCount);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRemoveDataSourceMetadataOlderThanDatasourceNotActiveAndOlderThanTimeShouldBeDeleted(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );

    // Try delete. Datasource should be deleted as it is not in excluded set and created time older than given time
    int deletedCount = coordinator.removeDataSourceMetadataOlderThan(System.currentTimeMillis(), ImmutableSet.of());

    // Datasource should be deleted
    org.junit.jupiter.api.Assertions.assertNull(
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );
    org.junit.jupiter.api.Assertions.assertEquals(1, deletedCount);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRemoveDataSourceMetadataOlderThanDatasourceNotActiveButNotOlderThanTimeShouldNotBeDeleted(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(defaultSegment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)
    );

    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );

    // Do delete. Datasource metadata should not be deleted. Datasource is not active but it was created just now so it's
    // created timestamp will be later than the timestamp 2012-01-01T00:00:00Z
    int deletedCount = coordinator.removeDataSourceMetadataOlderThan(
        DateTimes.of("2012-01-01T00:00:00Z").getMillis(),
        ImmutableSet.of()
    );

    // Datasource should not be deleted
    org.junit.jupiter.api.Assertions.assertEquals(
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        coordinator.retrieveDataSourceMetadata(SUPERVISOR_ID)
    );
    org.junit.jupiter.api.Assertions.assertEquals(0, deletedCount);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testMarkSegmentsAsUnusedWithinIntervalOneYear(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(ImmutableSet.of(existingSegment1, existingSegment2), new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

    // interval covers existingSegment1 and partially overlaps existingSegment2,
    // only existingSegment1 will be dropped
    coordinator.markSegmentsWithinIntervalAsUnused(
        existingSegment1.getDataSource(),
        Intervals.of("1994-01-01/1994-01-02T12Z"),
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(
        ImmutableSet.of(existingSegment1),
        ImmutableSet.copyOf(
            coordinator.retrieveUnusedSegmentsForInterval(
                existingSegment1.getDataSource(),
                existingSegment1.getInterval().withEnd(existingSegment1.getInterval().getEnd().plus(1)),
                null,
                null,
                null
            )
        )
    );
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testMarkSegmentsAsUnusedWithinIntervalTwoYears(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(ImmutableSet.of(existingSegment1, existingSegment2), new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION));

    // interval covers existingSegment1 and partially overlaps existingSegment2,
    // only existingSegment1 will be dropped
    coordinator.markSegmentsWithinIntervalAsUnused(
        existingSegment1.getDataSource(),
        Intervals.of("1993-12-31T12Z/1994-01-02T12Z"),
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(
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
    org.junit.jupiter.api.Assertions.assertEquals(
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

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUsedSegmentsAndCreatedDates(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    coordinator.commitSegments(Set.of(defaultSegment), null);

    List<Pair<DataSegment, String>> resultForIntervalOnTheLeft =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("2000/2001")));
    org.junit.jupiter.api.Assertions.assertTrue(resultForIntervalOnTheLeft.isEmpty());

    List<Pair<DataSegment, String>> resultForIntervalOnTheRight =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("3000/3001")));
    org.junit.jupiter.api.Assertions.assertTrue(resultForIntervalOnTheRight.isEmpty());

    List<Pair<DataSegment, String>> resultForExactInterval =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(defaultSegment.getInterval()));
    org.junit.jupiter.api.Assertions.assertEquals(1, resultForExactInterval.size());
    org.junit.jupiter.api.Assertions.assertEquals(defaultSegment, resultForExactInterval.get(0).lhs);

    List<Pair<DataSegment, String>> resultForIntervalWithLeftOverlap =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("2000/2015-01-02")));
    org.junit.jupiter.api.Assertions.assertEquals(resultForExactInterval, resultForIntervalWithLeftOverlap);

    List<Pair<DataSegment, String>> resultForIntervalWithRightOverlap =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.of("2015-01-01/3000")));
    org.junit.jupiter.api.Assertions.assertEquals(resultForExactInterval, resultForIntervalWithRightOverlap);

    List<Pair<DataSegment, String>> resultForEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(Intervals.ETERNITY));
    org.junit.jupiter.api.Assertions.assertEquals(resultForExactInterval, resultForEternity);

    List<Pair<DataSegment, String>> resultForFirstHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(firstHalfEternityRangeSegment.getInterval()));
    org.junit.jupiter.api.Assertions.assertEquals(resultForExactInterval, resultForFirstHalfEternity);

    List<Pair<DataSegment, String>> resultForSecondHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(secondHalfEternityRangeSegment.getInterval()));
    org.junit.jupiter.api.Assertions.assertEquals(resultForExactInterval, resultForSecondHalfEternity);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUsedSegmentsAndCreatedDatesFetchesEternityForAnyInterval(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    // Ensure that overlapping segments do not have the same version
    // Otherwise they cannot be added to a timeline
    coordinator.commitSegments(
        Set.of(DataSegment.builder(eternitySegment).version("v1").build()),
        null
    );
    // Commit these segments separately so that the older one is not overshadowed
    coordinator.commitSegments(
        Set.of(
            DataSegment.builder(firstHalfEternityRangeSegment).version("v2").build(),
            DataSegment.builder(secondHalfEternityRangeSegment).version("v3").build()
        ),
        null
    );

    List<Pair<DataSegment, String>> resultForRandomInterval =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(defaultSegment.getInterval()));
    org.junit.jupiter.api.Assertions.assertEquals(3, resultForRandomInterval.size());

    List<Pair<DataSegment, String>> resultForEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(eternitySegment.getInterval()));
    org.junit.jupiter.api.Assertions.assertEquals(3, resultForEternity.size());

    List<Pair<DataSegment, String>> resultForFirstHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(firstHalfEternityRangeSegment.getInterval()));
    org.junit.jupiter.api.Assertions.assertEquals(3, resultForFirstHalfEternity.size());

    List<Pair<DataSegment, String>> resultForSecondHalfEternity =
        coordinator.retrieveUsedSegmentsAndCreatedDates(defaultSegment.getDataSource(), Collections.singletonList(secondHalfEternityRangeSegment.getInterval()));
    org.junit.jupiter.api.Assertions.assertEquals(3, resultForSecondHalfEternity.size());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testTimelineVisibilityWith0CorePartitionTombstone(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final Interval interval = Intervals.of("2020/2021");
    // Create and commit a tombstone segment
    final DataSegment tombstoneSegment = createSegment(
        interval,
        "version",
        new TombstoneShardSpec()
    );

    final Set<DataSegment> tombstones = new HashSet<>(Collections.singleton(tombstoneSegment));
    org.junit.jupiter.api.Assertions.assertTrue(coordinator.commitSegments(tombstones, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)).containsAll(tombstones));

    // Allocate and commit a data segment by appending to the same interval
    final SegmentIdWithShardSpec identifier = allocatePendingSegment(
        TestDataSource.WIKI,
        "seq",
        tombstoneSegment.getVersion(),
        interval,
        NumberedPartialShardSpec.instance(),
        "version",
        false,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals("wiki_2020-01-01T00:00:00.000Z_2021-01-01T00:00:00.000Z_version_1", identifier.toString());
    org.junit.jupiter.api.Assertions.assertEquals(0, identifier.getShardSpec().getNumCorePartitions());

    final DataSegment dataSegment = createSegment(
        interval,
        "version",
        identifier.getShardSpec()
    );
    final Set<DataSegment> dataSegments = new HashSet<>(Collections.singleton(dataSegment));
    org.junit.jupiter.api.Assertions.assertTrue(coordinator.commitSegments(dataSegments, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)).containsAll(dataSegments));

    // Mark the tombstone as unused
    markAllSegmentsUnused(tombstones, DateTimes.nowUtc());

    final Collection<DataSegment> allUsedSegments = coordinator.retrieveAllUsedSegments(
        TestDataSource.WIKI,
        Segments.ONLY_VISIBLE
    );

    // The appended data segment will still be visible in the timeline since the
    // tombstone contains 0 core partitions
    SegmentTimeline segmentTimeline = SegmentTimeline.forSegments(allUsedSegments);
    org.junit.jupiter.api.Assertions.assertEquals(1, segmentTimeline.lookup(interval).size());
    org.junit.jupiter.api.Assertions.assertEquals(dataSegment, segmentTimeline.lookup(interval).get(0).getObject().getChunk(1).getObject());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testTimelineWith1CorePartitionTombstone(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
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
    org.junit.jupiter.api.Assertions.assertTrue(coordinator.commitSegments(tombstones, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)).containsAll(tombstones));

    // Allocate and commit a data segment by appending to the same interval
    final SegmentIdWithShardSpec identifier = allocatePendingSegment(
        TestDataSource.WIKI,
        "seq",
        tombstoneSegment.getVersion(),
        interval,
        NumberedPartialShardSpec.instance(),
        "version",
        false,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals("wiki_2020-01-01T00:00:00.000Z_2021-01-01T00:00:00.000Z_version_1", identifier.toString());
    org.junit.jupiter.api.Assertions.assertEquals(1, identifier.getShardSpec().getNumCorePartitions());

    final DataSegment dataSegment = createSegment(
        interval,
        "version",
        identifier.getShardSpec()
    );
    final Set<DataSegment> dataSegments = new HashSet<>(Collections.singleton(dataSegment));
    org.junit.jupiter.api.Assertions.assertTrue(coordinator.commitSegments(dataSegments, new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION)).containsAll(dataSegments));

    // Mark the tombstone as unused
    coordinator.markSegmentAsUnused(tombstoneSegment.getId());

    final Collection<DataSegment> allUsedSegments = coordinator.retrieveAllUsedSegments(
        TestDataSource.WIKI,
        Segments.ONLY_VISIBLE
    );

    // The appended data segment will not be visible in the timeline since the old generation
    // tombstone contains 1 core partition
    SegmentTimeline segmentTimeline = SegmentTimeline.forSegments(allUsedSegments);
    org.junit.jupiter.api.Assertions.assertEquals(0, segmentTimeline.lookup(interval).size());
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testSegmentIdShouldNotBeReallocated(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final SegmentIdWithShardSpec idWithNullTaskAllocator = allocatePendingSegment(
        TestDataSource.WIKI,
        "seq",
        "0",
        Intervals.ETERNITY,
        NumberedPartialShardSpec.instance(),
        "version",
        false,
        null
    );
    final DataSegment dataSegment0 = createSegment(
        idWithNullTaskAllocator.getInterval(),
        idWithNullTaskAllocator.getVersion(),
        idWithNullTaskAllocator.getShardSpec()
    );

    final SegmentIdWithShardSpec idWithValidTaskAllocator = allocatePendingSegment(
        TestDataSource.WIKI,
        "seq",
        "1",
        Intervals.ETERNITY,
        NumberedPartialShardSpec.instance(),
        "version",
        false,
        "taskAllocatorId"
    );
    final DataSegment dataSegment1 = createSegment(
        idWithValidTaskAllocator.getInterval(),
        idWithValidTaskAllocator.getVersion(),
        idWithValidTaskAllocator.getShardSpec()
    );

    // Insert pending segments
    coordinator.commitSegments(ImmutableSet.of(dataSegment0, dataSegment1), null);
    // Clean up pending segments corresponding to the valid task allocator id
    coordinator.deletePendingSegmentsForTaskAllocatorId(TestDataSource.WIKI, "taskAllocatorId");
    // Mark all segments as unused
    coordinator.markSegmentsWithinIntervalAsUnused(TestDataSource.WIKI, Intervals.ETERNITY, null);

    final SegmentIdWithShardSpec theId = allocatePendingSegment(
        TestDataSource.WIKI,
        "seq",
        "2",
        Intervals.ETERNITY,
        NumberedPartialShardSpec.instance(),
        "version",
        false,
        "taskAllocatorId"
    );
    org.junit.jupiter.api.Assertions.assertNull(coordinator.retrieveSegmentForId(theId.asSegmentId()));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUnusedSegmentsForExactIntervalAndVersion(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    DataSegment unusedForDifferentVersion = createSegment(
        Intervals.of("2024/2025"),
        "v0",
        new NumberedShardSpec(0, 0)
    );
    DataSegment unusedSegmentForExactIntervalAndVersion = createSegment(
        Intervals.of("2024/2025"),
        "v1",
        new NumberedShardSpec(0, 0)
    );
    DataSegment unusedSegmentForDifferentInterval = createSegment(
        Intervals.of("2023/2024"),
        "v1",
        new NumberedShardSpec(0, 0)
    );
    coordinator.commitSegments(
        ImmutableSet.of(
            unusedForDifferentVersion,
            unusedSegmentForDifferentInterval,
            unusedSegmentForExactIntervalAndVersion
        ),
        null
    );
    coordinator.markSegmentsWithinIntervalAsUnused(TestDataSource.WIKI, Intervals.ETERNITY, null);

    DataSegment usedSegmentForExactIntervalAndVersion = createSegment(
        Intervals.of("2024/2025"),
        "v1",
        new NumberedShardSpec(1, 0)
    );
    coordinator.commitSegments(ImmutableSet.of(usedSegmentForExactIntervalAndVersion), null);


    SegmentId highestUnusedId = transactionFactory.inReadWriteDatasourceTransaction(
        TestDataSource.WIKI,
        transaction -> transaction.noCacheSql().retrieveHighestUnusedSegmentId(
            TestDataSource.WIKI,
            Intervals.of("2024/2025"),
            "v1"
        )
    );
    org.junit.jupiter.api.Assertions.assertEquals(
        unusedSegmentForExactIntervalAndVersion.getId(),
        highestUnusedId
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUpgradedFromSegmentIds(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String datasource = defaultSegment.getDataSource();
    final Map<String, String> upgradedFromSegmentIdMap = new HashMap<>();
    upgradedFromSegmentIdMap.put(defaultSegment2.getId().toString(), defaultSegment.getId().toString());
    insertUsedSegments(ImmutableSet.of(defaultSegment, defaultSegment2), upgradedFromSegmentIdMap);
    coordinator.markSegmentsWithinIntervalAsUnused(datasource, Intervals.ETERNITY, null);
    upgradedFromSegmentIdMap.clear();
    upgradedFromSegmentIdMap.put(defaultSegment3.getId().toString(), defaultSegment.getId().toString());
    insertUsedSegments(ImmutableSet.of(defaultSegment3, defaultSegment4), upgradedFromSegmentIdMap);

    Map<String, String> expected = new HashMap<>();
    expected.put(defaultSegment2.getId().toString(), defaultSegment.getId().toString());
    expected.put(defaultSegment3.getId().toString(), defaultSegment.getId().toString());

    Set<String> segmentIds = new HashSet<>();
    segmentIds.add(defaultSegment.getId().toString());
    segmentIds.add(defaultSegment2.getId().toString());
    segmentIds.add(defaultSegment3.getId().toString());
    segmentIds.add(defaultSegment4.getId().toString());
    org.junit.jupiter.api.Assertions.assertEquals(
        expected,
        coordinator.retrieveUpgradedFromSegmentIds(datasource, segmentIds)
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUpgradedFromSegmentIdsInBatches(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Assumptions.assumeFalse(isCacheEnabled());

    final int size = 500;
    final int batchSize = 100;

    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      segments.add(
          new DataSegment(
              "DS",
              Intervals.ETERNITY,
              "v " + (i % 5),
              ImmutableMap.of("num", i / 5),
              ImmutableList.of("dim"),
              ImmutableList.of("agg"),
              new NumberedShardSpec(i / 5, 0),
              0,
              100L
          )
      );
    }
    Map<String, String> expected = new HashMap<>();
    for (int i = 0; i < batchSize; i++) {
      for (int j = 1; j < 5; j++) {
        expected.put(
            segments.get(5 * i + j).getId().toString(),
            segments.get(5 * i).getId().toString()
        );
      }
    }
    insertUsedSegments(ImmutableSet.copyOf(segments), expected);

    Map<String, String> actual = coordinator.retrieveUpgradedFromSegmentIds(
        "DS",
        segments.stream().map(DataSegment::getId).map(SegmentId::toString).collect(Collectors.toSet())
    );

    org.junit.jupiter.api.Assertions.assertEquals(400, actual.size());
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUpgradedToSegmentIds(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String datasource = defaultSegment.getDataSource();
    final Map<String, String> upgradedFromSegmentIdMap = new HashMap<>();
    upgradedFromSegmentIdMap.put(defaultSegment2.getId().toString(), defaultSegment.getId().toString());
    insertUsedSegments(ImmutableSet.of(defaultSegment, defaultSegment2), upgradedFromSegmentIdMap);
    coordinator.markSegmentsWithinIntervalAsUnused(datasource, Intervals.ETERNITY, null);
    upgradedFromSegmentIdMap.clear();
    upgradedFromSegmentIdMap.put(defaultSegment3.getId().toString(), defaultSegment.getId().toString());
    insertUsedSegments(ImmutableSet.of(defaultSegment3, defaultSegment4), upgradedFromSegmentIdMap);

    Map<String, Set<String>> expected = new HashMap<>();
    expected.put(defaultSegment.getId().toString(), new HashSet<>());
    expected.get(defaultSegment.getId().toString()).add(defaultSegment.getId().toString());
    expected.get(defaultSegment.getId().toString()).add(defaultSegment2.getId().toString());
    expected.get(defaultSegment.getId().toString()).add(defaultSegment3.getId().toString());

    Set<String> upgradedIds = new HashSet<>();
    upgradedIds.add(defaultSegment.getId().toString());
    org.junit.jupiter.api.Assertions.assertEquals(
        expected,
        coordinator.retrieveUpgradedToSegmentIds(datasource, upgradedIds)
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUpgradedToSegmentIdsInBatches(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final int size = 500;
    final int batchSize = 100;

    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      segments.add(
          new DataSegment(
              "DS",
              Intervals.ETERNITY,
              "v " + (i % 5),
              ImmutableMap.of("num", i / 5),
              ImmutableList.of("dim"),
              ImmutableList.of("agg"),
              new NumberedShardSpec(i / 5, 0),
              0,
              100L
          )
      );
    }

    Map<String, Set<String>> expected = new HashMap<>();
    for (DataSegment segment : segments) {
      final String id = segment.getId().toString();
      expected.put(id, new HashSet<>());
      expected.get(id).add(id);
    }
    Map<String, String> upgradeMap = new HashMap<>();
    for (int i = 0; i < batchSize; i++) {
      for (int j = 1; j < 5; j++) {
        upgradeMap.put(
            segments.get(5 * i + j).getId().toString(),
            segments.get(5 * i).getId().toString()
        );
        expected.get(segments.get(5 * i).getId().toString())
                .add(segments.get(5 * i + j).getId().toString());
      }
    }
    insertUsedSegments(ImmutableSet.copyOf(segments), upgradeMap);

    Map<String, Set<String>> actual = coordinator.retrieveUpgradedToSegmentIds(
        "DS",
        segments.stream().map(DataSegment::getId).map(SegmentId::toString).collect(Collectors.toSet())
    );

    org.junit.jupiter.api.Assertions.assertEquals(500, actual.size());
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testRetrieveUsedSegmentsForSegmentAllocation(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    final String datasource = "DS";
    DataSegment firstSegment;
    Set<DataSegment> nextSegments;
    final Map<String, Object> loadspec = ImmutableMap.of("loadSpec", "loadSpec");
    final List<String> dimensions = ImmutableList.of("dim1", "dim2");
    final List<String> metrics = ImmutableList.of("metric1", "metric2");
    final int numSegmentsPerInterval = 100;

    final Interval month = Intervals.of("2024-10-01/2024-11-01");

    final Interval year = Intervals.of("2024/2025");

    final Interval overlappingDay = Intervals.of("2024-10-01/2024-10-02");
    final Interval nonOverlappingDay = Intervals.of("2024-01-01/2024-01-02");

    final List<Interval> intervals = ImmutableList.of(month, year, overlappingDay, nonOverlappingDay);
    final List<String> versions = ImmutableList.of("v0", "v1", "v2", "v2");
    for (int i = 0; i < 4; i++) {
      nextSegments = new HashSet<>();
      firstSegment = new DataSegment(
          datasource,
          intervals.get(i),
          versions.get(i),
          loadspec,
          dimensions,
          metrics,
          new DimensionRangeShardSpec(dimensions, VirtualColumns.EMPTY, null, null, 0, 1),
          0,
          100
      );
      coordinator.commitSegments(Set.of(firstSegment), null);
      for (int j = 1; j < numSegmentsPerInterval; j++) {
        nextSegments.add(
            new DataSegment(
                datasource,
                intervals.get(i),
                versions.get(i),
                loadspec,
                dimensions,
                metrics,
                // The numCorePartitions is intentionally 0
                new NumberedShardSpec(j, 0),
                0,
                100
            )
        );
      }
      coordinator.commitSegments(nextSegments, null);
    }

    final Set<SegmentIdWithShardSpec> expected = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < numSegmentsPerInterval; j++) {
        expected.add(
            new SegmentIdWithShardSpec(
                datasource,
                intervals.get(i),
                versions.get(i),
                new NumberedShardSpec(j, 1)
            )
        );
      }
    }

    Set<SegmentIdWithShardSpec> observed = transactionFactory.inReadOnlyDatasourceTransaction(
        datasource,
        transaction ->
            coordinator.retrieveUsedSegmentsForAllocation(transaction, datasource, month)
                       .stream()
                       .map(SegmentIdWithShardSpec::fromDataSegment)
                       .collect(Collectors.toSet())
    );

    org.junit.jupiter.api.Assertions.assertEquals(expected, observed);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCachedTransaction_cannotReadWhatItWrites(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Assumptions.assumeTrue(isCacheEnabled());

    transactionFactory.inReadWriteDatasourceTransaction(
        TestDataSource.WIKI,
        transaction -> {
          final DataSegmentPlus wikiSegment =
              CreateDataSegments.ofDatasource(TestDataSource.WIKI).updatedNow().markUsed().asPlus();
          org.junit.jupiter.api.Assertions.assertEquals(1, transaction.insertSegments(Set.of(wikiSegment)));

          // Verify that segment is not present in cache
          org.junit.jupiter.api.Assertions.assertNull(transaction.findUsedSegment(wikiSegment.getDataSegment().getId()));

          // Verify that segment is present in metadata store
          org.junit.jupiter.api.Assertions.assertEquals(
              wikiSegment.getDataSegment(),
              transaction.findSegment(wikiSegment.getDataSegment().getId())
          );

          return 0;
        }
    );

    emitter.verifyValue(Metric.READ_WRITE_TRANSACTIONS, 1L);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testReadOperation_usesCache_ifSynced(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Assumptions.assumeTrue(isCacheEnabled());

    org.junit.jupiter.api.Assertions.assertTrue(segmentMetadataCache.isSyncedForRead());

    insertUsedSegments(Set.of(defaultSegment), Map.of());
    final Supplier<Set<DataSegment>> retrieveAction =
        () -> coordinator.retrieveAllUsedSegments(
            defaultSegment.getDataSource(),
            Segments.INCLUDING_OVERSHADOWED
        );

    // Retrieve returns empty since cache is not synced with metadata store yet
    org.junit.jupiter.api.Assertions.assertTrue(retrieveAction.get().isEmpty());

    refreshCache();
    org.junit.jupiter.api.Assertions.assertEquals(Set.of(defaultSegment), retrieveAction.get());

    emitter.verifyEmitted(Metric.READ_ONLY_TRANSACTIONS, 2);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testReadOperation_doesNotUseCache_ifNotSynced(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Assumptions.assumeTrue(isCacheEnabled());

    segmentMetadataCache.stopBeingLeader();
    org.junit.jupiter.api.Assertions.assertFalse(segmentMetadataCache.isSyncedForRead());

    final Supplier<Set<DataSegment>> retrieveAction =
        () -> coordinator.retrieveAllUsedSegments(
            defaultSegment.getDataSource(),
            Segments.INCLUDING_OVERSHADOWED
        );

    insertUsedSegments(Set.of(defaultSegment), Map.of());

    org.junit.jupiter.api.Assertions.assertEquals(Set.of(defaultSegment), retrieveAction.get());
    emitter.verifyNotEmitted(Metric.READ_ONLY_TRANSACTIONS);

    // Become leader but cache will still not be used
    segmentMetadataCache.becomeLeader();
    org.junit.jupiter.api.Assertions.assertFalse(segmentMetadataCache.isSyncedForRead());
    org.junit.jupiter.api.Assertions.assertEquals(Set.of(defaultSegment), retrieveAction.get());
    emitter.verifyNotEmitted(Metric.READ_ONLY_TRANSACTIONS);

    // Sync the cache so that it becomes ready for use
    refreshCache();
    refreshCache();
    org.junit.jupiter.api.Assertions.assertTrue(segmentMetadataCache.isSyncedForRead());
    org.junit.jupiter.api.Assertions.assertEquals(Set.of(defaultSegment), retrieveAction.get());
    emitter.verifyValue(Metric.READ_ONLY_TRANSACTIONS, 1L);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testWriteOperation_alwaysUsesCache_inModeIfSynced(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    Assumptions.assumeTrue(cacheMode == SegmentMetadataCache.UsageMode.IF_SYNCED);

    // Lose and regain leadership
    segmentMetadataCache.stopBeingLeader();
    segmentMetadataCache.becomeLeader();

    org.junit.jupiter.api.Assertions.assertTrue(segmentMetadataCache.isEnabled());
    org.junit.jupiter.api.Assertions.assertFalse(segmentMetadataCache.isSyncedForRead());

    final Supplier<Set<DataSegment>> writeAction =
        () -> coordinator.commitSegments(Set.of(defaultSegment), null);

    // Cache is not synced yet and will be used only for write operations
    org.junit.jupiter.api.Assertions.assertEquals(Set.of(defaultSegment), writeAction.get());
    emitter.verifyValue(Metric.WRITE_ONLY_TRANSACTIONS, 1L);

    // Sync the cache to use it for both read and write operations
    refreshCache();
    refreshCache();
    org.junit.jupiter.api.Assertions.assertTrue(segmentMetadataCache.isSyncedForRead());

    org.junit.jupiter.api.Assertions.assertTrue(writeAction.get().isEmpty());
    emitter.verifyValue(Metric.READ_WRITE_TRANSACTIONS, 1L);
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitSegmentsAndMetadata_marksPendingIndexingStateAsActive(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    String fingerprint = "vanillaFingerprint";
    CompactionState state = createTestIndexingState();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, fingerprint, state, DateTimes.nowUtc());
    org.junit.jupiter.api.Assertions.assertEquals(Boolean.TRUE, indexingStateStorage.isIndexingStatePending(fingerprint));

    final DataSegment segment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                   .startingAt("2023-01-01")
                                                   .withIndexingStateFingerprint(fingerprint)
                                                   .eachOfSizeInMb(500)
                                                   .get(0);

    coordinator.commitSegmentsAndMetadata(
        ImmutableSet.of(segment),
        SUPERVISOR_ID,
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableMap.of("foo", "bar")),
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(Boolean.FALSE, indexingStateStorage.isIndexingStatePending(fingerprint));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitReplaceSegments_marksPendingIndexingStateAsActive(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    String fingerprint = "replaceFingerprint";
    CompactionState state = createTestIndexingState();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, fingerprint, state, DateTimes.nowUtc());
    org.junit.jupiter.api.Assertions.assertEquals(Boolean.TRUE, indexingStateStorage.isIndexingStatePending(fingerprint));

    final DataSegment segment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                   .startingAt("2023-01-01")
                                                   .withIndexingStateFingerprint(fingerprint)
                                                   .eachOfSizeInMb(500)
                                                   .get(0);

    final String replaceTaskId = "replaceTask";
    final ReplaceTaskLock replaceLock = new ReplaceTaskLock(
        replaceTaskId,
        Intervals.of("2023-01-01/2023-01-02"),
        "2024-01-01"
    );

    coordinator.commitReplaceSegments(
        ImmutableSet.of(segment),
        ImmutableSet.of(replaceLock),
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(Boolean.FALSE, indexingStateStorage.isIndexingStatePending(fingerprint));
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void testCommitAppendSegments_marksPendingIndexingStateAsActive(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorTest(cacheMode);
    String fingerprint = "appendFingerprint";
    CompactionState state = createTestIndexingState();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, fingerprint, state, DateTimes.nowUtc());
    org.junit.jupiter.api.Assertions.assertEquals(Boolean.TRUE, indexingStateStorage.isIndexingStatePending(fingerprint));

    final DataSegment segment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                   .startingAt("2023-01-01")
                                                   .withIndexingStateFingerprint(fingerprint)
                                                   .eachOfSizeInMb(500)
                                                   .get(0);

    final String taskAllocatorId = "appendTask";

    coordinator.commitAppendSegments(
        ImmutableSet.of(segment),
        Map.of(),
        taskAllocatorId,
        null
    );

    org.junit.jupiter.api.Assertions.assertEquals(Boolean.FALSE, indexingStateStorage.isIndexingStatePending(fingerprint));
  }

  private CompactionState createTestIndexingState()
  {
    return CompactionState.builder()
                          .partitionsSpec(new DynamicPartitionsSpec(100, null))
                          .indexSpec(IndexSpec.getDefault())
                          .build();
  }

  private SegmentIdWithShardSpec allocatePendingSegment(
      String datasource,
      String sequenceName,
      String previousSegmentId,
      Interval interval,
      PartialShardSpec partialShardSpec,
      String maxVersion,
      boolean skipSegmentLineageCheck,
      String taskAllocatorId
  )
  {
    return coordinator.allocatePendingSegment(
        datasource,
        interval,
        skipSegmentLineageCheck,
        new SegmentCreateRequest(
            sequenceName,
            previousSegmentId,
            maxVersion,
            partialShardSpec,
            taskAllocatorId
        )
    );
  }

  private SegmentIdWithShardSpec allocatePendingSegmentForAppendTask(
      String dataSource,
      Interval interval,
      String taskAllocatorId
  )
  {
    return coordinator.allocatePendingSegment(
        dataSource,
        interval,
        true,
        new SegmentCreateRequest(
            IdUtils.getRandomId(),
            null,
            PendingSegmentRecord.DEFAULT_VERSION_FOR_CONCURRENT_APPEND,
            NumberedPartialShardSpec.instance(),
            taskAllocatorId
        )
    );
  }

  private int insertPendingSegments(
      String dataSource,
      List<PendingSegmentRecord> pendingSegments,
      boolean skipLineageCheck
  )
  {
    return transactionFactory.inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> transaction.insertPendingSegments(pendingSegments, skipLineageCheck)
    );
  }

  private void insertUsedSegments(Set<DataSegment> segments, Map<String, String> upgradedFromSegmentIdMap)
  {
    insertUsedSegments(segments, upgradedFromSegmentIdMap, derbyConnectorRule, mapper);
  }

  private static DataSegment asSegment(SegmentIdWithShardSpec pendingSegment)
  {
    final SegmentId id = pendingSegment.asSegmentId();
    return DataSegment.builder(id)
                      .shardSpec(pendingSegment.getShardSpec())
                      .loadSpec(Map.of(id.toString(), id.toString()))
                      .build();
  }

  private void verifyIntervalHasUsedSegments(
      String dataSource,
      Interval interval,
      DataSegment... expectedSegments
  )
  {
    org.junit.jupiter.api.Assertions.assertEquals(
        Set.of(expectedSegments),
        coordinator.retrieveUsedSegmentsForIntervals(dataSource, List.of(interval), Segments.INCLUDING_OVERSHADOWED)
    );
  }

  private void verifyIntervalHasVisibleSegments(
      String dataSource,
      Interval interval,
      DataSegment... expectedSegments
  )
  {
    org.junit.jupiter.api.Assertions.assertEquals(
        Set.of(expectedSegments),
        coordinator.retrieveUsedSegmentsForIntervals(dataSource, List.of(interval), Segments.ONLY_VISIBLE)
    );
  }

  private DataSegmentPlus toSegmentPlusUpgradedId(DataSegment segment, String upgradedFromSegmentId)
  {
    return new DataSegmentPlus(segment, null, null, null, null, null, upgradedFromSegmentId, null);
  }
}
