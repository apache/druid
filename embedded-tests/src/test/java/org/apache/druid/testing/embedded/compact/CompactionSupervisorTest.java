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

package org.apache.druid.testing.embedded.compact;

import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Embedded test that runs inline-config compaction supervisors.
 */
public class CompactionSupervisorTest extends CompactionSupervisorTestBase
{
  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void test_ingestDayGranularity_andCompactToMonthGranularity_andCompactToYearGranularity_withInlineConfig(CompactionEngine compactionEngine)
  {
    configureCompaction(compactionEngine);

    // Ingest data at DAY granularity and verify
    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,shirt,105"
        + "\n2025-06-02T00:00:00.000Z,trousers,210"
        + "\n2025-06-03T00:00:00.000Z,jeans,150"
    );
    Assertions.assertEquals(3, getNumSegmentsWith(Granularities.DAY));

    // Create a compaction config with MONTH granularity
    InlineSchemaDataSourceCompactionConfig monthGranularityConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
            )
            .withTuningConfig(
                createTuningConfigWithPartitionsSpec(
                    new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false)
                )
            )
            .build();

    runCompactionWithSpec(monthGranularityConfig);
    waitForAllCompactionTasksToFinish();

    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(1, getNumSegmentsWith(Granularities.MONTH));

    verifyCompactedSegmentsHaveFingerprints(monthGranularityConfig);

    InlineSchemaDataSourceCompactionConfig yearGranConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
            )
            .withTuningConfig(
                createTuningConfigWithPartitionsSpec(
                    new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false)
                )
            )
            .build();

    overlord.latchableEmitter().flush(); // flush events so wait for works correctly on the next round of compaction
    runCompactionWithSpec(yearGranConfig);
    waitForAllCompactionTasksToFinish();

    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.MONTH));
    Assertions.assertEquals(1, getNumSegmentsWith(Granularities.YEAR));

    verifyCompactedSegmentsHaveFingerprints(yearGranConfig);
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void test_compaction_withPersistLastCompactionStateFalse_storesOnlyFingerprint(CompactionEngine compactionEngine)
  {
    // Configure cluster with storeCompactionStatePerSegment=false
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(
            new ClusterCompactionConfig(1.0, 100, null, true, compactionEngine, false)
        )
    );
    Assertions.assertTrue(updateResponse.isSuccess());

    // Ingest data at DAY granularity
    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,shirt,105\n"
        + "2025-06-02T00:00:00.000Z,trousers,210"
    );
    Assertions.assertEquals(2, getNumSegmentsWith(Granularities.DAY));

    // Create compaction config to compact to MONTH granularity
    InlineSchemaDataSourceCompactionConfig monthConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
            )
            .withTuningConfig(
                createTuningConfigWithPartitionsSpec(
                    new DimensionRangePartitionsSpec(1000, null, List.of("item"), false)
                )
            )
            .build();

    runCompactionWithSpec(monthConfig);
    waitForAllCompactionTasksToFinish();

    verifySegmentsHaveNullLastCompactionStateAndNonNullFingerprint();
  }

  /**
   * Tests that when a compaction task filters out all rows using a transform spec,
   * tombstones are created to properly drop the old segments. This test covers both
   * hash and range partitioning strategies.
   *
   * This regression test addresses a bug where compaction with transforms that filter
   * all rows would succeed but not create tombstones, leaving old segments visible
   * and causing indefinite compaction retries.
   */
  @MethodSource("getEngineAndPartitionType")
  @ParameterizedTest(name = "compactionEngine={0}, partitionType={1}")
  public void test_compactionWithTransformFilteringAllRows_createsTombstones(
      CompactionEngine compactionEngine,
      String partitionType
  )
  {
    configureCompaction(compactionEngine);

    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,hat,105"
        + "\n2025-06-02T00:00:00.000Z,shirt,210"
        + "\n2025-06-03T00:00:00.000Z,shirt,150"
    );

    int initialSegmentCount = getNumSegmentsWith(Granularities.DAY);
    Assertions.assertEquals(3, initialSegmentCount, "Should have 3 initial segments");

    InlineSchemaDataSourceCompactionConfig.Builder builder = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(dataSource)
        .withSkipOffsetFromLatest(Period.seconds(0))
        .withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
        )
        .withTransformSpec(
            // This filter drops all rows: expression "false" always evaluates to false
            new CompactionTransformSpec(
                new NotDimFilter(new EqualityFilter("item", ColumnType.STRING, "shirt", null)),
                null
            )
        );

    if (compactionEngine == CompactionEngine.NATIVE) {
      builder = builder.withIoConfig(
          // Enable REPLACE mode to create tombstones when no segments are produced
          new UserCompactionTaskIOConfig(true)
      );
    }

    // Add partitioning spec based on test parameter
    if ("range".equals(partitionType)) {
      builder.withTuningConfig(
          new UserCompactionTaskQueryTuningConfig(
              null,
              null,
              null,
              null,
              null,
              new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null
          )
      );
    } else {
      // Hash partitioning
      builder.withTuningConfig(
          new UserCompactionTaskQueryTuningConfig(
              null,
              null,
              null,
              null,
              null,
              new HashedPartitionsSpec(null, null, null),
              null,
              null,
              null,
              null,
              null,
              2,
              null,
              null,
              null,
              null,
              null,
              null,
              null
          )
      );
    }

    InlineSchemaDataSourceCompactionConfig compactionConfig = builder.build();

    runCompactionWithSpec(compactionConfig);
    waitForAllCompactionTasksToFinish();
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    int finalSegmentCount = getNumSegmentsWith(Granularities.DAY);
    Assertions.assertEquals(
        1,
        finalSegmentCount,
        "2 of 3 segments should be dropped via tombstones when transform filters all rows where item = 'shirt'"
    );
  }

  private void verifySegmentsHaveNullLastCompactionStateAndNonNullFingerprint()
  {
    overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        .forEach(segment -> {
          Assertions.assertNull(
              segment.getLastCompactionState(),
              "Segment " + segment.getId() + " should have null lastCompactionState"
          );
          Assertions.assertNotNull(
              segment.getIndexingStateFingerprint(),
              "Segment " + segment.getId() + " should have non-null indexingStateFingerprint"
          );
        });
  }

  public static List<Object[]> getEngineAndPartitionType()
  {
    List<Object[]> params = new ArrayList<>();
    for (CompactionEngine engine : List.of(CompactionEngine.NATIVE, CompactionEngine.MSQ)) {
      for (String partitionType : List.of("range", "hash")) {
        if (engine == CompactionEngine.MSQ && "hash".equals(partitionType)) {
          // MSQ does not support hash partitioning in this context
          continue;
        }
        params.add(new Object[]{engine, partitionType});
      }
    }
    return params;
  }
}
