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

package org.apache.druid.testing.embedded.deltalake;

import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.delta.common.DeltaLakeDruidModule;
import org.apache.druid.delta.input.DeltaInputSource;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * End-to-end ingestion test for the Delta Lake input source running inside an embedded Druid cluster.
 *
 * <p>The test ingests a Delta table whose two Parquet files contain 2000 rows each (4000 rows total).
 * Because each file has more than the Delta kernel's default batch size of 1024 rows, this exercises the
 * per-file batch-drain path in {@code DeltaInputSourceReader} that regressed in
 * <a href="https://github.com/apache/druid/issues/18606">GH-18606</a>: the reader only returned the first
 * 1024 rows of each file, yielding {@code 1024 * 2 = 2048} rows instead of 4000. This is the integration-level
 * counterpart of the unit regression test {@code DeltaInputSourceTest.BatchDrainRegressionTests}.
 */
public class DeltaLakeInputSourceIngestionTest extends EmbeddedClusterTestBase
{
  /**
   * Delta table with 2 Parquet files x 2000 rows = 4000 rows total, columns {@code id} (long) and
   * {@code name} (string). Copied from the {@code druid-deltalake-extensions} test resources
   * ({@code large-row-group-table}).
   */
  private static final String DELTA_TABLE_RESOURCE = "delta/large-row-group-table";
  private static final int EXPECTED_ROW_COUNT = 4000;

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addExtension(DeltaLakeDruidModule.class)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  @Test
  public void testIngestAllRowsFromDeltaTable()
  {
    final File deltaTableDir = Resources.getFileForResource(DELTA_TABLE_RESOURCE);

    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    final IndexTask task = TaskBuilder
        .ofTypeIndex()
        .inputSource(new DeltaInputSource(deltaTableDir.getAbsolutePath(), null, null, null))
        // The Delta input source produces rows directly, so no inputFormat is required.
        .dataSource(dataSource)
        // The 'id' column holds POSIX seconds, used here as the timestamp.
        .dataSchema(builder -> builder.withTimestamp(new TimestampSpec("id", "posix", null)))
        .dimensions("name")
        .granularitySpec("DAY", "NONE", false)
        .dynamicPartitionWithMaxRows(EXPECTED_ROW_COUNT + 1)
        .appendToExisting(false)
        .withId(taskId);

    cluster.callApi().runTask(task, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // The core regression assertion: all 4000 rows must be ingested. Before the GH-18606 fix this
    // returned 1024 * 2 = 2048 because only the first batch of each Parquet file was read. COUNT(*) is
    // exact (unlike SQL COUNT(DISTINCT), which is approximate by default), so the comparison is stable.
    Assertions.assertEquals(
        String.valueOf(EXPECTED_ROW_COUNT),
        cluster.runSql("SELECT COUNT(*) FROM %s", dataSource),
        "Expected all rows to be ingested. A count of 2048 indicates the per-file batch-drain bug (GH-18606) regressed."
    );

    // The 'id' column was used as the POSIX timestamp; its documented min/max (0 and 3999) bound __time,
    // confirming rows from both Parquet files (not just the first batch of each) were ingested.
    Assertions.assertEquals(
        "1970-01-01T00:00:00.000Z,1970-01-01T01:06:39.000Z",
        cluster.runSql("SELECT MIN(__time), MAX(__time) FROM %s", dataSource)
    );
  }
}
