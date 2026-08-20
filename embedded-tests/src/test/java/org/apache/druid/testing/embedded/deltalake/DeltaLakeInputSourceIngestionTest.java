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

import org.apache.druid.delta.common.DeltaLakeDruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * End-to-end ingestion test for the Delta Lake input source running inside an embedded Druid cluster.
 * Modeled on the Iceberg embedded test ({@code IcebergRestCatalogIngestionTest}): it ingests an external
 * table with an MSQ {@code INSERT ... EXTERN(...)} SQL statement and verifies the result over the cluster.
 * Unlike Iceberg, Delta needs no catalog or testcontainer since it reads directly from a filesystem path.
 *
 * <p>The table has two Parquet files of 2000 rows each (4000 rows total). Because each file exceeds the
 * Delta kernel's default batch size of 1024 rows, this exercises the per-file batch-drain path in
 * {@code DeltaInputSourceReader} that regressed in
 * <a href="https://github.com/apache/druid/issues/18606">GH-18606</a>, where the reader returned only the
 * first 1024 rows of each file ({@code 1024 * 2 = 2048} instead of 4000). This is the integration-level
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
    final EmbeddedMSQApis msqApis = new EmbeddedMSQApis(cluster, overlord);

    final File deltaTableDir = Resources.getFileForResource(DELTA_TABLE_RESOURCE);
    // Escape backslashes so a Windows path stays valid inside the JSON input source spec.
    final String tablePath = StringUtils.replace(deltaTableDir.getAbsolutePath(), "\\", "\\\\");

    final String sql = StringUtils.format(
        "INSERT INTO %s\n"
        + "SELECT\n"
        // The 'id' column holds POSIX seconds; convert to a millisecond timestamp.
        + "  MILLIS_TO_TIMESTAMP(\"id\" * 1000) AS __time,\n"
        + "  \"name\"\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"delta\",\"tablePath\":\"%s\"}',\n"
        // EXTERN requires a non-null inputFormat, but the Delta input source reads Parquet via the
        // Delta kernel and ignores it (DeltaInputSource.needsFormat() == false). This value is unused.
        + "    '{\"type\":\"json\"}',\n"
        + "    '[{\"type\":\"long\",\"name\":\"id\"},{\"type\":\"string\",\"name\":\"name\"}]'\n"
        + "  )\n"
        + ")\n"
        + "PARTITIONED BY DAY",
        dataSource,
        tablePath
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // The core regression assertion: all 4000 rows must be ingested. Before the GH-18606 fix this
    // returned 1024 * 2 = 2048 because only the first batch of each Parquet file was read.
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM %s",
        dataSource,
        String.valueOf(EXPECTED_ROW_COUNT)
    );

    // Secondary sanity check on timestamp parsing and value range: the 'id' column (used as the POSIX
    // timestamp) has a documented min of 0 and max of 3999, which should bound __time after ingestion.
    // Completeness across both files is guaranteed by the COUNT(*) assertion above, not by these bounds.
    cluster.callApi().verifySqlQuery(
        "SELECT MIN(__time), MAX(__time) FROM %s",
        dataSource,
        "1970-01-01T00:00:00.000Z,1970-01-01T01:06:39.000Z"
    );
  }
}
