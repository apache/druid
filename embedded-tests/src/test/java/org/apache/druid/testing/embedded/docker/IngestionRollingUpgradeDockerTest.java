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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.storage.s3.output.S3StorageConnectorModule;
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.DruidContainer;
import org.apache.druid.testing.MountedDir;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.apache.druid.testing.embedded.msq.MinIODurableStorageResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests that ingestion works correctly during a rolling upgrade, by using a
 * cluster which has a Historical/Indexer on an old version and
 * another Indexer + all other services on the latest version.
 * Validates that segments and intermediate shuffle data written to S3 by one version can be
 * read by the other.
 * <p>
 * <h3>Task distribution guarantees</h3>
 * The Overlord uses {@code EqualDistributionWorkerSelectStrategy} by default,
 * which assigns each task to the worker with the most available capacity.
 * Both indexers are configured with capacity 2 (4 total slots).
 * <ul>
 * <li><b>Native batch ({@code index})</b>: Two IndexTasks are submitted
 *     concurrently to separate datasources. The first task reduces one
 *     indexer's available capacity, so the second is assigned to the other
 *     indexer.</li>
 * <li><b>Native batch ({@code index_parallel})</b>: The supervisor task
 *     occupies one slot, then 3 subtasks (one per input file) are submitted.
 *     With 3 remaining slots across 2 indexers, subtasks are guaranteed to
 *     land on both.</li>
 * <li><b>MSQ</b>: With {@code maxNumTasks=3} (1 controller + 2 workers), the
 *     controller task fills one slot, so the two worker tasks are distributed
 *     across both indexers. This guarantees cross-version S3 shuffle I/O.</li>
 * </ul>
 */
public class IngestionRollingUpgradeDockerTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private final EmbeddedIndexer embeddedIndexer = new EmbeddedIndexer();

  private final MinIOStorageResource storageResource = new MinIOStorageResource();
  private final MinIODurableStorageResource durableStorageResource =
      new MinIODurableStorageResource(storageResource);

  /**
   * Datasources created during a test, cleaned up in {@link #cleanUp()}.
   */
  private final List<String> createdDataSources = new ArrayList<>();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    // Use port 7091 for the embedded indexer to avoid conflict with Docker INDEXER on 8091
    embeddedIndexer
        .setServerMemory(300_000_000)
        .addProperty("druid.plaintextPort", "7091")
        .addProperty("druid.worker.capacity", "2")
        .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    // Mount the test data directory so the Docker indexer can read local input files
    // at the same absolute paths used by the embedded indexer.
    final File testDataDir = Resources.DataFile.tinyWiki1Json().getParentFile();
    final MountedDir testDataMount = new MountedDir(testDataDir, testDataDir);

    DruidContainerResource containerIndexer = new DruidContainerResource(DruidCommand.Server.INDEXER)
        .usingImage(DruidContainer.Image.APACHE_33)
        .addMount(testDataMount);

    DruidContainerResource historicalContainer = new DruidContainerResource(DruidCommand.Server.HISTORICAL)
        .usingImage(DruidContainer.Image.APACHE_33);

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useContainerFriendlyHostname()
        .useDefaultTimeoutForLatchableEmitter(60)
        .addExtensions(S3StorageConnectorModule.class)
        .addResource(storageResource)
        .addResource(durableStorageResource)
        .addResource(containerIndexer)
        .addResource(historicalContainer)
        .addServer(new EmbeddedCoordinator())
        .addServer(overlord)
        .addServer(embeddedIndexer)
        .addServer(broker)
        .addCommonProperty(
            "druid.extensions.loadList",
            "[\"druid-s3-extensions\", \"druid-multi-stage-query\"]"
        );
  }

  @AfterEach
  public void cleanUp()
  {
    for (String ds : createdDataSources) {
      cluster.callApi().onLeaderOverlord(o -> o.markSegmentsAsUnused(ds));
    }
    createdDataSources.clear();
  }

  /**
   * Submits two native batch IndexTasks concurrently to separate datasources.
   * With {@code EqualDistributionWorkerSelectStrategy} and equal capacity on
   * both indexers, the two tasks are guaranteed to land on different indexers.
   * Verifies that segments produced by both old and new indexers are loaded
   * and queryable by the Historical.
   */
  @Test
  public void test_nativeBatchIngestion_historicalsCanLoadSegments_withMixedVersionIndexers()
  {
    final int numSegmentsPerTask = 10;

    final String ds1 = newDataSource();
    final String ds2 = newDataSource();

    final String taskId1 = IdUtils.getRandomId();
    final String taskId2 = IdUtils.getRandomId();

    final IndexTask task1 = MoreResources.Task.BASIC_INDEX.get().dataSource(ds1).withId(taskId1);
    final IndexTask task2 = MoreResources.Task.BASIC_INDEX.get().dataSource(ds2).withId(taskId2);

    // Submit both tasks before waiting so they are assigned to different indexers
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId1, task1));
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId2, task2));

    cluster.callApi().waitForTaskToSucceed(taskId1, overlord);
    cluster.callApi().waitForTaskToSucceed(taskId2, overlord);

    waitForSegmentsToBeQueryable(ds1, numSegmentsPerTask);
    waitForSegmentsToBeQueryable(ds2, numSegmentsPerTask);

    cluster.callApi().verifySqlQuery("SELECT * FROM %s", ds1, Resources.InlineData.CSV_10_DAYS);
    cluster.callApi().verifySqlQuery("SELECT * FROM %s", ds2, Resources.InlineData.CSV_10_DAYS);
  }

  /**
   * Submits an {@code index_parallel} task with 3 local input files and
   * {@code maxNumConcurrentSubTasks=3}. Using {@code MaxSizeSplitHintSpec(1)}
   * forces one subtask per file. The supervisor occupies one slot on one
   * indexer, leaving 3 slots across 2 indexers for the subtasks -- guaranteeing
   * that subtasks land on both the old and new indexer.
   */
  @Test
  public void test_nativeBatchIndexParallelIngestion_mixedVersionSubtasksShareShuffle_createsValidSegments()
  {
    final String ds = newDataSource();
    final String taskId = IdUtils.getRandomId();

    final ParallelIndexSupervisorTask task =
        TaskBuilder.ofTypeIndexParallel()
                   .dataSource(ds)
                   .timestampColumn("timestamp")
                   .jsonInputFormat()
                   .localInputSourceWithFiles(
                       Resources.DataFile.tinyWiki1Json(),
                       Resources.DataFile.tinyWiki2Json(),
                       Resources.DataFile.tinyWiki3Json()
                   )
                   .dimensions()
                   .tuningConfig(
                       t -> t.withMaxNumConcurrentSubTasks(3)
                             .withSplitHintSpec(new MaxSizeSplitHintSpec(1L, null))
                             .withPartitionsSpec(new HashedPartitionsSpec(null, null, null))
                   )
                   .withId(taskId);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);

    waitForSegmentsToBeQueryable(ds, 1);
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM %s", ds, "10");
  }

  /**
   * Submits an MSQ INSERT with durable shuffle storage and {@code maxNumTasks=3}
   * (1 controller + 2 workers). The controller fills one slot on one indexer,
   * so the two worker tasks are distributed across both indexers by the
   * {@code EqualDistributionWorkerSelectStrategy}. Validates that MSQ shuffle
   * files written to S3 by one version can be read by the other.
   */
  @Test
  public void test_msqIngestion_withDurableShuffleStorage_mixedVersionIndexersShareShuffle_createsValidSegments()
  {
    final String ds = newDataSource();
    final String sql = StringUtils.format(
        "SET durableShuffleStorage = TRUE;\n"
        + "SET maxNumTasks = 3;\n"
        + MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        ds,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    );

    final EmbeddedMSQApis msqApis = new EmbeddedMSQApis(cluster, overlord);
    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);

    waitForSegmentsToBeQueryable(ds, 1);
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM %s",
        ds,
        "2"
    );
  }

  private String newDataSource()
  {
    final String ds = EmbeddedClusterApis.createTestDatasourceName();
    createdDataSources.add(ds);
    return ds;
  }

  private void waitForSegmentsToBeQueryable(String dsName, int numSegments)
  {
    broker.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/schemaCache/refresh/count")
                       .hasService("druid/broker")
                       .hasDimension(DruidMetrics.DATASOURCE, dsName),
        agg -> agg.hasSumAtLeast(numSegments)
    );
  }
}
