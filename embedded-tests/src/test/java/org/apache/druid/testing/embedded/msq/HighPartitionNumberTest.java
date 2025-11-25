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

package org.apache.druid.testing.embedded.msq;

import com.google.common.collect.Iterables;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * Test to verify that high partition numbers (above the limit of {@link PartitionIds#ROOT_GEN_END_PARTITION_ID})
 * work correctly when segment locking is not in play.
 */
public class HighPartitionNumberTest extends EmbeddedClusterTestBase
{
  /**
   * Expected number of rows for three copies of {@link Resources.DataFile#tinyWiki1Json()}.
   */
  private static final int EXPECTED_TOTAL_ROWS = 9;

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");

  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(180_000)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  @BeforeAll
  public void initTestClient()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
  }

  @Test
  public void testHighPartitionNumbers()
  {
    insertFirstSegment();
    insertSecondSegment();
    insertLastSegments();


    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // Verify that we have segments with partition numbers above the old limit
    final int maxPartitionNum = Integer.parseInt(cluster.runSql(
        "SELECT MAX(partition_num) FROM sys.segments WHERE datasource=%s",
        Calcites.escapeStringLiteral(dataSource)
    ).trim());

    Assertions.assertEquals(32769 /* larger than Short.MAX_VALUE */, maxPartitionNum);

    // Verify that all data is queryable
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM %s",
        dataSource,
        String.valueOf(EXPECTED_TOTAL_ROWS)
    );
  }

  /**
   * Inserts {@link Resources.DataFile#tinyWiki1Json()} with partition number zero, using SQL.
   */
  private void insertFirstSegment()
  {
    // Insert tinyWiki1Json in 1 segment (it's a 3 line file).
    String queryLocal = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json()
    );

    Map<String, Object> context = Map.of(
        MultiStageQueryContext.CTX_CLUSTER_STATISTICS_MERGE_MODE,
        ClusterStatisticsMergeMode.PARALLEL
    );

    cluster.callApi().waitForTaskToSucceed(msqApis.submitTaskSql(context, queryLocal).getTaskId(), overlord);
  }

  /**
   * Reinserts the segment from {@link #insertFirstSegment()} directly into metadata storage,
   * with partition number 32767.
   */
  private void insertSecondSegment()
  {
    // Get the segment and reinsert it with a higher partition number.
    final DataSegment firstSegment =
        Iterables.getOnlyElement(
            overlord.bindings()
                    .segmentsMetadataStorage()
                    .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        );

    overlord.bindings().segmentsMetadataStorage().commitSegments(
        Set.of(
            firstSegment.withShardSpec(
                new NumberedShardSpec(
                    Short.MAX_VALUE - 1,
                    firstSegment.getShardSpec().getNumCorePartitions()
                )
            )
        ),
        null
    );
  }

  /**
   * Inserts {@link Resources.DataFile#tinyWiki1Json()} with SQL into three segments, starting at
   * {@link Short#MAX_VALUE} and going to {@link Short#MAX_VALUE} + 2.
   */
  private void insertLastSegments()
  {
    // Insert tinyWiki1Json in 3 segment (it's a 3 line file).
    String queryLocal = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json()
    );

    Map<String, Object> context = Map.of(
        MultiStageQueryContext.CTX_CLUSTER_STATISTICS_MERGE_MODE,
        ClusterStatisticsMergeMode.PARALLEL,
        MultiStageQueryContext.CTX_ROWS_PER_SEGMENT,
        1
    );

    cluster.callApi().waitForTaskToSucceed(msqApis.submitTaskSql(context, queryLocal).getTaskId(), overlord);
  }
}
