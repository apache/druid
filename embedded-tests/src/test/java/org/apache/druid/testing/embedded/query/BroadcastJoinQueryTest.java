/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.druid.testing.embedded.query;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.loading.BroadcastJoinableMMappedQueryableSegmentizerFactory;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexer.AbstractIndexerTest;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/**
 * Test for broadcast join with segments created using
 * {@link BroadcastJoinableMMappedQueryableSegmentizerFactory}.
 */
public class BroadcastJoinQueryTest extends AbstractIndexerTest
{
  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    broker.addBeforeStartHook((c, self) -> {
      self.addProperty(
          "druid.segmentCache.locations",
          StringUtils.format(
              "[{\"path\":\"%s\",\"maxSize\":\"%s\"}]",
              c.getTestFolder().newFolder().getAbsolutePath(),
              HumanReadableBytes.parse("100M")
          )
      );
    });
  }

  @Test
  public void testJoin()
  {
    ingestDataIntoBaseDatasource();

    // Set broadcast rules for the indexed-table datasource
    final String joinDatasource = EmbeddedClusterApis.createTestDatasourceName();
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateRulesForDatasource(
            joinDatasource,
            List.of(new ForeverBroadcastDistributionRule())
        )
    );

    // Ingest a single segment into the indexed-table datasource
    final Task task2 = MoreResources.Task.INDEX_TASK_WITH_AGGREGATORS
        .get()
        .tuningConfig(
            t -> t.withIndexSpec(
                IndexSpec.builder().withSegmentLoader(
                    new BroadcastJoinableMMappedQueryableSegmentizerFactory(
                        TestIndex.INDEX_IO,
                        Set.of("user", "language", "added", "deleted")
                    )
                ).build())
        )
        .dynamicPartitionWithMaxRows(100)
        .granularitySpec("YEAR", "SECOND", true)
        .dataSource(joinDatasource)
        .withId(IdUtils.getRandomId());
    cluster.callApi().runTask(task2, overlord);

    // Wait for the broadcast segment to be loaded on the historical and the broker
    coordinator.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension(DruidMetrics.DATASOURCE, joinDatasource),
        agg -> agg.hasCountAtLeast(2)
    );

    // Run some metadata queries
    cluster.callApi().waitForAllSegmentsToBeAvailable(joinDatasource, coordinator, broker);
    cluster.callApi().verifySqlQuery(
        "SELECT IS_JOINABLE, IS_BROADCAST FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'",
        joinDatasource,
        "YES,YES"
    );
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'",
        joinDatasource,
        "21"
    );

    // Run some queries
    Assertions.assertEquals(
        "triplets,2,1810.0\n" +
        "speed,2,918.0\n" +
        "masterYi,2,246.0\n" +
        "nuclear,2,114.0\n" +
        "stringer,2,2.0",
        cluster.runSql("SELECT \"user\", COUNT(*), SUM(\"added\") FROM %s GROUP BY 1 ORDER BY 3 DESC", joinDatasource)
    );
    Assertions.assertEquals(
        "triplets,2,1810.0\n" +
        "speed,2,918.0\n" +
        "masterYi,2,246.0\n" +
        "nuclear,2,114.0\n" +
        "stringer,2,2.0",
        cluster.runSql("SELECT \"user\", COUNT(*), SUM(\"added\") FROM %s GROUP BY 1 ORDER BY 3 DESC", dataSource)
    );

    // Run join query, the sums have doubled due to the Cartesian product
    Assertions.assertEquals(
        "triplets,4,3620.0\n" +
        "speed,4,1836.0\n" +
        "masterYi,4,492.0\n" +
        "nuclear,4,228.0\n" +
        "stringer,4,4.0",
        cluster.runSql(
            "SELECT a.\"user\", COUNT(*), SUM(a.\"added\")" +
            " FROM %s a INNER JOIN %s b ON a.\"user\" = b.\"user\"" +
            " GROUP BY 1 ORDER BY 3 DESC",
            dataSource, joinDatasource
        )
    );
  }

  private void ingestDataIntoBaseDatasource()
  {
    final Task task1 = MoreResources.Task.INDEX_TASK_WITH_AGGREGATORS
        .get()
        .dataSource(dataSource)
        .withId(IdUtils.getRandomId());
    cluster.callApi().runTask(task1, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }
}
