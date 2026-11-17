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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.dart.controller.DartControllerContextFactory;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.QueryTestRunner;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.TestTimelineServerView;
import org.apache.druid.sql.calcite.util.datasets.TestDataSet;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Tests that a GROUP BY without ORDER BY hash-partitions its partial aggregates when
 * {@link QueryContextParameters#MAX_NON_LEAF_WORKERS} is above 1. Runs Dart with several historicals, so the final
 * aggregation can run on several workers.
 */
@SqlTestFrameworkConfig.ComponentSupplier(DartGroupByHashPartitioningTest.MultiHistoricalDartComponentSupplier.class)
public class DartGroupByHashPartitioningTest extends BaseCalciteQueryTest
{
  private static final String DATASOURCE = "numbers_multi";
  private static final int NUM_SEGMENTS = 8;
  private static final int NUM_HISTORICALS = 4;
  private static final int MAX_NON_LEAF_WORKERS = 4;
  private static final int TARGET_PARTITIONS_PER_WORKER = 2;
  private static final String GROUP_BY =
      "SELECT label, \"number\", SUM(cnt) AS c FROM " + DATASOURCE + " GROUP BY label, \"number\"";

  public static class MultiHistoricalDartComponentSupplier extends DartComponentSupplier
  {
    public MultiHistoricalDartComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(
        final SpecificSegmentsQuerySegmentWalker walker,
        final ObjectMapper jsonMapper
    )
    {
      // The same rows in each segment, so every group has partial aggregates from every segment.
      for (int i = 0; i < NUM_SEGMENTS; i++) {
        final QueryableIndex index =
            TestDataSet.NUMBERS.makeIndex(jsonMapper, getTempDirProducer().newTempFolder("tmp_numbers_multi_" + i));
        walker.add(
            DataSegment.builder(SegmentId.of(DATASOURCE, index.getDataInterval(), "1", i))
                       .shardSpec(new LinearShardSpec(i))
                       .size(0)
                       .build(),
            index
        );
      }
      return super.addSegmentsToWalker(walker, jsonMapper);
    }

    @Override
    public DruidModule getOverrideModule()
    {
      return DruidModuleCollection.of(super.getOverrideModule(), new MultiHistoricalModule());
    }
  }

  static class MultiHistoricalModule implements DruidModule
  {
    @Provides
    @LazySingleton
    TimelineServerView makeTimelineServerView(final SpecificSegmentsQuerySegmentWalker walker)
    {
      return new MultiHistoricalTimelineServerView(walker.getSegments());
    }

    @Override
    public void configure(Binder binder)
    {
    }
  }

  /**
   * Advertises {@link #NUM_HISTORICALS} historicals. Test segments have no server, so Dart assigns them round-robin.
   */
  static class MultiHistoricalTimelineServerView extends TestTimelineServerView
  {
    MultiHistoricalTimelineServerView(final List<DataSegment> segments)
    {
      super(segments);
    }

    @Override
    public List<ImmutableDruidServer> getDruidServers()
    {
      final List<ImmutableDruidServer> servers = new ArrayList<>();
      for (int i = 0; i < NUM_HISTORICALS; i++) {
        final DruidServerMetadata metadata = new DruidServerMetadata(
            "historical" + i,
            "historical" + i + ":8083",
            null,
            0L,
            null,
            ServerType.HISTORICAL,
            "_default_tier",
            0
        );
        servers.add(new ImmutableDruidServer(metadata, 0L, ImmutableMap.of(), 0));
      }
      return servers;
    }
  }

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  @Test
  public void testOneNonLeafWorkerKeepsOnePartition()
  {
    run(GROUP_BY, 1);

    final List<MSQStagesReport.Stage> stages = lastQueryStages();
    Assertions.assertEquals(2, stages.size());
    Assertions.assertEquals(ShuffleKind.GLOBAL_SORT, stages.get(0).getStageDefinition().getShuffleSpec().kind());
    Assertions.assertEquals(1, stages.get(0).getPartitionCount());
    Assertions.assertEquals(1, stages.get(1).getWorkerCount());
  }

  @Test
  public void testNoOrderBy()
  {
    final List<List<Object>> expected = run(GROUP_BY, 1);
    final List<List<Object>> actual = run(GROUP_BY, MAX_NON_LEAF_WORKERS);
    Assertions.assertEquals(expected.size(), actual.size());
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));

    final List<MSQStagesReport.Stage> stages = lastQueryStages();
    Assertions.assertEquals(2, stages.size());
    assertHashPartitioned(stages.get(0));
    Assertions.assertEquals(MAX_NON_LEAF_WORKERS, stages.get(1).getWorkerCount());
    // The final stage keeps the hash partitions; nothing sorts them into one.
    Assertions.assertFalse(stages.get(1).getStageDefinition().doesShuffle());
  }

  @Test
  public void testOrderByKeepsRangePartitioning()
  {
    final String sql = GROUP_BY + " ORDER BY \"number\", label";
    final List<List<Object>> expected = run(sql, 1);
    final List<List<Object>> actual = run(sql, MAX_NON_LEAF_WORKERS);
    Assertions.assertEquals(expected, actual);

    final StageDefinition stageDef = lastQueryStages().get(0).getStageDefinition();
    Assertions.assertEquals(ShuffleKind.GLOBAL_SORT, stageDef.getShuffleSpec().kind());
    Assertions.assertTrue(stageDef.mustGatherResultKeyStatistics());
  }

  @Test
  public void testOrderByMatchingGroupByKeepsOnePartition()
  {
    // This plan relies on the shuffle's key order instead of sorting again, so it must not use hash partitions.
    assertOnePartition(GROUP_BY + " ORDER BY label, \"number\"");
  }

  @Test
  public void testLimitWithoutOrderByKeepsOnePartition()
  {
    assertOnePartition(GROUP_BY + " LIMIT 3");
  }

  @Test
  public void testSubqueryKeepsRangePartitioning()
  {
    final String sql = "SELECT label, COUNT(*) AS n FROM (" + GROUP_BY + ") GROUP BY label";
    final List<List<Object>> expected = run(sql, 1);
    final List<List<Object>> actual = run(sql, MAX_NON_LEAF_WORKERS);
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));

    // Stages 0 and 1 are the subquery; stages 2 and 3 are the outer query.
    final List<MSQStagesReport.Stage> stages = lastQueryStages();
    Assertions.assertEquals(4, stages.size());
    Assertions.assertEquals(ShuffleKind.GLOBAL_SORT, stages.get(0).getStageDefinition().getShuffleSpec().kind());
    assertHashPartitioned(stages.get(2));
  }

  /**
   * Checks that, with several non-leaf workers, the query still sends its partial aggregates to one partition and
   * returns the same rows in the same order as with one.
   */
  private void assertOnePartition(final String sql)
  {
    final List<List<Object>> expected = run(sql, 1);
    final List<List<Object>> actual = run(sql, MAX_NON_LEAF_WORKERS);
    Assertions.assertEquals(expected, actual);

    final MSQStagesReport.Stage stage = lastQueryStages().get(0);
    Assertions.assertEquals(ShuffleKind.GLOBAL_SORT, stage.getStageDefinition().getShuffleSpec().kind());
    Assertions.assertEquals(1, stage.getPartitionCount());
  }

  private static void assertHashPartitioned(final MSQStagesReport.Stage stage)
  {
    final StageDefinition stageDef = stage.getStageDefinition();
    Assertions.assertEquals(ShuffleKind.HASH_LOCAL_SORT, stageDef.getShuffleSpec().kind());
    Assertions.assertFalse(stageDef.mustGatherResultKeyStatistics());
    Assertions.assertEquals(MAX_NON_LEAF_WORKERS * TARGET_PARTITIONS_PER_WORKER, stage.getPartitionCount());
  }

  private List<List<Object>> run(final String sql, final int maxNonLeafWorkers)
  {
    final Map<String, Object> context = new HashMap<>();
    context.put(QueryContextParameters.PLANNER_STRATEGY.getName(), QueryContexts.PlannerStrategy.COUPLED.name());
    context.put(QueryContextParameters.MAX_NON_LEAF_WORKERS.getName(), maxNonLeafWorkers);
    context.put(QueryContextParameters.TARGET_PARTITIONS_PER_WORKER.getName(), TARGET_PARTITIONS_PER_WORKER);
    context.put(QueryContextParameters.USE_COMBINER.getName(), true);

    final QueryTestRunner.QueryResults results = testBuilder().sql(sql).queryContext(context).results();
    if (results.exception != null) {
      throw results.exception;
    }

    final List<List<Object>> rows = new ArrayList<>();
    for (final Object[] row : results.results) {
      rows.add(Arrays.asList(row));
    }
    return rows;
  }

  private List<MSQStagesReport.Stage> lastQueryStages()
  {
    final TestDartControllerContextFactoryImpl factory = (TestDartControllerContextFactoryImpl)
        queryFramework().injector().getInstance(DartControllerContextFactory.class);
    final MSQTaskReport report =
        factory.controller.liveReports().<MSQTaskReport>findReport(MSQTaskReport.REPORT_KEY).orElseThrow();
    return report.getPayload().getStages().getStages();
  }
}
