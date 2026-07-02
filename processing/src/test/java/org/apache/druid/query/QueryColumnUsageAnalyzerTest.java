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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryColumnUsageAnalyzer.ColumnUsage;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class QueryColumnUsageAnalyzerTest
{
  @Test
  public void testScan()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("events")
        .intervals(everyInterval())
        .columns("userId", "page")
        .filters(new SelectorDimFilter("country", "US", null))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("events");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), cols.get("userId"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), cols.get("page"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.FILTER), cols.get("country"));
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("total", "amount"))
        .setDimFilter(new SelectorDimFilter("status", "active", null))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), cols.get("country"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.AGGREGATION), cols.get("amount"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.FILTER), cols.get("status"));
  }

  @Test
  public void testTopN()
  {
    Query<?> query = new TopNQueryBuilder()
        .dataSource("sales")
        .intervals(everyInterval())
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("country", "country"))
        .metric("total")
        .threshold(10)
        .aggregators(new LongSumAggregatorFactory("total", "amount"))
        .filters(new SelectorDimFilter("status", "active", null))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), cols.get("country"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.AGGREGATION), cols.get("amount"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.FILTER), cols.get("status"));
  }

  @Test
  public void testTimeseries()
  {
    Query<?> query = Druids.newTimeseriesQueryBuilder()
        .dataSource("sales")
        .intervals(everyInterval())
        .granularity(Granularities.ALL)
        .aggregators(new LongSumAggregatorFactory("total", "amount"))
        .filters(new SelectorDimFilter("status", "active", null))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.AGGREGATION), cols.get("amount"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.FILTER), cols.get("status"));
  }

  @Test
  public void testVirtualColumnExpandsToBaseColumns()
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setVirtualColumns(VirtualColumns.create(ImmutableList.of(
            new ExpressionVirtualColumn("v0", "\"base\" * 2", ColumnType.LONG, ExprMacroTable.nil())
        )))
        .setDimensions(new DefaultDimensionSpec("v0", "v0"))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(Collections.singleton("base"), cols.keySet());
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), cols.get("base"));
  }

  @Test
  public void testColumnUsedInMultipleRoles()
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setDimFilter(new SelectorDimFilter("country", "US", null))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY, ColumnUsage.FILTER), cols.get("country"));
  }

  @Test
  public void testJoinAttributedPerSide()
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("sales"),
        new TableDataSource("users"),
        "j0.",
        "\"country\" == \"j0.country\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(join)
        .intervals(everyInterval())
        .columns("country", "j0.age")
        .build();
    Map<String, Map<String, EnumSet<ColumnUsage>>> result = QueryColumnUsageAnalyzer.analyze(query);
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION, ColumnUsage.JOIN), result.get("sales").get("country"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), result.get("users").get("age"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.JOIN), result.get("users").get("country"));
  }

  @Test
  public void testJoinBaseTableFilter()
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("sales"),
        new TableDataSource("users"),
        "j0.",
        "\"country\" == \"j0.country\"",
        JoinType.INNER,
        new SelectorDimFilter("status", "active", null),
        ExprMacroTable.nil(),
        null,
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(join)
        .intervals(everyInterval())
        .columns("country")
        .build();
    Map<String, EnumSet<ColumnUsage>> sales = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.FILTER), sales.get("status"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION, ColumnUsage.JOIN), sales.get("country"));
  }

  @Test
  public void testLookupJoinDropsRightColumns()
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("sales"),
        new LookupDataSource("country_lookup"),
        "j0.",
        "\"country\" == \"j0.k\"",
        JoinType.LEFT,
        null,
        ExprMacroTable.nil(),
        null,
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(join)
        .intervals(everyInterval())
        .columns("country", "j0.v")
        .build();
    Map<String, Map<String, EnumSet<ColumnUsage>>> result = QueryColumnUsageAnalyzer.analyze(query);
    // Lookup has no base table -> no lookup entry, and "j0.v"/"j0.k" are not fabricated onto "sales".
    Assertions.assertEquals(Collections.singleton("sales"), result.keySet());
    Assertions.assertEquals(Collections.singleton("country"), result.get("sales").keySet());
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION, ColumnUsage.JOIN), result.get("sales").get("country"));
  }

  @Test
  public void testSubQueryRecursesToBaseTable()
  {
    GroupByQuery inner = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("total", "amount"))
        .build();
    GroupByQuery outer = GroupByQuery.builder()
        .setDataSource(new QueryDataSource(inner))
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("grand", "total"))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(outer).get("sales");
    // Base columns come from the sub-query; the outer references to sub-query outputs are not fabricated.
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), cols.get("country"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.AGGREGATION), cols.get("amount"));
    Assertions.assertFalse(cols.containsKey("total"));
    Assertions.assertFalse(cols.containsKey("grand"));
  }

  @Test
  public void testUnionReplicatesToMembers()
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource(new UnionDataSource(ImmutableList.of(
            new TableDataSource("sales_a"),
            new TableDataSource("sales_b")
        )))
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .build();
    Map<String, Map<String, EnumSet<ColumnUsage>>> result = QueryColumnUsageAnalyzer.analyze(query);
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), result.get("sales_a").get("country"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), result.get("sales_b").get("country"));
  }

  @Test
  public void testFilteredAggregatorSplitsRoles()
  {
    Query<?> query = Druids.newTimeseriesQueryBuilder()
        .dataSource("sales")
        .intervals(everyInterval())
        .granularity(Granularities.ALL)
        .aggregators(new FilteredAggregatorFactory(
            new LongSumAggregatorFactory("s", "added"),
            new SelectorDimFilter("status", "active", null)
        ))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.AGGREGATION), cols.get("added"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.FILTER), cols.get("status"));
  }

  @Test
  public void testFilteredDataSourceCapturesFilterColumns()
  {
    FilteredDataSource filtered = FilteredDataSource.create(
        new TableDataSource("sales"),
        new SelectorDimFilter("status", "active", null)
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(filtered)
        .intervals(everyInterval())
        .columns("country")
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), cols.get("country"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.FILTER), cols.get("status"));
  }

  @Test
  public void testSelectStarYieldsEmpty()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("events")
        .intervals(everyInterval())
        .build();
    Assertions.assertTrue(QueryColumnUsageAnalyzer.analyze(query).isEmpty());
  }

  @Test
  public void testUnnestReplacesSyntheticColumnWithSource()
  {
    UnnestDataSource unnest = UnnestDataSource.create(
        new TableDataSource("sales"),
        new ExpressionVirtualColumn("unnestOut", "\"tags\"", ColumnType.STRING, ExprMacroTable.nil()),
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(unnest)
        .intervals(everyInterval())
        .columns("unnestOut")
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    // The synthetic unnest output is dropped; the underlying source column is projected instead.
    Assertions.assertEquals(Collections.singleton("tags"), cols.keySet());
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), cols.get("tags"));
  }

  @Test
  public void testNestedJoinAttributesSecondPrefix()
  {
    JoinDataSource inner = JoinDataSource.create(
        new TableDataSource("sales_a"),
        new TableDataSource("sales_b"),
        "j0.",
        "\"a_id\" == \"j0.b_id\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        null
    );
    JoinDataSource outer = JoinDataSource.create(
        inner,
        new TableDataSource("sales_c"),
        "_j0.",
        "\"a_id\" == \"_j0.c_id\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(outer)
        .intervals(everyInterval())
        .columns("a_col", "j0.b_col", "_j0.c_col")
        .build();
    Map<String, Map<String, EnumSet<ColumnUsage>>> result = QueryColumnUsageAnalyzer.analyze(query);
    // "_j0." (longer) must win over "j0." so the second-join column lands on sales_c, not sales_b.
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), result.get("sales_c").get("c_col"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), result.get("sales_b").get("b_col"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.JOIN), result.get("sales_c").get("c_id"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.JOIN), result.get("sales_b").get("b_id"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.PROJECTION), result.get("sales_a").get("a_col"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.JOIN), result.get("sales_a").get("a_id"));
  }

  @Test
  public void testJoinKeyAlsoAggregatedMergesRoles()
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("sales"),
        new TableDataSource("users"),
        "j0.",
        "\"amount\" == \"j0.amount\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        null
    );
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource(join)
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("total", "j0.amount"))
        .build();
    // The right-side "amount" is both a join key and an aggregation input: roles must merge, not clobber.
    Assertions.assertEquals(
        EnumSet.of(ColumnUsage.AGGREGATION, ColumnUsage.JOIN),
        QueryColumnUsageAnalyzer.analyze(query).get("users").get("amount")
    );
  }

  @Test
  public void testRestrictedDataSourceAttributesToBase()
  {
    RestrictedDataSource restricted = RestrictedDataSource.create(
        new TableDataSource("sales"),
        NoRestrictionPolicy.instance()
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(restricted)
        .intervals(everyInterval())
        .columns("country")
        .build();
    Assertions.assertEquals(
        EnumSet.of(ColumnUsage.PROJECTION),
        QueryColumnUsageAnalyzer.analyze(query).get("sales").get("country")
    );
  }

  @Test
  public void testChainedVirtualColumnsExpandTransitively()
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setVirtualColumns(VirtualColumns.create(ImmutableList.of(
            new ExpressionVirtualColumn("v0", "\"base\" * 2", ColumnType.LONG, ExprMacroTable.nil()),
            new ExpressionVirtualColumn("v1", "\"v0\" + 1", ColumnType.LONG, ExprMacroTable.nil())
        )))
        .setDimensions(new DefaultDimensionSpec("v1", "v1"))
        .build();
    Map<String, EnumSet<ColumnUsage>> cols = QueryColumnUsageAnalyzer.analyze(query).get("sales");
    // v1 -> v0 -> base: only the real base column survives, carrying the consuming role.
    Assertions.assertEquals(Collections.singleton("base"), cols.keySet());
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), cols.get("base"));
  }

  @Test
  public void testUnionQueryRecursesEachBranch()
  {
    GroupByQuery branchA = GroupByQuery.builder()
        .setDataSource("sales_a")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .build();
    GroupByQuery branchB = GroupByQuery.builder()
        .setDataSource("sales_b")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setAggregatorSpecs(new LongSumAggregatorFactory("total", "amount"))
        .build();
    UnionQuery union = new UnionQuery(List.of(branchA, branchB));
    Map<String, Map<String, EnumSet<ColumnUsage>>> result = QueryColumnUsageAnalyzer.analyze(union);
    // Each branch's own column-bearing parts must be captured (not discarded via getDataSources()).
    Assertions.assertEquals(EnumSet.of(ColumnUsage.GROUP_BY), result.get("sales_a").get("country"));
    Assertions.assertEquals(EnumSet.of(ColumnUsage.AGGREGATION), result.get("sales_b").get("amount"));
  }

  @Test
  public void testInlineDataSourceDropped()
  {
    RowSignature signature = RowSignature.builder()
        .add("id", ColumnType.LONG)
        .add("name", ColumnType.STRING)
        .build();
    InlineDataSource inline = InlineDataSource.fromIterable(
        ImmutableList.of(new Object[]{1L, "alice"}),
        signature
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(inline)
        .intervals(everyInterval())
        .columns("name")
        .build();
    // Inline data has no base table; it is dropped, never fabricated as a pseudo-table.
    Assertions.assertTrue(QueryColumnUsageAnalyzer.analyze(query).isEmpty());
  }

  @Test
  public void testUnsupportedQueryTypeYieldsEmpty()
  {
    Query<?> query = Druids.newTimeBoundaryQueryBuilder().dataSource("events").build();
    Map<String, Map<String, EnumSet<ColumnUsage>>> result = QueryColumnUsageAnalyzer.analyze(query);
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.isEmpty());
  }

  private static QuerySegmentSpec everyInterval()
  {
    return new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2024-01-01/2024-01-02")));
  }
}
