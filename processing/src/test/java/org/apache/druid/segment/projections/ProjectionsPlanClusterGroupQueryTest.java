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

package org.apache.druid.segment.projections;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.NotFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Coverage for {@link Projections#planClusterGroupQuery}, exercised through both facets of the returned
 * {@link ClusterGroupQueryPlan}: {@code survivingGroups()} (which input groups the query's filter can't rule out
 * via the clustering tuple) and {@code rewriteFor(group)} (the per-group rewritten filter, with clustering-column
 * leaves folded to {@link TrueFilter} / {@link FalseFilter} and propagated through AND / OR / NOT).
 */
class ProjectionsPlanClusterGroupQueryTest
{
  private static TableClusterGroupSpec stringGroup(String tenant)
  {
    final RowSignature clustering = RowSignature.builder().add("tenant", ColumnType.STRING).build();
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        clustering,
        List.of(Arrays.asList(tenant))   // Arrays.asList allows tenant == null
    );
    new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of("tenant", ColumnHolder.TIME_COLUMN_NAME, "metric"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(OrderBy.ascending("tenant"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
    return built.specs().get(0);
  }

  private static TableClusterGroupSpec longGroup(long priority)
  {
    final RowSignature clustering = RowSignature.builder().add("priority", ColumnType.LONG).build();
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        clustering,
        List.of(List.of(priority))
    );
    new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of("priority", ColumnHolder.TIME_COLUMN_NAME, "metric"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(OrderBy.ascending("priority"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
    return built.specs().get(0);
  }

  private static TableClusterGroupSpec multiGroup(String tenant, String region)
  {
    final RowSignature clustering = RowSignature.builder()
                                                .add("tenant", ColumnType.STRING)
                                                .add("region", ColumnType.STRING)
                                                .build();
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        clustering,
        List.of(Arrays.asList(tenant, region))
    );
    new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of("tenant", "region", ColumnHolder.TIME_COLUMN_NAME, "metric"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(
            OrderBy.ascending("tenant"),
            OrderBy.ascending("region"),
            OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)
        ),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
    return built.specs().get(0);
  }

  private static TableClusterGroupSpec tenantRegionGroup(String tenant, String region)
  {
    return multiGroup(tenant, region);
  }

  private static TableClusterGroupSpec deviceRegionGroup(long deviceId, String region)
  {
    final RowSignature clustering = RowSignature.builder()
                                                .add("device_id", ColumnType.LONG)
                                                .add("region", ColumnType.STRING)
                                                .build();
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        clustering,
        List.of(Arrays.asList(deviceId, region))
    );
    new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of("device_id", "region", ColumnHolder.TIME_COLUMN_NAME, "temperature"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(
            OrderBy.ascending("device_id"),
            OrderBy.ascending("region"),
            OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)
        ),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
    return built.specs().get(0);
  }

  private static TableClusterGroupSpec partitionGroup(String key)
  {
    final RowSignature clustering = RowSignature.builder().add("partition", ColumnType.STRING).build();
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        clustering,
        List.of(Arrays.asList(key))
    );
    new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of("partition", ColumnHolder.TIME_COLUMN_NAME, "payload"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(OrderBy.ascending("partition"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
    return built.specs().get(0);
  }

  private static TableClusterGroupSpec virtualClusteringGroup(String loweredTenant)
  {
    final RowSignature clustering = RowSignature.builder().add("tenant_lower", ColumnType.STRING).build();
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        clustering,
        List.of(Arrays.asList(loweredTenant))
    );
    new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.create(
            new ExpressionVirtualColumn(
                "tenant_lower",
                "lower(tenant)",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        ),
        List.of("tenant_lower", ColumnHolder.TIME_COLUMN_NAME, "metric"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        List.of(OrderBy.ascending("tenant_lower"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        clustering,
        null,
        built.dictionaries(),
        built.specs()
    );
    return built.specs().get(0);
  }

  private static CursorBuildSpec buildSpec(@Nullable Filter filter)
  {
    return buildSpec(filter, VirtualColumns.EMPTY);
  }

  private static CursorBuildSpec buildSpec(@Nullable Filter filter, VirtualColumns virtualColumns)
  {
    return CursorBuildSpec.builder().setFilter(filter).setVirtualColumns(virtualColumns).build();
  }

  private static List<TableClusterGroupSpec> pruneClusterGroups(
      List<TableClusterGroupSpec> groups,
      @Nullable Filter filter
  )
  {
    return Projections.planClusterGroupQuery(groups, buildSpec(filter)).survivingGroups();
  }

  private static Filter rewrite(Filter filter, TableClusterGroupSpec group)
  {
    return Projections.planClusterGroupQuery(List.of(group), buildSpec(filter)).rewriteFor(group);
  }

  private static List<TableClusterGroupSpec> groups(TableClusterGroupSpec... gs)
  {
    return ImmutableList.copyOf(gs);
  }

  private static LinkedHashSet<Filter> filters(Filter... fs)
  {
    LinkedHashSet<Filter> out = new LinkedHashSet<>();
    for (Filter f : fs) {
      out.add(f);
    }
    return out;
  }

  @Test
  void testNullFilterReturnsAllGroups()
  {
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Assertions.assertSame(all, pruneClusterGroups(all, null));
  }

  @Test
  void testEmptyGroupsReturnsAllGroups()
  {
    List<TableClusterGroupSpec> empty = List.of();
    Assertions.assertSame(empty, pruneClusterGroups(empty, new EqualityFilter("tenant", ColumnType.STRING, "acme", null)));
  }

  @Test
  void testEqualityFilterOnStringClusteringColumnSelectsOneGroup()
  {
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new EqualityFilter("tenant", ColumnType.STRING, "acme", null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
    // Surviving group's rewrite collapses the clustering leaf to TRUE.
    Assertions.assertSame(TrueFilter.instance(), rewrite(f, all.get(0)));
  }

  @Test
  void testEqualityFilterOnLongClusteringColumn()
  {
    List<TableClusterGroupSpec> all = groups(longGroup(5L), longGroup(10L), longGroup(20L));
    Filter f = new EqualityFilter("priority", ColumnType.LONG, 10L, null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(1), kept.get(0));
  }

  @Test
  void testTypedInFilterOnLongClusteringColumn()
  {
    List<TableClusterGroupSpec> all = groups(longGroup(5L), longGroup(10L), longGroup(20L));
    Filter f = new TypedInFilter("priority", ColumnType.LONG, List.of(5L, 20L), null, null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(2, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
    Assertions.assertSame(all.get(2), kept.get(1));
  }

  @Test
  void testFilterOnNonClusteringColumnKeepsAllGroups()
  {
    // Filter doesn't reference a clustering column → can't prune anything, leaf is preserved verbatim.
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new EqualityFilter("metric", ColumnType.LONG, 42L, null);
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
    Assertions.assertSame(f, rewrite(f, all.get(0)));
  }

  @Test
  void testAndFilterIntersectsSubFilterResults()
  {
    List<TableClusterGroupSpec> all = groups(
        multiGroup("acme", "us-east-1"),
        multiGroup("acme", "us-west-2"),
        multiGroup("globex", "us-east-1")
    );
    Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("region", ColumnType.STRING, "us-west-2", null)
    ));
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(1), kept.get(0));
  }

  @Test
  void testOrFilterUnionsSubFilterResults()
  {
    List<TableClusterGroupSpec> all = groups(
        stringGroup("acme"),
        stringGroup("globex"),
        stringGroup("oscorp")
    );
    Filter f = new OrFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("tenant", ColumnType.STRING, "oscorp", null)
    ));
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(2, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
    Assertions.assertSame(all.get(2), kept.get(1));
  }

  @Test
  void testNotFilterInvertsMatch()
  {
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new NotFilter(new EqualityFilter("tenant", ColumnType.STRING, "acme", null));
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(1), kept.get(0));
    // NOT inverts the per-leaf rewrite: pruned group rewrites to FalseFilter, surviving group to TrueFilter.
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, all.get(0)));
    Assertions.assertSame(TrueFilter.instance(), rewrite(f, all.get(1)));
  }

  @Test
  void testNotOverNonClusteringColumnKeepsAllGroups()
  {
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new NotFilter(new EqualityFilter("metric", ColumnType.LONG, 42L, null));
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
    // Non-clustering NOT is structurally preserved in the per-group rewrite.
    Assertions.assertEquals(f, rewrite(f, all.get(0)));
  }

  @Test
  void testDoubleNotPreservesPositive()
  {
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new NotFilter(new NotFilter(new EqualityFilter("tenant", ColumnType.STRING, "acme", null)));
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
  }

  @Test
  void testAndOfMustNotMatchAndUnknownPrunes()
  {
    // tenant=globex is MUST_NOT_MATCH for tenant=acme group; AND with anything stays MUST_NOT_MATCH → prune.
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"));
    Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "globex", null),   // MUST_NOT_MATCH for acme
        new EqualityFilter("metric", ColumnType.LONG, 42L, null)           // UNKNOWN
    ));
    Assertions.assertTrue(pruneClusterGroups(all, f).isEmpty());
  }

  @Test
  void testOrOfMustMatchAndUnknownKeeps()
  {
    // tenant=acme is MUST_MATCH for tenant=acme group; OR short-circuits to MUST_MATCH regardless of the other.
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"));
    Filter f = new OrFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),     // MUST_MATCH
        new EqualityFilter("metric", ColumnType.LONG, 42L, null)           // UNKNOWN
    ));
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
  }

  @Test
  void testOrOfMustNotMatchAndUnknownKeeps()
  {
    // tenant=globex is MUST_NOT_MATCH for tenant=acme group; metric=42 is UNKNOWN.
    // OR(MUST_NOT_MATCH, UNKNOWN) = UNKNOWN → keep (rows with metric=42 might exist).
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"));
    Filter f = new OrFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "globex", null),
        new EqualityFilter("metric", ColumnType.LONG, 42L, null)
    ));
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
  }

  @Test
  void testNotOfAndWithMustNotMatchKeeps()
  {
    // inner: AND(tenant=globex on tenant=acme group, otherCol=foo)
    //   AND(MUST_NOT_MATCH, UNKNOWN) = MUST_NOT_MATCH (no row has tenant=globex, so AND is always false)
    // NOT(MUST_NOT_MATCH) = MUST_MATCH → keep (NOT(false) is true for every row).
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"));
    Filter f = new NotFilter(new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "globex", null),
        new EqualityFilter("metric", ColumnType.LONG, 42L, null)
    )));
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
  }

  @Test
  void testNotOfAndWithUnknownKeeps()
  {
    // inner: AND(tenant=acme, otherCol=foo) on tenant=acme group
    //   AND(MUST_MATCH, UNKNOWN) = UNKNOWN
    // NOT(UNKNOWN) = UNKNOWN → keep.
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"));
    Filter f = new NotFilter(new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("metric", ColumnType.LONG, 42L, null)
    )));
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
  }

  @Test
  void testMixedClusteringAndNonClusteringWithAndFilter()
  {
    // AND combines a clustering filter with a non-clustering filter; the latter contributes no info → result is
    // determined purely by the clustering filter.
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("metric", ColumnType.LONG, 42L, null)
    ));
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
  }

  @Test
  void testEmptyResultWhenNoGroupMatches()
  {
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new EqualityFilter("tenant", ColumnType.STRING, "unknown", null);
    Assertions.assertTrue(pruneClusterGroups(all, f).isEmpty());
    // Pruned groups' rewrite collapses the clustering leaf to FALSE.
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, all.get(0)));
  }

  @Test
  void testUnsupportedFilterTypeIsConservative()
  {
    // LikeDimFilter isn't recognized by the pruner → keep all groups (no pruning info).
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    Filter f = new LikeDimFilter("tenant", "acm%", null, null).toFilter();
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
  }

  @Test
  void testVirtualClusteringMatchesQueryVirtualColumnByEquivalence()
  {
    List<TableClusterGroupSpec> all = groups(
        virtualClusteringGroup("acme"),
        virtualClusteringGroup("globex")
    );
    // Query has its own virtual column with a different output name but equivalent expression.
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "query_lower",
            "lower(tenant)",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        )
    );
    Filter f = new EqualityFilter("query_lower", ColumnType.STRING, "acme", null);
    List<TableClusterGroupSpec> kept = Projections.planClusterGroupQuery(all, buildSpec(f, queryVcs)).survivingGroups();
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
  }

  @Test
  void testVirtualClusteringRejectsNonEquivalentQueryVirtualColumn()
  {
    List<TableClusterGroupSpec> all = groups(virtualClusteringGroup("acme"));
    // Query VC is upper(tenant), not lower(tenant); not equivalent.
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "query_upper",
            "upper(tenant)",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        )
    );
    Filter f = new EqualityFilter("query_upper", ColumnType.STRING, "ACME", null);
    // No equivalence found → can't prune; keep group conservatively.
    Assertions.assertEquals(all, Projections.planClusterGroupQuery(all, buildSpec(f, queryVcs)).survivingGroups());
  }

  @Test
  void testVirtualClusteringWithoutQueryVirtualColumnsIsConservative()
  {
    // If queryVirtualColumns is empty (or not provided), the pruner can't resolve the filter column to a
    // clustering column via virtual-column equivalence → keep all groups.
    List<TableClusterGroupSpec> all = groups(virtualClusteringGroup("acme"), virtualClusteringGroup("globex"));
    Filter f = new EqualityFilter("query_lower", ColumnType.STRING, "acme", null);
    Assertions.assertEquals(all, Projections.planClusterGroupQuery(all, buildSpec(f)).survivingGroups());
    // Same with the no-virtual-columns convenience overload.
    Assertions.assertEquals(all, pruneClusterGroups(all, f));
  }

  @Test
  void testDirectMatchWorksWithUnrelatedQueryVirtualColumns()
  {
    // Query VC by a name unrelated to any clustering column → query-VC check finds nothing for "tenant" → falls
    // through to the direct-name path against the clustering signature. Unrelated VCs don't perturb the match.
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "ts_floor",
            "timestamp_floor(__time, 'P1D', null, null)",
            ColumnType.LONG,
            TestExprMacroTable.INSTANCE
        )
    );
    Filter f = new EqualityFilter("tenant", ColumnType.STRING, "acme", null);
    List<TableClusterGroupSpec> kept = Projections.planClusterGroupQuery(all, buildSpec(f, queryVcs)).survivingGroups();
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
  }

  @Test
  void testQueryVirtualColumnShadowingClusteringNameWithoutEquivalenceIsConservative()
  {
    // Query has a VC named "tenant" that is NOT equivalent to the clustering column "tenant" (different
    // expression, totally different semantics). Once the query defines a VC by that name, the filter's "tenant"
    // resolves to the VC's value, NOT the clustering column. The pruner must not mistake the VC reference for a
    // clustering-column reference and prune on the clustering tuple — that would silently prune live rows whose
    // VC value happens to equal "acme" even though the clustering tenant is "globex". Without the query-VC-first
    // check this regressed and pruned the globex group.
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"), stringGroup("globex"));
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "tenant",                       // shadows the clustering column name
            "lower(other)",                 // unrelated expression
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        )
    );
    Filter f = new EqualityFilter("tenant", ColumnType.STRING, "acme", null);
    Assertions.assertEquals(all, Projections.planClusterGroupQuery(all, buildSpec(f, queryVcs)).survivingGroups());
  }

  @Test
  void testQueryVirtualColumnShadowingClusteringNameWithSameNameEquivalencePrunes()
  {
    // Group clusters on "tenant_lower" via a group VC of the same name (lower(tenant)). The query also defines
    // a VC named "tenant_lower" with the identical expression — names collide AND the VCs are equivalent. The
    // pruner should treat the filter as a clustering-column reference and prune normally (same-name target →
    // no remap entry needed; the rewritten filter is identical to the original).
    List<TableClusterGroupSpec> all = groups(
        virtualClusteringGroup("acme"),
        virtualClusteringGroup("globex")
    );
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "tenant_lower",                 // SAME name as the group's clustering column / group VC
            "lower(tenant)",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        )
    );
    Filter f = new EqualityFilter("tenant_lower", ColumnType.STRING, "acme", null);
    List<TableClusterGroupSpec> kept = Projections.planClusterGroupQuery(all, buildSpec(f, queryVcs)).survivingGroups();
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
  }

  @Test
  void testQueryVirtualColumnShadowsOneOfMultipleClusteringColumnsStillPrunesOthers()
  {
    // Two-column clustering (tenant, region). Query VC named "tenant" shadows that clustering column with a
    // non-equivalent expression, but "region" is not shadowed. The AND filter on both columns should still let
    // the region leaf prune groups where region doesn't match (Kleene: UNKNOWN AND FALSE = FALSE), while groups
    // whose region matches stay (UNKNOWN AND TRUE = UNKNOWN → keep conservatively).
    List<TableClusterGroupSpec> all = groups(
        multiGroup("acme", "us-east-1"),
        multiGroup("acme", "us-west-2"),
        multiGroup("globex", "us-east-1")
    );
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "tenant",
            "lower(other)",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        )
    );
    Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("region", ColumnType.STRING, "us-east-1", null)
    ));
    List<TableClusterGroupSpec> kept = Projections.planClusterGroupQuery(all, buildSpec(f, queryVcs)).survivingGroups();
    Assertions.assertEquals(2, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));   // (acme, us-east-1)
    Assertions.assertSame(all.get(2), kept.get(1));   // (globex, us-east-1)
  }

  @Test
  void testNullFilterMatchesNullClusteringValue()
  {
    final TableClusterGroupSpec nullGroup = stringGroup(null);
    final TableClusterGroupSpec acmeGroup = stringGroup("acme");
    List<TableClusterGroupSpec> all = groups(nullGroup, acmeGroup);
    Filter f = NullFilter.forColumn("tenant");
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(nullGroup, kept.get(0));
    // Surviving group's null-on-null clustering leaf rewrites to TRUE.
    Assertions.assertSame(TrueFilter.instance(), rewrite(f, nullGroup));
  }

  @Test
  void testNullFilterPrunesNonNullClusteringValue()
  {
    List<TableClusterGroupSpec> all = groups(stringGroup("acme"));
    Filter f = NullFilter.forColumn("tenant");
    Assertions.assertTrue(pruneClusterGroups(all, f).isEmpty());
    // Null-on-non-null clustering leaf rewrites to FALSE.
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, all.get(0)));
  }

  @Test
  void testEqualityFilterPrunesNullClusteringValue()
  {
    // EqualityFilter does not match nulls (filter constructor rejects null match values; null group's rows never
    // match a non-null literal). A group whose clustering value is null is pruned regardless of the literal.
    List<TableClusterGroupSpec> all = groups(stringGroup(null), stringGroup("acme"));
    Filter f = new EqualityFilter("tenant", ColumnType.STRING, "acme", null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(all.get(1), kept.get(0));
    // Equality on a null clustering value rewrites to FALSE.
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, all.get(0)));
  }

  @Test
  void testTypedInFilterIncludingNullMatchesNullClusteringValue()
  {
    final TableClusterGroupSpec nullGroup = stringGroup(null);
    final TableClusterGroupSpec acmeGroup = stringGroup("acme");
    final TableClusterGroupSpec globexGroup = stringGroup("globex");
    List<TableClusterGroupSpec> all = groups(nullGroup, acmeGroup, globexGroup);
    Filter f = new TypedInFilter("tenant", ColumnType.STRING, Arrays.asList(null, "globex"), null, null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(2, kept.size());
    Assertions.assertSame(nullGroup, kept.get(0));
    Assertions.assertSame(globexGroup, kept.get(1));
  }

  @Test
  void testTypedInFilterWithoutNullPrunesNullClusteringValue()
  {
    final TableClusterGroupSpec nullGroup = stringGroup(null);
    final TableClusterGroupSpec acmeGroup = stringGroup("acme");
    List<TableClusterGroupSpec> all = groups(nullGroup, acmeGroup);
    Filter f = new TypedInFilter("tenant", ColumnType.STRING, List.of("acme", "globex"), null, null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(acmeGroup, kept.get(0));
  }

  @Test
  void testNotNullFilterOverClusteringColumnIsTriState()
  {
    // NOT IS NULL: must keep groups with non-null clustering values, prune groups with null clustering values.
    final TableClusterGroupSpec nullGroup = stringGroup(null);
    final TableClusterGroupSpec acmeGroup = stringGroup("acme");
    List<TableClusterGroupSpec> all = groups(nullGroup, acmeGroup);
    Filter f = new NotFilter(NullFilter.forColumn("tenant"));
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(acmeGroup, kept.get(0));
  }

  @Test
  void testIoTEqualityOnLongClusteringColumn()
  {
    final TableClusterGroupSpec d101East = deviceRegionGroup(101L, "us-east-1");
    final TableClusterGroupSpec d202East = deviceRegionGroup(202L, "us-east-1");
    final TableClusterGroupSpec d101West = deviceRegionGroup(101L, "us-west-2");
    List<TableClusterGroupSpec> all = groups(d101East, d202East, d101West);
    Filter f = new EqualityFilter("device_id", ColumnType.LONG, 101L, null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(2, kept.size());
    Assertions.assertSame(d101East, kept.get(0));
    Assertions.assertSame(d101West, kept.get(1));
  }

  @Test
  void testIoTAndAcrossMixedTypeClusteringColumns()
  {
    // AND across a LONG and a STRING clustering column; exercises per-type dictionary routing.
    final TableClusterGroupSpec d101East = deviceRegionGroup(101L, "us-east-1");
    final TableClusterGroupSpec d202East = deviceRegionGroup(202L, "us-east-1");
    final TableClusterGroupSpec d101West = deviceRegionGroup(101L, "us-west-2");
    List<TableClusterGroupSpec> all = groups(d101East, d202East, d101West);
    Filter f = new AndFilter(filters(
        new EqualityFilter("device_id", ColumnType.LONG, 101L, null),
        new EqualityFilter("region", ColumnType.STRING, "us-east-1", null)
    ));
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(d101East, kept.get(0));
  }

  @Test
  void testIoTTypedInFilterOnLongDeviceId()
  {
    // Typed IN on the LONG column; pruner must route via the column's type, not the STRING dict.
    List<TableClusterGroupSpec> all = groups(
        deviceRegionGroup(101L, "us-east-1"),
        deviceRegionGroup(202L, "us-east-1"),
        deviceRegionGroup(303L, "us-east-1")
    );
    Filter f = new TypedInFilter("device_id", ColumnType.LONG, List.of(101L, 303L), null, null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(2, kept.size());
    Assertions.assertSame(all.get(0), kept.get(0));
    Assertions.assertSame(all.get(2), kept.get(1));
  }

  @Test
  void testGenericPartitionClusteringPrunesOnEqualityFilter()
  {
    final TableClusterGroupSpec pA = partitionGroup("A");
    final TableClusterGroupSpec pB = partitionGroup("B");
    final TableClusterGroupSpec pC = partitionGroup("C");
    List<TableClusterGroupSpec> all = groups(pA, pB, pC);
    Filter f = new EqualityFilter("partition", ColumnType.STRING, "B", null);
    List<TableClusterGroupSpec> kept = pruneClusterGroups(all, f);
    Assertions.assertEquals(1, kept.size());
    Assertions.assertSame(pB, kept.get(0));
  }

  @Test
  void testNullFilterPreservedWhenInputIsNull()
  {
    Assertions.assertNull(rewrite(null, tenantRegionGroup("acme", "us-east-1")));
  }

  @Test
  void testTypedInFilterMatchingReturnsTrue()
  {
    final Filter f = new TypedInFilter("tenant", ColumnType.STRING, List.of("acme", "globex"), null, null);
    Assertions.assertSame(TrueFilter.instance(), rewrite(f, tenantRegionGroup("acme", "us-east-1")));
  }

  @Test
  void testTypedInFilterNonMatchingReturnsFalse()
  {
    final Filter f = new TypedInFilter("tenant", ColumnType.STRING, List.of("acme", "globex"), null, null);
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, tenantRegionGroup("initech", "us-east-1")));
  }

  @Test
  void testUnrecognizedFilterTypeLeftUnchanged()
  {
    // The rewriter walks AND/OR/NOT + NullFilter/EqualityFilter/TypedInFilter. Anything else (e.g., a NotFilter
    // wrapping a non-clustering leaf) passes through structurally with non-clustering leaves preserved.
    final Filter inner = new EqualityFilter("metric", ColumnType.STRING, "page-views", null);
    final Filter f = new NotFilter(inner);
    Assertions.assertEquals(f, rewrite(f, tenantRegionGroup("acme", "us-east-1")));
  }

  @Test
  void testAndDropsTrueChildren()
  {
    // tenant=acme is TRUE (clustering matches), region=us-east-1 is TRUE, metric=page-views stays as-is. The two
    // TRUE children get dropped; only the non-clustering leaf survives — and since it's a single survivor, the
    // AND wrapper unwraps to the leaf directly.
    final Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("region", ColumnType.STRING, "us-east-1", null),
        new EqualityFilter("metric", ColumnType.STRING, "page-views", null)
    ));
    final Filter expected = new EqualityFilter("metric", ColumnType.STRING, "page-views", null);
    Assertions.assertEquals(expected, rewrite(f, tenantRegionGroup("acme", "us-east-1")));
  }

  @Test
  void testAndShortCircuitsOnFalseChild()
  {
    final Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),     // FALSE on globex group
        new EqualityFilter("metric", ColumnType.STRING, "page-views", null)
    ));
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, tenantRegionGroup("globex", "us-east-1")));
  }

  @Test
  void testAndCollapsesToTrueWhenAllChildrenTrue()
  {
    final Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("region", ColumnType.STRING, "us-east-1", null)
    ));
    Assertions.assertSame(TrueFilter.instance(), rewrite(f, tenantRegionGroup("acme", "us-east-1")));
  }

  @Test
  void testOrDropsFalseChildren()
  {
    // tenant=acme is FALSE on globex group; the non-clustering metric leaf survives and unwraps to itself.
    final Filter f = new OrFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("metric", ColumnType.STRING, "page-views", null)
    ));
    final Filter expected = new EqualityFilter("metric", ColumnType.STRING, "page-views", null);
    Assertions.assertEquals(expected, rewrite(f, tenantRegionGroup("globex", "us-east-1")));
  }

  @Test
  void testOrShortCircuitsOnTrueChild()
  {
    // tenant=acme is TRUE on the acme group → entire OR collapses to TRUE, dropping the non-clustering leaf.
    final Filter f = new OrFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("metric", ColumnType.STRING, "page-views", null)
    ));
    Assertions.assertSame(TrueFilter.instance(), rewrite(f, tenantRegionGroup("acme", "us-east-1")));
  }

  @Test
  void testOrCollapsesToFalseWhenAllChildrenFalse()
  {
    final Filter f = new OrFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("tenant", ColumnType.STRING, "globex", null)
    ));
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, tenantRegionGroup("initech", "us-east-1")));
  }

  @Test
  void testMixedAndKeepsOnlyNonClusteringLeaf()
  {
    final Filter f = new AndFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),     // TRUE → dropped
        new NotFilter(new EqualityFilter("metric", ColumnType.STRING, "ads", null))   // structural, kept
    ));
    final Filter expected = new NotFilter(new EqualityFilter("metric", ColumnType.STRING, "ads", null));
    Assertions.assertEquals(expected, rewrite(f, tenantRegionGroup("acme", "us-east-1")));
  }

  @Test
  void testMultipleClusteringLeavesUnderOrCollapseIndependently()
  {
    // (tenant=acme OR region=us-west-2) — first leaf is FALSE on globex group, second leaf is also FALSE.
    final Filter f = new OrFilter(filters(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new EqualityFilter("region", ColumnType.STRING, "us-west-2", null)
    ));
    Assertions.assertSame(FalseFilter.instance(), rewrite(f, tenantRegionGroup("globex", "us-east-1")));
  }

  @Test
  void testQueryVcEquivalentToClusteringVcRewritesViaEquivalentName()
  {
    // Group clusters on tenant_lower := lower(tenant); query writes the filter against its own VC named
    // query_lower with the same expression. The rewriter should resolve query_lower → tenant_lower via
    // findEquivalent and collapse the leaf against the group's clustering value.
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn("query_lower", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final Filter f = new EqualityFilter("query_lower", ColumnType.STRING, "acme", null);
    final TableClusterGroupSpec acmeGroup = virtualClusteringGroup("acme");
    final TableClusterGroupSpec globexGroup = virtualClusteringGroup("globex");
    Assertions.assertSame(
        TrueFilter.instance(),
        Projections.planClusterGroupQuery(List.of(acmeGroup), buildSpec(f, queryVcs)).rewriteFor(acmeGroup)
    );
    Assertions.assertSame(
        FalseFilter.instance(),
        Projections.planClusterGroupQuery(List.of(globexGroup), buildSpec(f, queryVcs)).rewriteFor(globexGroup)
    );
  }

  @Test
  void testQueryVcShadowingClusteringNameWithoutEquivalenceLeavesLeafUnchanged()
  {
    // Query defines a VC named "tenant" with an unrelated expression. The rewriter must NOT silently collapse the
    // leaf against the clustering tuple — instead, leave the leaf in the rewritten filter so the cursor evaluates
    // it through the query VCs as it would any other (non-clustering) filter. This is the cursor-side analogue of
    // the pruner's "treat as UNKNOWN" disposition.
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn("tenant", "lower(otherCol)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final Filter f = new EqualityFilter("tenant", ColumnType.STRING, "acme", null);
    final TableClusterGroupSpec group = tenantRegionGroup("acme", "us-east-1");
    Assertions.assertEquals(
        f,
        Projections.planClusterGroupQuery(List.of(group), buildSpec(f, queryVcs)).rewriteFor(group)
    );
  }
}
