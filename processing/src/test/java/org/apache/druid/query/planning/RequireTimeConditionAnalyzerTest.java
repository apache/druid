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

package org.apache.druid.query.planning;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RequireTimeConditionAnalyzerTest
{
  private static final QuerySegmentSpec BOUNDED = new MultipleIntervalSegmentSpec(
      ImmutableList.of(Intervals.of("2000/3000"))
  );
  private static final QuerySegmentSpec ETERNITY = new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY);
  private static final TableDataSource TABLE_FOO = new TableDataSource("foo");
  private static final TableDataSource TABLE_BAR = new TableDataSource("bar");
  private static final LookupDataSource LOOKUP_LOOKYLOO = new LookupDataSource("lookyloo");
  private static final InlineDataSource INLINE = InlineDataSource.fromIterable(
      ImmutableList.of(new Object[0]),
      RowSignature.builder().add("column", ColumnType.STRING).build()
  );

  @Test
  public void testBoundedScanOnTableIsSatisfied()
  {
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(scan(TABLE_FOO, BOUNDED)));
  }

  @Test
  public void testEternityScanOnTableIsNotSatisfied()
  {
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(scan(TABLE_FOO, ETERNITY)));
  }

  @Test
  public void testNestedSubqueryWithBoundedInnerIsSatisfied()
  {
    // Outer query is unbounded, inner is bounded. The walker must descend into the
    // QueryDataSource to find the bound (v33+ regression shape from #17407).
    GroupByQuery inner = groupBy(TABLE_FOO, BOUNDED);
    GroupByQuery outer = groupBy(new QueryDataSource(inner), ETERNITY);
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(outer));
  }

  @Test
  public void testBoundedOuterOverUnboundedQueryDataSourceIsNotSatisfied()
  {
    // Non-collapsible QueryDataSource outside any join. ScanQuery.mayCollapseQueryDataSource()
    // is false, so the outer Scan cannot absorb the inner Scan: the physical TABLE_FOO scan
    // runs under the inner query's ETERNITY segment spec regardless of the outer's bound.
    // Guards Frank's follow-up P1 on #20441.
    ScanQuery inner = scan(TABLE_FOO, ETERNITY);
    ScanQuery outer = scan(new QueryDataSource(inner), BOUNDED);
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(outer));
  }

  @Test
  public void testBoundedOuterGroupByOverUnboundedInnerGroupByIsNotSatisfied()
  {
    // Nested groupBy: GroupByQuery.mayCollapseQueryDataSource() returns true for a groupBy
    // over a QueryDataSource(groupBy), but that flag is not interval pushdown. At runtime,
    // GroupByQueryQueryToolChest.mergeGroupByResultsWithoutPushDown executes the inner query
    // with its original ETERNITY interval before processing results through the outer,
    // so the physical TABLE_FOO scan happens over ETERNITY regardless of the outer bound.
    // Guards Frank's second follow-up P1 on #20441 (comment 4219000942).
    GroupByQuery inner = groupBy(TABLE_FOO, ETERNITY);
    GroupByQuery outer = groupBy(new QueryDataSource(inner), BOUNDED);
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(outer));
  }

  @Test
  public void testGlobalLookupIsSatisfied()
  {
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(scan(LOOKUP_LOOKYLOO, ETERNITY)));
  }

  @Test
  public void testGlobalInlineIsSatisfied()
  {
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(scan(INLINE, ETERNITY)));
  }

  @Test
  public void testJoinWithBoundedLeftAndLookupRightIsSatisfied()
  {
    ScanQuery query = scan(join(TABLE_FOO, LOOKUP_LOOKYLOO), BOUNDED);
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(query));
  }

  @Test
  public void testJoinWithBoundedOuterButRightLegHasNoIndependentBoundIsNotSatisfied()
  {
    // Right leg's physical leaf (TABLE_BAR) crosses a join-leg boundary before finding
    // any bounded ancestor, so requireTimeCondition is not satisfied. Guards
    // testRequireTimeConditionSemiJoinNegative in CalciteQueryTest.
    ScanQuery query = scan(join(TABLE_FOO, TABLE_BAR), BOUNDED);
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(query));
  }

  @Test
  public void testJoinWithLimitWrappedUnboundedRightIsNotSatisfied()
  {
    // Right leg is an unbounded QueryDataSource (SQL equivalent: SELECT ... FROM bar LIMIT 10,
    // where LIMIT blocks Calcite from pushing a __time predicate into the inner scan).
    // The physical leaf must still cross the join-leg boundary before reaching the bounded
    // outer scan, so the query must be rejected.
    QueryDataSource limitWrapper = new QueryDataSource(scan(TABLE_BAR, ETERNITY));
    ScanQuery query = scan(join(TABLE_FOO, limitWrapper), BOUNDED);
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(query));
  }

  @Test
  public void testNestedJoinWithDeepUnboundedLegIsNotSatisfied()
  {
    // Join(TABLE_FOO, Join(TABLE_FOO, TABLE_BAR)) under a bounded outer scan. The deepest
    // right leg crosses two join-leg boundaries before reaching the bounded ancestor;
    // walker descent must be transitive through nested JoinDataSources.
    ScanQuery query = scan(join(TABLE_FOO, join(TABLE_FOO, TABLE_BAR)), BOUNDED);
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(query));
  }

  @Test
  public void testJoinRightLegHasOwnBoundedInnerQueryIsSatisfied()
  {
    // Right leg is a QueryDataSource whose inner query owns a bounded segment spec.
    // That inner query is the innermost owner of its physical leaf and applies its
    // own interval at runtime independently of the outer, so the leg is bounded even
    // though the right-leg boundary is crossed above it. Guards against over-rejection.
    QueryDataSource boundedRight = new QueryDataSource(scan(TABLE_BAR, BOUNDED));
    ScanQuery query = scan(join(TABLE_FOO, boundedRight), BOUNDED);
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(query));
  }

  @Test
  public void testIntermediateQueryDataSourceBoundIsRespected()
  {
    // outer(ETERNITY) -> QDS -> middle(BOUNDED) -> QDS -> inner(ETERNITY)
    // Only the innermost query owns the physical scan at runtime; the middle layer's
    // bound does not reach the leaf. Must be rejected.
    ScanQuery inner = scan(TABLE_FOO, ETERNITY);
    ScanQuery middle = scan(new QueryDataSource(inner), BOUNDED);
    ScanQuery outer = scan(new QueryDataSource(middle), ETERNITY);
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(outer));
  }

  @Test
  public void testIntermediateBoundedInnerUnderUnboundedOuterIsSatisfied()
  {
    // outer(ETERNITY) -> QDS -> inner(BOUNDED, scan on TABLE_FOO). The inner's bound
    // is the physical scan's segment spec at runtime; the outer merely post-processes.
    ScanQuery inner = scan(TABLE_FOO, BOUNDED);
    ScanQuery outer = scan(new QueryDataSource(inner), ETERNITY);
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(outer));
  }

  @Test
  public void testUnionOfTablesUnderBoundedOuterIsSatisfied()
  {
    // UnionDataSource over same-schema tables shares the outer query's segment spec
    // (there is no per-member Query owner). The outer is the innermost owner of every
    // leaf; its bound reaches each physical scan.
    UnionDataSource union = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    ScanQuery query = scan(union, BOUNDED);
    Assertions.assertTrue(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(query));
  }

  @Test
  public void testUnionOfTablesUnderUnboundedOuterIsNotSatisfied()
  {
    UnionDataSource union = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    ScanQuery query = scan(union, ETERNITY);
    Assertions.assertFalse(RequireTimeConditionAnalyzer.hasTimeFilterOnAllLegs(query));
  }

  private static ScanQuery scan(DataSource ds, QuerySegmentSpec spec)
  {
    return Druids.newScanQueryBuilder().dataSource(ds).intervals(spec).build();
  }

  private static GroupByQuery groupBy(DataSource ds, QuerySegmentSpec spec)
  {
    return GroupByQuery.builder()
        .setDataSource(ds)
        .setInterval(spec)
        .setGranularity(Granularities.ALL)
        .build();
  }

  private static JoinDataSource join(DataSource left, DataSource right)
  {
    return JoinDataSource.create(
        left,
        right,
        "j.",
        JoinConditionAnalysis.forExpression("x == \"j.x\"", "j.", ExprMacroTable.nil()).getOriginalExpression(),
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );
  }
}
