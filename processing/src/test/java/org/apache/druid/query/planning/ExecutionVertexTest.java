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
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.ProjectDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExecutionVertexTest
{
  private static final List<Interval> MILLENIUM_INTERVALS = ImmutableList.of(Intervals.of("2000/3000"));
  private static final TableDataSource TABLE_FOO = new TableDataSource("foo");
  private static final TableDataSource TABLE_BAR = new TableDataSource("bar");
  private static final RestrictedDataSource RESTRICTED_FOO = RestrictedDataSource.create(
      TABLE_FOO,
      NoRestrictionPolicy.instance()
  );
  public static final LookupDataSource LOOKUP_LOOKYLOO = new LookupDataSource("lookyloo");
  public static final InlineDataSource INLINE = InlineDataSource.fromIterable(
      ImmutableList.of(new Object[0]),
      RowSignature.builder().add("column", ColumnType.STRING).build()
  );

  /**
   * Virtual column that takes the name of the table's "dim1", and reads "dim2" in its place.
   */
  private static final VirtualColumn DIM1_FROM_DIM2 =
      new ExpressionVirtualColumn("dim1", "\"dim2\"", ColumnType.STRING, ExprMacroTable.nil());

  /**
   * Filter that matches "dim1" equal to "z", which {@link #SEGMENT_DIM1_A_TO_B} cannot hold.
   */
  private static final DimFilter DIM1_IS_Z = new EqualityFilter("dim1", ColumnType.STRING, "z", null);

  /**
   * Segment of {@link #TABLE_FOO} that is range-partitioned on "dim1", holding values from "a" up to "b".
   */
  private static final DataSegment SEGMENT_DIM1_A_TO_B = makeDim1RangeSegment();

  @Test
  public void testTable()
  {
    ScanQuery makeScanQuery = makeScanQuery(TABLE_FOO);
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery);

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testRestricted()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(RESTRICTED_FOO));

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testRestrictedInJoin()
  {
    JoinDataSource ds = join(
        RESTRICTED_FOO,
        LOOKUP_LOOKYLOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(ds));

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testUnion()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(unionDataSource));

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(unionDataSource, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void test_splitUnion_union()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(RESTRICTED_FOO, TABLE_BAR));
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(unionDataSource));

    // A union's arms read different segments, so it has no segment map function or pruner until it is split by arm.
    Assertions.assertThrows(DruidException.class, () -> ev.createSegmentMapFunction(NoopPolicyEnforcer.instance()));
    Assertions.assertThrows(DruidException.class, ev::getSegmentPruner);

    final List<ExecutionVertex> arms = ev.splitUnion();
    Assertions.assertEquals(
        List.of(TABLE_FOO, TABLE_BAR),
        arms.stream().map(ExecutionVertex::getBaseDataSource).toList()
    );
    for (final ExecutionVertex arm : arms) {
      Assertions.assertNotNull(arm.createSegmentMapFunction(NoopPolicyEnforcer.instance()));
    }
  }

  @Test
  public void test_splitUnion_table()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(TABLE_FOO));

    final List<ExecutionVertex> arms = ev.splitUnion();
    Assertions.assertEquals(1, arms.size());
    Assertions.assertSame(ev, arms.get(0));
  }

  @Test
  public void test_splitUnion_unionSegmentMapFunctions()
  {
    // The outer groupBy collapses into the inner one, so the projection above the union is part of this vertex.
    final ExecutionVertex ev = ExecutionVertex.of(
        makeGroupByQuery(
            makeQueryDS(
                new ProjectDataSource(
                    new UnionDataSource(ImmutableList.of(FilteredDataSource.create(TABLE_FOO, DIM1_IS_Z), TABLE_BAR)),
                    VirtualColumns.create(DIM1_FROM_DIM2)
                )
            )
        )
    );

    Assertions.assertEquals(
        List.of("project->filtered->base", "project->base"),
        debugStrings(ev.splitUnionSegmentMapFunctions(NoopPolicyEnforcer.instance()))
    );
  }

  @Test
  public void test_splitUnion_SegmentMapFunctions_table()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(FilteredDataSource.create(TABLE_FOO, DIM1_IS_Z)));

    Assertions.assertEquals(
        List.of("filtered->base"),
        debugStrings(ev.splitUnionSegmentMapFunctions(NoopPolicyEnforcer.instance()))
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void test_splitUnion_segmentMapFunctions_closeSharedAfterLastArm(final boolean reverseOrder) throws IOException
  {
    final AtomicInteger lookupReleases = new AtomicInteger();
    final SegmentMapFunction arm0 = Mockito.mock(SegmentMapFunction.class);
    final SegmentMapFunction arm1 = Mockito.mock(SegmentMapFunction.class);
    Mockito.when(arm0.apply(Optional.empty())).thenReturn(Optional.empty());
    Mockito.when(arm1.apply(Optional.empty())).thenReturn(Optional.empty());
    final ExecutionVertex vertex = lookupJoinOverUnion(
        List.of(tableWithMapFunction("foo", arm0), tableWithMapFunction("bar", arm1)),
        lookupReleases::incrementAndGet
    );
    final List<SegmentMapFunction> functions = vertex.splitUnionSegmentMapFunctions(NoopPolicyEnforcer.instance());
    final SegmentMapFunction first = functions.get(reverseOrder ? 1 : 0);
    final SegmentMapFunction last = functions.get(reverseOrder ? 0 : 1);

    Assertions.assertEquals(0, lookupReleases.get());
    first.close();
    first.close();
    Assertions.assertEquals(0, lookupReleases.get());
    Assertions.assertEquals(Optional.empty(), last.apply(Optional.empty()));
    last.close();
    last.close();

    Assertions.assertEquals(1, lookupReleases.get());
    Mockito.verify(arm0).close();
    Mockito.verify(arm1).close();
  }

  @Test
  public void test_splitUnion_segmentMapFunctions_closeFailureStillReleasesAllResources() throws IOException
  {
    final AtomicInteger lookupReleases = new AtomicInteger();
    final IOException closeFailure = new IOException("arm close failed");
    final IOException secondCloseFailure = new IOException("second arm close failed");
    final SegmentMapFunction arm0 = Mockito.mock(SegmentMapFunction.class);
    final SegmentMapFunction arm1 = Mockito.mock(SegmentMapFunction.class);
    Mockito.doThrow(closeFailure).when(arm0).close();
    Mockito.doThrow(secondCloseFailure).when(arm1).close();
    final ExecutionVertex vertex = lookupJoinOverUnion(
        List.of(tableWithMapFunction("foo", arm0), tableWithMapFunction("bar", arm1)),
        lookupReleases::incrementAndGet
    );
    final List<SegmentMapFunction> functions = vertex.splitUnionSegmentMapFunctions(NoopPolicyEnforcer.instance());

    Assertions.assertSame(
        closeFailure,
        Assertions.assertThrows(IOException.class, () -> CloseableUtils.closeAll(functions))
    );
    CloseableUtils.closeAll(functions);
    Assertions.assertEquals(1, lookupReleases.get());
    Assertions.assertArrayEquals(new Throwable[]{secondCloseFailure}, closeFailure.getSuppressed());
    Mockito.verify(arm0).close();
    Mockito.verify(arm1).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void test_splitUnion_segmentMapFunctions_buildFailureReleasesResources(final boolean failOnClose)
      throws IOException
  {
    final AtomicInteger lookupReleases = new AtomicInteger();
    final IllegalStateException buildFailure = new IllegalStateException("arm creation failed");
    final IOException closeFailure = new IOException("arm close failed");
    final SegmentMapFunction arm0 = Mockito.mock(SegmentMapFunction.class);
    if (failOnClose) {
      Mockito.doThrow(closeFailure).when(arm0).close();
    }
    final TableDataSource failingArm = Mockito.spy(new TableDataSource("bar"));
    Mockito.doThrow(buildFailure).when(failingArm).createSegmentMapFunction(Mockito.any());
    final ExecutionVertex vertex = lookupJoinOverUnion(
        List.of(tableWithMapFunction("foo", arm0), failingArm),
        lookupReleases::incrementAndGet
    );

    Assertions.assertSame(
        buildFailure,
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> vertex.splitUnionSegmentMapFunctions(NoopPolicyEnforcer.instance())
        )
    );
    Mockito.verify(arm0).close();
    Assertions.assertEquals(1, lookupReleases.get());
    Assertions.assertArrayEquals(
        failOnClose ? new Throwable[]{closeFailure} : new Throwable[0],
        buildFailure.getSuppressed()
    );
  }

  @Test
  public void test_getSegmentPruner_table()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeDim1ScanQuery(TABLE_FOO));

    // The filter reads the table's own "dim1", so it prunes a segment whose "dim1" values cannot match it.
    Assertions.assertFalse(ev.getSegmentPruner().include(SEGMENT_DIM1_A_TO_B));
  }

  @Test
  public void test_getSegmentPruner_projectedColumn()
  {
    final ProjectDataSource dataSource = new ProjectDataSource(TABLE_FOO, VirtualColumns.create(DIM1_FROM_DIM2));
    final ExecutionVertex ev = ExecutionVertex.of(makeDim1ScanQuery(dataSource));

    // The filter reads the projection's "dim1" rather than the table's, so the table's partitioning cannot prune.
    Assertions.assertTrue(ev.getSegmentPruner().include(SEGMENT_DIM1_A_TO_B));
  }

  @Test
  public void test_getSegmentPruner_unnestedColumn()
  {
    final UnnestDataSource dataSource = UnnestDataSource.create(TABLE_FOO, DIM1_FROM_DIM2, null);
    final ExecutionVertex ev = ExecutionVertex.of(makeDim1ScanQuery(dataSource));

    // The filter reads the unnested "dim1" rather than the table's, so the table's partitioning cannot prune.
    Assertions.assertTrue(ev.getSegmentPruner().include(SEGMENT_DIM1_A_TO_B));
  }

  @Test
  public void test_getSegmentPruner_filteredDataSource()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(FilteredDataSource.create(TABLE_FOO, DIM1_IS_Z)));

    // The datasource's own filter prunes, even though the query has none.
    Assertions.assertFalse(ev.getSegmentPruner().include(SEGMENT_DIM1_A_TO_B));
  }

  @Test
  public void test_getSegmentPruner_policyUnderProjection()
  {
    final DataSource dataSource = new ProjectDataSource(
        RestrictedDataSource.create(TABLE_FOO, RowFilterPolicy.from(DIM1_IS_Z)),
        VirtualColumns.create(new ExpressionVirtualColumn("v", "\"dim2\"", ColumnType.STRING, ExprMacroTable.nil()))
    );
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(dataSource));

    // The policy prunes from underneath the projection.
    Assertions.assertFalse(ev.getSegmentPruner().include(SEGMENT_DIM1_A_TO_B));
  }

  @Test
  public void test_getSegmentPruner_joinLeftColumn()
  {
    final ExecutionVertex ev =
        ExecutionVertex.of(makeDim1ScanQuery(join(TABLE_FOO, LOOKUP_LOOKYLOO, "j.", JoinType.INNER)));

    Assertions.assertFalse(ev.getSegmentPruner().include(SEGMENT_DIM1_A_TO_B));
  }

  @Test
  public void test_getSegmentPruner_joinRightColumn()
  {
    final ExecutionVertex ev =
        ExecutionVertex.of(makeDim1ScanQuery(join(TABLE_FOO, LOOKUP_LOOKYLOO, "d", JoinType.INNER)));

    // With "d" as the right-hand prefix, the filter reads the right-hand "dim1", which the table's partitioning says
    // nothing about, even though the table has a column of that name.
    Assertions.assertNull(ev.getSegmentPruner());
  }

  @Test
  public void test_getSegmentPruner_joinLeftFilter()
  {
    final ExecutionVertex inner =
        ExecutionVertex.of(makeScanQuery(join(TABLE_FOO, LOOKUP_LOOKYLOO, "j.", JoinType.INNER, DIM1_IS_Z)));
    final ExecutionVertex right =
        ExecutionVertex.of(makeScanQuery(join(TABLE_FOO, LOOKUP_LOOKYLOO, "j.", JoinType.RIGHT, DIM1_IS_Z)));

    // The left filter prunes, unless the join also emits right-hand rows that matched nothing on the left.
    Assertions.assertFalse(inner.getSegmentPruner().include(SEGMENT_DIM1_A_TO_B));
    Assertions.assertNull(right.getSegmentPruner());
  }

  @Test
  public void test_getSegmentPruner_subquery()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeDim1ScanQuery(makeQueryDS(TABLE_FOO)));

    // The filter reads the subquery's results, which the table's partitioning says nothing about.
    Assertions.assertNull(ev.getSegmentPruner());
  }

  @Test
  public void testSubQueryOnTable()
  {
    final QueryDataSource queryDataSource = makeQueryDS(TABLE_FOO);
    final ExecutionVertex ev = ExecutionVertex.of(makeGroupByQuery(queryDataSource));

    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testQueryOnUnion()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final QueryDataSource queryDataSource = makeQueryDS(unionDataSource);
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(queryDataSource));

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testLookup()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(LOOKUP_LOOKYLOO));

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(LOOKUP_LOOKYLOO, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testQueryOnLookup()
  {
    final QueryDataSource queryDataSource = makeQueryDS(LOOKUP_LOOKYLOO);
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(queryDataSource));

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testInline()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(INLINE));

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(INLINE, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinSimpleLeftLeaning()
  {
    // Join of a table onto a variety of simple joinable objects (lookup, inline, subquery) with a left-leaning
    // structure (no right children are joins themselves).

    final JoinDataSource joinDataSource =
        join(
            join(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER
                ),
                INLINE,
                "2.",
                JoinType.LEFT
            ),
            makeQueryDS(LOOKUP_LOOKYLOO),
            "3.",
            JoinType.FULL
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(Optional.empty(), analysis.getJoinBaseTableFilter());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause((JoinDataSource) ((JoinDataSource) joinDataSource.getLeft()).getLeft()),
            new PreJoinableClause((JoinDataSource) joinDataSource.getLeft()),
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinSimpleLeftLeaningWithLeftFilter()
  {
    final JoinDataSource joinDataSource =
        join(
            join(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER,
                    TrueDimFilter.instance()
                ),
                INLINE,
                "2.",
                JoinType.LEFT
            ),
            makeQueryDS(LOOKUP_LOOKYLOO),
            "3.",
            JoinType.FULL
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(null, analysis.getJoinBaseTableFilter().orElse(null));
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause((JoinDataSource) ((JoinDataSource) joinDataSource.getLeft()).getLeft()),
            new PreJoinableClause((JoinDataSource) joinDataSource.getLeft()),
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinSimpleRightLeaning()
  {
    // Join of a table onto a variety of simple joinable objects (lookup, inline, subquery) with a right-leaning
    // structure (no left children are joins themselves).
    //
    // Note that unlike the left-leaning stack, which is fully flattened, this one will not get flattened at all.

    final JoinDataSource rightLeaningJoinStack =
        join(
            LOOKUP_LOOKYLOO,
            join(
                INLINE,
                makeQueryDS(LOOKUP_LOOKYLOO),
                "1.",
                JoinType.LEFT
            ),
            "2.",
            JoinType.FULL
        );

    final JoinDataSource joinDataSource =
        join(
            TABLE_FOO,
            rightLeaningJoinStack,
            "3.",
            JoinType.RIGHT
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(Optional.empty(), analysis.getJoinBaseTableFilter());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinSimpleRightLeaningWithLeftFilter()
  {
    final JoinDataSource rightLeaningJoinStack =
        join(
            LOOKUP_LOOKYLOO,
            join(
                INLINE,
                makeQueryDS(LOOKUP_LOOKYLOO),
                "1.",
                JoinType.LEFT
            ),
            "2.",
            JoinType.FULL
        );

    final JoinDataSource joinDataSource =
        join(
            TABLE_FOO,
            rightLeaningJoinStack,
            "3.",
            JoinType.RIGHT,
            TrueDimFilter.instance()
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(null, analysis.getJoinBaseTableFilter().orElse(null));
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinOverTableSubquery()
  {
    final JoinDataSource joinDataSource = join(
        TABLE_FOO,
        makeQueryDS(TABLE_FOO),
        "1.",
        JoinType.INNER,
        TrueDimFilter.instance()
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(null, analysis.getJoinBaseTableFilter().orElse(null));
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinTableUnionToLookup()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final JoinDataSource joinDataSource = join(
        unionDataSource,
        LOOKUP_LOOKYLOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis1 = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(Optional.empty(), analysis1.getJoinBaseTableFilter());
    Assertions.assertEquals(unionDataSource, ev.getBaseDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis1.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinUnderTopLevelSubqueries()
  {
    final QueryDataSource queryDataSource =
        makeQueryDS(
            makeQueryDS(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER,
                    TrueDimFilter.instance()
                )
            )
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(queryDataSource));

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testSubqueriesAnalysis()
  {
    final JoinDataSource joinDataSource;
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource(
            makeQueryDS(
                joinDataSource = join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER,
                    TrueDimFilter.instance()
                )
            )
        )
        .setInterval(Intervals.ONLY_ETERNITY)
        .setGranularity(Granularities.ALL)
        .build();

    final ExecutionVertex ev = ExecutionVertex.of(query);
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertTrue(ev.isTableBased());
    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assertions.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assertions.assertEquals(null, analysis.getJoinBaseTableFilter().orElse(null));
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testSubQuery3()
  {
    final QueryDataSource queryDataSource =
        makeQueryDS(
            makeQueryDS(
                makeQueryDS(
                    TABLE_FOO
                )
            )
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(queryDataSource));

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertFalse(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinLookupToLookup()
  {
    final JoinDataSource joinDataSource = join(
        LOOKUP_LOOKYLOO,
        LOOKUP_LOOKYLOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertTrue(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(LOOKUP_LOOKYLOO, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(Optional.empty(), analysis.getJoinBaseTableFilter());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinLookupToTable()
  {
    final JoinDataSource joinDataSource = join(
        LOOKUP_LOOKYLOO,
        TABLE_FOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(LOOKUP_LOOKYLOO, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertEquals(Optional.empty(), analysis.getJoinBaseTableFilter());
    Assertions.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testJoinSubqueryToLookup()
  {
    final QueryDataSource queryDataSource = new QueryDataSource(
        GroupByQuery.builder()
                    .setDataSource(TABLE_FOO)
                    .setInterval(Intervals.ONLY_ETERNITY)
                    .setGranularity(Granularities.ALL)
                    .build()
    );
    final JoinDataSource joinDataSource = join(
        queryDataSource,
        LOOKUP_LOOKYLOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));

    // The subquery is executed separately, so it is the base, and the effective intervals are the outer query's.
    Assertions.assertFalse(ev.isProcessable());
    Assertions.assertFalse(ev.isTableBased());
    Assertions.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assertions.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assertions.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assertions.assertTrue(ev.isSegmentMapFunctionExpensive());
  }

  @Test
  public void testHashCodeIsNotSupported()
  {
    assertThrows(Exception.class, () -> ExecutionVertex.of(makeScanQuery(TABLE_FOO)).hashCode());
  }

  @Test
  public void testEqualsIsNotSupported()
  {
    assertThrows(Exception.class, () -> ExecutionVertex.of(makeScanQuery(TABLE_FOO)).equals(null));
  }

  /**
   * Generate a datasource that joins on a column named "x" on both sides.
   */
  private static JoinDataSource join(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinType joinType,
      final DimFilter dimFilter
  )
  {
    return JoinDataSource.create(
        left,
        right,
        rightPrefix,
        joinClause(rightPrefix).getOriginalExpression(),
        joinType,
        dimFilter,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );
  }

  private static JoinDataSource join(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinType joinType
  )
  {
    return join(left, right, rightPrefix, joinType, null);
  }

  /**
   * Generate a join clause that joins on a column named "x" on both sides.
   */
  private static JoinConditionAnalysis joinClause(
      final String rightPrefix
  )
  {
    return JoinConditionAnalysis.forExpression(
        StringUtils.format("x == \"%sx\"", rightPrefix),
        rightPrefix,
        ExprMacroTable.nil()
    );
  }

  /**
   * Generate a datasource that does a subquery on another datasource. The specific kind of query doesn't matter
   * much for the purpose of this test class, so it's always the same.
   */
  private static QueryDataSource makeQueryDS(final DataSource dataSource)
  {
    return new QueryDataSource(makeGroupByQuery(dataSource));
  }

  private static GroupByQuery makeGroupByQuery(final DataSource dataSource)
  {
    return GroupByQuery.builder()
        .setDataSource(dataSource)
        .setInterval(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS))
        .setGranularity(Granularities.ALL)
        .build();
  }

  public static ScanQuery makeScanQuery(final DataSource dataSource)
  {
    return Druids.newScanQueryBuilder()
        .dataSource(dataSource)
        .intervals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS))
        .build();
  }

  /**
   * Returns the debug string of the segment that each function makes from a base segment whose debug string is
   * {@code base}, which shows the wrappers each function applies, outermost first.
   */
  private static List<String> debugStrings(final List<SegmentMapFunction> fns)
  {
    final Segment segment = Mockito.mock(Segment.class);
    Mockito.when(segment.getDebugString()).thenReturn("base");
    return fns.stream().map(fn -> fn.apply(Optional.of(segment)).orElseThrow().getDebugString()).toList();
  }

  private static TableDataSource tableWithMapFunction(final String name, final SegmentMapFunction function)
  {
    final TableDataSource table = Mockito.spy(new TableDataSource(name));
    Mockito.doReturn(function).when(table).createSegmentMapFunction(Mockito.any());
    return table;
  }

  private static ExecutionVertex lookupJoinOverUnion(final List<DataSource> arms, final Closeable lookupCloser)
  {
    final JoinableFactory factory = Mockito.mock(JoinableFactory.class);
    Mockito.when(factory.build(Mockito.any(), Mockito.any())).thenAnswer(
        invocation -> Optional.of(LookupJoinable.wrap(new MapLookupExtractor(Map.of("key", "value"), false), lookupCloser))
    );
    return ExecutionVertex.of(makeScanQuery(JoinDataSource.create(
        new UnionDataSource(arms),
        LOOKUP_LOOKYLOO,
        "j.",
        "x == \"j.k\"",
        JoinType.LEFT,
        null,
        ExprMacroTable.nil(),
        new JoinableFactoryWrapper(factory),
        JoinAlgorithm.BROADCAST
    )));
  }

  private static DataSegment makeDim1RangeSegment()
  {
    final Interval interval = Intervals.of("2000/2001");
    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        List.of("dim1"),
        null,
        StringTuple.create("a"),
        StringTuple.create("b"),
        0,
        1
    );
    return DataSegment.builder(SegmentId.of(TABLE_FOO.getName(), interval, "0", shardSpec))
                      .shardSpec(shardSpec)
                      .build();
  }

  /**
   * Returns a scan query filtered by {@link #DIM1_IS_Z}.
   */
  private static ScanQuery makeDim1ScanQuery(final DataSource dataSource)
  {
    return Druids.newScanQueryBuilder()
        .dataSource(dataSource)
        .intervals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS))
        .filters(DIM1_IS_Z)
        .build();
  }
}
