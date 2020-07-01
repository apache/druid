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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DataSourceAnalysisTest
{
  private static final List<Interval> MILLENIUM_INTERVALS = ImmutableList.of(Intervals.of("2000/3000"));
  private static final TableDataSource TABLE_FOO = new TableDataSource("foo");
  private static final TableDataSource TABLE_BAR = new TableDataSource("bar");
  private static final LookupDataSource LOOKUP_LOOKYLOO = new LookupDataSource("lookyloo");
  private static final InlineDataSource INLINE = InlineDataSource.fromIterable(
      ImmutableList.of(new Object[0]),
      RowSignature.builder().add("column", ValueType.STRING).build()
  );

  @Test
  public void testTable()
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(TABLE_FOO);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(TABLE_FOO, analysis.getDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.of(TABLE_FOO), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(Collections.emptyList(), analysis.getPreJoinableClauses());
  }

  @Test
  public void testUnion()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(unionDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(unionDataSource, analysis.getDataSource());
    Assert.assertEquals(unionDataSource, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(Collections.emptyList(), analysis.getPreJoinableClauses());
  }

  @Test
  public void testQueryOnTable()
  {
    final QueryDataSource queryDataSource = subquery(TABLE_FOO);
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(queryDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertTrue(analysis.isQuery());
    Assert.assertEquals(queryDataSource, analysis.getDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.of(TABLE_FOO), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.of(queryDataSource.getQuery()), analysis.getBaseQuery());
    Assert.assertEquals(
        Optional.of(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS)),
        analysis.getBaseQuerySegmentSpec()
    );
    Assert.assertEquals(Collections.emptyList(), analysis.getPreJoinableClauses());
  }

  @Test
  public void testQueryOnUnion()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final QueryDataSource queryDataSource = subquery(unionDataSource);
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(queryDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertTrue(analysis.isQuery());
    Assert.assertEquals(queryDataSource, analysis.getDataSource());
    Assert.assertEquals(unionDataSource, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.of(queryDataSource.getQuery()), analysis.getBaseQuery());
    Assert.assertEquals(
        Optional.of(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS)),
        analysis.getBaseQuerySegmentSpec()
    );
    Assert.assertEquals(Collections.emptyList(), analysis.getPreJoinableClauses());
  }

  @Test
  public void testLookup()
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(LOOKUP_LOOKYLOO);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertFalse(analysis.isConcreteTableBased());
    Assert.assertTrue(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(LOOKUP_LOOKYLOO, analysis.getDataSource());
    Assert.assertEquals(LOOKUP_LOOKYLOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(Collections.emptyList(), analysis.getPreJoinableClauses());
  }

  @Test
  public void testQueryOnLookup()
  {
    final QueryDataSource queryDataSource = subquery(LOOKUP_LOOKYLOO);
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(queryDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertFalse(analysis.isConcreteTableBased());
    Assert.assertTrue(analysis.isGlobal());
    Assert.assertTrue(analysis.isQuery());
    Assert.assertEquals(queryDataSource, analysis.getDataSource());
    Assert.assertEquals(LOOKUP_LOOKYLOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.of(queryDataSource.getQuery()), analysis.getBaseQuery());
    Assert.assertEquals(
        Optional.of(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS)),
        analysis.getBaseQuerySegmentSpec()
    );
    Assert.assertEquals(Collections.emptyList(), analysis.getPreJoinableClauses());
  }

  @Test
  public void testInline()
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(INLINE);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertFalse(analysis.isConcreteTableBased());
    Assert.assertTrue(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(INLINE, analysis.getDataSource());
    Assert.assertEquals(INLINE, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(Collections.emptyList(), analysis.getPreJoinableClauses());
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
            subquery(LOOKUP_LOOKYLOO),
            "3.",
            JoinType.FULL
        );

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(joinDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(joinDataSource, analysis.getDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.of(TABLE_FOO), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause("1.", LOOKUP_LOOKYLOO, JoinType.INNER, joinClause("1.")),
            new PreJoinableClause("2.", INLINE, JoinType.LEFT, joinClause("2.")),
            new PreJoinableClause("3.", subquery(LOOKUP_LOOKYLOO), JoinType.FULL, joinClause("3."))
        ),
        analysis.getPreJoinableClauses()
    );
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
                subquery(LOOKUP_LOOKYLOO),
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

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(joinDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(joinDataSource, analysis.getDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.of(TABLE_FOO), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause("3.", rightLeaningJoinStack, JoinType.RIGHT, joinClause("3."))
        ),
        analysis.getPreJoinableClauses()
    );
  }

  @Test
  public void testJoinOverTableSubquery()
  {
    final JoinDataSource joinDataSource = join(
        TABLE_FOO,
        subquery(TABLE_FOO),
        "1.",
        JoinType.INNER
    );

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(joinDataSource);

    Assert.assertFalse(analysis.isConcreteBased());
    Assert.assertFalse(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(joinDataSource, analysis.getDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.of(TABLE_FOO), analysis.getBaseTableDataSource());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause("1.", subquery(TABLE_FOO), JoinType.INNER, joinClause("1."))
        ),
        analysis.getPreJoinableClauses()
    );
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

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(joinDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(joinDataSource, analysis.getDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(unionDataSource, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause("1.", LOOKUP_LOOKYLOO, JoinType.INNER, joinClause("1."))
        ),
        analysis.getPreJoinableClauses()
    );
  }

  @Test
  public void testJoinUnderTopLevelSubqueries()
  {
    final QueryDataSource queryDataSource =
        subquery(
            subquery(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER
                )
            )
        );

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(queryDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertTrue(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertTrue(analysis.isQuery());
    Assert.assertEquals(queryDataSource, analysis.getDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.of(TABLE_FOO), analysis.getBaseTableDataSource());
    Assert.assertEquals(
        Optional.of(
            subquery(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER
                )
            ).getQuery()
        ),
        analysis.getBaseQuery()
    );
    Assert.assertEquals(
        Optional.of(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS)),
        analysis.getBaseQuerySegmentSpec()
    );
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause("1.", LOOKUP_LOOKYLOO, JoinType.INNER, joinClause("1."))
        ),
        analysis.getPreJoinableClauses()
    );
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

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(joinDataSource);

    Assert.assertTrue(analysis.isConcreteBased());
    Assert.assertFalse(analysis.isConcreteTableBased());
    Assert.assertTrue(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(joinDataSource, analysis.getDataSource());
    Assert.assertEquals(LOOKUP_LOOKYLOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause("1.", LOOKUP_LOOKYLOO, JoinType.INNER, joinClause("1."))
        ),
        analysis.getPreJoinableClauses()
    );
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

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(joinDataSource);

    Assert.assertFalse(analysis.isConcreteBased());
    Assert.assertFalse(analysis.isConcreteTableBased());
    Assert.assertFalse(analysis.isGlobal());
    Assert.assertFalse(analysis.isQuery());
    Assert.assertEquals(joinDataSource, analysis.getDataSource());
    Assert.assertEquals(LOOKUP_LOOKYLOO, analysis.getBaseDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuery());
    Assert.assertEquals(Optional.empty(), analysis.getBaseQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause("1.", TABLE_FOO, JoinType.INNER, joinClause("1."))
        ),
        analysis.getPreJoinableClauses()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DataSourceAnalysis.class)
                  .usingGetClass()
                  .withNonnullFields("dataSource")

                  // These fields are not necessary, because they're wholly determined by "dataSource"
                  .withIgnoredFields("baseDataSource", "baseQuery", "preJoinableClauses")
                  .verify();
  }

  /**
   * Generate a datasource that joins on a column named "x" on both sides.
   */
  private static JoinDataSource join(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinType joinType
  )
  {
    return JoinDataSource.create(
        left,
        right,
        rightPrefix,
        joinClause(rightPrefix).getOriginalExpression(),
        joinType,
        ExprMacroTable.nil()
    );
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
  private static QueryDataSource subquery(final DataSource dataSource)
  {
    return new QueryDataSource(
        GroupByQuery.builder()
                    .setDataSource(dataSource)
                    .setInterval(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS))
                    .setGranularity(Granularities.ALL)
                    .build()
    );
  }
}
