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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.config.NullHandlingTest;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinableFactoryWrapperTest extends NullHandlingTest
{
  public static final JoinableFactoryWrapper NOOP_JOINABLE_FACTORY_WRAPPER = new JoinableFactoryWrapper(
      NoopJoinableFactory.INSTANCE
  );

  private static final Map<String, String> TEST_LOOKUP =
      ImmutableMap.<String, String>builder()
                  .put("MX", "Mexico")
                  .put("NO", "Norway")
                  .put("SV", "El Salvador")
                  .put("US", "United States")
                  .put("", "Empty key")
                  .build();

  private static final Set<String> TEST_LOOKUP_KEYS =
      NullHandling.sqlCompatible()
      ? TEST_LOOKUP.keySet()
      : Sets.difference(TEST_LOOKUP.keySet(), Collections.singleton(""));

  private static final InlineDataSource INDEXED_TABLE_DS = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{"Mexico"},
          new Object[]{"Norway"},
          new Object[]{"El Salvador"},
          new Object[]{"United States"},
          new Object[]{"United States"}
      ),
      RowSignature.builder().add("country", ColumnType.STRING).build()
  );

  private static final InlineDataSource NULL_INDEXED_TABLE_DS = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{null}
      ),
      RowSignature.builder().add("nullCol", ColumnType.STRING).build()
  );

  private static final IndexedTable TEST_INDEXED_TABLE = new RowBasedIndexedTable<>(
      INDEXED_TABLE_DS.getRowsAsList(),
      INDEXED_TABLE_DS.rowAdapter(),
      INDEXED_TABLE_DS.getRowSignature(),
      ImmutableSet.of("country"),
      DateTimes.nowUtc().toString()
  );

  private static final IndexedTable TEST_NULL_INDEXED_TABLE = new RowBasedIndexedTable<>(
      NULL_INDEXED_TABLE_DS.getRowsAsList(),
      NULL_INDEXED_TABLE_DS.rowAdapter(),
      NULL_INDEXED_TABLE_DS.getRowSignature(),
      ImmutableSet.of("nullCol"),
      DateTimes.nowUtc().toString()
  );

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void test_checkClausePrefixesForDuplicatesAndShadowing_noConflicts()
  {
    List<String> prefixes = Arrays.asList(
        "AA",
        "AB",
        "AC",
        "aa",
        "ab",
        "ac",
        "BA"
    );

    JoinPrefixUtils.checkPrefixesForDuplicatesAndShadowing(prefixes);
  }

  @Test
  public void test_checkClausePrefixesForDuplicatesAndShadowing_duplicate()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Detected duplicate prefix in join clauses: [AA]");

    List<String> prefixes = Arrays.asList(
        "AA",
        "AA",
        "ABCD"
    );

    JoinPrefixUtils.checkPrefixesForDuplicatesAndShadowing(prefixes);
  }

  @Test
  public void test_checkClausePrefixesForDuplicatesAndShadowing_shadowing()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Detected conflicting prefixes in join clauses: [ABC.DEF, ABC.]");

    List<String> prefixes = Arrays.asList(
        "BASE.",
        "BASEBALL",
        "123.456",
        "23.45",
        "ABC.",
        "ABC.DEF"
    );

    JoinPrefixUtils.checkPrefixesForDuplicatesAndShadowing(prefixes);
  }

  @Test
  public void test_convertJoinsToFilters_convertInnerJoin()
  {
    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(
            new JoinableClause(
                "j.",
                LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
                JoinType.INNER,
                JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
            )
        ),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS)),
            ImmutableList.of()
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToPartialFilters_convertInnerJoin()
  {
    JoinableClause joinableClause = new JoinableClause(
        "j.",
        new IndexedTableJoinable(TEST_INDEXED_TABLE),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.country\"", "j.", ExprMacroTable.nil())
    );
    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(joinableClause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter(
                                 "x",
                                 INDEXED_TABLE_DS.getRowsAsList().stream().map(row -> row[0].toString()).collect(Collectors.toSet())
                             )
            ),
            ImmutableList.of(joinableClause)
            // the joinable clause remains intact since we've duplicates in country column
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_convertTwoInnerJoins()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "__j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("x == \"__j.k\"", "__j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS), new InDimFilter("x", TEST_LOOKUP_KEYS)),
            ImmutableList.of(clauses.get(2))
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToPartialAndFullFilters_convertMultipleInnerJoins()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ), // this joinable will be fully converted to a filter
        new JoinableClause(
            "_j.",
            new IndexedTableJoinable(TEST_INDEXED_TABLE),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"_j.country\"", "_j.", ExprMacroTable.nil())
        ), // this joinable will be partially converted to a filter since we've duplicates on country column
        new JoinableClause(
            "__j.",
            new IndexedTableJoinable(TEST_INDEXED_TABLE),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("x == \"__j.country\"", "__j.", ExprMacroTable.nil())
        ) // this joinable will not be converted to filter since its a LEFT join
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(
                new InDimFilter("x", TEST_LOOKUP_KEYS),
                new InDimFilter(
                    "x",
                    INDEXED_TABLE_DS.getRowsAsList().stream().map(row -> row[0].toString()).collect(Collectors.toSet())
                )
            ),
            ImmutableList.of(clauses.get(1), clauses.get(2))
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertTooManyValues()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(
            clause
        ),
        ImmutableSet.of("x"),
        2
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertLeftJoin()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_partialConvertWhenColumnIsUsed()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x", "j.k"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS)),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_convertToFalseFilterWhenOnlyNullValues()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        new IndexedTableJoinable(TEST_NULL_INDEXED_TABLE),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.nullCol\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(FalseFilter.instance()),
            ImmutableList.of()
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertLhsFunctions()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("concat(x,'') == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertRhsFunctions()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == concat(\"j.k\",'')", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertNonEquiJoin()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x != \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertJoinsDependedOnByLaterJoins()
  {
    // in this multi-join, a join matching two right sides is kept first to ensure :
    // 1. there is no filter on the right side table column j.k
    // 2. the right side matching join gets considered for join conversion always (since it is the first join clause)
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("\"j.k\" == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            clauses
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_partialConvertJoinsDependedOnByLaterJoins()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("\"j.k\" == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "__j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("x == \"__j.k\"", "__j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS)),
            clauses
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_partialConvertJoinsDependedOnByLaterJoins2()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "__j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("\"_j.v\" == \"__j.k\"", "__j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS), new InDimFilter("x", TEST_LOOKUP_KEYS)),
            clauses.subList(1, clauses.size())
        ),
        conversion
    );
  }
}
