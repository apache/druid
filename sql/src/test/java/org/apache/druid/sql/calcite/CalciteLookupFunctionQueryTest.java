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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.GroupingAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rule.ReverseLookupRule;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CalciteLookupFunctionQueryTest extends BaseCalciteQueryTest
{
  private static final Map<String, Object> QUERY_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .putAll(QUERY_CONTEXT_DEFAULT)
                  .put(PlannerContext.CTX_SQL_REVERSE_LOOKUP, true)
                  .put(ReverseLookupRule.CTX_MAX_OPTIMIZE_COUNT, 1)
                  .build();

  private static final ExtractionFn EXTRACTION_FN =
      new RegisteredLookupExtractionFn(null, "lookyloo", false, null, null, false);

  private static final ExtractionFn EXTRACTION_FN_121 =
      new RegisteredLookupExtractionFn(null, "lookyloo121", false, null, null, false);

  @Test
  public void testFilterEquals()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') = 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension("'xabc'", equality("dim1", "abc", ColumnType.STRING)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterLookupOfFunction()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(LOWER(dim1), 'lookyloo') = 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lower(\"dim1\")", ColumnType.STRING),
            equality("v0", "abc", ColumnType.STRING)
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterFunctionOfLookup()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOWER(LOOKUP(dim1, 'lookyloo')) = 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lower(lookup(\"dim1\",'lookyloo'))", ColumnType.STRING),
            equality("v0", "xabc", ColumnType.STRING)
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterLookupOfConcat()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(CONCAT(dim1, 'b', dim2), 'lookyloo') = 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension(
            "'xa'", // dim1 must be 'a', and lookup of 'a' is 'xa'
            and(
                equality("dim1", "a", ColumnType.STRING),
                equality("dim2", "c", ColumnType.STRING)
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterInLookupOfConcat()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(CONCAT(dim1, 'a', dim2), 'lookyloo') IN ('xa', 'xabc')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            or(
                and(
                    equality("dim1", "", ColumnType.STRING),
                    equality("dim2", "", ColumnType.STRING)
                ),
                and(
                    equality("dim1", "", ColumnType.STRING),
                    equality("dim2", "bc", ColumnType.STRING)
                )
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterConcatOfLookup()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("CONCAT(LOOKUP(dim1, 'lookyloo'), ' (', dim1, ')') = 'xabc (abc)'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension(
            "'xabc'",
            equality("dim1", "abc", ColumnType.STRING)
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInConcatOfLookup()
  {
    cannotVectorize();

    // One optimize call is needed for each "IN" value, because this expression is decomposed into a sequence of
    // [(LOOKUP(dim1, 'lookyloo') = 'xabc' AND dim1 = 'abc') OR ...]. They can't be collected and combined.

    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(PlannerContext.CTX_SQL_REVERSE_LOOKUP, true)
                    .put(ReverseLookupRule.CTX_MAX_OPTIMIZE_COUNT, 2)
                    .build();

    testQuery(
        buildFilterTestSql("CONCAT(LOOKUP(dim1, 'lookyloo'), ' (', dim1, ')') IN ('xa (a)', 'xabc (abc)')"),
        queryContext,
        buildFilterTestExpectedQuery(in("dim1", ImmutableList.of("a", "abc"), null)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterConcatOfLookupOfConcat()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "CONCAT(LOOKUP(CONCAT(dim1, 'b', dim2), 'lookyloo'), ' (', CONCAT(dim1, 'b', dim2), ')') = 'xabc (abc)'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension(
            "'xa'", // dim1 must be 'a', and lookup of 'a' is 'xa'
            and(
                equality("dim1", "a", ColumnType.STRING),
                equality("dim2", "c", ColumnType.STRING)
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterInConcatOfLookupOfConcat()
  {
    cannotVectorize();

    // One optimize call is needed for each "IN" value, because this expression is decomposed into a sequence of
    // [(LOOKUP(dim1, 'lookyloo') = 'xabc' AND dim1 = 'abc') OR ...]. They can't be collected and combined.

    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(PlannerContext.CTX_SQL_REVERSE_LOOKUP, true)
                    .put(ReverseLookupRule.CTX_MAX_OPTIMIZE_COUNT, 2)
                    .build();

    testQuery(
        buildFilterTestSql(
            "CONCAT(LOOKUP(CONCAT(dim1, 'a', dim2), 'lookyloo'), ' (', CONCAT(dim1, 'a', dim2), ')')\n"
            + "IN ('xa (a)', 'xabc (abc)')"),
        queryContext,
        buildFilterTestExpectedQuery(
            or(
                and(
                    equality("dim1", "", ColumnType.STRING),
                    equality("dim2", "", ColumnType.STRING)
                ),
                and(
                    equality("dim1", "", ColumnType.STRING),
                    equality("dim2", "bc", ColumnType.STRING)
                )
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterConcatOfCoalesceLookupOfConcat()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "CONCAT(COALESCE(LOOKUP(CONCAT(dim1, 'b', dim2), 'lookyloo'), 'N/A'), ' (', CONCAT(dim1, 'b', dim2), ')') = 'xabc (abc)'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension(
            "'xa'", // dim1 must be 'a', and lookup of 'a' is 'xa'
            and(
                equality("dim1", "a", ColumnType.STRING),
                equality("dim2", "c", ColumnType.STRING)
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterImpossibleLookupOfConcat()
  {
    cannotVectorize();

    // No keys in the lookup table begin with 'key:', so this is always false.
    testQuery(
        buildFilterTestSql("LOOKUP('key:' || dim1, 'lookyloo') = 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryAlwaysFalse(),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterChainedEquals()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(LOOKUP(dim1, 'lookyloo'), 'lookyloo-chain') = 'zabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension("'xabc'", equality("dim1", "abc", ColumnType.STRING)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterEqualsLiteralFirst()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("'xabc' = LOOKUP(dim1, 'lookyloo')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension("'xabc'", equality("dim1", "abc", ColumnType.STRING)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterEqualsAlwaysFalse()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') = 'nonexistent'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryAlwaysFalse(),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterIsNotDistinctFrom()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IS NOT DISTINCT FROM 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension("'xabc'", equality("dim1", "abc", ColumnType.STRING)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterMultipleIsNotDistinctFrom()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IS NOT DISTINCT FROM 'xabc' OR "
                           + "LOOKUP(dim1, 'lookyloo') IS NOT DISTINCT FROM 'x6' OR "
                           + "LOOKUP(dim1, 'lookyloo') IS NOT DISTINCT FROM 'nonexistent'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(in("dim1", Arrays.asList("6", "abc"), null)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterIn()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IN ('xabc', 'x6', 'nonexistent')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(in("dim1", Arrays.asList("6", "abc"), null)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInOverMaxSize()
  {
    cannotVectorize();

    // Set sqlReverseLookupThreshold = 1 to stop the LOOKUP call from being reversed.
    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(PlannerContext.CTX_SQL_REVERSE_LOOKUP, true)
                    .put(ReverseLookupRule.CTX_THRESHOLD, 1)
                    .build();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IN ('xabc', 'x6', 'nonexistent')"),
        queryContext,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            in("v0", ImmutableList.of("nonexistent", "x6", "xabc"), null)
        )
        : buildFilterTestExpectedQuery(
            in("dim1", ImmutableList.of("nonexistent", "x6", "xabc"), EXTRACTION_FN)
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInOverMaxSize2()
  {
    cannotVectorize();

    // Set inSubQueryThreshold = 1 to stop the LOOKUP call from being reversed.
    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(PlannerContext.CTX_SQL_REVERSE_LOOKUP, true)
                    .put(QueryContexts.IN_SUB_QUERY_THRESHOLD_KEY, 1)
                    .build();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') = 'xabc' OR LOOKUP(dim1, 'lookyloo') = 'x6'"),
        queryContext,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            in("v0", ImmutableList.of("x6", "xabc"), null)
        )
        : buildFilterTestExpectedQuery(
            in("dim1", ImmutableList.of("x6", "xabc"), EXTRACTION_FN)
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInOrIsNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "LOOKUP(dim1, 'lookyloo') IN ('xabc', 'x6', 'nonexistent') OR LOOKUP(dim1, 'lookyloo') IS NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            or(
                in("dim1", Arrays.asList("6", "abc"), null),
                isNull("v0")
            )
        )
        : buildFilterTestExpectedQuery(
            or(
                in("dim1", Arrays.asList("6", "abc"), null),
                selector("dim1", null, EXTRACTION_FN)
            )
        ),
        ImmutableList.of(new Object[]{NullHandling.defaultStringValue(), 5L}, new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInAndIsNotNull()
  {
    cannotVectorize();

    // Ideally we'd be able to eliminate "AND LOOKUP(dim1, 'lookyloo') IS NOT NULL", because it's implied by
    // "LOOKUP(dim1, 'lookyloo') IN ('xabc', 'x6', 'nonexistent')". We're not currently able to do that.

    testQuery(
        buildFilterTestSql(
            "LOOKUP(dim1, 'lookyloo') IN ('xabc', 'x6', 'nonexistent') AND LOOKUP(dim1, 'lookyloo') IS NOT NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            and(
                in("dim1", ImmutableList.of("6", "abc"), null),
                not(isNull("v0"))
            )
        )
        : buildFilterTestExpectedQuery(
            and(
                in("dim1", Arrays.asList("6", "abc"), null),
                not(selector("dim1", null, EXTRACTION_FN))
            )
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInOrIsNullInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "LOOKUP(dim1, 'lookyloo121') IN ('xabc', 'x6', 'nonexistent') OR LOOKUP(dim1, 'lookyloo121') IS NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            or(isNull("dim1"), equality("dim1", "abc", ColumnType.STRING))
        )
        : buildFilterTestExpectedQueryConstantDimension(
            "'xabc'",
            equality("dim1", "abc", ColumnType.STRING)
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterNotInAndIsNotNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "LOOKUP(dim1, 'lookyloo') NOT IN ('x6', 'nonexistent') AND LOOKUP(dim1, 'lookyloo') IS NOT NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            and(
                not(equality("v0", "x6", ColumnType.STRING)),
                not(equality("v0", "nonexistent", ColumnType.STRING)),
                notNull("v0")
            )
        )
        : buildFilterTestExpectedQuery(
            and(
                not(selector("dim1", "6", null)),
                not(selector("dim1", null, EXTRACTION_FN))
            )
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInIsNotTrueAndIsNotNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "(LOOKUP(dim1, 'lookyloo') IN ('xabc', 'x6', 'nonexistent')) IS NOT TRUE "
            + "AND LOOKUP(dim1, 'lookyloo') IS NOT NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            and(
                not(istrue(in("dim1", ImmutableList.of("6", "abc"), null))),
                notNull("v0")
            )
        )
        : buildFilterTestExpectedQuery(
            and(
                not(in("dim1", ImmutableList.of("6", "abc"), null)),
                not(selector("dim1", null, EXTRACTION_FN))
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterNotInAndIsNotNullInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "LOOKUP(dim1, 'lookyloo121') NOT IN ('xabc', 'xdef', 'nonexistent') "
            + "AND LOOKUP(dim1, 'lookyloo121') IS NOT NULL"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? and(
                not(isNull("dim1")),
                not(in("dim1", ImmutableList.of("abc", "def"), null))
            )
            : not(in("dim1", ImmutableList.of("abc", "def"), null))),
        ImmutableList.of(new Object[]{NULL_STRING, 4L})
    );
  }

  @Test
  public void testFilterNotInOrIsNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "LOOKUP(dim1, 'lookyloo') NOT IN ('x6', 'nonexistent') OR LOOKUP(dim1, 'lookyloo') IS NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            or(
                and(
                    not(equality("v0", "x6", ColumnType.STRING)),
                    not(equality("v0", "nonexistent", ColumnType.STRING))
                ),
                isNull("v0")
            )
        )
        : buildFilterTestExpectedQuery(
            or(
                not(selector("dim1", "6", null)),
                selector("dim1", null, EXTRACTION_FN)
            )
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterInIsNotTrueOrIsNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "(LOOKUP(dim1, 'lookyloo') IN ('x6', 'nonexistent')) IS NOT TRUE OR LOOKUP(dim1, 'lookyloo') IS NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            or(
                not(istrue(equality("dim1", "6", ColumnType.STRING))),
                isNull("v0")
            )
        )
        : buildFilterTestExpectedQuery(
            or(
                not(selector("dim1", "6", null)),
                selector("dim1", null, EXTRACTION_FN)
            )
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterNotIn()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') NOT IN ('x6', 'nonexistent')"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            and(not(equality("v0", "x6", ColumnType.STRING)), not(equality("v0", "nonexistent", ColumnType.STRING)))
        )
        : buildFilterTestExpectedQuery(not(equality("dim1", "6", ColumnType.STRING))),
        // sql compatible mode expression filter (correctly) leaves out null values
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{"xabc", 1L})
        : ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterInIsNotTrue()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IN ('x6', 'nonexistent') IS NOT TRUE"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(istrue(equality("dim1", "6", ColumnType.STRING)))
            : not(equality("dim1", "6", ColumnType.STRING))
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterNotInInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo121') NOT IN ('xabc', 'xdef', 'nonexistent')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(not(in("dim1", ImmutableList.of("abc", "def"), null))),
        ImmutableList.of(new Object[]{NULL_STRING, 4L})
    );
  }

  @Test
  public void testFilterNotInWithReplaceMissingValue()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo', 'xyzzy') NOT IN ('xabc', 'x6', 'nonexistent')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(not(in("dim1", ImmutableList.of("6", "abc"), null))),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterInIsNotTrueWithReplaceMissingValue()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo', 'xyzzy') IN ('xabc', 'x6', 'nonexistent') IS NOT TRUE"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(istrue(in("dim1", ImmutableList.of("6", "abc"), null)))
            : not(in("dim1", ImmutableList.of("6", "abc"), null))),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterMvContains()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_CONTAINS(LOOKUP(dim1, 'lookyloo'), 'xabc')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(equality("dim1", "abc", ColumnType.STRING)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterMvContainsNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_CONTAINS(LOOKUP(dim1, 'lookyloo'), NULL)"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(expressionFilter("array_contains(lookup(\"dim1\",'lookyloo'),null)")),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterMvContainsNullInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_CONTAINS(LOOKUP(dim1, 'lookyloo121'), NULL)"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(expressionFilter("array_contains(\"dim1\",null)"))
        : buildFilterTestExpectedQueryAlwaysFalse(),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterMvOverlap()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_OVERLAP(lookup(dim1, 'lookyloo'), ARRAY['xabc', 'x6', 'nonexistent'])"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(in("dim1", ImmutableSet.of("6", "abc"), null)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterMvOverlapNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_OVERLAP(lookup(dim1, 'lookyloo'), ARRAY['xabc', 'x6', 'nonexistent', NULL])"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            in("dim1", Arrays.asList(null, "nonexistent", "x6", "xabc"), EXTRACTION_FN)
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterMvOverlapNullInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_OVERLAP(lookup(dim1, 'lookyloo121'), ARRAY['xabc', 'x6', 'nonexistent', NULL])"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? in("dim1", Arrays.asList(null, "abc"), null)
            : equality("dim1", "abc", ColumnType.STRING)
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterNotMvContains()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("NOT MV_CONTAINS(lookup(dim1, 'lookyloo'), 'xabc')"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            not(equality("v0", "xabc", ColumnType.STRING))
        )
        : buildFilterTestExpectedQuery(
            not(equality("dim1", "abc", ColumnType.STRING))
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of()
        : ImmutableList.of(new Object[]{"", 5L})
    );
  }

  @Test
  public void testFilterMvContainsIsNotTrue()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_CONTAINS(lookup(dim1, 'lookyloo'), 'xabc') IS NOT TRUE"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            not(equality("v0", "xabc", ColumnType.STRING))
        )
        : buildFilterTestExpectedQuery(
            not(equality("dim1", "abc", ColumnType.STRING))
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of()
        : ImmutableList.of(new Object[]{"", 5L})
    );
  }

  @Test
  public void testFilterNotMvContainsInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("NOT MV_CONTAINS(LOOKUP(dim1, 'lookyloo121'), 'xabc')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(not(equality("dim1", "abc", ColumnType.STRING))),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterNotMvOverlap()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("NOT MV_OVERLAP(lookup(dim1, 'lookyloo'), ARRAY['xabc', 'x6', 'nonexistent'])"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(in("dim1", ImmutableList.of("nonexistent", "x6", "xabc"), EXTRACTION_FN))
            : not(in("dim1", ImmutableList.of("6", "abc"), null))
        ),
        NullHandling.sqlCompatible()
        ? Collections.emptyList()
        : ImmutableList.of(new Object[]{NullHandling.defaultStringValue(), 5L})
    );
  }

  @Test
  public void testFilterMvOverlapIsNotTrue()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_OVERLAP(lookup(dim1, 'lookyloo'), ARRAY['xabc', 'x6', 'nonexistent']) IS NOT TRUE"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(in("dim1", ImmutableList.of("x6", "xabc", "nonexistent"), EXTRACTION_FN))
            : not(in("dim1", ImmutableList.of("6", "abc"), null))
        ),
        NullHandling.sqlCompatible()
        ? Collections.emptyList()
        : ImmutableList.of(new Object[]{NullHandling.defaultStringValue(), 5L})
    );
  }

  @Test
  public void testFilterNotMvOverlapInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("NOT MV_OVERLAP(lookup(dim1, 'lookyloo121'), ARRAY['xabc', 'x6', 'nonexistent'])"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(not(equality("dim1", "abc", ColumnType.STRING))),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterMultipleIsDistinctFrom()
  {
    cannotVectorize();

    // One optimize call is needed for each "IS DISTINCT FROM", because "x IS DISTINCT FROM y" is sugar for
    // "(x = y) IS NOT TRUE", and ReverseLookupRule doesn't peek into the "IS NOT TRUE" calls nested beneatth
    // the "AND".

    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(PlannerContext.CTX_SQL_REVERSE_LOOKUP, true)
                    .put(ReverseLookupRule.CTX_MAX_OPTIMIZE_COUNT, 3)
                    .build();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IS DISTINCT FROM 'xabc' AND "
                           + "LOOKUP(dim1, 'lookyloo') IS DISTINCT FROM 'x6' AND "
                           + "LOOKUP(dim1, 'lookyloo') IS DISTINCT FROM 'nonexistent'"),
        queryContext,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? and(
                not(istrue(equality("dim1", "abc", ColumnType.STRING))),
                not(istrue(equality("dim1", "6", ColumnType.STRING)))
            )
            : and(
                not(equality("dim1", "abc", ColumnType.STRING)),
                not(equality("dim1", "6", ColumnType.STRING))
            )
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L}
        )
    );
  }

  @Test
  public void testFilterIsNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IS NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            isNull("v0")
        )
        : buildFilterTestExpectedQuery(selector("dim1", null, EXTRACTION_FN)),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterIsNullInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo121') IS NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQueryConstantDimension("null", isNull("dim1"))
        : buildFilterTestExpectedQueryAlwaysFalse(),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterIsNotNull()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IS NOT NULL"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            not(isNull("v0"))
        )
        : buildFilterTestExpectedQuery(
            not(selector("dim1", null, EXTRACTION_FN))
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterIsNotNullInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo121') IS NOT NULL"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(isNull("dim1"))
            : null
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterNotEquals()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') <> 'x6'"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            not(equality("v0", "x6", ColumnType.STRING))
        )
        : buildFilterTestExpectedQuery(
            not(selector("dim1", "6", null))
        ),
        // sql compatible mode expression filter (correctly) leaves out null values
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{"xabc", 1L})
        : ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterNotEqualsInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo121') <> 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(not(equality("dim1", "abc", ColumnType.STRING))),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterEqualsIsNotTrue()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') = 'x6' IS NOT TRUE"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(istrue(equality("dim1", "6", ColumnType.STRING)))
            : not(selector("dim1", "6", null))
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterEqualsIsNotTrueInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo121') = 'xabc' IS NOT TRUE"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(istrue(equality("dim1", "abc", ColumnType.STRING)))
            : not(equality("dim1", "abc", ColumnType.STRING))),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterIsDistinctFrom()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo') IS DISTINCT FROM 'x6'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(istrue(equality("dim1", "6", ColumnType.STRING)))
            : not(equality("dim1", "6", ColumnType.STRING))
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterIsDistinctFromReplaceMissingValueWithSameLiteral()
  {
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn =
        new RegisteredLookupExtractionFn(null, "lookyloo", false, "x6", null, false);

    testQuery(
        buildFilterTestSql("LOOKUP(dim1, 'lookyloo', 'x6') IS DISTINCT FROM 'x6'"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo','x6')", ColumnType.STRING),
            not(istrue(equality("v0", "x6", ColumnType.STRING)))
        )
        : buildFilterTestExpectedQuery(
            not(selector("dim1", "x6", extractionFn))
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterNotEquals2()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("NOT (LOOKUP(dim1, 'lookyloo') = 'x6' OR cnt = 2)"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo')", ColumnType.STRING),
            and(
                not(equality("v0", "x6", ColumnType.STRING)),
                not(equality("cnt", 2L, ColumnType.LONG))
            )
        )
        : buildFilterTestExpectedQuery(
            and(
                not(equality("dim1", "6", ColumnType.STRING)),
                not(equality("cnt", 2L, ColumnType.LONG))
            )
        ),
        // sql compatible mode expression filter (correctly) leaves out null values
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{"xabc", 1L})
        : ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterEqualsIsNotTrue2()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("(LOOKUP(dim1, 'lookyloo') = 'x6' OR cnt = 2) IS NOT TRUE"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? not(istrue(or(
                equality("dim1", "6", ColumnType.STRING),
                equality("cnt", 2L, ColumnType.LONG)
            )))
            : not(or(
                equality("dim1", "6", ColumnType.STRING),
                equality("cnt", 2L, ColumnType.LONG)
            ))),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterNotEquals2Injective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("NOT (LOOKUP(dim1, 'lookyloo121') = 'xdef' OR cnt = 2)"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(and(
            not(equality("dim1", "def", ColumnType.STRING)),
            not(equality("cnt", 2L, ColumnType.LONG))
        )),
        ImmutableList.of(
            new Object[]{NULL_STRING, 4L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterCoalesceSameLiteral()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), 'x6') = 'x6'"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo','x6')", ColumnType.STRING),
            equality("v0", "x6", ColumnType.STRING)
        )
        : buildFilterTestExpectedQuery(
            selector("dim1", "x6", new RegisteredLookupExtractionFn(null, "lookyloo", false, "x6", null, false))
        ),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterCoalesceSameLiteralInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo121'), 'x6') = 'x6'"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQueryConstantDimension("null", isNull("dim1"))
        : buildFilterTestExpectedQueryAlwaysFalse(),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterInCoalesceSameLiteral()
  {
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn =
        new RegisteredLookupExtractionFn(null, "lookyloo", false, "x6", null, false);

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), 'x6') IN ('xa', 'xabc', 'x6')"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo','x6')", ColumnType.STRING),
            or(
                in("dim1", Arrays.asList("a", "abc"), null),
                equality("v0", "x6", ColumnType.STRING)
            )
        )
        : buildFilterTestExpectedQuery(
            or(
                in("dim1", Arrays.asList("a", "abc"), null),
                selector("dim1", "x6", extractionFn)
            )
        ),
        ImmutableList.of(new Object[]{NullHandling.defaultStringValue(), 5L}, new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterInCoalesceSameLiteralInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo121'), 'x2') IN ('xabc', 'xdef', 'x2')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            NullHandling.sqlCompatible()
            ? or(in("dim1", Arrays.asList("2", "abc", "def"), null), isNull("dim1"))
            : in("dim1", Arrays.asList("2", "abc", "def"), null)
        ),
        ImmutableList.of(new Object[]{NullHandling.defaultStringValue(), 2L}, new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterMvContainsCoalesceSameLiteral()
  {
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn =
        new RegisteredLookupExtractionFn(null, "lookyloo", false, "x6", null, false);

    testQuery(
        buildFilterTestSql("MV_CONTAINS(COALESCE(LOOKUP(dim1, 'lookyloo'), 'x6'), 'x6')"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo','x6')", ColumnType.STRING),
            equality("v0", "x6", ColumnType.STRING)
        )
        : buildFilterTestExpectedQuery(
            selector("dim1", "x6", extractionFn)
        ),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterMvOverlapCoalesceSameLiteral()
  {
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn =
        new RegisteredLookupExtractionFn(null, "lookyloo", false, "x6", null, false);

    testQuery(
        buildFilterTestSql("MV_OVERLAP(COALESCE(LOOKUP(dim1, 'lookyloo'), 'x6'), ARRAY['xabc', 'x6', 'nonexistent'])"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(in("dim1", ImmutableList.of("xabc", "x6", "nonexistent"), extractionFn)),
        ImmutableList.of(new Object[]{NULL_STRING, 5L}, new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterCoalesceSameLiteralNotEquals()
  {
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn =
        new RegisteredLookupExtractionFn(null, "lookyloo", false, "x6", null, false);

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), 'x6') <> 'x6'"),
        QUERY_CONTEXT,
        NullHandling.sqlCompatible()
        ? buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "lookup(\"dim1\",'lookyloo','x6')", ColumnType.STRING),
            not(equality("v0", "x6", ColumnType.STRING))
        )
        : buildFilterTestExpectedQuery(
            not(selector("dim1", "x6", extractionFn))
        ),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterCoalesceSameLiteralNotEqualsInjective()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo121'), 'xabc') <> 'xabc'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(not(equality("dim1", "abc", ColumnType.STRING))),
        ImmutableList.of(new Object[]{NULL_STRING, 5L})
    );
  }

  @Test
  public void testFilterCoalesceDifferentLiteral()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), 'xyzzy') = 'x6'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension("'x6'", equality("dim1", "6", ColumnType.STRING)),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterCoalesceDifferentLiteralAlwaysFalse()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), 'xyzzy') = 'nonexistent'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryAlwaysFalse(),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterCoalesceCastVarcharDifferentLiteral()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(CAST(LOOKUP(dim1, 'lookyloo') AS VARCHAR), 'xyzzy') = 'x6'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQueryConstantDimension("'x6'", equality("dim1", "6", ColumnType.STRING)),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterCoalesceCastBigintDifferentLiteral()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(CAST(LOOKUP(dim1, 'lookyloo') AS BIGINT), 1) = 6"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "nvl(CAST(lookup(\"dim1\",'lookyloo'), 'LONG'),1)", ColumnType.LONG),
            equality("v0", 6L, ColumnType.LONG)
        ),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterMvContainsCoalesceDifferentLiteral()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("MV_CONTAINS(COALESCE(LOOKUP(dim1, 'lookyloo'), 'xyzzy'), 'x6')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(equality("dim1", "6", ColumnType.STRING)),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterMvOverlapCoalesceDifferentLiteral()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql(
            "MV_OVERLAP(COALESCE(LOOKUP(dim1, 'lookyloo'), 'xyzzy'), ARRAY['xabc', 'x6', 'nonexistent'])"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(in("dim1", ImmutableSet.of("6", "abc"), null)),
        ImmutableList.of(new Object[]{"xabc", 1L})
    );
  }

  @Test
  public void testFilterCoalesceDifferentLiteralNotEquals()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), 'xyzzy') <> 'x6'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(not(equality("dim1", "6", ColumnType.STRING))),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterCoalesceDifferentLiteralNotEqualsAlwaysTrue()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), 'xyzzy') <> 'nonexistent'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(null),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterCoalesceSameColumn()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), dim1) = 'x6'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "nvl(lookup(\"dim1\",'lookyloo'),\"dim1\")", ColumnType.STRING),
            equality("v0", "x6", ColumnType.STRING)
        ),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterInCoalesceSameColumn()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), dim1) IN ('xabc', '10.1')"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "nvl(lookup(\"dim1\",'lookyloo'),\"dim1\")", ColumnType.STRING),
            in("v0", ImmutableList.of("10.1", "xabc"), null)
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 1L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  public void testFilterCoalesceFunctionOfSameColumn()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), dim1 || '') = 'x6'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "nvl(lookup(\"dim1\",'lookyloo'),concat(\"dim1\",''))", ColumnType.STRING),
            equality("v0", "x6", ColumnType.STRING)
        ),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterCoalesceDifferentColumn()
  {
    cannotVectorize();

    testQuery(
        buildFilterTestSql("COALESCE(LOOKUP(dim1, 'lookyloo'), dim2) = 'x6'"),
        QUERY_CONTEXT,
        buildFilterTestExpectedQuery(
            expressionVirtualColumn("v0", "nvl(lookup(\"dim1\",'lookyloo'),\"dim2\")", ColumnType.STRING),
            equality("v0", "x6", ColumnType.STRING)
        ),
        Collections.emptyList()
    );
  }

  @Test
  public void testFilterMaxUnapplyCount()
  {
    // Test to verify that "maxUnapplyCountForDruidReverseLookupRule" works properly. This ensures that the *other*
    // tests are correctly validating that we aren't doing too many reverse lookups.
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> testQuery(
            buildFilterTestSql("LOOKUP(dim1, 'lookyloo') = 'xabc' OR LOOKUP(dim2, 'lookyloo') = 'x6'"),
            QUERY_CONTEXT,
            ImmutableList.of(),
            ImmutableList.of()
        )
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Too many optimize calls[2]"))
    );
  }

  @Test
  public void testLookupReplaceMissingValueWith()
  {
    // Cannot vectorize due to extraction dimension specs.
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn1 =
        new RegisteredLookupExtractionFn(null, "lookyloo", false, "Missing_Value", null, false);
    testQuery(
        "SELECT\n"
        + "  LOOKUP(dim1, 'lookyloo', 'Missing_Value'),\n"
        + "  COALESCE(LOOKUP(dim1, 'lookyloo'), 'Missing_Value'), -- converted to the first form\n"
        + "  LOOKUP(dim1, 'lookyloo', null) as rmvNull,\n"
        + "  COUNT(*)\n"
        + "FROM foo\n"
        + "GROUP BY 1,2,3",
        QUERY_CONTEXT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d0",
                                    ColumnType.STRING,
                                    extractionFn1
                                ),
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d1",
                                    ColumnType.STRING,
                                    extractionFn1
                                ),
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d2",
                                    ColumnType.STRING,
                                    EXTRACTION_FN
                                )
                            )
                        )
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"Missing_Value", "Missing_Value", NullHandling.defaultStringValue(), 5L},
            new Object[]{"xabc", "xabc", "xabc", 1L}
        )
    );
  }

  @Test
  public void testCountDistinctOfLookup()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT LOOKUP(dim1, 'lookyloo')) FROM foo",
        QUERY_CONTEXT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CardinalityAggregatorFactory(
                          "a0",
                          null,
                          ImmutableList.of(new ExtractionDimensionSpec("dim1", null, EXTRACTION_FN)),
                          false,
                          true
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }

  @Test
  public void testLookupOnValueThatIsNull()
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.<Object[]>builder().add(
          new Object[]{NULL_STRING, NULL_STRING},
          new Object[]{NULL_STRING, NULL_STRING},
          new Object[]{NULL_STRING, NULL_STRING}
      ).build();
    } else {
      expected = ImmutableList.<Object[]>builder().add(
          new Object[]{NULL_STRING, NULL_STRING},
          new Object[]{NULL_STRING, NULL_STRING}
      ).build();
    }
    testQuery(
        "SELECT dim2 ,lookup(dim2,'lookyloo') from foo where dim2 is null",
        QUERY_CONTEXT,
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "null", ColumnType.STRING)
                )
                .columns("v0")
                .legacy(false)
                .filters(isNull("dim2"))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testLookupOnValueThatIsNotDistinctFromNull()
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.<Object[]>builder().add(
          new Object[]{NULL_STRING, NULL_STRING},
          new Object[]{NULL_STRING, NULL_STRING},
          new Object[]{NULL_STRING, NULL_STRING}
      ).build();
    } else {
      expected = ImmutableList.<Object[]>builder().add(
          new Object[]{NULL_STRING, NULL_STRING},
          new Object[]{NULL_STRING, NULL_STRING}
      ).build();
    }
    testQuery(
        "SELECT dim2 ,lookup(dim2,'lookyloo') from foo where dim2 is not distinct from null",
        QUERY_CONTEXT,
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "null", ColumnType.STRING)
                )
                .columns("v0")
                .legacy(false)
                .filters(isNull("dim2"))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testExactCountDistinct()
  {
    msqIncompatible();
    final String sqlQuery = "SELECT CAST(LOOKUP(dim1, 'lookyloo') AS VARCHAR), "
                            + "COUNT(DISTINCT foo.dim2), "
                            + "SUM(foo.cnt) FROM druid.foo "
                            + "GROUP BY 1";

    // ExtractionDimensionSpec cannot be vectorized
    cannotVectorize();

    testQuery(
        PLANNER_CONFIG_NO_HLL.withOverrides(
            ImmutableMap.of(
                PlannerConfig.CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
                "true"
            )
        ),
        QUERY_CONTEXT,
        sqlQuery,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(
                                                new ExtractionDimensionSpec(
                                                    "dim1",
                                                    "d0",
                                                    ColumnType.STRING,
                                                    EXTRACTION_FN
                                                ),
                                                new DefaultDimensionSpec("dim2", "d1", ColumnType.STRING)
                                            ))
                                            .setAggregatorSpecs(
                                                aggregators(
                                                    new LongSumAggregatorFactory("a0", "cnt"),
                                                    new GroupingAggregatorFactory(
                                                        "a1",
                                                        Arrays.asList("dim1", "dim2")
                                                    )
                                                )
                                            )
                                            .setSubtotalsSpec(
                                                ImmutableList.of(
                                                    ImmutableList.of("d0", "d1"),
                                                    ImmutableList.of("d0")
                                                )
                                            )
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("d0", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a0"),
                                and(
                                    notNull("d1"),
                                    equality("a1", 0L, ColumnType.LONG)
                                )
                            ),
                            new FilteredAggregatorFactory(
                                new LongMinAggregatorFactory("_a1", "a0"),
                                equality("a1", 1L, ColumnType.LONG)
                            )
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), NullHandling.replaceWithDefault() ? 2L : 3L, 5L},
            new Object[]{"xabc", 0L, 1L}
        )
    );
  }

  @Test
  public void testPullUpLookup()
  {
    testQuery(
        "SELECT LOOKUP(dim1, 'lookyloo121'), COUNT(*) FROM druid.foo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "lookup(\"d0\",'lookyloo121')", ColumnType.STRING))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"x", 1L},
            new Object[]{"x1", 1L},
            new Object[]{"x10.1", 1L},
            new Object[]{"x2", 1L},
            new Object[]{"xabc", 1L},
            new Object[]{"xdef", 1L}
        )
    );
  }

  @Test
  public void testPullUpAndReverseLookup()
  {
    testQuery(
        "SELECT LOOKUP(dim1, 'lookyloo121'), COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE LOOKUP(dim1, 'lookyloo121') IN ('xabc', 'xdef')\n"
        + "GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim1", ImmutableList.of("abc", "def"), null))
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "lookup(\"d0\",'lookyloo121')", ColumnType.STRING))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"xabc", 1L},
            new Object[]{"xdef", 1L}
        )
    );
  }

  @Test
  public void testDontPullUpLookupWhenUsedByAggregation()
  {
    cannotVectorize();

    testQuery(
        "SELECT LOOKUP(dim1, 'lookyloo121'), COUNT(LOOKUP(dim1, 'lookyloo121')) FROM druid.foo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            NullHandling.sqlCompatible()
                            ? new VirtualColumn[]{
                                expressionVirtualColumn(
                                    "v0",
                                    "lookup(\"dim1\",'lookyloo121')",
                                    ColumnType.STRING
                                )
                            }
                            : new VirtualColumn[0]
                        )
                        .setDimensions(dimensions(new ExtractionDimensionSpec("dim1", "d0", EXTRACTION_FN_121)))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    NullHandling.sqlCompatible()
                                    ? not(isNull("v0"))
                                    : not(selector("dim1", null, EXTRACTION_FN_121))
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"x", 1L},
            new Object[]{"x1", 1L},
            new Object[]{"x10.1", 1L},
            new Object[]{"x2", 1L},
            new Object[]{"xabc", 1L},
            new Object[]{"xdef", 1L}
        )
    );
  }

  @Test
  public void testPullUpLookupGroupOnLookupInput()
  {
    testQuery(
        "SELECT dim1, LOOKUP(dim1, 'lookyloo121'), COUNT(*) FROM druid.foo GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "lookup(\"d0\",'lookyloo121')", ColumnType.STRING))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "x", 1L},
            new Object[]{"1", "x1", 1L},
            new Object[]{"10.1", "x10.1", 1L},
            new Object[]{"2", "x2", 1L},
            new Object[]{"abc", "xabc", 1L},
            new Object[]{"def", "xdef", 1L}
        )
    );
  }

  @Test
  public void testPullUpLookupMoreDimensions()
  {
    testQuery(
        "SELECT COUNT(*), dim2, dim1, LOOKUP(dim1, 'lookyloo121') FROM druid.foo GROUP BY 2, 3",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING),
                            new DefaultDimensionSpec("dim2", "d1", ColumnType.STRING)
                        ))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "lookup(\"d0\",'lookyloo121')", ColumnType.STRING))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, "a", "", "x"},
            new Object[]{1L, "a", "1", "x1"},
            new Object[]{1L, NullHandling.defaultStringValue(), "10.1", "x10.1"},
            new Object[]{1L, "", "2", "x2"},
            new Object[]{1L, NullHandling.defaultStringValue(), "abc", "xabc"},
            new Object[]{1L, "abc", "def", "xdef"}
        )
    );
  }

  @Test
  public void testPullUpLookupOneInjectiveOneNot()
  {
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*), LOOKUP(dim1, 'lookyloo'), LOOKUP(dim1, 'lookyloo121') FROM druid.foo GROUP BY 2, 3",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(
                            new ExtractionDimensionSpec("dim1", "d0", EXTRACTION_FN),
                            new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
                        ))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "lookup(\"d1\",'lookyloo121')", ColumnType.STRING))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, NullHandling.defaultStringValue(), "x"},
            new Object[]{1L, NullHandling.defaultStringValue(), "x1"},
            new Object[]{1L, NullHandling.defaultStringValue(), "x10.1"},
            new Object[]{1L, NullHandling.defaultStringValue(), "x2"},
            new Object[]{1L, NullHandling.defaultStringValue(), "xdef"},
            new Object[]{1L, "xabc", "xabc"}
        )
    );
  }

  private String buildFilterTestSql(final String conditionSql)
  {
    return "SELECT LOOKUP(dim1, 'lookyloo'), COUNT(*) FROM foo\n"
           + "WHERE (" + conditionSql + ") AND TIME_IN_INTERVAL(__time, '2000/3000')\n"
           + "GROUP BY LOOKUP(dim1, 'lookyloo')";
  }

  private List<Query<?>> buildFilterTestExpectedQuery(
      @Nullable final VirtualColumn expectedVirtualColumn,
      @Nullable final DimFilter expectedFilter
  )
  {
    return ImmutableList.of(
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Intervals.of("2000/3000")))
                    .setVirtualColumns(expectedVirtualColumn != null
                                       ? VirtualColumns.create(expectedVirtualColumn)
                                       : VirtualColumns.EMPTY)
                    .setGranularity(Granularities.ALL)
                    .setDimFilter(expectedFilter)
                    .setDimensions(new ExtractionDimensionSpec("dim1", "d0", ColumnType.STRING, EXTRACTION_FN))
                    .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                    .setContext(QUERY_CONTEXT)
                    .build()
    );
  }

  private List<Query<?>> buildFilterTestExpectedQuery(@Nullable final DimFilter expectedFilter)
  {
    return buildFilterTestExpectedQuery(null, expectedFilter);
  }

  private List<Query<?>> buildFilterTestExpectedQueryConstantDimension(
      final String expectedConstantDimension,
      @Nullable final DimFilter expectedFilter
  )
  {
    return ImmutableList.of(
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Intervals.of("2000/3000")))
                    .setVirtualColumns(expressionVirtualColumn("v0", expectedConstantDimension, ColumnType.STRING))
                    .setGranularity(Granularities.ALL)
                    .setDimFilter(expectedFilter)
                    .setDimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING))
                    .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                    .setContext(QUERY_CONTEXT)
                    .build()
    );
  }

  private List<Query<?>> buildFilterTestExpectedQueryAlwaysFalse()
  {
    return ImmutableList.of(
        Druids.newScanQueryBuilder()
              .dataSource(InlineDataSource.fromIterable(
                  ImmutableList.of(),
                  RowSignature.builder()
                              .add("EXPR$0", ColumnType.STRING)
                              .add("$f1", ColumnType.LONG)
                              .build()
              ))
              .intervals(querySegmentSpec(Filtration.eternity()))
              .columns("$f1", "EXPR$0")
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .context(QUERY_CONTEXT)
              .legacy(false)
              .build()
    );
  }
}
