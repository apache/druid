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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestQuery;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class JoinablesTest
{
  private static final JoinFilterRewriteConfig DEFAULT_JOIN_FILTER_REWRITE_CONFIG = new JoinFilterRewriteConfig(
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN,
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE,
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS,
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void test_validatePrefix_null()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have null or empty prefix");

    Joinables.validatePrefix(null);
  }

  @Test
  public void test_validatePrefix_empty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have null or empty prefix");

    Joinables.validatePrefix("");
  }

  @Test
  public void test_validatePrefix_underscore()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have prefix[_]");

    Joinables.validatePrefix("_");
  }

  @Test
  public void test_validatePrefix_timeColumn()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have prefix[__time]");

    Joinables.validatePrefix(ColumnHolder.TIME_COLUMN_NAME);
  }

  @Test
  public void test_isPrefixedBy()
  {
    Assert.assertTrue(Joinables.isPrefixedBy("foo", ""));
    Assert.assertTrue(Joinables.isPrefixedBy("foo", "f"));
    Assert.assertTrue(Joinables.isPrefixedBy("foo", "fo"));
    Assert.assertFalse(Joinables.isPrefixedBy("foo", "foo"));
  }

  @Test
  public void test_createSegmentMapFn_noClauses()
  {
    final Function<SegmentReference, SegmentReference> segmentMapFn = Joinables.createSegmentMapFn(
        ImmutableList.of(),
        NoopJoinableFactory.INSTANCE,
        new AtomicLong(),
        null
    );

    Assert.assertSame(Function.identity(), segmentMapFn);
  }

  @Test
  public void test_createSegmentMapFn_unusableClause()
  {
    final LookupDataSource lookupDataSource = new LookupDataSource("lookyloo");
    final PreJoinableClause clause = new PreJoinableClause(
        "j.",
        lookupDataSource,
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression("x == \"j.x\"", "j.", ExprMacroTable.nil())
    );

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("dataSource is not joinable");

    final Function<SegmentReference, SegmentReference> ignored = Joinables.createSegmentMapFn(
        ImmutableList.of(clause),
        NoopJoinableFactory.INSTANCE,
        new AtomicLong(),
        null
    );
  }

  @Test
  public void test_createSegmentMapFn_usableClause()
  {
    final LookupDataSource lookupDataSource = new LookupDataSource("lookyloo");
    final JoinConditionAnalysis conditionAnalysis = JoinConditionAnalysis.forExpression(
        "x == \"j.x\"",
        "j.",
        ExprMacroTable.nil()
    );
    final PreJoinableClause clause = new PreJoinableClause(
        "j.",
        lookupDataSource,
        JoinType.LEFT,
        conditionAnalysis
    );

    final Function<SegmentReference, SegmentReference> segmentMapFn = Joinables.createSegmentMapFn(
        ImmutableList.of(clause),
        new JoinableFactory()
        {
          @Override
          public boolean isDirectlyJoinable(DataSource dataSource)
          {
            return dataSource.equals(lookupDataSource);
          }

          @Override
          public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
          {
            if (dataSource.equals(lookupDataSource) && condition.equals(conditionAnalysis)) {
              return Optional.of(
                  LookupJoinable.wrap(new MapLookupExtractor(ImmutableMap.of("k", "v"), false))
              );
            } else {
              return Optional.empty();
            }
          }
        },
        new AtomicLong(),
        new TestQuery(
            new TableDataSource("test"),
            new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
            false,
            new HashMap()
        )
    );

    Assert.assertNotSame(Function.identity(), segmentMapFn);
  }

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

    Joinables.checkPrefixesForDuplicatesAndShadowing(prefixes);
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

    Joinables.checkPrefixesForDuplicatesAndShadowing(prefixes);
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

    Joinables.checkPrefixesForDuplicatesAndShadowing(prefixes);
  }
}
