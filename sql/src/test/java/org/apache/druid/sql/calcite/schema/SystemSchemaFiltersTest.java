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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 * Unit tests for the predicate-extraction utilities in {@link SystemSchemaFilters}. RexNodes are
 * built by hand (as Calcite's filter-scan rule would hand them to a {@code ProjectableFilterableTable})
 * so the extraction logic can be exercised directly without a full planner run.
 */
public class SystemSchemaFiltersTest
{
  // Two arbitrary string columns to constrain, plus a numeric-ish column used for "other column" cases.
  private static final int COL_A = 1;
  private static final int COL_B = 2;
  private static final int COL_OTHER = 4;

  private RexBuilder rexBuilder;
  private RexLiteral foo;
  private RexLiteral bar;
  private RexLiteral baz;
  private RexNode aRef;
  private RexNode bRef;
  private RexNode otherRef;

  @Before
  public void setUp()
  {
    rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
    foo = (RexLiteral) rexBuilder.makeLiteral("foo");
    bar = (RexLiteral) rexBuilder.makeLiteral("bar");
    baz = (RexLiteral) rexBuilder.makeLiteral("baz");
    // Match the input-ref type to the literal type so Calcite does not wrap the literal in a CAST.
    aRef = rexBuilder.makeInputRef(foo.getType(), COL_A);
    bRef = rexBuilder.makeInputRef(foo.getType(), COL_B);
    otherRef = rexBuilder.makeInputRef(foo.getType(), COL_OTHER);
  }

  @Test
  public void testEquals()
  {
    // col = 'foo'
    Assert.assertEquals(
        ImmutableSet.of("foo"),
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, aRef, foo)),
            COL_A
        )
    );
  }

  @Test
  public void testEqualsReversedOperands()
  {
    // 'foo' = col
    Assert.assertEquals(
        ImmutableSet.of("foo"),
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, foo, aRef)),
            COL_A
        )
    );
  }

  @Test
  public void testIn()
  {
    // col IN ('foo', 'bar') -- Calcite normalizes IN to a SEARCH over a points Sarg
    Assert.assertEquals(
        ImmutableSet.of("foo", "bar"),
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeIn(aRef, ImmutableList.of(foo, bar))),
            COL_A
        )
    );
  }

  @Test
  public void testOrOfEqualities()
  {
    // col = 'foo' OR col = 'bar'
    Assert.assertEquals(
        ImmutableSet.of("foo", "bar"),
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(
                SqlStdOperatorTable.OR,
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, aRef, foo),
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, aRef, bar)
            )),
            COL_A
        )
    );
  }

  @Test
  public void testOrWithUnextractableDisjunctReturnsNull()
  {
    // col = 'foo' OR col > 'bar' -- one disjunct cannot bound the set, so the whole OR is unbounded
    Assert.assertNull(
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(
                SqlStdOperatorTable.OR,
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, aRef, foo),
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, aRef, bar)
            )),
            COL_A
        )
    );
  }

  @Test
  public void testTopLevelConjunctsIntersect()
  {
    // A top-level filter list is implicitly ANDed: IN ('foo','bar') AND IN ('bar','baz') => {'bar'}
    Assert.assertEquals(
        ImmutableSet.of("bar"),
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(
                rexBuilder.makeIn(aRef, ImmutableList.of(foo, bar)),
                rexBuilder.makeIn(aRef, ImmutableList.of(bar, baz))
            ),
            COL_A
        )
    );
  }

  @Test
  public void testNestedAndConjunctsIntersect()
  {
    // A whole WHERE passed as a single AND(...) RexCall: IN ('foo','bar') AND IN ('bar','baz') => {'bar'}
    Assert.assertEquals(
        ImmutableSet.of("bar"),
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeIn(aRef, ImmutableList.of(foo, bar)),
                rexBuilder.makeIn(aRef, ImmutableList.of(bar, baz))
            )),
            COL_A
        )
    );
  }

  @Test
  public void testAndIgnoresNonMatchingConjunct()
  {
    // col_a = 'foo' AND col_other = 'foo' => {'foo'} (the non-target conjunct is ignored)
    Assert.assertEquals(
        ImmutableSet.of("foo"),
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, aRef, foo),
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, otherRef, foo)
            )),
            COL_A
        )
    );
  }

  @Test
  public void testAndWithNoConstraintOnColumnReturnsNull()
  {
    // AND of predicates that never touch the target column => null (full scan retained)
    Assert.assertNull(
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, otherRef, foo),
                rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, otherRef, bar)
            )),
            COL_A
        )
    );
  }

  @Test
  public void testEmptyFilterListReturnsNull()
  {
    Assert.assertNull(SystemSchemaFilters.extractColumnValues(ImmutableList.of(), COL_A));
  }

  @Test
  public void testRangePredicateReturnsNull()
  {
    // col > 'foo' -- a range cannot bound the value set
    Assert.assertNull(
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, aRef, foo)),
            COL_A
        )
    );
  }

  @Test
  public void testEqualityOnOtherColumnReturnsNull()
  {
    Assert.assertNull(
        SystemSchemaFilters.extractColumnValues(
            ImmutableList.of(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, otherRef, foo)),
            COL_A
        )
    );
  }

  @Test
  public void testNonRexCallNodeReturnsNull()
  {
    // A bare input ref (not a RexCall) constrains nothing.
    Assert.assertNull(SystemSchemaFilters.extractColumnValues(ImmutableList.of(aRef), COL_A));
  }

  @Test
  public void testMultiColumnExtractsEachIndependently()
  {
    // col_a = 'foo' AND col_b IN ('bar', 'baz')
    final RexNode filter = rexBuilder.makeCall(
        SqlStdOperatorTable.AND,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, aRef, foo),
        rexBuilder.makeIn(bRef, ImmutableList.of(bar, baz))
    );
    final Map<Integer, Set<String>> result =
        SystemSchemaFilters.extractColumnValues(ImmutableList.of(filter), COL_A, COL_B);
    Assert.assertEquals(
        ImmutableMap.of(
            COL_A, ImmutableSet.of("foo"),
            COL_B, ImmutableSet.of("bar", "baz")
        ),
        result
    );
  }

  @Test
  public void testMultiColumnOmitsUnconstrainedColumn()
  {
    // Only col_a is constrained; col_b must be absent from the map (not mapped to null).
    final RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, aRef, foo);
    final Map<Integer, Set<String>> result =
        SystemSchemaFilters.extractColumnValues(ImmutableList.of(filter), COL_A, COL_B);
    Assert.assertEquals(ImmutableMap.of(COL_A, ImmutableSet.of("foo")), result);
    Assert.assertFalse(result.containsKey(COL_B));
  }
}
