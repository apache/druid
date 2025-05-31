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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.join.JoinType;
import org.junit.Assert;
import org.junit.Test;

public class PreJoinableClauseTest
{
  private final PreJoinableClause clause = makePreJoinableClause();

  private PreJoinableClause makePreJoinableClause()
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("bar"),
        new TableDataSource("foo"),
        "j.",
        "x == \"j.x\"",
        JoinType.LEFT,
        TrueDimFilter.instance(),
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST

    );
    return new PreJoinableClause(join);
  }


  @Test
  public void test_getPrefix()
  {
    Assert.assertEquals("j.", clause.getPrefix());
  }

  @Test
  public void test_getJoinType()
  {
    Assert.assertEquals(JoinType.LEFT, clause.getJoinType());
  }

  @Test
  public void test_getCondition()
  {
    Assert.assertEquals("x == \"j.x\"", clause.getCondition().getOriginalExpression());
  }

  @Test
  public void test_getDataSource()
  {
    Assert.assertEquals(new TableDataSource("foo"), clause.getDataSource());
  }

  @Test
  public void test_maybeUnwrapRestrictedDataSource()
  {
    Assert.assertEquals(new TableDataSource("foo"), clause.maybeUnwrapRestrictedDataSource());

    RestrictedDataSource left = RestrictedDataSource.create(
        new TableDataSource("bar"),
        RowFilterPolicy.from(new NullFilter("col", null))
    );
    RestrictedDataSource restrictedDataSource1 = RestrictedDataSource.create(
        new TableDataSource("foo"),
        NoRestrictionPolicy.instance()
    );
    RestrictedDataSource restrictedDataSource2 = RestrictedDataSource.create(
        new TableDataSource("foo"),
        RowFilterPolicy.from(new NullFilter("col", null))
    );
    JoinDataSource join1CanbeUnwrapped = JoinDataSource.create(
        left,
        restrictedDataSource1,
        "j.",
        "x == \"j.x\"",
        JoinType.LEFT,
        TrueDimFilter.instance(),
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );
    JoinDataSource join2NotSupported = JoinDataSource.create(
        left,
        restrictedDataSource2,
        "j.",
        "x == \"j.x\"",
        JoinType.LEFT,
        TrueDimFilter.instance(),
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );
    Assert.assertEquals(
        new TableDataSource("foo"),
        new PreJoinableClause(join1CanbeUnwrapped).maybeUnwrapRestrictedDataSource()
    );
    QueryUnsupportedException e = Assert.assertThrows(
        QueryUnsupportedException.class,
        () -> new PreJoinableClause(join2NotSupported).maybeUnwrapRestrictedDataSource()
    );
    Assert.assertEquals(
        "Restricted data source [foo] with policy [RowFilterPolicy{rowFilter=col IS NULL}] is not supported",
        e.getMessage()
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(PreJoinableClause.class)
                  .usingGetClass()
                  .withNonnullFields("prefix", "dataSource", "joinType", "condition")
                  .verify();
  }
}
