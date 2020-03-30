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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JoinableClauseTest
{
  public ExpectedException expectedException = ExpectedException.none();

  private Joinable joinable;
  private JoinableClause clause;

  @Before
  public void setUp() throws Exception
  {
    joinable = new IndexedTableJoinable(JoinTestHelper.createCountriesIndexedTable());
    clause = new JoinableClause(
        "j.",
        joinable,
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression("\"j.x\" == y", "j.", ExprMacroTable.nil())
    );
  }

  @Test
  public void test_getPrefix()
  {
    Assert.assertEquals("j.", clause.getPrefix());
  }

  @Test
  public void test_getJoinable()
  {
    Assert.assertEquals(joinable, clause.getJoinable());
  }

  @Test
  public void test_getJoinType()
  {
    Assert.assertEquals(JoinType.LEFT, clause.getJoinType());
  }

  @Test
  public void test_getCondition()
  {
    Assert.assertEquals("\"j.x\" == y", clause.getCondition().getOriginalExpression());
  }

  @Test
  public void test_getAvailableColumnsPrefixed()
  {
    Assert.assertEquals(
        ImmutableList.of("j.countryNumber", "j.countryIsoCode", "j.countryName"),
        clause.getAvailableColumnsPrefixed()
    );
  }

  @Test
  public void test_includesColumn_included()
  {
    Assert.assertTrue(clause.includesColumn("j.countryNumber"));
  }

  @Test
  public void test_includesColumn_notIncluded()
  {
    Assert.assertFalse(clause.includesColumn("countryNumber"));
  }

  @Test
  public void test_unprefix_included()
  {
    Assert.assertEquals("countryNumber", clause.unprefix("j.countryNumber"));
  }

  @Test
  public void test_unprefix_notIncluded()
  {
    expectedException.expect(IllegalArgumentException.class);
    clause.includesColumn("countryNumber");
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(JoinableClause.class).usingGetClass().verify();
  }
}
