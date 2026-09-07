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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JoinableClauseTest
{
  private Joinable joinable;
  private JoinableClause clause;

  @BeforeEach
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
    Assertions.assertEquals("j.", clause.getPrefix());
  }

  @Test
  public void test_getJoinable()
  {
    Assertions.assertEquals(joinable, clause.getJoinable());
  }

  @Test
  public void test_getJoinType()
  {
    Assertions.assertEquals(JoinType.LEFT, clause.getJoinType());
  }

  @Test
  public void test_getCondition()
  {
    Assertions.assertEquals("\"j.x\" == y", clause.getCondition().getOriginalExpression());
  }

  @Test
  public void test_getAvailableColumnsPrefixed()
  {
    Assertions.assertEquals(
        ImmutableList.of("j.countryNumber", "j.countryIsoCode", "j.countryName"),
        clause.getAvailableColumnsPrefixed()
    );
  }

  @Test
  public void test_includesColumn_included()
  {
    Assertions.assertTrue(clause.includesColumn("j.countryNumber"));
  }

  @Test
  public void test_includesColumn_notIncluded()
  {
    Assertions.assertFalse(clause.includesColumn("countryNumber"));
  }

  @Test
  public void test_unprefix_included()
  {
    Assertions.assertEquals("countryNumber", clause.unprefix("j.countryNumber"));
  }

  @Test
  public void test_unprefix_notIncluded()
  {
    clause.includesColumn("countryNumber");
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(JoinableClause.class).usingGetClass().verify();
  }
}
