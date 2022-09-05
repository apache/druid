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

package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.join.JoinType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Collections;

public class JoinDataSourceTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final TableDataSource fooTable = new TableDataSource("foo");
  private final TableDataSource barTable = new TableDataSource("bar");
  private final LookupDataSource lookylooLookup = new LookupDataSource("lookyloo");

  private final JoinDataSource joinTableToLookup = JoinDataSource.create(
      fooTable,
      lookylooLookup,
      "j.",
      "x == \"j.x\"",
      JoinType.LEFT,
      null,
      ExprMacroTable.nil()
  );

  private final JoinDataSource joinTableToTable = JoinDataSource.create(
      fooTable,
      barTable,
      "j.",
      "x == \"j.x\"",
      JoinType.LEFT,
      null,
      ExprMacroTable.nil()
  );

  @Test
  public void test_getTableNames_tableToTable()
  {
    Assert.assertEquals(ImmutableSet.of("foo", "bar"), joinTableToTable.getTableNames());
  }

  @Test
  public void test_getTableNames_tableToLookup()
  {
    Assert.assertEquals(Collections.singleton("foo"), joinTableToLookup.getTableNames());
  }

  @Test
  public void test_getChildren_tableToTable()
  {
    Assert.assertEquals(ImmutableList.of(fooTable, barTable), joinTableToTable.getChildren());
  }

  @Test
  public void test_getChildren_tableToLookup()
  {
    Assert.assertEquals(ImmutableList.of(fooTable, lookylooLookup), joinTableToLookup.getChildren());
  }

  @Test
  public void test_isCacheable_tableToTable()
  {
    Assert.assertTrue(joinTableToTable.isCacheable(true));
    Assert.assertTrue(joinTableToTable.isCacheable(false));
  }

  @Test
  public void test_isCacheable_lookup()
  {
    Assert.assertFalse(joinTableToLookup.isCacheable(true));
    Assert.assertFalse(joinTableToLookup.isCacheable(false));
  }

  @Test
  public void test_isConcrete_tableToTable()
  {
    Assert.assertFalse(joinTableToTable.isConcrete());
  }

  @Test
  public void test_isConcrete_tableToLookup()
  {
    Assert.assertFalse(joinTableToLookup.isConcrete());
  }

  @Test
  public void test_isGlobal_tableToTable()
  {
    Assert.assertFalse(joinTableToTable.isGlobal());
  }

  @Test
  public void test_isGlobal_tableToLookup()
  {
    Assert.assertFalse(joinTableToLookup.isGlobal());
  }

  @Test
  public void test_withChildren_empty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected [2] children, got [0]");

    final DataSource ignored = joinTableToTable.withChildren(Collections.emptyList());
  }

  @Test
  public void test_withChildren_two()
  {
    final DataSource transformed = joinTableToTable.withChildren(ImmutableList.of(fooTable, lookylooLookup));

    Assert.assertEquals(joinTableToLookup, transformed);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(JoinDataSource.class)
                  .usingGetClass()
                  .withNonnullFields("left", "right", "rightPrefix", "conditionAnalysis", "joinType")
                  .verify();
  }

  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    JoinDataSource joinDataSource = JoinDataSource.create(
        new TableDataSource("table1"),
        new TableDataSource("table2"),
        "j.",
        "x == \"j.x\"",
        JoinType.LEFT,
        TrueDimFilter.instance(),
        ExprMacroTable.nil()
    );

    final JoinDataSource deserialized = (JoinDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(joinDataSource),
        DataSource.class
    );

    Assert.assertEquals(joinDataSource, deserialized);
  }

  @Test
  public void testException_leftFilterOnNonTableSource()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("left filter is only supported if left data source is direct table access");
    JoinDataSource ignored = JoinDataSource.create(
        new QueryDataSource(Mockito.mock(Query.class)),
        new TableDataSource("table"),
        "j.",
        "x == \"j.x\"",
        JoinType.LEFT,
        TrueDimFilter.instance(),
        ExprMacroTable.nil()
    );
  }

  @Test
  public void testLeftFilter()
  {
    JoinDataSource dataSource = JoinDataSource.create(
        new TableDataSource("table1"),
        new TableDataSource("table2"),
        "j.",
        "x == \"j.x\"",
        JoinType.LEFT,
        TrueDimFilter.instance(),
        ExprMacroTable.nil()
    );
    Assert.assertEquals(TrueDimFilter.instance(), dataSource.getLeftFilter());
  }

  @Test
  public void testVirtualColumnCandidates()
  {
    JoinDataSource dataSource = JoinDataSource.create(
        new TableDataSource("table1"),
        new TableDataSource("table2"),
        "j.",
        "x == \"j.x\"",
        JoinType.LEFT,
        null,
        ExprMacroTable.nil()
    );
    Assert.assertEquals(dataSource.getVirtualColumnCandidates(), ImmutableSet.of("x"));
  }
}
