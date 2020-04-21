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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

public class QueryDataSourceTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final TimeseriesQuery queryOnTable =
      Druids.newTimeseriesQueryBuilder()
            .dataSource("foo")
            .intervals("2000/3000")
            .granularity(Granularities.ALL)
            .build();

  private final TimeseriesQuery queryOnLookup =
      Druids.newTimeseriesQueryBuilder()
            .dataSource(new LookupDataSource("lookyloo"))
            .intervals("2000/3000")
            .granularity(Granularities.ALL)
            .build();

  private final QueryDataSource queryOnTableDataSource = new QueryDataSource(queryOnTable);
  private final QueryDataSource queryOnLookupDataSource = new QueryDataSource(queryOnLookup);

  @Test
  public void test_getTableNames_table()
  {
    Assert.assertEquals(Collections.singleton("foo"), queryOnTableDataSource.getTableNames());
  }

  @Test
  public void test_getTableNames_lookup()
  {
    Assert.assertEquals(Collections.emptySet(), queryOnLookupDataSource.getTableNames());
  }

  @Test
  public void test_getChildren_table()
  {
    Assert.assertEquals(Collections.singletonList(new TableDataSource("foo")), queryOnTableDataSource.getChildren());
  }

  @Test
  public void test_getChildren_lookup()
  {
    Assert.assertEquals(
        Collections.singletonList(new LookupDataSource("lookyloo")),
        queryOnLookupDataSource.getChildren()
    );
  }

  @Test
  public void test_isCacheable_table()
  {
    Assert.assertFalse(queryOnTableDataSource.isCacheable());
  }

  @Test
  public void test_isCacheable_lookup()
  {
    Assert.assertFalse(queryOnLookupDataSource.isCacheable());
  }

  @Test
  public void test_isConcrete_table()
  {
    Assert.assertFalse(queryOnTableDataSource.isConcrete());
  }

  @Test
  public void test_isConcrete_lookup()
  {
    Assert.assertFalse(queryOnLookupDataSource.isConcrete());
  }

  @Test
  public void test_isGlobal_table()
  {
    Assert.assertFalse(queryOnTableDataSource.isGlobal());
  }

  @Test
  public void test_isGlobal_lookup()
  {
    Assert.assertTrue(queryOnLookupDataSource.isGlobal());
  }

  @Test
  public void test_withChildren_empty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Must have exactly one child");

    final DataSource ignored = queryOnLookupDataSource.withChildren(Collections.emptyList());
  }

  @Test
  public void test_withChildren_single()
  {
    final TableDataSource barTable = new TableDataSource("bar");

    final QueryDataSource transformed =
        (QueryDataSource) queryOnLookupDataSource.withChildren(Collections.singletonList(barTable));

    Assert.assertEquals(barTable, transformed.getQuery().getDataSource());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(QueryDataSource.class).usingGetClass().withNonnullFields("query").verify();
  }

  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final QueryDataSource deserialized = (QueryDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(queryOnTableDataSource),
        DataSource.class
    );

    Assert.assertEquals(queryOnTableDataSource, deserialized);
  }
}
