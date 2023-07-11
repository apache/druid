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

package org.apache.druid.query.operator;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Set;

/**
 * Tests the WindowOperatorQuery, it would actually be a lot better to run this through some tests that actually
 * validate the operation of queries, but all of the efforts to build out test scaffolding and framework have gone
 * into building things out for SQL query operations.  As such, all of the tests validating the actual native
 * functionality actually run from the `druid-sql` module instead of this module.  It would be really cool to move
 * the SQL stuff into the processing module so that it can be handled in the same location here.
 * <p>
 * For now, view CalciteWindowQueryTest for actual tests that validate behavior.
 */
public class WindowOperatorQueryTest
{
  WindowOperatorQuery query;

  @Before
  public void setUp()
  {
    query = new WindowOperatorQuery(
        InlineDataSource.fromIterable(new ArrayList<>(), RowSignature.empty()),
        ImmutableMap.of("sally", "sue"),
        RowSignature.empty(),
        new ArrayList<>()
    );
  }

  @Test
  public void getOperators()
  {
    Assert.assertTrue(query.getOperators().isEmpty());
  }

  @Test
  public void getRowSignature()
  {
    Assert.assertEquals(0, query.getRowSignature().size());
  }

  @Test
  public void hasFilters()
  {
    Assert.assertFalse(query.hasFilters());
  }

  @Test
  public void getFilter()
  {
    Assert.assertNull(query.getFilter());
  }

  @Test
  public void getType()
  {
    Assert.assertEquals("windowOperator", query.getType());
  }

  @Test
  public void withOverriddenContext()
  {
    Assert.assertEquals("sue", query.context().get("sally"));
    final QueryContext context = query.withOverriddenContext(ImmutableMap.of("sally", "soo")).context();
    Assert.assertEquals("soo", context.get("sally"));
  }

  @Test
  public void withDataSource()
  {
    final Set<String> tableNames = query.getDataSource().getTableNames();
    Assert.assertEquals(0, tableNames.size());

    boolean exceptionThrown = false;
    try {
      query.withDataSource(new TableDataSource("bob"));
    }
    catch (IAE e) {
      // should fail trying to set a TableDataSource as TableDataSource is not currently allowed.
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void testEquals()
  {
    Assert.assertEquals(query, query);
    Assert.assertEquals(query, query.withDataSource(query.getDataSource()));
    Assert.assertNotEquals(query, query.toString());
  }
}
