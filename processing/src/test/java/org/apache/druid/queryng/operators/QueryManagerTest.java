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

package org.apache.druid.queryng.operators;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.queryng.config.QueryNGConfig;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.fragment.NullQueryManagerFactory;
import org.apache.druid.queryng.fragment.QueryManager;
import org.apache.druid.queryng.fragment.QueryManagerFactory;
import org.apache.druid.queryng.fragment.QueryManagerFactoryImpl;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class QueryManagerTest
{
  @Test
  public void testConfig()
  {
    Query<?> scanQuery = new Druids.ScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    Query<?> scanQueryWithContext = scanQuery.withOverriddenContext(
        ImmutableMap.of(QueryNGConfig.CONTEXT_VAR, true));
    Query<?> otherQuery = Druids.newTimeseriesQueryBuilder()
        .dataSource("foo")
        .intervals(new MultipleIntervalSegmentSpec(
            ImmutableList.of(Intervals.ETERNITY)))
        .build();

    // Completely disabled.
    QueryNGConfig config = QueryNGConfig.create(false, false);
    assertFalse(config.enabled());
    assertFalse(config.isEnabled(scanQuery));
    assertFalse(config.isEnabled(scanQueryWithContext));
    assertFalse(config.isEnabled(otherQuery));

    // Enabled.
    config = QueryNGConfig.create(true, false);
    assertTrue(config.enabled());
    assertTrue(config.isEnabled(scanQuery));
    assertTrue(config.isEnabled(scanQueryWithContext));
    assertTrue(config.isEnabled(otherQuery));

    // Enabled, but only if requested in context.
    config = QueryNGConfig.create(true, true);
    assertTrue(config.enabled());
    assertFalse(config.isEnabled(scanQuery));
    assertTrue(config.isEnabled(scanQueryWithContext));
    assertFalse(config.isEnabled(otherQuery));
  }

  @Test
  public void testFactory()
  {
    Query<?> query = new Druids.ScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();

    // Operators blocked by query: no gating context variable
    QueryNGConfig enableConfig = QueryNGConfig.create(true, true);
    assertTrue(enableConfig.enabled());
    QueryManagerFactory enableFactory = new QueryManagerFactoryImpl(enableConfig);
    assertNull(enableFactory.create(query));
    QueryManagerFactory nullFactory = new NullQueryManagerFactory();

    QueryNGConfig disableConfig = QueryNGConfig.create(false, false);
    assertFalse(disableConfig.enabled());
    QueryManagerFactory disableFactory = new QueryManagerFactoryImpl(disableConfig);
    assertNull(disableFactory.create(query));
    assertNull(nullFactory.create(query));

    // Enable at query level. Use of operators gated by config.
    query = query.withOverriddenContext(
        ImmutableMap.of(QueryNGConfig.CONTEXT_VAR, true));
    assertNotNull(enableFactory.create(query));
    assertNull(disableFactory.create(query));
    assertNull(nullFactory.create(query));
  }

  @Test
  public void testQueryPlus()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Query<?> query = new Druids.ScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<?> queryPlus = QueryPlus.wrap(query);
    assertFalse(QueryNGConfig.enabledFor(queryPlus));
    queryPlus = queryPlus.withFragment(fragment);
    assertTrue(QueryNGConfig.enabledFor(queryPlus));
    assertSame(fragment, queryPlus.fragment());
  }

  private static class DummySupplier<T> implements Supplier<T>
  {
    T results;

    @Override
    public T get()
    {
      return results;
    }
  }

  @Test
  public void testMultipleFragments()
  {
    FragmentManager rootFragment = Fragments.defaultFragment();
    QueryManager query = rootFragment.query();

    // Root fragment
    DummySupplier<List<Integer>> innerResults = new DummySupplier<>();
    Operator<Integer> op = new IterableReader<Integer>(rootFragment, innerResults);
    rootFragment.registerRoot(op);

    // Child fragment
    FragmentManager childFragment = query.createChildFragment(
        "child",
        ResponseContext.createEmpty()
    );
    MockOperator<Integer> op2 = MockOperator.ints(childFragment, 4);
    childFragment.registerRoot(op2);
    rootFragment.registerChild(op, 0, 2);

    // Simulate a concurrent run.
    innerResults.results = childFragment.toList();
    List<Integer> results = rootFragment.toList();
    assertEquals(4, results.size());
    assertEquals(innerResults.results, results);
  }
}
