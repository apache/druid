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
import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestQuery;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.FalseDimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class JoinableFactoryWrapperTest
{
  private static final JoinFilterRewriteConfig DEFAULT_JOIN_FILTER_REWRITE_CONFIG = new JoinFilterRewriteConfig(
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN,
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE,
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS,
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
  );

  private static final JoinableFactoryWrapper NOOP_JOINABLE_FACTORY_WRAPPER = new JoinableFactoryWrapper(
      NoopJoinableFactory.INSTANCE);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void test_createSegmentMapFn_noClauses()
  {
    final Function<SegmentReference, SegmentReference> segmentMapFn = NOOP_JOINABLE_FACTORY_WRAPPER.createSegmentMapFn(
        null,
        ImmutableList.of(),
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

    final Function<SegmentReference, SegmentReference> ignored = NOOP_JOINABLE_FACTORY_WRAPPER.createSegmentMapFn(
        null,
        ImmutableList.of(clause),
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

    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactory()
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
    });
    final Function<SegmentReference, SegmentReference> segmentMapFn = joinableFactoryWrapper.createSegmentMapFn(
        null,
        ImmutableList.of(clause),
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
  public void test_computeJoinDataSourceCacheKey_noClauses()
  {
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    DataSource dataSource = new NoopDataSource();
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.emptyList());
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty());
    EasyMock.expect(analysis.getDataSource()).andReturn(dataSource);
    EasyMock.replay(analysis);
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());

    expectedException.expect(IAE.class);
    expectedException.expectMessage(StringUtils.format(
        "No join clauses to build the cache key for data source [%s]",
        dataSource
    ));
    joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_noHashJoin()
  {
    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.");
    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_2", "x != \"h.x\"", "h.");
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Arrays.asList(clause1, clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.of(TrueDimFilter.instance())).anyTimes();
    EasyMock.replay(analysis);
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());
    Optional<byte[]> cacheKey = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);

    Assert.assertFalse(cacheKey.isPresent());
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_cachingUnsupported()
  {
    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.");
    DataSource dataSource = new LookupDataSource("lookup");
    PreJoinableClause clause2 = makePreJoinableClause(dataSource, "x == \"h.x\"", "h.", JoinType.LEFT);
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Arrays.asList(clause1, clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.of(TrueDimFilter.instance())).anyTimes();
    EasyMock.replay(analysis);
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());
    Optional<byte[]> cacheKey = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);

    Assert.assertFalse(cacheKey.isPresent());
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_usableClauses()
  {

    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.");
    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_2", "x == \"h.x\"", "h.");
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Arrays.asList(clause1, clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    EasyMock.replay(analysis);
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());
    Optional<byte[]> cacheKey = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);

    Assert.assertTrue(cacheKey.isPresent());
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_keyChangesWithExpression()
  {
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());

    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "y == \"j.y\"", "j.");
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause1)).anyTimes();
    EasyMock.replay(analysis);

    Optional<byte[]> cacheKey1 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey1.isPresent());
    Assert.assertNotEquals(0, cacheKey1.get().length);

    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.");
    EasyMock.reset(analysis);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    EasyMock.replay(analysis);
    Optional<byte[]> cacheKey2 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey2.isPresent());

    Assert.assertFalse(Arrays.equals(cacheKey1.get(), cacheKey2.get()));
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_keyChangesWithJoinType()
  {
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());

    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.", JoinType.LEFT);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause1)).anyTimes();
    EasyMock.replay(analysis);

    Optional<byte[]> cacheKey1 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey1.isPresent());
    Assert.assertNotEquals(0, cacheKey1.get().length);

    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.", JoinType.INNER);
    EasyMock.reset(analysis);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    EasyMock.replay(analysis);
    Optional<byte[]> cacheKey2 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey2.isPresent());

    Assert.assertFalse(Arrays.equals(cacheKey1.get(), cacheKey2.get()));
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_keyChangesWithPrefix()
  {
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());

    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "abc == xyz", "ab");
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause1)).anyTimes();
    EasyMock.replay(analysis);

    Optional<byte[]> cacheKey1 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey1.isPresent());
    Assert.assertNotEquals(0, cacheKey1.get().length);

    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_1", "abc == xyz", "xy");
    EasyMock.reset(analysis);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    EasyMock.replay(analysis);
    Optional<byte[]> cacheKey2 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey2.isPresent());

    Assert.assertFalse(Arrays.equals(cacheKey1.get(), cacheKey2.get()));
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_keyChangesWithBaseFilter()
  {
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.of(TrueDimFilter.instance())).anyTimes();
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());

    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "abc == xyz", "ab");
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause1)).anyTimes();
    EasyMock.replay(analysis);

    Optional<byte[]> cacheKey1 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey1.isPresent());
    Assert.assertNotEquals(0, cacheKey1.get().length);

    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_1", "abc == xyz", "ab");
    EasyMock.reset(analysis);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.of(FalseDimFilter.instance())).anyTimes();
    EasyMock.replay(analysis);
    Optional<byte[]> cacheKey2 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey2.isPresent());

    Assert.assertFalse(Arrays.equals(cacheKey1.get(), cacheKey2.get()));
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_keyChangesWithJoinable()
  {
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());

    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.");
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause1)).anyTimes();
    EasyMock.replay(analysis);

    Optional<byte[]> cacheKey1 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey1.isPresent());
    Assert.assertNotEquals(0, cacheKey1.get().length);

    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_2", "x == \"j.x\"", "j.");
    EasyMock.reset(analysis);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();

    EasyMock.replay(analysis);
    Optional<byte[]> cacheKey2 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey2.isPresent());

    Assert.assertFalse(Arrays.equals(cacheKey1.get(), cacheKey2.get()));
  }

  @Test
  public void test_computeJoinDataSourceCacheKey_sameKeyForSameJoin()
  {
    DataSourceAnalysis analysis = EasyMock.mock(DataSourceAnalysis.class);
    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new JoinableFactoryWithCacheKey());

    PreJoinableClause clause1 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.");
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause1)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    EasyMock.replay(analysis);

    Optional<byte[]> cacheKey1 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey1.isPresent());
    Assert.assertNotEquals(0, cacheKey1.get().length);

    PreJoinableClause clause2 = makeGlobalPreJoinableClause("dataSource_1", "x == \"j.x\"", "j.");
    EasyMock.reset(analysis);
    EasyMock.expect(analysis.getPreJoinableClauses()).andReturn(Collections.singletonList(clause2)).anyTimes();
    EasyMock.expect(analysis.getJoinBaseTableFilter()).andReturn(Optional.empty()).anyTimes();
    EasyMock.replay(analysis);
    Optional<byte[]> cacheKey2 = joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis);
    Assert.assertTrue(cacheKey2.isPresent());

    Assert.assertArrayEquals(cacheKey1.get(), cacheKey2.get());
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

    JoinPrefixUtils.checkPrefixesForDuplicatesAndShadowing(prefixes);
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

    JoinPrefixUtils.checkPrefixesForDuplicatesAndShadowing(prefixes);
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

    JoinPrefixUtils.checkPrefixesForDuplicatesAndShadowing(prefixes);
  }

  private PreJoinableClause makeGlobalPreJoinableClause(String tableName, String expression, String prefix)
  {
    return makeGlobalPreJoinableClause(tableName, expression, prefix, JoinType.LEFT);
  }

  private PreJoinableClause makeGlobalPreJoinableClause(
      String tableName,
      String expression,
      String prefix,
      JoinType joinType
  )
  {
    GlobalTableDataSource dataSource = new GlobalTableDataSource(tableName);
    return makePreJoinableClause(dataSource, expression, prefix, joinType);
  }

  private PreJoinableClause makePreJoinableClause(
      DataSource dataSource,
      String expression,
      String prefix,
      JoinType joinType
  )
  {
    JoinConditionAnalysis conditionAnalysis = JoinConditionAnalysis.forExpression(
        expression,
        prefix,
        ExprMacroTable.nil()
    );
    return new PreJoinableClause(
        prefix,
        dataSource,
        joinType,
        conditionAnalysis
    );
  }

  private static class JoinableFactoryWithCacheKey extends NoopJoinableFactory
  {
    @Override
    public Optional<byte[]> computeJoinCacheKey(DataSource dataSource, JoinConditionAnalysis condition)
    {
      if (dataSource.isCacheable(false) && condition.canHashJoin()) {
        String tableName = Iterators.getOnlyElement(dataSource.getTableNames().iterator());
        return Optional.of(StringUtils.toUtf8(tableName));
      }
      return Optional.empty();
    }
  }
}
