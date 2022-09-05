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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.config.NullHandlingTest;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestQuery;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.FalseDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JoinableFactoryWrapperTest extends NullHandlingTest
{
  public static final JoinableFactoryWrapper NOOP_JOINABLE_FACTORY_WRAPPER = new JoinableFactoryWrapper(
      NoopJoinableFactory.INSTANCE
  );

  private static final Map<String, String> TEST_LOOKUP =
      ImmutableMap.<String, String>builder()
          .put("MX", "Mexico")
          .put("NO", "Norway")
          .put("SV", "El Salvador")
          .put("US", "United States")
          .put("", "Empty key")
          .build();

  private static final Set<String> TEST_LOOKUP_KEYS =
      NullHandling.sqlCompatible()
      ? TEST_LOOKUP.keySet()
      : Sets.difference(TEST_LOOKUP.keySet(), Collections.singleton(""));

  private static final InlineDataSource INDEXED_TABLE_DS = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{"Mexico"},
          new Object[]{"Norway"},
          new Object[]{"El Salvador"},
          new Object[]{"United States"},
          new Object[]{"United States"}
      ),
      RowSignature.builder().add("country", ColumnType.STRING).build()
  );

  private static final InlineDataSource NULL_INDEXED_TABLE_DS = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{null}
      ),
      RowSignature.builder().add("nullCol", ColumnType.STRING).build()
  );

  private static final IndexedTable TEST_INDEXED_TABLE = new RowBasedIndexedTable<>(
      INDEXED_TABLE_DS.getRowsAsList(),
      INDEXED_TABLE_DS.rowAdapter(),
      INDEXED_TABLE_DS.getRowSignature(),
      ImmutableSet.of("country"),
      DateTimes.nowUtc().toString()
  );

  private static final IndexedTable TEST_NULL_INDEXED_TABLE = new RowBasedIndexedTable<>(
      NULL_INDEXED_TABLE_DS.getRowsAsList(),
      NULL_INDEXED_TABLE_DS.rowAdapter(),
      NULL_INDEXED_TABLE_DS.getRowSignature(),
      ImmutableSet.of("nullCol"),
      DateTimes.nowUtc().toString()
  );

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
    final PreJoinableClause clause = makePreJoinableClause(
        INDEXED_TABLE_DS,
        "country == \"j.country\"",
        "j.",
        JoinType.LEFT
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
    final PreJoinableClause clause = makePreJoinableClause(
        INDEXED_TABLE_DS,
        "country == \"j.country\"",
        "j.",
        JoinType.LEFT
    );

    JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new InlineJoinableFactory());
    final Function<SegmentReference, SegmentReference> segmentMapFn = joinableFactoryWrapper.createSegmentMapFn(
        null,
        ImmutableList.of(clause),
        new AtomicLong(),
        new TestQuery(
            new TableDataSource("test"),
            new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
            false,
            new HashMap<>()
        )
    );

    Assert.assertNotSame(Function.identity(), segmentMapFn);
  }

  @Test
  public void test_createSegmentMapFn_usableClause_joinToFilterEnabled() throws IOException
  {
    final PreJoinableClause clause = makePreJoinableClause(
        INDEXED_TABLE_DS,
        "country == \"j.country\"",
        "j.",
        JoinType.INNER
    );
    // required columns are necessary for the rewrite
    final TestQuery queryWithRequiredColumnsAndJoinFilterRewrite = (TestQuery) new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        new HashMap<>()
    ).withOverriddenContext(ImmutableMap.of(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, "true"));
    queryWithRequiredColumnsAndJoinFilterRewrite.setRequiredColumns(ImmutableSet.of("country"));

    final JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(new InlineJoinableFactory());
    final Function<SegmentReference, SegmentReference> segmentMapFn = joinableFactoryWrapper.createSegmentMapFn(
        null,
        ImmutableList.of(clause),
        new AtomicLong(),
        queryWithRequiredColumnsAndJoinFilterRewrite
    );

    // dummy segment
    final SegmentReference baseSegmentReference = ReferenceCountingSegment.wrapRootGenerationSegment(
        new QueryableIndexSegment(
            JoinTestHelper.createFactIndexBuilder(temporaryFolder.newFolder()).buildMMappedIndex(),
            SegmentId.dummy("facts")
        )
    );

    // check the output contains the conversion filter
    Assert.assertNotSame(Function.identity(), segmentMapFn);
    final SegmentReference joinSegmentReference = segmentMapFn.apply(baseSegmentReference);
    Assert.assertTrue(joinSegmentReference instanceof HashJoinSegment);
    HashJoinSegment hashJoinSegment = (HashJoinSegment) joinSegmentReference;
    Assert.assertEquals(
        hashJoinSegment.getBaseFilter(),
        new InDimFilter(
            "country",
            INDEXED_TABLE_DS.getRowsAsList().stream().map(row -> row[0].toString()).collect(Collectors.toSet())
        )
    );
    // the returned clause list is not comparable with an expected clause list since the Joinable
    // class member in JoinableClause doesn't implement equals method in its implementations
    Assert.assertEquals(hashJoinSegment.getClauses().size(), 1);
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

  @Test
  public void test_convertJoinsToFilters_convertInnerJoin()
  {
    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(
            new JoinableClause(
                "j.",
                LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
                JoinType.INNER,
                JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
            )
        ),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS)),
            ImmutableList.of()
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToPartialFilters_convertInnerJoin()
  {
    JoinableClause joinableClause = new JoinableClause(
        "j.",
        new IndexedTableJoinable(TEST_INDEXED_TABLE),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.country\"", "j.", ExprMacroTable.nil())
    );
    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(joinableClause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter(
                "x",
                INDEXED_TABLE_DS.getRowsAsList().stream().map(row -> row[0].toString()).collect(Collectors.toSet()))
            ),
            ImmutableList.of(joinableClause) // the joinable clause remains intact since we've duplicates in country column
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_convertTwoInnerJoins()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "__j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("x == \"__j.k\"", "__j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS), new InDimFilter("x", TEST_LOOKUP_KEYS)),
            ImmutableList.of(clauses.get(2))
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToPartialAndFullFilters_convertMultipleInnerJoins()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ), // this joinable will be fully converted to a filter
        new JoinableClause(
            "_j.",
            new IndexedTableJoinable(TEST_INDEXED_TABLE),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"_j.country\"", "_j.", ExprMacroTable.nil())
        ), // this joinable will be partially converted to a filter since we've duplicates on country column
        new JoinableClause(
            "__j.",
            new IndexedTableJoinable(TEST_INDEXED_TABLE),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("x == \"__j.country\"", "__j.", ExprMacroTable.nil())
        ) // this joinable will not be converted to filter since its a LEFT join
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(
                new InDimFilter("x", TEST_LOOKUP_KEYS),
                new InDimFilter(
                    "x",
                    INDEXED_TABLE_DS.getRowsAsList().stream().map(row -> row[0].toString()).collect(Collectors.toSet())
                )
            ),
            ImmutableList.of(clauses.get(1), clauses.get(2))
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertTooManyValues()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(
            clause
        ),
        ImmutableSet.of("x"),
        2
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertLeftJoin()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_partialConvertWhenColumnIsUsed()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x", "j.k"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS)),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_convertToFalseFilterWhenOnlyNullValues()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        new IndexedTableJoinable(TEST_NULL_INDEXED_TABLE),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == \"j.nullCol\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(FalseFilter.instance()),
            ImmutableList.of()
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertLhsFunctions()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("concat(x,'') == \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertRhsFunctions()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x == concat(\"j.k\",'')", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertNonEquiJoin()
  {
    final JoinableClause clause = new JoinableClause(
        "j.",
        LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
        JoinType.INNER,
        JoinConditionAnalysis.forExpression("x != \"j.k\"", "j.", ExprMacroTable.nil())
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        ImmutableList.of(clause),
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            ImmutableList.of(clause)
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_dontConvertJoinsDependedOnByLaterJoins()
  {
    // in this multi-join, a join matching two right sides is kept first to ensure :
    // 1. there is no filter on the right side table column j.k
    // 2. the right side matching join gets considered for join conversion always (since it is the first join clause)
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("\"j.k\" == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(),
            clauses
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_partialConvertJoinsDependedOnByLaterJoins()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("\"j.k\" == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "__j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("x == \"__j.k\"", "__j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS)),
            clauses
        ),
        conversion
    );
  }

  @Test
  public void test_convertJoinsToFilters_partialConvertJoinsDependedOnByLaterJoins2()
  {
    final ImmutableList<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            "j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"j.k\"", "j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "_j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.INNER,
            JoinConditionAnalysis.forExpression("x == \"_j.k\"", "_j.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "__j.",
            LookupJoinable.wrap(new MapLookupExtractor(TEST_LOOKUP, false)),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("\"_j.v\" == \"__j.k\"", "__j.", ExprMacroTable.nil())
        )
    );

    final Pair<List<Filter>, List<JoinableClause>> conversion = JoinableFactoryWrapper.convertJoinsToFilters(
        clauses,
        ImmutableSet.of("x"),
        Integer.MAX_VALUE
    );

    Assert.assertEquals(
        Pair.of(
            ImmutableList.of(new InDimFilter("x", TEST_LOOKUP_KEYS), new InDimFilter("x", TEST_LOOKUP_KEYS)),
            clauses.subList(1, clauses.size())
        ),
        conversion
    );
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
