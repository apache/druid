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

package org.apache.druid.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.MapLookupExtractorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.join.HashJoinSegment;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinTestHelper;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.SegmentId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class JoinAndLookupBenchmark
{
  private static final String LOOKUP_COUNTRY_CODE_TO_NAME = "country_code_to_name";
  private static final String LOOKUP_COUNTRY_NUMBER_TO_NAME = "country_number_to_name";

  static {
    NullHandling.initializeForTests();
  }

  @Param({"500000"})
  int rows;

  @Param({"0", "16384"})
  int columnCacheSizeBytes;

  private File tmpDir = null;
  private QueryableIndex index = null;
  private Segment baseSegment = null;
  private Segment hashJoinLookupStringKeySegment = null;
  private Segment hashJoinLookupLongKeySegment = null;
  private Segment hashJoinIndexedTableStringKeySegment = null;
  private Segment hashJoinIndexedTableLongKeySegment = null;
  private VirtualColumns lookupVirtualColumns = null;

  @TearDown
  public void tearDown() throws IOException
  {
    if (index != null) {
      index.close();
    }

    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Setup()
  public void setup() throws IOException
  {
    tmpDir = FileUtils.createTempDir();
    ColumnConfig columnConfig = () -> columnCacheSizeBytes;
    index = JoinTestHelper.createFactIndexBuilder(tmpDir, rows).buildMMappedIndex(columnConfig);

    final String prefix = "c.";

    baseSegment = new QueryableIndexSegment(index, SegmentId.dummy("join"));

    List<JoinableClause> joinableClausesLookupStringKey = ImmutableList.of(
        new JoinableClause(
            prefix,
            LookupJoinable.wrap(JoinTestHelper.createCountryIsoCodeToNameLookup()),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression(
                StringUtils.format("countryIsoCode == \"%sk\"", prefix),
                prefix,
                ExprMacroTable.nil()
            )
        )
    );

    JoinFilterPreAnalysis preAnalysisLookupStringKey =
        JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
            new JoinFilterPreAnalysisKey(
                new JoinFilterRewriteConfig(
                    false,
                    false,
                    false,
                    0
                ),
                joinableClausesLookupStringKey,
                VirtualColumns.EMPTY,
                null
            )
        );

    hashJoinLookupStringKeySegment = new HashJoinSegment(
        ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
        joinableClausesLookupStringKey,
        preAnalysisLookupStringKey
    );

    List<JoinableClause> joinableClausesLookupLongKey = ImmutableList.of(
        new JoinableClause(
            prefix,
            LookupJoinable.wrap(JoinTestHelper.createCountryIsoCodeToNameLookup()),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression(
                StringUtils.format("countryIsoCode == \"%sk\"", prefix),
                prefix,
                ExprMacroTable.nil()
            )
        )
    );

    JoinFilterPreAnalysis preAnalysisLookupLongKey =
        JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
            new JoinFilterPreAnalysisKey(
                new JoinFilterRewriteConfig(
                    false,
                    false,
                    false,
                    0
                ),
                joinableClausesLookupLongKey,
                VirtualColumns.EMPTY,
                null
            )
        );

    hashJoinLookupLongKeySegment = new HashJoinSegment(
        ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
        joinableClausesLookupLongKey,
        preAnalysisLookupLongKey
    );

    List<JoinableClause> joinableClausesIndexedTableStringKey = ImmutableList.of(
        new JoinableClause(
            prefix,
            new IndexedTableJoinable(JoinTestHelper.createCountriesIndexedTable()),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression(
                StringUtils.format("countryIsoCode == \"%scountryIsoCode\"", prefix),
                prefix,
                ExprMacroTable.nil()
            )
        )
    );

    JoinFilterPreAnalysis preAnalysisIndexedStringKey =
        JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
            new JoinFilterPreAnalysisKey(
                new JoinFilterRewriteConfig(
                    false,
                    false,
                    false,
                    0
                ),
                joinableClausesLookupLongKey,
                VirtualColumns.EMPTY,
                null
            )
        );

    hashJoinIndexedTableStringKeySegment = new HashJoinSegment(
        ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
        joinableClausesIndexedTableStringKey,
        preAnalysisIndexedStringKey
    );

    List<JoinableClause> joinableClausesIndexedTableLongKey = ImmutableList.of(
        new JoinableClause(
            prefix,
            new IndexedTableJoinable(JoinTestHelper.createCountriesIndexedTable()),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression(
                StringUtils.format("countryNumber == \"%scountryNumber\"", prefix),
                prefix,
                ExprMacroTable.nil()
            )
        )
    );

    JoinFilterPreAnalysis preAnalysisIndexedLongKey =
        JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
            new JoinFilterPreAnalysisKey(
                new JoinFilterRewriteConfig(
                    false,
                    false,
                    false,
                    0
                ),
                joinableClausesIndexedTableLongKey,
                VirtualColumns.EMPTY,
                null
            )
        );

    hashJoinIndexedTableLongKeySegment = new HashJoinSegment(
        ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
        joinableClausesIndexedTableLongKey,
        preAnalysisIndexedLongKey
    );

    final Map<String, String> countryCodeToNameMap = JoinTestHelper.createCountryIsoCodeToNameLookup().getMap();
    final Map<String, String> countryNumberToNameMap = JoinTestHelper.createCountryNumberToNameLookup().getMap();

    final ExprMacroTable exprMacroTable = new ExprMacroTable(
        ImmutableList.of(
            new LookupExprMacro(
                new LookupExtractorFactoryContainerProvider()
                {
                  @Override
                  public Set<String> getAllLookupNames()
                  {
                    return ImmutableSet.of(LOOKUP_COUNTRY_CODE_TO_NAME, LOOKUP_COUNTRY_NUMBER_TO_NAME);
                  }

                  @Override
                  public Optional<LookupExtractorFactoryContainer> get(String lookupName)
                  {
                    if (LOOKUP_COUNTRY_CODE_TO_NAME.equals(lookupName)) {
                      return Optional.of(
                          new LookupExtractorFactoryContainer(
                              "0",
                              new MapLookupExtractorFactory(countryCodeToNameMap, false)
                          )
                      );
                    } else if (LOOKUP_COUNTRY_NUMBER_TO_NAME.equals(lookupName)) {
                      return Optional.of(
                          new LookupExtractorFactoryContainer(
                              "0",
                              new MapLookupExtractorFactory(countryNumberToNameMap, false)
                          )
                      );
                    } else {
                      return Optional.empty();
                    }
                  }
                }
            )
        )
    );

    lookupVirtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                LOOKUP_COUNTRY_CODE_TO_NAME,
                "lookup(countryIsoCode, '" + LOOKUP_COUNTRY_CODE_TO_NAME + "')",
                ValueType.STRING,
                exprMacroTable
            ),
            new ExpressionVirtualColumn(
                LOOKUP_COUNTRY_NUMBER_TO_NAME,
                "lookup(countryNumber, '" + LOOKUP_COUNTRY_NUMBER_TO_NAME + "')",
                ValueType.STRING,
                exprMacroTable
            )
        )
    );
  }

  private static String getLastValue(final Sequence<Cursor> cursors, final String dimension)
  {
    return cursors.map(
        cursor -> {
          final DimensionSelector selector = cursor.getColumnSelectorFactory()
                                                   .makeDimensionSelector(DefaultDimensionSpec.of(dimension));

          if (selector.getValueCardinality() < 0) {
            String lastValue = null;
            while (!cursor.isDone()) {
              final IndexedInts row = selector.getRow();
              final int sz = row.size();
              for (int i = 0; i < sz; i++) {
                lastValue = selector.lookupName(row.get(i));
              }
              cursor.advance();
            }
            return lastValue;
          } else {
            int lastValue = -1;
            while (!cursor.isDone()) {
              final IndexedInts row = selector.getRow();
              final int sz = row.size();
              for (int i = 0; i < sz; i++) {
                lastValue = row.get(i);
              }
              cursor.advance();
            }
            return selector.lookupName(lastValue);
          }
        }
    ).accumulate(null, (acc, in) -> in);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void baseSegment(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = baseSegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "countryIsoCode"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void baseSegmentWithFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = baseSegment.asStorageAdapter().makeCursors(
        new SelectorDimFilter("countryIsoCode", "CA", null).toFilter(),
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "countryIsoCode"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinLookupStringKey(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinLookupStringKeySegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.v"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinLookupStringKeyWithFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinLookupStringKeySegment.asStorageAdapter().makeCursors(
        new SelectorDimFilter("c.v", "Canada", null).toFilter(),
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.v"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinLookupLongKey(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinLookupLongKeySegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.v"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinLookupLongKeyWithFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinLookupLongKeySegment.asStorageAdapter().makeCursors(
        new SelectorDimFilter("c.v", "Canada", null).toFilter(),
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.v"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinIndexedTableLongKey(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinIndexedTableLongKeySegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.countryName"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinIndexedTableLongKeyWithFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinIndexedTableLongKeySegment.asStorageAdapter().makeCursors(
        new SelectorDimFilter("c.countryName", "Canada", null).toFilter(),
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.countryName"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinIndexedTableStringKey(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinIndexedTableStringKeySegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.countryName"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void joinIndexedTableStringKeyWithFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = hashJoinIndexedTableStringKeySegment.asStorageAdapter().makeCursors(
        new SelectorDimFilter("c.countryName", "Canada", null).toFilter(),
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, "c.countryName"));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void lookupVirtualColumnStringKey(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = baseSegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        lookupVirtualColumns,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, LOOKUP_COUNTRY_CODE_TO_NAME));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void lookupVirtualColumnStringKeyWithFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = baseSegment.asStorageAdapter().makeCursors(
        new SelectorDimFilter(LOOKUP_COUNTRY_CODE_TO_NAME, "Canada", null).toFilter(),
        Intervals.ETERNITY,
        lookupVirtualColumns,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, LOOKUP_COUNTRY_CODE_TO_NAME));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void lookupVirtualColumnLongKey(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = baseSegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        lookupVirtualColumns,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, LOOKUP_COUNTRY_NUMBER_TO_NAME));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void lookupVirtualColumnLongKeyWithFilter(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = baseSegment.asStorageAdapter().makeCursors(
        new SelectorDimFilter(LOOKUP_COUNTRY_NUMBER_TO_NAME, "Canada", null).toFilter(),
        Intervals.ETERNITY,
        lookupVirtualColumns,
        Granularities.ALL,
        false,
        null
    );

    blackhole.consume(getLastValue(cursors, LOOKUP_COUNTRY_NUMBER_TO_NAME));
  }
}
