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
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.join.HashJoinSegment;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.segment.join.table.BroadcastSegmentIndexedTable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class IndexedTableJoinCursorBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final List<Set<String>> PROJECTIONS = ImmutableList.of(
      // 0 string key rhs
      ImmutableSet.of("j0.stringKey"),
      // 1 string key lhs
      ImmutableSet.of("stringKey"),
      // 2 numeric key rhs
      ImmutableSet.of("j0.longKey"),
      // 3 numeric key lhs
      ImmutableSet.of("longKey"),
      // 4 string rhs
      ImmutableSet.of("j0.string5"),
      // 5 string lhs
      ImmutableSet.of("string5"),
      // 6 numeric lhs
      ImmutableSet.of("j0.long4"),
      // 7 numeric rhs
      ImmutableSet.of("long4"),
      // 8 multi column projection all rhs
      ImmutableSet.of("j0.stringKey", "j0.longKey", "j0.string1"),
      // 9 multi column projection all lhs
      ImmutableSet.of("stringKey", "longKey", "string1"),
      // 10 big projection all rhs
      ImmutableSet.of("j0.string1", "j0.string2", "j0.string3", "j0.string4", "j0.string5", "j0.long1", "j0.float1", "j0.double1"),
      // 11 big projection, all lhs
      ImmutableSet.of("string1", "string2", "string3", "string4", "string5", "long1", "float1", "double1"),
      // 12 big projection, mix of lhs and rhs
      ImmutableSet.of("j0.string1", "string2", "j0.string3", "string4", "j0.string5", "long1", "j0.float1", "j0.double1")
  );

  @Param({"50000"})
  int rowsPerSegment;

  @Param({"5000000"})
  int rowsPerTableSegment;

  @Param({"segment"})
  String indexedTableType;

  @Param({"0", "1", "2", "3", "6", "7", "8", "9", "10", "11", "12"})
  int projection;

  @Param({"string1,stringKey", "stringKey,stringKey", "long3,longKey", "longKey,longKey"})
  String joinColumns;

  private Set<String> keyColumns = ImmutableSet.of("stringKey", "longKey");

  boolean enableFilterPushdown = false;
  boolean enableFilterRewrite = false;
  boolean enableFilterRewriteValueFilters = false;
  private Set<String> projectionColumns = null;

  private IndexedTable table = null;
  private QueryableIndexSegment baseSegment = null;
  private QueryableIndexSegment joinSegment = null;
  private Segment hashJoinSegment = null;

  private Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup()
  {
    baseSegment = makeQueryableIndexSegment(closer, "regular", rowsPerSegment);
    joinSegment = makeQueryableIndexSegment(closer, "join", rowsPerTableSegment);
    table = closer.register(makeTable(indexedTableType, keyColumns, joinSegment));

    final String prefix = "j0.";
    projectionColumns = PROJECTIONS.get(projection);
    final String[] split = joinColumns.split(",");
    final String lhsJoinColumn = split[0];
    final String rhsJoinColumn = split[1];
    
    final List<JoinableClause> clauses = ImmutableList.of(
        new JoinableClause(
            prefix,
            new IndexedTableJoinable(table),
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression(
                StringUtils.format("%s == \"%s%s\"", lhsJoinColumn, prefix, rhsJoinColumn),
                prefix,
                ExprMacroTable.nil()
            )
        )
    );

    final JoinFilterPreAnalysis preAnalysis =
        JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
            new JoinFilterPreAnalysisKey(
                new JoinFilterRewriteConfig(
                    enableFilterPushdown,
                    enableFilterRewrite,
                    enableFilterRewriteValueFilters,
                    QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
                ),
                clauses,
                VirtualColumns.EMPTY,
                null
            )
        );

    hashJoinSegment = closer.register(
        new HashJoinSegment(
            ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
            null,
            clauses,
            preAnalysis
        )
    );
  }

  @TearDown
  public void tearDown() throws IOException
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void hashJoinCursorColumnValueSelectors(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = makeCursors();
    int rowCount = processRowsValueSelector(blackhole, cursors, projectionColumns);
    blackhole.consume(rowCount);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void hashJoinCursorDimensionSelectors(Blackhole blackhole)
  {
    final Sequence<Cursor> cursors = makeCursors();
    int rowCount = processRowsDimensionSelectors(blackhole, cursors, projectionColumns);
    blackhole.consume(rowCount);
  }

  private Sequence<Cursor> makeCursors()
  {
    return hashJoinSegment.asStorageAdapter().makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
  }


  public static IndexedTable makeTable(
      final String indexedTableType,
      Set<String> keyColumns,
      QueryableIndexSegment tableSegment
  )
  {
    IndexedTable table;
    switch (indexedTableType) {
      case "segment":
        table = new BroadcastSegmentIndexedTable(tableSegment, keyColumns, tableSegment.getId().getVersion());
        break;
      default:
        throw new IAE("Unknown table type %s", indexedTableType);
    }
    return table;
  }

  public static QueryableIndexSegment makeQueryableIndexSegment(Closer closer, String dataSource, int rowsPerSegment)
  {
    final List<GeneratorColumnSchema> schemaColumnsInfo = ImmutableList.of(
        GeneratorColumnSchema.makeSequential("stringKey", ValueType.STRING, false, 1, null, 0, rowsPerSegment),
        GeneratorColumnSchema.makeSequential("longKey", ValueType.LONG, false, 1, null, 0, rowsPerSegment),
        GeneratorColumnSchema.makeLazyZipf("string1", ValueType.STRING, false, 1, 0.1, 0, rowsPerSegment, 2.0),
        GeneratorColumnSchema.makeLazyZipf("string2", ValueType.STRING, false, 1, 0.3, 0, 1000000, 1.5),
        GeneratorColumnSchema.makeLazyZipf("string3", ValueType.STRING, false, 1, 0.12, 0, 1000, 1.25),
        GeneratorColumnSchema.makeLazyZipf("string4", ValueType.STRING, false, 1, 0.22, 0, 12000, 3.0),
        GeneratorColumnSchema.makeLazyZipf("string5", ValueType.STRING, false, 1, 0.05, 0, 33333, 1.8),
        GeneratorColumnSchema.makeLazyZipf("long1", ValueType.LONG, false, 1, 0.1, 0, 1001, 2.0),
        GeneratorColumnSchema.makeLazyZipf("long2", ValueType.LONG, false, 1, 0.01, 0, 666666, 2.2),
        GeneratorColumnSchema.makeLazyZipf("long3", ValueType.LONG, false, 1, 0.12, 0, 1000000, 2.5),
        GeneratorColumnSchema.makeLazyZipf("long4", ValueType.LONG, false, 1, 0.4, 0, 23, 1.2),
        GeneratorColumnSchema.makeLazyZipf("long5", ValueType.LONG, false, 1, 0.33, 0, 9999, 1.5),
        GeneratorColumnSchema.makeLazyZipf("double1", ValueType.DOUBLE, false, 1, 0.1, 0, 333, 2.2),
        GeneratorColumnSchema.makeLazyZipf("double2", ValueType.DOUBLE, false, 1, 0.01, 0, 4021, 2.5),
        GeneratorColumnSchema.makeLazyZipf("double3", ValueType.DOUBLE, false, 1, 0.41, 0, 90210, 4.0),
        GeneratorColumnSchema.makeLazyZipf("double4", ValueType.DOUBLE, false, 1, 0.5, 0, 5555555, 1.2),
        GeneratorColumnSchema.makeLazyZipf("double5", ValueType.DOUBLE, false, 1, 0.23, 0, 80, 1.8),
        GeneratorColumnSchema.makeLazyZipf("float1", ValueType.FLOAT, false, 1, 0.11, 0, 1000000, 1.7),
        GeneratorColumnSchema.makeLazyZipf("float2", ValueType.FLOAT, false, 1, 0.4, 0, 10, 1.5),
        GeneratorColumnSchema.makeLazyZipf("float3", ValueType.FLOAT, false, 1, 0.8, 0, 5000, 2.3),
        GeneratorColumnSchema.makeLazyZipf("float4", ValueType.FLOAT, false, 1, 0.999, 0, 14440, 2.0),
        GeneratorColumnSchema.makeLazyZipf("float5", ValueType.FLOAT, false, 1, 0.001, 0, 1029, 1.5)
    );
    final List<AggregatorFactory> aggs = new ArrayList<>();
    aggs.add(new CountAggregatorFactory("rows"));

    final Interval interval = Intervals.of("2000-01-01/P1D");

    final GeneratorSchemaInfo schema = new GeneratorSchemaInfo(
        schemaColumnsInfo,
        aggs,
        interval,
        false
    );
    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource(dataSource)
                                               .interval(schema.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final QueryableIndex index = closer.register(new SegmentGenerator())
                                       .generate(dataSegment, schema, Granularities.NONE, rowsPerSegment);
    return closer.register(new QueryableIndexSegment(index, SegmentId.dummy(dataSource)));
  }

  private static int processRowsDimensionSelectors(
      final Blackhole blackhole,
      final Sequence<Cursor> cursors,
      final Set<String> columns
  )
  {
    if (columns.size() == 1) {
      return processRowsSingleDimensionSelector(blackhole, cursors, Iterables.getOnlyElement(columns));
    }
    return cursors.map(
        cursor -> {
          List<DimensionSelector> selectors = columns.stream().map(column -> {
            ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
            return factory.makeDimensionSelector(DefaultDimensionSpec.of(column));
          }).collect(Collectors.toList());

          int rowCount = 0;
          while (!cursor.isDone()) {
            for (DimensionSelector selector : selectors) {
              if (selector.getValueCardinality() < 0) {
                final IndexedInts row = selector.getRow();
                final int sz = row.size();
                for (int i = 0; i < sz; i++) {
                  blackhole.consume(selector.lookupName(row.get(i)));
                }
              } else {
                final IndexedInts row = selector.getRow();
                final int sz = row.size();
                for (int i = 0; i < sz; i++) {
                  blackhole.consume(row.get(i));
                }
              }
            }

            rowCount++;
            cursor.advance();
          }
          return rowCount;
        }).accumulate(0, (acc, in) -> acc + in);
  }

  private static int processRowsSingleDimensionSelector(
      final Blackhole blackhole,
      final Sequence<Cursor> cursors,
      final String dimension
  )
  {
    return cursors.map(
        cursor -> {
          final DimensionSelector selector = cursor.getColumnSelectorFactory()
                                                   .makeDimensionSelector(DefaultDimensionSpec.of(dimension));

          int rowCount = 0;
          if (selector.getValueCardinality() < 0) {
            String lastValue;
            while (!cursor.isDone()) {
              final IndexedInts row = selector.getRow();
              final int sz = row.size();
              for (int i = 0; i < sz; i++) {
                lastValue = selector.lookupName(row.get(i));
                blackhole.consume(lastValue);
              }
              rowCount++;
              cursor.advance();
            }
            return rowCount;
          } else {
            int lastValue;
            while (!cursor.isDone()) {
              final IndexedInts row = selector.getRow();
              final int sz = row.size();
              for (int i = 0; i < sz; i++) {
                lastValue = row.get(i);
                blackhole.consume(lastValue);
              }
              rowCount++;
              cursor.advance();
            }
            return rowCount;
          }
        }
    ).accumulate(0, (acc, in) -> acc + in);
  }

  private static int processRowsValueSelector(final Blackhole blackhole, final Sequence<Cursor> cursors, final Set<String> columns)
  {
    return cursors.map(
        cursor -> {
          ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

          List<BaseObjectColumnValueSelector> selectors =
              columns.stream().map(factory::makeColumnValueSelector).collect(Collectors.toList());
          int rowCount = 0;
          while (!cursor.isDone()) {
            for (BaseObjectColumnValueSelector<?> selector : selectors) {
              blackhole.consume(selector.getObject());
            }

            rowCount++;
            cursor.advance();
          }
          return rowCount;
        }).accumulate(0, (acc, in) -> acc + in);
  }
}
