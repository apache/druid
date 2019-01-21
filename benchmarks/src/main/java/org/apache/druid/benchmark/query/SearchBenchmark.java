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


package org.apache.druid.benchmark.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.hll.HyperLogLogHash;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Druids.SearchQueryBuilder;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.extraction.DimExtractionFn;
import org.apache.druid.query.extraction.IdentityExtractionFn;
import org.apache.druid.query.extraction.LowerExtractionFn;
import org.apache.druid.query.extraction.StrlenExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.extraction.UpperExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.search.SearchHit;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchQueryConfig;
import org.apache.druid.query.search.SearchQueryQueryToolChest;
import org.apache.druid.query.search.SearchQueryRunnerFactory;
import org.apache.druid.query.search.SearchResultValue;
import org.apache.druid.query.search.SearchStrategySelector;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class SearchBenchmark
{
  @Param({"1"})
  private int numSegments;

  @Param({"750000"})
  private int rowsPerSegment;

  @Param({"basic.A"})
  private String schemaAndQuery;

  @Param({"1000"})
  private int limit;

  private static final Logger log = new Logger(SearchBenchmark.class);
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private List<IncrementalIndex> incIndexes;
  private List<QueryableIndex> qIndexes;

  private QueryRunnerFactory factory;
  private BenchmarkSchemaInfo schemaInfo;
  private Druids.SearchQueryBuilder queryBuilder;
  private SearchQuery query;
  private File tmpDir;

  private ExecutorService executorService;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private static final Map<String, Map<String, Druids.SearchQueryBuilder>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    final Map<String, SearchQueryBuilder> basicQueries = new LinkedHashMap<>();
    final BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    final List<String> queryTypes = ImmutableList.of("A", "B", "C", "D");
    for (final String eachType : queryTypes) {
      basicQueries.put(eachType, makeQuery(eachType, basicSchema));
    }

    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }

  private static SearchQueryBuilder makeQuery(final String name, final BenchmarkSchemaInfo basicSchema)
  {
    switch (name) {
      case "A":
        return basicA(basicSchema);
      case "B":
        return basicB(basicSchema);
      case "C":
        return basicC(basicSchema);
      case "D":
        return basicD(basicSchema);
      default:
        return null;
    }
  }

  private static SearchQueryBuilder basicA(final BenchmarkSchemaInfo basicSchema)
  {
    final QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

    return Druids.newSearchQueryBuilder()
                 .dataSource("blah")
                 .granularity(Granularities.ALL)
                 .intervals(intervalSpec)
                 .query("123");
  }

  private static SearchQueryBuilder basicB(final BenchmarkSchemaInfo basicSchema)
  {
    final QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

    final List<String> dimUniformFilterVals = new ArrayList<>();
    int resultNum = (int) (100000 * 0.1);
    int step = 100000 / resultNum;
    for (int i = 1; i < 100001 && dimUniformFilterVals.size() < resultNum; i += step) {
      dimUniformFilterVals.add(String.valueOf(i));
    }

    List<String> dimHyperUniqueFilterVals = new ArrayList<>();
    resultNum = (int) (100000 * 0.1);
    step = 100000 / resultNum;
    for (int i = 0; i < 100001 && dimHyperUniqueFilterVals.size() < resultNum; i += step) {
      dimHyperUniqueFilterVals.add(String.valueOf(i));
    }

    final List<DimFilter> dimFilters = new ArrayList<>();
    dimFilters.add(new InDimFilter("dimUniform", dimUniformFilterVals, null));
    dimFilters.add(new InDimFilter("dimHyperUnique", dimHyperUniqueFilterVals, null));

    return Druids.newSearchQueryBuilder()
                 .dataSource("blah")
                 .granularity(Granularities.ALL)
                 .intervals(intervalSpec)
                 .query("")
                 .dimensions(Lists.newArrayList("dimUniform", "dimHyperUnique"))
                 .filters(new AndDimFilter(dimFilters));
  }

  private static SearchQueryBuilder basicC(final BenchmarkSchemaInfo basicSchema)
  {
    final QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

    final List<String> dimUniformFilterVals = new ArrayList<>();
    final int resultNum = (int) (100000 * 0.1);
    final int step = 100000 / resultNum;
    for (int i = 1; i < 100001 && dimUniformFilterVals.size() < resultNum; i += step) {
      dimUniformFilterVals.add(String.valueOf(i));
    }

    final String dimName = "dimUniform";
    final List<DimFilter> dimFilters = new ArrayList<>();
    dimFilters.add(new InDimFilter(dimName, dimUniformFilterVals, IdentityExtractionFn.getInstance()));
    dimFilters.add(new SelectorDimFilter(dimName, "3", StrlenExtractionFn.instance()));
    dimFilters.add(new BoundDimFilter(dimName, "100", "10000", true, true, true, new DimExtractionFn()
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{0xF};
      }

      @Override
      public String apply(String value)
      {
        return String.valueOf(Long.parseLong(value) + 1);
      }

      @Override
      public boolean preservesOrdering()
      {
        return false;
      }

      @Override
      public ExtractionType getExtractionType()
      {
        return ExtractionType.ONE_TO_ONE;
      }
    }, null));
    dimFilters.add(new InDimFilter(dimName, dimUniformFilterVals, new LowerExtractionFn(null)));
    dimFilters.add(new InDimFilter(dimName, dimUniformFilterVals, new UpperExtractionFn(null)));
    dimFilters.add(new InDimFilter(dimName, dimUniformFilterVals, new SubstringDimExtractionFn(1, 3)));

    return Druids.newSearchQueryBuilder()
                 .dataSource("blah")
                 .granularity(Granularities.ALL)
                 .intervals(intervalSpec)
                 .query("")
                 .dimensions(Collections.singletonList("dimUniform"))
                 .filters(new AndDimFilter(dimFilters));
  }

  private static SearchQueryBuilder basicD(final BenchmarkSchemaInfo basicSchema)
  {
    final QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(basicSchema.getDataInterval())
    );

    final List<String> dimUniformFilterVals = new ArrayList<>();
    final int resultNum = (int) (100000 * 0.1);
    final int step = 100000 / resultNum;
    for (int i = 1; i < 100001 && dimUniformFilterVals.size() < resultNum; i += step) {
      dimUniformFilterVals.add(String.valueOf(i));
    }

    final String dimName = "dimUniform";
    final List<DimFilter> dimFilters = new ArrayList<>();
    dimFilters.add(new InDimFilter(dimName, dimUniformFilterVals, null));
    dimFilters.add(new SelectorDimFilter(dimName, "3", null));
    dimFilters.add(new BoundDimFilter(dimName, "100", "10000", true, true, true, null, null));

    return Druids.newSearchQueryBuilder()
                 .dataSource("blah")
                 .granularity(Granularities.ALL)
                 .intervals(intervalSpec)
                 .query("")
                 .dimensions(Collections.singletonList("dimUniform"))
                 .filters(new AndDimFilter(dimFilters));
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + +System.currentTimeMillis());

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }
    executorService = Execs.multiThreaded(numSegments, "SearchThreadPool");

    setupQueries();

    String[] schemaQuery = schemaAndQuery.split("\\.");
    String schemaName = schemaQuery[0];
    String queryName = schemaQuery[1];

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schemaName);
    queryBuilder = SCHEMA_QUERY_MAP.get(schemaName).get(queryName);
    queryBuilder.limit(limit);
    query = queryBuilder.build();

    incIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      log.info("Generating rows for segment " + i);
      BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
          schemaInfo.getColumnSchemas(),
          System.currentTimeMillis(),
          schemaInfo.getDataInterval(),
          rowsPerSegment
      );

      IncrementalIndex incIndex = makeIncIndex();

      for (int j = 0; j < rowsPerSegment; j++) {
        InputRow row = gen.nextRow();
        if (j % 10000 == 0) {
          log.info(j + " rows generated.");
        }
        incIndex.add(row);
      }
      incIndexes.add(incIndex);
    }

    tmpDir = Files.createTempDir();
    log.info("Using temp dir: " + tmpDir.getAbsolutePath());

    qIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      File indexFile = INDEX_MERGER_V9.persist(
          incIndexes.get(i),
          tmpDir,
          new IndexSpec(),
          null
      );

      QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      qIndexes.add(qIndex);
    }

    final SearchQueryConfig config = new SearchQueryConfig().withOverrides(query);
    factory = new SearchQueryRunnerFactory(
        new SearchStrategySelector(Suppliers.ofInstance(config)),
        new SearchQueryQueryToolChest(
            config,
            QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );
  }

  @TearDown
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(schemaInfo.getAggsArray())
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
  }

  private static <T> List<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    Sequence<T> queryResult = theRunner.run(QueryPlus.wrap(query), new HashMap<>());
    return queryResult.toList();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleIncrementalIndex(Blackhole blackhole)
  {
    QueryRunner<SearchHit> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("incIndex"),
        new IncrementalIndexSegment(incIndexes.get(0), SegmentId.dummy("incIndex"))
    );

    List<Result<SearchResultValue>> results = SearchBenchmark.runQuery(factory, runner, query);
    List<SearchHit> hits = results.get(0).getValue().getValue();
    for (SearchHit hit : hits) {
      blackhole.consume(hit);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndex(Blackhole blackhole)
  {
    final QueryRunner<Result<SearchResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("qIndex"),
        new QueryableIndexSegment(qIndexes.get(0), SegmentId.dummy("qIndex"))
    );

    List<Result<SearchResultValue>> results = SearchBenchmark.runQuery(factory, runner, query);
    List<SearchHit> hits = results.get(0).getValue().getValue();
    for (SearchHit hit : hits) {
      blackhole.consume(hit);
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndex(Blackhole blackhole)
  {
    List<QueryRunner<Row>> singleSegmentRunners = new ArrayList<>();
    QueryToolChest toolChest = factory.getToolchest();
    for (int i = 0; i < numSegments; i++) {
      String segmentName = "qIndex" + i;
      final QueryRunner<Result<SearchResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
          factory,
          SegmentId.dummy(segmentName),
          new QueryableIndexSegment(qIndexes.get(i), SegmentId.dummy(segmentName))
      );
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }

    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(executorService, singleSegmentRunners)),
            toolChest
        )
    );

    Sequence<Result<SearchResultValue>> queryResult = theRunner.run(
        QueryPlus.wrap(query),
        new HashMap<>()
    );
    List<Result<SearchResultValue>> results = queryResult.toList();

    for (Result<SearchResultValue> result : results) {
      List<SearchHit> hits = result.getValue().getValue();
      for (SearchHit hit : hits) {
        blackhole.consume(hit);
      }
    }
  }
}
