/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.select.SelectResultValue;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = "-server", value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class SelectBenchmark
{
  @Param({"1"})
  private int numSegments;

  @Param({"25000"})
  private int rowsPerSegment;

  @Param({"basic.A"})
  private String schemaAndQuery;

  @Param({"1000"})
  private int pagingThreshold;

  private static final Logger log = new Logger(SelectBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private List<IncrementalIndex> incIndexes;
  private List<QueryableIndex> qIndexes;

  private QueryRunnerFactory factory;

  private BenchmarkSchemaInfo schemaInfo;
  private Druids.SelectQueryBuilder queryBuilder;
  private SelectQuery query;

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
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  private static final Map<String, Map<String, Druids.SelectQueryBuilder>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, Druids.SelectQueryBuilder> basicQueries = new LinkedHashMap<>();
    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    { // basic.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Arrays.asList(basicSchema.getDataInterval()));

      Druids.SelectQueryBuilder queryBuilderA =
          Druids.newSelectQueryBuilder()
                .dataSource(new TableDataSource("blah"))
                .dimensionSpecs(DefaultDimensionSpec.toSpec(Arrays.<String>asList()))
                .metrics(Arrays.<String>asList())
                .intervals(intervalSpec)
                .granularity(QueryGranularities.ALL)
                .descending(false);

      basicQueries.put("A", queryBuilderA);
    }

    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }

    executorService = Execs.multiThreaded(numSegments, "SelectThreadPool");

    setupQueries();

    String[] schemaQuery = schemaAndQuery.split("\\.");
    String schemaName = schemaQuery[0];
    String queryName = schemaQuery[1];

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schemaName);
    queryBuilder = SCHEMA_QUERY_MAP.get(schemaName).get(queryName);
    queryBuilder.pagingSpec(PagingSpec.newSpec(pagingThreshold));
    query = queryBuilder.build();

    incIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
          schemaInfo.getColumnSchemas(),
          RNG_SEED + i,
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

    File tmpFile = Files.createTempDir();
    log.info("Using temp dir: " + tmpFile.getAbsolutePath());
    tmpFile.deleteOnExit();

    qIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      File indexFile = INDEX_MERGER_V9.persist(
          incIndexes.get(i),
          tmpFile,
          new IndexSpec()
      );
      QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      qIndexes.add(qIndex);
    }

    factory = new SelectQueryRunnerFactory(
        new SelectQueryQueryToolChest(
            JSON_MAPPER,
            QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new SelectQueryEngine(),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder()
            .withQueryGranularity(QueryGranularities.NONE)
            .withMetrics(schemaInfo.getAggsArray())
            .withDimensionsSpec(new DimensionsSpec(null, null, null))
            .build(),
        true,
        false,
        true,
        rowsPerSegment
    );
  }

  private static <T> List<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {

    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    Sequence<T> queryResult = theRunner.run(query, Maps.<String, Object>newHashMap());
    return Sequences.toList(queryResult, Lists.<T>newArrayList());
  }

  // don't run this benchmark with a query that doesn't use QueryGranularity.ALL,
  // this pagination function probably doesn't work correctly in that case.
  private SelectQuery incrementQueryPagination(SelectQuery query, SelectResultValue prevResult)
  {
    Map<String, Integer> pagingIdentifiers = prevResult.getPagingIdentifiers();
    Map<String, Integer> newPagingIdentifers = new HashMap<>();

    for (String segmentId : pagingIdentifiers.keySet()) {
      int newOffset = pagingIdentifiers.get(segmentId) + 1;
      newPagingIdentifers.put(segmentId, newOffset);
    }

    return query.withPagingSpec(new PagingSpec(newPagingIdentifers, pagingThreshold));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryIncrementalIndex(Blackhole blackhole) throws Exception
  {
    SelectQuery queryCopy = query.withPagingSpec(PagingSpec.newSpec(pagingThreshold));

    String segmentId = "incIndex";
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        segmentId,
        new IncrementalIndexSegment(incIndexes.get(0), segmentId)
    );

    boolean done = false;
    while (!done) {
      List<Result<SelectResultValue>> results = SelectBenchmark.runQuery(factory, runner, queryCopy);
      SelectResultValue result = results.get(0).getValue();
      if (result.getEvents().size() == 0) {
        done = true;
      } else {
        for (EventHolder eh : result.getEvents()) {
          blackhole.consume(eh);
        }
        queryCopy = incrementQueryPagination(queryCopy, result);
      }
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryQueryableIndex(Blackhole blackhole) throws Exception
  {
    SelectQuery queryCopy = query.withPagingSpec(PagingSpec.newSpec(pagingThreshold));

    String segmentId = "qIndex";
    QueryRunner<Result<SelectResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        segmentId,
        new QueryableIndexSegment(segmentId, qIndexes.get(0))
    );

    boolean done = false;
    while (!done) {
      List<Result<SelectResultValue>> results = SelectBenchmark.runQuery(factory, runner, queryCopy);
      SelectResultValue result = results.get(0).getValue();
      if (result.getEvents().size() == 0) {
        done = true;
      } else {
        for (EventHolder eh : result.getEvents()) {
          blackhole.consume(eh);
        }
        queryCopy = incrementQueryPagination(queryCopy, result);
      }
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndex(Blackhole blackhole) throws Exception
  {
    SelectQuery queryCopy = query.withPagingSpec(PagingSpec.newSpec(pagingThreshold));

    String segmentName;
    List<QueryRunner<Result<SelectResultValue>>> singleSegmentRunners = Lists.newArrayList();
    QueryToolChest toolChest = factory.getToolchest();
    for (int i = 0; i < numSegments; i++) {
      segmentName = "qIndex" + i;
      QueryRunner<Result<SelectResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
          factory,
          segmentName,
          new QueryableIndexSegment(segmentName, qIndexes.get(i))
      );
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }

    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(executorService, singleSegmentRunners)),
            toolChest
        )
    );


    boolean done = false;
    while (!done) {
      Sequence<Result<SelectResultValue>> queryResult = theRunner.run(queryCopy, Maps.<String, Object>newHashMap());
      List<Result<SelectResultValue>> results = Sequences.toList(queryResult, Lists.<Result<SelectResultValue>>newArrayList());
      
      SelectResultValue result = results.get(0).getValue();

      if (result.getEvents().size() == 0) {
        done = true;
      } else {
        for (EventHolder eh : result.getEvents()) {
          blackhole.consume(eh);
        }
        queryCopy = incrementQueryPagination(queryCopy, result);
      }
    }
  }
}
