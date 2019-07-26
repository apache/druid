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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.select.EventHolder;
import org.apache.druid.query.select.PagingSpec;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.select.SelectQueryConfig;
import org.apache.druid.query.select.SelectQueryEngine;
import org.apache.druid.query.select.SelectQueryQueryToolChest;
import org.apache.druid.query.select.SelectQueryRunnerFactory;
import org.apache.druid.query.select.SelectResultValue;
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

  private static final Map<String, Map<String, Druids.SelectQueryBuilder>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, Druids.SelectQueryBuilder> basicQueries = new LinkedHashMap<>();
    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    { // basic.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      Druids.SelectQueryBuilder queryBuilderA =
          Druids.newSelectQueryBuilder()
                .dataSource(new TableDataSource("blah"))
                .dimensionSpecs(DefaultDimensionSpec.toSpec(Collections.emptyList()))
                .metrics(Collections.emptyList())
                .intervals(intervalSpec)
                .granularity(Granularities.ALL)
                .descending(false);

      basicQueries.put("A", queryBuilderA);
    }

    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

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

    final Supplier<SelectQueryConfig> selectConfigSupplier = Suppliers.ofInstance(new SelectQueryConfig(true));

    factory = new SelectQueryRunnerFactory(
        new SelectQueryQueryToolChest(
            JSON_MAPPER,
            QueryBenchmarkUtil.noopIntervalChunkingQueryRunnerDecorator()
        ),
        new SelectQueryEngine(),
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

    Sequence<T> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    return queryResult.toList();
  }

  /**
   * Don't run this benchmark with a query that doesn't use {@link Granularities#ALL},
   * this pagination function probably doesn't work correctly in that case.
   */
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
  public void queryIncrementalIndex(Blackhole blackhole)
  {
    SelectQuery queryCopy = query.withPagingSpec(PagingSpec.newSpec(pagingThreshold));

    SegmentId segmentId = SegmentId.dummy("incIndex");
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
  public void queryQueryableIndex(Blackhole blackhole)
  {
    SelectQuery queryCopy = query.withPagingSpec(PagingSpec.newSpec(pagingThreshold));

    SegmentId segmentId = SegmentId.dummy("qIndex");
    QueryRunner<Result<SelectResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        segmentId,
        new QueryableIndexSegment(qIndexes.get(0), segmentId)
    );

    boolean done = false;
    while (!done) {
      List<Result<SelectResultValue>> results = SelectBenchmark.runQuery(factory, runner, queryCopy);
      SelectResultValue result = results.get(0).getValue();
      if (result.getEvents().size() == 0) {
        done = true;
      } else {
        blackhole.consume(result);
        queryCopy = incrementQueryPagination(queryCopy, result);
      }
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndex(Blackhole blackhole)
  {
    SelectQuery queryCopy = query.withPagingSpec(PagingSpec.newSpec(pagingThreshold));

    List<QueryRunner<Result<SelectResultValue>>> singleSegmentRunners = new ArrayList<>();
    QueryToolChest toolChest = factory.getToolchest();
    for (int i = 0; i < numSegments; i++) {
      SegmentId segmentId = SegmentId.dummy("qIndex" + i);
      QueryRunner<Result<SelectResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
          factory,
          segmentId,
          new QueryableIndexSegment(qIndexes.get(i), segmentId)
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
      Sequence<Result<SelectResultValue>> queryResult = theRunner.run(QueryPlus.wrap(queryCopy), ResponseContext.createEmpty());
      List<Result<SelectResultValue>> results = queryResult.toList();
      
      SelectResultValue result = results.get(0).getValue();

      if (result.getEvents().size() == 0) {
        done = true;
      } else {
        blackhole.consume(result);
        queryCopy = incrementQueryPagination(queryCopy, result);
      }
    }
  }
}
