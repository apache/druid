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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This class provides general utility to test any druid aggregation implementation given raw data,
 * parser spec, aggregator specs and a group-by query.
 * It allows you to create index from raw data, run a group by query on it which simulates query processing inside
 * of a druid cluster exercising most of the features from aggregation and returns the results that you could verify.
 */
public class AggregationTestHelper implements Closeable
{
  private final ObjectMapper mapper;
  private final IndexMerger indexMerger;
  private final IndexIO indexIO;
  private final QueryToolChest toolChest;
  private final QueryRunnerFactory factory;

  private final TemporaryFolder tempFolder;
  private final Closer resourceCloser;

  private AggregationTestHelper(
      ObjectMapper mapper,
      IndexMerger indexMerger,
      IndexIO indexIO,
      QueryToolChest toolchest,
      QueryRunnerFactory factory,
      TemporaryFolder tempFolder,
      List<? extends Module> jsonModulesToRegister,
      Closer resourceCloser
  )
  {
    this.mapper = mapper;
    this.indexMerger = indexMerger;
    this.indexIO = indexIO;
    this.toolChest = toolchest;
    this.factory = factory;
    this.tempFolder = tempFolder;
    this.resourceCloser = resourceCloser;

    for (Module mod : jsonModulesToRegister) {
      mapper.registerModule(mod);
    }
  }

  public static AggregationTestHelper createGroupByQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      GroupByQueryConfig config,
      TemporaryFolder tempFolder
  )
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final Pair<GroupByQueryRunnerFactory, Closer> factoryAndCloser = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        mapper,
        config
    );
    final GroupByQueryRunnerFactory factory = factoryAndCloser.lhs;
    final Closer closer = factoryAndCloser.rhs;

    IndexIO indexIO = new IndexIO(
        mapper,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );

    return new AggregationTestHelper(
        mapper,
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance()),
        indexIO,
        factory.getToolchest(),
        factory,
        tempFolder,
        jsonModulesToRegister,
        closer
    );
  }

  public static AggregationTestHelper createTimeseriesQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      TemporaryFolder tempFolder
  )
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

    TimeseriesQueryQueryToolChest toolchest = new TimeseriesQueryQueryToolChest();

    TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        toolchest,
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    IndexIO indexIO = new IndexIO(
        mapper,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );

    return new AggregationTestHelper(
        mapper,
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance()),
        indexIO,
        toolchest,
        factory,
        tempFolder,
        jsonModulesToRegister,
        Closer.create()
    );
  }

  public static AggregationTestHelper createTopNQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      TemporaryFolder tempFolder
  )
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

    TopNQueryQueryToolChest toolchest = new TopNQueryQueryToolChest(new TopNQueryConfig());

    final CloseableStupidPool<ByteBuffer> pool = new CloseableStupidPool<>(
        "TopNQueryRunnerFactory-bufferPool",
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(10 * 1024 * 1024);
          }
        }
    );
    final Closer resourceCloser = Closer.create();
    TopNQueryRunnerFactory factory = new TopNQueryRunnerFactory(
        pool,
        toolchest,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    IndexIO indexIO = new IndexIO(
        mapper,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );

    return new AggregationTestHelper(
        mapper,
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance()),
        indexIO,
        toolchest,
        factory,
        tempFolder,
        jsonModulesToRegister,
        resourceCloser
    );
  }

  public Sequence<ResultRow> createIndexAndRunQueryOnSegment(
      File inputDataFile,
      String parserJson,
      String aggregators,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      String groupByQueryJson
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataFile, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount, true);
    return runQueryOnSegments(Collections.singletonList(segmentDir), groupByQueryJson);
  }

  public Sequence<ResultRow> createIndexAndRunQueryOnSegment(
      File inputDataFile,
      String parserJson,
      String aggregators,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      boolean rollup,
      String groupByQueryJson
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataFile, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount, rollup);
    return runQueryOnSegments(Collections.singletonList(segmentDir), groupByQueryJson);
  }

  public Sequence<ResultRow> createIndexAndRunQueryOnSegment(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      String groupByQueryJson
  ) throws Exception
  {
    return createIndexAndRunQueryOnSegment(
        inputDataStream,
        parserJson,
        aggregators,
        minTimestamp,
        gran,
        maxRowCount,
        true,
        groupByQueryJson
    );
  }

  public Sequence<ResultRow> createIndexAndRunQueryOnSegment(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      boolean rollup,
      String groupByQueryJson
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataStream, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount, rollup);
    return runQueryOnSegments(Collections.singletonList(segmentDir), groupByQueryJson);
  }

  public void createIndex(
      File inputDataFile,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      Granularity gran,
      int maxRowCount
  ) throws Exception
  {
    createIndex(
        new FileInputStream(inputDataFile),
        parserJson,
        aggregators,
        outDir,
        minTimestamp,
        gran,
        maxRowCount,
        true
    );
  }

  public void createIndex(
      File inputDataFile,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      boolean rollup
  ) throws Exception
  {
    createIndex(
        new FileInputStream(inputDataFile),
        parserJson,
        aggregators,
        outDir,
        minTimestamp,
        gran,
        maxRowCount,
        rollup
    );
  }

  public void createIndex(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      Granularity gran,
      int maxRowCount,
      boolean rollup
  ) throws Exception
  {
    try {
      StringInputRowParser parser = mapper.readValue(parserJson, StringInputRowParser.class);

      LineIterator iter = IOUtils.lineIterator(inputDataStream, "UTF-8");
      List<AggregatorFactory> aggregatorSpecs = mapper.readValue(
          aggregators,
          new TypeReference<List<AggregatorFactory>>()
          {
          }
      );

      createIndex(
          iter,
          parser,
          aggregatorSpecs.toArray(new AggregatorFactory[0]),
          outDir,
          minTimestamp,
          gran,
          true,
          maxRowCount,
          rollup
      );
    }
    finally {
      Closeables.close(inputDataStream, true);
    }
  }

  public void createIndex(
      Iterator rows,
      InputRowParser parser,
      final AggregatorFactory[] metrics,
      File outDir,
      long minTimestamp,
      Granularity gran,
      boolean deserializeComplexMetrics,
      int maxRowCount,
      boolean rollup
  ) throws Exception
  {
    IncrementalIndex index = null;
    List<File> toMerge = new ArrayList<>();

    try {
      index = new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withMinTimestamp(minTimestamp)
                  .withDimensionsSpec(parser.getParseSpec().getDimensionsSpec())
                  .withQueryGranularity(gran)
                  .withMetrics(metrics)
                  .withRollup(rollup)
                  .build()
          )
          .setDeserializeComplexMetrics(deserializeComplexMetrics)
          .setMaxRowCount(maxRowCount)
          .buildOnheap();

      while (rows.hasNext()) {
        Object row = rows.next();
        if (!index.canAppendRow()) {
          File tmp = tempFolder.newFolder();
          toMerge.add(tmp);
          indexMerger.persist(index, tmp, new IndexSpec(), null);
          index.close();
          index = new IncrementalIndex.Builder()
              .setIndexSchema(
                  new IncrementalIndexSchema.Builder()
                      .withMinTimestamp(minTimestamp)
                      .withDimensionsSpec(parser.getParseSpec().getDimensionsSpec())
                      .withQueryGranularity(gran)
                      .withMetrics(metrics)
                      .withRollup(rollup)
                      .build()
              )
              .setDeserializeComplexMetrics(deserializeComplexMetrics)
              .setMaxRowCount(maxRowCount)
              .buildOnheap();
        }
        if (row instanceof String && parser instanceof StringInputRowParser) {
          //Note: this is required because StringInputRowParser is InputRowParser<ByteBuffer> as opposed to
          //InputRowsParser<String>
          index.add(((StringInputRowParser) parser).parse((String) row));
        } else {
          index.add(((List<InputRow>) parser.parseBatch(row)).get(0));
        }
      }

      if (toMerge.size() > 0) {
        File tmp = tempFolder.newFolder();
        toMerge.add(tmp);
        indexMerger.persist(index, tmp, new IndexSpec(), null);

        List<QueryableIndex> indexes = new ArrayList<>(toMerge.size());
        for (File file : toMerge) {
          indexes.add(indexIO.loadIndex(file));
        }
        indexMerger.mergeQueryableIndex(indexes, rollup, metrics, outDir, new IndexSpec(), null);

        for (QueryableIndex qi : indexes) {
          qi.close();
        }
      } else {
        indexMerger.persist(index, outDir, new IndexSpec(), null);
      }
    }
    finally {
      if (index != null) {
        index.close();
      }
    }
  }

  public Query readQuery(final String queryJson)
  {
    try {
      return mapper.readValue(queryJson, Query.class);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static IncrementalIndex createIncrementalIndex(
      Iterator rows,
      InputRowParser parser,
      final AggregatorFactory[] metrics,
      long minTimestamp,
      Granularity gran,
      boolean deserializeComplexMetrics,
      int maxRowCount,
      boolean rollup
  ) throws Exception
  {
    IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(minTimestamp)
                .withQueryGranularity(gran)
                .withMetrics(metrics)
                .withRollup(rollup)
                .build()
        )
        .setDeserializeComplexMetrics(deserializeComplexMetrics)
        .setMaxRowCount(maxRowCount)
        .buildOnheap();

    while (rows.hasNext()) {
      Object row = rows.next();
      if (!index.canAppendRow()) {
        throw new IAE("Can't add row to index");
      }
      if (row instanceof String && parser instanceof StringInputRowParser) {
        //Note: this is required because StringInputRowParser is InputRowParser<ByteBuffer> as opposed to
        //InputRowsParser<String>
        index.add(((StringInputRowParser) parser).parse((String) row));
      } else {
        index.add(((List<InputRow>) parser.parseBatch(row)).get(0));
      }
    }

    return index;
  }

  public Segment persistIncrementalIndex(
      IncrementalIndex index,
      File outDir
  ) throws Exception
  {
    if (outDir == null) {
      outDir = tempFolder.newFolder();
    }
    indexMerger.persist(index, outDir, new IndexSpec(), null);

    return new QueryableIndexSegment(indexIO.loadIndex(outDir), SegmentId.dummy(""));
  }

  //Simulates running group-by query on individual segments as historicals would do, json serialize the results
  //from each segment, later deserialize and merge and finally return the results
  public Sequence<ResultRow> runQueryOnSegments(final List<File> segmentDirs, final String queryJson)
  {
    return runQueryOnSegments(segmentDirs, readQuery(queryJson));
  }

  public Sequence<ResultRow> runQueryOnSegments(final List<File> segmentDirs, final Query query)
  {
    final List<Segment> segments = Lists.transform(
        segmentDirs,
        new Function<File, Segment>()
        {
          @Override
          public Segment apply(File segmentDir)
          {
            try {
              return new QueryableIndexSegment(indexIO.loadIndex(segmentDir), SegmentId.dummy(""));
            }
            catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
        }
    );

    try {
      return runQueryOnSegmentsObjs(segments, query);
    }
    finally {
      for (Segment segment : segments) {
        CloseQuietly.close(segment);
      }
    }
  }

  public Sequence<ResultRow> runQueryOnSegmentsObjs(final List<Segment> segments, final Query query)
  {
    final FinalizeResultsQueryRunner baseRunner = new FinalizeResultsQueryRunner(
        toolChest.postMergeQueryDecoration(
            toolChest.mergeResults(
                toolChest.preMergeQueryDecoration(
                    factory.mergeRunners(
                        Execs.directExecutor(),
                        Lists.transform(
                            segments,
                            new Function<Segment, QueryRunner>()
                            {
                              @Override
                              public QueryRunner apply(final Segment segment)
                              {
                                try {
                                  return makeStringSerdeQueryRunner(
                                      mapper,
                                      toolChest,
                                      factory.createRunner(segment)
                                  );
                                }
                                catch (Exception ex) {
                                  throw new RuntimeException(ex);
                                }
                              }
                            }
                        )
                    )
                )
            )
        ),
        toolChest
    );

    return baseRunner.run(QueryPlus.wrap(query));
  }

  public QueryRunner<ResultRow> makeStringSerdeQueryRunner(
      final ObjectMapper mapper,
      final QueryToolChest toolChest,
      final QueryRunner<ResultRow> baseRunner
  )
  {
    return new QueryRunner<ResultRow>()
    {
      @Override
      public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext map)
      {
        try {
          Sequence<ResultRow> resultSeq = baseRunner.run(queryPlus, ResponseContext.createEmpty());
          final Yielder yielder = resultSeq.toYielder(
              null,
              new YieldingAccumulator()
              {
                @Override
                public Object accumulate(Object accumulated, Object in)
                {
                  yield();
                  return in;
                }
              }
          );
          String resultStr = mapper.writer().writeValueAsString(yielder);

          List resultRows = Lists.transform(
              readQueryResultArrayFromString(resultStr),
              toolChest.makePreComputeManipulatorFn(
                  queryPlus.getQuery(),
                  MetricManipulatorFns.deserializing()
              )
          );
          return Sequences.simple(resultRows);
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  private List readQueryResultArrayFromString(String str) throws Exception
  {
    List result = new ArrayList();

    JsonParser jp = mapper.getFactory().createParser(str);

    if (jp.nextToken() != JsonToken.START_ARRAY) {
      throw new IAE("not an array [%s]", str);
    }

    ObjectCodec objectCodec = jp.getCodec();

    while (jp.nextToken() != JsonToken.END_ARRAY) {
      result.add(objectCodec.readValue(jp, toolChest.getBaseResultType()));
    }
    return result;
  }

  public ObjectMapper getObjectMapper()
  {
    return mapper;
  }

  public <T> T[] runRelocateVerificationTest(
      AggregatorFactory factory,
      ColumnSelectorFactory selector,
      Class<T> clazz
  )
  {
    T[] results = (T[]) Array.newInstance(clazz, 2);
    BufferAggregator agg = factory.factorizeBuffered(selector);
    ByteBuffer myBuf = ByteBuffer.allocate(10040902);
    agg.init(myBuf, 0);
    agg.aggregate(myBuf, 0);
    results[0] = (T) agg.get(myBuf, 0);

    byte[] theBytes = new byte[factory.getMaxIntermediateSizeWithNulls()];
    myBuf.get(theBytes);

    ByteBuffer newBuf = ByteBuffer.allocate(941209);
    newBuf.position(7574);
    newBuf.put(theBytes);
    newBuf.position(0);
    agg.relocate(0, 7574, myBuf, newBuf);
    results[1] = (T) agg.get(newBuf, 7574);
    return results;
  }

  @Override
  public void close() throws IOException
  {
    resourceCloser.close();
  }
}

