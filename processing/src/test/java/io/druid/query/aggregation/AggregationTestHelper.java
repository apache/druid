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

package io.druid.query.aggregation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class provides general utility to test any druid aggregation implementation given raw data,
 * parser spec, aggregator specs and a group-by query.
 * It allows you to create index from raw data, run a group by query on it which simulates query processing inside
 * of a druid cluster exercising most of the features from aggregation and returns the results that you could verify.
 */
public class AggregationTestHelper
{
  private final ObjectMapper mapper;
  private final IndexMerger indexMerger;
  private final IndexIO indexIO;
  private final GroupByQueryQueryToolChest toolChest;
  private final GroupByQueryRunnerFactory factory;

  private final TemporaryFolder tempFolder;

  public AggregationTestHelper(List<? extends Module> jsonModulesToRegister, TemporaryFolder tempFoler)
  {
    this.tempFolder = tempFoler;
    mapper = new DefaultObjectMapper();
    indexIO = TestHelper.getTestIndexIO();
    indexMerger = TestHelper.getTestIndexMerger();

    for(Module mod : jsonModulesToRegister) {
      mapper.registerModule(mod);
    }

    Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(new GroupByQueryConfig());
    StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        });

    QueryWatcher noopQueryWatcher = new QueryWatcher()
    {
      @Override
      public void registerQuery(Query query, ListenableFuture future)
      {

      }
    };

    GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);
    toolChest = new GroupByQueryQueryToolChest(
        configSupplier, mapper, engine, pool,
        NoopIntervalChunkingQueryRunnerDecorator()
    );
    factory = new GroupByQueryRunnerFactory(
        engine,
        noopQueryWatcher,
        configSupplier,
        toolChest,
        pool
    );
  }

  public Sequence<Row> createIndexAndRunQueryOnSegment(
      File inputDataFile,
      String parserJson,
      String aggregators,
      long minTimestamp,
      QueryGranularity gran,
      int maxRowCount,
      String groupByQueryJson
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataFile, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount);
    return runQueryOnSegments(Lists.newArrayList(segmentDir), groupByQueryJson);
  }

  public Sequence<Row> createIndexAndRunQueryOnSegment(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      long minTimestamp,
      QueryGranularity gran,
      int maxRowCount,
      String groupByQueryJson
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createIndex(inputDataStream, parserJson, aggregators, segmentDir, minTimestamp, gran, maxRowCount);
    return runQueryOnSegments(Lists.newArrayList(segmentDir), groupByQueryJson);
  }

  public void createIndex(
      File inputDataFile,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      QueryGranularity gran,
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
        maxRowCount
    );
  }

  public void createIndex(
      InputStream inputDataStream,
      String parserJson,
      String aggregators,
      File outDir,
      long minTimestamp,
      QueryGranularity gran,
      int maxRowCount
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
          maxRowCount
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
      QueryGranularity gran,
      boolean deserializeComplexMetrics,
      int maxRowCount
  ) throws Exception
  {
    IncrementalIndex index = null;
    List<File> toMerge = new ArrayList<>();

    try {
      index = new OnheapIncrementalIndex(minTimestamp, gran, metrics, deserializeComplexMetrics, maxRowCount);
      while (rows.hasNext()) {
        Object row = rows.next();
        try {
          if (row instanceof String && parser instanceof StringInputRowParser) {
            //Note: this is required because StringInputRowParser is InputRowParser<ByteBuffer> as opposed to
            //InputRowsParser<String>
            index.add(((StringInputRowParser) parser).parse((String) row));
          } else {
            index.add(parser.parse(row));
          }
        }
        catch (IndexSizeExceededException ex) {
          File tmp = tempFolder.newFolder();
          toMerge.add(tmp);
          indexMerger.persist(index, tmp, null, new IndexSpec());
          index.close();
          index = new OnheapIncrementalIndex(minTimestamp, gran, metrics, deserializeComplexMetrics, maxRowCount);
        }
      }

      if (toMerge.size() > 0) {
        File tmp = tempFolder.newFolder();
        toMerge.add(tmp);
        indexMerger.persist(index, tmp, null, new IndexSpec());

        List<QueryableIndex> indexes = new ArrayList<>(toMerge.size());
        for (File file : toMerge) {
          indexes.add(indexIO.loadIndex(file));
        }
        indexMerger.mergeQueryableIndex(indexes, metrics, outDir, new IndexSpec());

        for (QueryableIndex qi : indexes) {
          qi.close();
        }
      } else {
        indexMerger.persist(index, outDir, null, new IndexSpec());
      }
    }
    finally {
      if (index != null) {
        index.close();
      }
    }
  }

  //Simulates running group-by query on individual segments as historicals would do, json serialize the results
  //from each segment, later deserialize and merge and finally return the results
  public Sequence<Row> runQueryOnSegments(final List<File> segmentDirs, final String groupByQueryJson) throws Exception
  {
    return runQueryOnSegments(segmentDirs, mapper.readValue(groupByQueryJson, GroupByQuery.class));
  }

  public Sequence<Row> runQueryOnSegments(final List<File> segmentDirs, final GroupByQuery query)
  {
    final List<QueryableIndexSegment> segments = Lists.transform(
        segmentDirs,
        new Function<File, QueryableIndexSegment>()
        {
          @Override
          public QueryableIndexSegment apply(File segmentDir)
          {
            try {
              return new QueryableIndexSegment("", indexIO.loadIndex(segmentDir));
            }
            catch (IOException ex) {
              throw Throwables.propagate(ex);
            }
          }
        }
    );

    try {
      final FinalizeResultsQueryRunner baseRunner = new FinalizeResultsQueryRunner(
          toolChest.postMergeQueryDecoration(
              toolChest.mergeResults(
                  toolChest.preMergeQueryDecoration(
                      new ConcatQueryRunner(
                          Sequences.simple(
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
                                            query,
                                            factory.createRunner(segment)
                                        );
                                      }
                                      catch (Exception ex) {
                                        throw Throwables.propagate(ex);
                                      }
                                    }
                                  }
                              )
                          )
                      )
                  )
              )
          ),
          toolChest
      );

      return baseRunner.run(query, Maps.newHashMap());
    } finally {
      for(Segment segment: segments) {
        CloseQuietly.close(segment);
      }
    }

  }

  public QueryRunner<Row> makeStringSerdeQueryRunner(final ObjectMapper mapper, final QueryToolChest toolChest, final Query<Row> query, final QueryRunner<Row> baseRunner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> map)
      {
        try {
          Sequence<Row> resultSeq = baseRunner.run(query, Maps.<String, Object>newHashMap());
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
              (List<Row>)mapper.readValue(
                  resultStr, new TypeReference<List<Row>>() {}
              ),
              toolChest.makePreComputeManipulatorFn(
                  query,
                  MetricManipulatorFns.deserializing()
              )
          );
          return Sequences.simple(resultRows);
        } catch(Exception ex) {
          throw Throwables.propagate(ex);
        }
      }
    };
  }

  public static IntervalChunkingQueryRunnerDecorator NoopIntervalChunkingQueryRunnerDecorator()
  {
    return new IntervalChunkingQueryRunnerDecorator(null, null, null) {
      @Override
      public <T> QueryRunner<T> decorate(final QueryRunner<T> delegate,
                                         QueryToolChest<T, ? extends Query<T>> toolChest) {
        return new QueryRunner<T>() {
          @Override
          public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
          {
            return delegate.run(query, responseContext);
          }
        };
      }
    };
  }

  public ObjectMapper getObjectMapper()
  {
    return mapper;
  }
}

