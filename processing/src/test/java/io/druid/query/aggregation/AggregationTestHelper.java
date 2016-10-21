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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
  private final QueryToolChest toolChest;
  private final QueryRunnerFactory factory;

  private final TemporaryFolder tempFolder;

  private AggregationTestHelper(
      ObjectMapper mapper,
      IndexMerger indexMerger,
      IndexIO indexIO,
      QueryToolChest toolchest,
      QueryRunnerFactory factory,
      TemporaryFolder tempFolder,
      List<? extends Module> jsonModulesToRegister
  )
  {
    this.mapper = mapper;
    this.indexMerger = indexMerger;
    this.indexIO = indexIO;
    this.toolChest = toolchest;
    this.factory = factory;
    this.tempFolder = tempFolder;

    for(Module mod : jsonModulesToRegister) {
      mapper.registerModule(mod);
    }
  }

  public static final AggregationTestHelper createGroupByQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      TemporaryFolder tempFolder
  )
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    GroupByQueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig());

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
        new IndexMerger(mapper, indexIO),
        indexIO,
        factory.getToolchest(),
        factory,
        tempFolder,
        jsonModulesToRegister
    );
  }

  public static final AggregationTestHelper createSelectQueryAggregationTestHelper(
      List<? extends Module> jsonModulesToRegister,
      TemporaryFolder tempFolder
  )
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    SelectQueryQueryToolChest toolchest = new SelectQueryQueryToolChest(
        new DefaultObjectMapper(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );

    SelectQueryRunnerFactory factory = new SelectQueryRunnerFactory(
        new SelectQueryQueryToolChest(
            new DefaultObjectMapper(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new SelectQueryEngine(),
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
        new IndexMerger(mapper, indexIO),
        indexIO,
        toolchest,
        factory,
        tempFolder,
        jsonModulesToRegister
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
      index = new OnheapIncrementalIndex(minTimestamp, gran, metrics, deserializeComplexMetrics, true, true, maxRowCount);
      while (rows.hasNext()) {
        Object row = rows.next();
        if (!index.canAppendRow()) {
          File tmp = tempFolder.newFolder();
          toMerge.add(tmp);
          indexMerger.persist(index, tmp, new IndexSpec());
          index.close();
          index = new OnheapIncrementalIndex(minTimestamp, gran, metrics, deserializeComplexMetrics, true, true, maxRowCount);
        }
        if (row instanceof String && parser instanceof StringInputRowParser) {
          //Note: this is required because StringInputRowParser is InputRowParser<ByteBuffer> as opposed to
          //InputRowsParser<String>
          index.add(((StringInputRowParser) parser).parse((String) row));
        } else {
          index.add(parser.parse(row));
        }
      }

      if (toMerge.size() > 0) {
        File tmp = tempFolder.newFolder();
        toMerge.add(tmp);
        indexMerger.persist(index, tmp, new IndexSpec());

        List<QueryableIndex> indexes = new ArrayList<>(toMerge.size());
        for (File file : toMerge) {
          indexes.add(indexIO.loadIndex(file));
        }
        indexMerger.mergeQueryableIndex(indexes, true, metrics, outDir, new IndexSpec());

        for (QueryableIndex qi : indexes) {
          qi.close();
        }
      } else {
        indexMerger.persist(index, outDir, new IndexSpec());
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
  public Sequence<Row> runQueryOnSegments(final List<File> segmentDirs, final String queryJson) throws Exception
  {
    return runQueryOnSegments(segmentDirs, mapper.readValue(queryJson, Query.class));
  }

  public Sequence<Row> runQueryOnSegments(final List<File> segmentDirs, final Query query)
  {
    final List<Segment> segments = Lists.transform(
        segmentDirs,
        new Function<File, Segment>()
        {
          @Override
          public Segment apply(File segmentDir)
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
      return runQueryOnSegmentsObjs(segments, query);
    } finally {
      for(Segment segment: segments) {
        CloseQuietly.close(segment);
      }
    }
  }

  public Sequence<Row> runQueryOnSegmentsObjs(final List<Segment> segments, final Query query)
  {
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

          TypeFactory typeFactory = mapper.getTypeFactory();
          JavaType baseType = typeFactory.constructType(toolChest.getResultTypeReference());

          List resultRows = Lists.transform(
              readQueryResultArrayFromString(resultStr),
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

  private List readQueryResultArrayFromString(String str) throws Exception
  {
    List result = new ArrayList();

    JsonParser jp = mapper.getFactory().createParser(str);

    if (jp.nextToken() != JsonToken.START_ARRAY) {
      throw new IAE("not an array [%s]", str);
    }

    ObjectCodec objectCodec = jp.getCodec();

    while(jp.nextToken() != JsonToken.END_ARRAY) {
      result.add(objectCodec.readValue(jp, toolChest.getResultTypeReference()));
    }
    return result;
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

