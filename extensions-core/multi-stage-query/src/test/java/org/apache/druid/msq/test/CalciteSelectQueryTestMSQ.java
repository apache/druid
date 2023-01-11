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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.querykit.LazyResourceHolder;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.sql.calcite.CalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE3;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.INDEX_SCHEMA_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS2;

public class CalciteSelectQueryTestMSQ extends CalciteQueryTest
{

  private MSQTestOverlordServiceClient indexingServiceClient;
  private AtomicReference<MSQTestOverlordServiceClient> indexingServiceClientRef;

  private TestGroupByBuffers groupByBuffers;

  @Before
  public void setup2()
  {
    groupByBuffers = TestGroupByBuffers.createDefault();
  }

  @After
  public void teardown2()
  {
    groupByBuffers.close();
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(binder -> {
      final LookupReferencesManager lookupReferencesManager =
          EasyMock.createStrictMock(LookupReferencesManager.class);
      EasyMock.expect(lookupReferencesManager.getAllLookupNames()).andReturn(Collections.emptySet()).anyTimes();
      EasyMock.replay(lookupReferencesManager);
      binder.bind(LookupReferencesManager.class).toInstance(lookupReferencesManager);
      binder.bind(AppenderatorsManager.class).toProvider(() -> null);

      // Requirements of JoinableFactoryModule
      binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class));

      binder.bind(new TypeLiteral<Set<NodeRole>>()
      {
      }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));

      DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
      {
        @Override
        public String getFormatString()
        {
          return "test";
        }
      };
      binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
      binder.bind(QueryProcessingPool.class)
            .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));

      // Select queries donot require this
      Injector dummyInjector = GuiceInjectors.makeStartupInjectorWithModules(
          ImmutableList.of(
              binder1 -> {
                binder1.bind(ExprMacroTable.class).toInstance(CalciteTests.createExprMacroTable());
                binder1.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
              }
          )
      );
      ObjectMapper testMapper = MSQTestBase.setupObjectMapper(dummyInjector);
      IndexIO indexIO = new IndexIO(testMapper, () -> 0);
      SegmentCacheManager segmentCacheManager = null;
      try {
        segmentCacheManager = new SegmentCacheManagerFactory(testMapper).manufacturate(temporaryFolder.newFolder("test"));
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
      MSQTestSegmentManager segmentManager = new MSQTestSegmentManager(segmentCacheManager, indexIO);
      try {
        config.storageDirectory = temporaryFolder.newFolder("localsegments");
      }
      catch (IOException e) {
        throw new ISE(e, "Unable to create folder");
      }
      binder.bind(DataSegmentPusher.class).toProvider(() -> new MSQTestDelegateDataSegmentPusher(
          new LocalDataSegmentPusher(config),
          segmentManager
      ));
      binder.bind(DataSegmentAnnouncer.class).toInstance(new NoopDataSegmentAnnouncer());
      binder.bind(DataSegmentProvider.class)
            .toInstance((dataSegment, channelCounters) ->
                            new LazyResourceHolder<>(getSupplierForSegment(dataSegment)));

      GroupByQueryConfig groupByQueryConfig = new GroupByQueryConfig();
      binder.bind(GroupByStrategySelector.class)
            .toInstance(GroupByQueryRunnerTest.makeQueryRunnerFactory(groupByQueryConfig, groupByBuffers)
                                              .getStrategySelector());
    });

    builder.addModule(new IndexingServiceTuningConfigModule());
    builder.addModule(new JoinableFactoryModule());
    builder.addModule(new MSQExternalDataSourceModule());
    builder.addModule(new MSQIndexingModule());

  }


  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf,
      ObjectMapper queryJsonMapper,
      Injector injector
  )
  {
    final WorkerMemoryParameters workerMemoryParameters =
        WorkerMemoryParameters.createInstance(
            WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
            WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
            2,
            10,
            2
        );
    indexingServiceClient = new MSQTestOverlordServiceClient(
        queryJsonMapper,
        injector,
        new MSQTestTaskActionClient(queryJsonMapper),
        workerMemoryParameters
    );
    return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper);
  }

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig())
        .addCustomVerification(new VerifyMSQSupportedNativeQueriesFactory())
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(false);
  }

  Supplier<Pair<Segment, Closeable>> getSupplierForSegment(SegmentId segmentId)
  {
    final TemporaryFolder temporaryFolder = new TemporaryFolder();
    try {
      temporaryFolder.create();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    final QueryableIndex index;
    try {
      switch (segmentId.getDataSource()) {
        case DATASOURCE1:
          IncrementalIndexSchema foo1Schema = new IncrementalIndexSchema.Builder()
              .withMetrics(
                  new CountAggregatorFactory("cnt"),
                  new FloatSumAggregatorFactory("m1", "m1"),
                  new DoubleSumAggregatorFactory("m2", "m2"),
                  new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
              )
              .withRollup(false)
              .build();
          index = IndexBuilder
              .create()
              .tmpDir(new File(temporaryFolder.newFolder(), "1"))
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(foo1Schema)
              .rows(ROWS1)
              .buildMMappedIndex();
          break;
        case DATASOURCE2:
          final IncrementalIndexSchema indexSchemaDifferentDim3M1Types = new IncrementalIndexSchema.Builder()
              .withDimensionsSpec(
                  new DimensionsSpec(
                      ImmutableList.of(
                          new StringDimensionSchema("dim1"),
                          new StringDimensionSchema("dim2"),
                          new LongDimensionSchema("dim3")
                      )
                  )
              )
              .withMetrics(
                  new CountAggregatorFactory("cnt"),
                  new LongSumAggregatorFactory("m1", "m1"),
                  new DoubleSumAggregatorFactory("m2", "m2"),
                  new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
              )
              .withRollup(false)
              .build();
          index = IndexBuilder
              .create()
              .tmpDir(new File(temporaryFolder.newFolder(), "2"))
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(indexSchemaDifferentDim3M1Types)
              .rows(ROWS2)
              .buildMMappedIndex();
          break;
        case DATASOURCE3:
          index = IndexBuilder
              .create()
              .tmpDir(new File(temporaryFolder.newFolder(), "3"))
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(INDEX_SCHEMA_NUMERIC_DIMS)
              .rows(ROWS1_WITH_NUMERIC_DIMS)
              .buildMMappedIndex();
          break;
        default:
          throw new ISE("Cannot query segment %s in test runner", segmentId);

      }
    }
    catch (IOException e) {
      throw new ISE(e, "Unable to load index for segment %s", segmentId);
    }
    Segment segment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return segmentId;
      }

      @Override
      public Interval getDataInterval()
      {
        return segmentId.getInterval();
      }

      @Nullable
      @Override
      public QueryableIndex asQueryableIndex()
      {
        return index;
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        return new QueryableIndexStorageAdapter(index);
      }

      @Override
      public void close()
      {
      }
    };
    return new Supplier<Pair<Segment, Closeable>>()
    {
      @Override
      public Pair<Segment, Closeable> get()
      {
        return new Pair<>(segment, Closer.create());
      }
    };
  }

  protected Map<String, Object> defaultScanQueryContext(final RowSignature signature)
  {
    try {
      return ImmutableMap.<String, Object>builder()
                         .putAll(MSQTestBase.DEFAULT_MSQ_CONTEXT)
                         .put(
                             DruidQuery.CTX_SCAN_SIGNATURE,
                             queryFramework().queryJsonMapper().writeValueAsString(signature)
                         )
                         .build();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }


  @Test
  public void testFilterOnTimeFloorMisaligned()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE floor(__time TO month) = TIMESTAMP '2000-01-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec())
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{0L})
    );
  }



  @Ignore
  @Test
  public void testOrderThenLimitThenFilter()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testQueryWithMSQ(
        "SELECT dim1 FROM "
        + "(SELECT __time, dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) "
        + "WHERE dim1 IN ('abc', 'def')",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    new QueryDataSource(
                        newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .columns(ImmutableList.of("__time", "dim1"))
                            .limit(4)
                            .order(ScanQuery.Order.DESCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .context(defaultScanQueryContext(resultSignature))
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("dim1"))
                .filters(in("dim1", Arrays.asList("abc", "def"), null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(defaultScanQueryContext(resultSignature))
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        ),
        new ExtractResultsFactory(() -> indexingServiceClient)
    );
  }

  @Ignore("Union datasource not supported by MSQ")
  @Override
  public void testUnionAllSameTableTwiceWithSameMapping()
  {

  }

  @Ignore
  @Override
  public void testUnionAllQueriesWithLimit()
  {

  }

  @Override
  @Ignore
  public void testGroupByNothingWithLiterallyFalseFilter()
  {

  }

  @Ignore
  @Override
  public void testSubqueryTypeMismatchWithLiterals()
  {

  }

  @Ignore
  @Override
  public void testTextcat()
  {

  }

  @Ignore
  @Override
  public void testGroupByWithLiteralInSubqueryGrouping()
  {

  }

  @Ignore
  @Override
  public void testSqlIsNullToInFilter()
  {
  }

  @Ignore
  @Override
  public void testGroupBySortPushDown()
  {
  }


  @Ignore
  @Override
  public void testStringLatestGroupBy()
  {

  }

  @Ignore
  @Override
  public void testGroupByFloorTimeAndOneOtherDimensionWithOrderBy()
  {

  }

  @Ignore
  @Override
  public void testColumnComparison()
  {

  }

  @Ignore
  @Override
  public void testGroupByCaseWhenOfTripleAnd()
  {

  }

  @Ignore
  @Override
  public void testTopNWithSelectProjections()
  {

  }


  @Ignore
  @Override
  public void testProjectAfterSort3WithoutAmbiguity()
  {

  }

  @Ignore
  @Override
  public void testGroupByCaseWhen()
  {

  }

  @Ignore
  @Override
  public void testGroupByWithSortOnPostAggregationDefault()
  {

  }

  @Ignore
  @Override
  public void testGroupByWithSelectProjections()
  {

  }

  @Ignore
  @Override
  public void testGroupByTimeAndOtherDimension()
  {

  }

  @Ignore
  @Override
  public void testOrderByAnyLong()
  {

  }

  @Ignore
  @Override
  public void testBitwiseAggregatorsGroupBy()
  {

  }

  @Ignore
  @Override
  public void testGroupByWithSelectAndOrderByProjections()
  {

  }

  @Ignore
  @Override
  public void testQueryContextOuterLimit()
  {

  }

  @Ignore
  @Override
  public void testGroupByWithSortOnPostAggregationNoTopNContext()
  {

  }

  @Ignore
  @Override
  public void testUsingSubqueryAsFilterWithInnerSort()
  {

  }

  @Ignore
  @Override
  public void testOrderByAnyDouble()
  {

  }

  @Ignore
  @Override
  public void testGroupByLimitWrappingOrderByAgg()
  {

  }

  @Ignore
  @Override
  public void testTopNWithSelectAndOrderByProjections()
  {

  }

  @Ignore
  @Override
  public void testOrderByAnyFloat()
  {

  }

  @Ignore
  @Override
  public void testRegexpExtract()
  {

  }

  @Ignore
  @Override
  public void testTimeseriesLosAngelesViaQueryContext()
  {

  }

  @Ignore
  @Override
  public void testGroupByLimitPushDownWithHavingOnLong()
  {

  }

  @Ignore
  @Override
  public void testGroupBySingleColumnDescendingNoTopN()
  {

  }

  @Ignore
  @Override
  public void testProjectAfterSort2()
  {

  }

  @Ignore
  @Override
  public void testFilterLongDimension()
  {

  }

  @Ignore
  @Override
  public void testHavingOnExactCountDistinct()
  {

  }

  @Ignore
  @Override
  public void testStringAgg()
  {

  }

}
