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
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.CalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.SegmentId;
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
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE3;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.INDEX_SCHEMA_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS2;

/**
 * Runs {@link CalciteQueryTest} but with MSQ engine
 */
public class CalciteSelectQueryTestMSQ extends CalciteQueryTest
{

  private MSQTestOverlordServiceClient indexingServiceClient;
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
    builder.addModules(CalciteMSQTestsHelper.fetchModules(temporaryFolder, groupByBuffers).toArray(new Module[0]));
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

  // Query not supported by MSQ
  @Ignore
  @Override
  public void testGroupingAggregatorDifferentOrder()
  {

  }

  @Ignore
  @Override
  public void testInformationSchemaColumnsOnTable()
  {

  }

  @Ignore
  @Override
  public void testMultipleExactCountDistinctWithGroupingAndOtherAggregators()
  {

  }

  @Ignore
  @Override
  public void testJoinUnionAllDifferentTablesWithMapping()
  {

  }

  @Ignore
  @Override
  public void testGroupingAggregatorWithPostAggregator()
  {

  }

  @Ignore
  @Override
  public void testGroupByRollupDifferentOrder()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsWithNumericDimension()
  {

  }

  @Ignore
  @Override
  public void testViewAndJoin()
  {

  }

  @Ignore
  @Override
  public void testUnionAllDifferentTablesWithMapping()
  {

  }

  @Ignore
  @Override
  public void testInformationSchemaTables()
  {

  }

  @Ignore
  @Override
  public void testInformationSchemaColumnsOnView()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsWithOrderByDimension()
  {

  }

  @Ignore
  @Override
  public void testExactCountDistinctWithFilter()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsWithLimit()
  {

  }

  @Ignore
  @Override
  public void testUnionAllSameTableTwice()
  {

  }

  @Ignore
  @Override
  public void testUnionAllSameTableThreeTimes()
  {

  }

  @Ignore
  @Override
  public void testGroupByCube()
  {

  }

  @Ignore
  @Override
  public void testInformationSchemaColumnsOnForbiddenTable()
  {

  }

  @Ignore
  @Override
  public void testQueryWithSelectProjectAndIdentityProjectDoesNotRename()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsNoSuperset()
  {

  }

  @Ignore
  @Override
  public void testExactCountDistinctUsingSubqueryOnUnionAllTables()
  {

  }

  @Ignore
  @Override
  public void testGroupByExpressionFromLookup()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsWithOrderByAggregator()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsWithLimitOrderByGran()
  {

  }

  @Ignore
  @Override
  public void testInformationSchemaSchemata()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsWithDummyDimension()
  {

  }

  @Ignore
  @Override
  public void testGroupingSets()
  {

  }

  @Ignore
  @Override
  public void testUnionAllSameTableThreeTimesWithSameMapping()
  {

  }

  @Ignore
  @Override
  public void testAggregatorsOnInformationSchemaColumns()
  {

  }

  @Ignore
  @Override
  public void testUnionAllTablesColumnTypeMismatchFloatLong()
  {

  }

  @Ignore
  @Override
  public void testInformationSchemaColumnsOnAnotherView()
  {

  }

  @Ignore
  @Override
  public void testGroupingSetsWithOrderByAggregatorWithLimit()
  {

  }

  // Cast failures

  // Ad hoc failures
  @Ignore("Fails because the MSQ engine creates no worker task corresponding to the generated query definition")
  @Override
  public void testFilterOnTimeFloorMisaligned()
  {

  }

  // Fails because the MSQ engine creates no worker task corresponding to the generated query definition
  @Ignore
  @Override
  public void testGroupByWithImpossibleTimeFilter()
  {

  }

  @Ignore
  @Override
  public void testGroupByNothingWithImpossibleTimeFilter()
  {

  }

  // Long cannot be converted to HyperLogLogCollector
  @Ignore
  @Override
  public void testHavingOnApproximateCountDistinct()
  {

  }

  // MSQ validation layer rejects the query
  @Ignore
  @Override
  public void testEmptyGroupWithOffsetDoesntInfiniteLoop()
  {

  }

  // External slice cannot be converted to StageinputSlice
  @Ignore
  @Override
  public void testGroupingWithNullInFilter()
  {

  }

  // ====

  // Serializable Pair Failures during aggregation
  @Ignore
  @Override

  public void testOrderByEarliestFloat()
  {

  }

  @Ignore
  @Override
  public void testPrimitiveEarliestInSubquery()
  {

  }

  @Ignore
  @Override
  public void testGreatestFunctionForStringWithIsNull()
  {

  }

  @Ignore
  @Override
  public void testGroupByAggregatorDefaultValuesNonVectorized()
  {

  }

  @Ignore
  @Override
  public void testPrimitiveLatestInSubquery()
  {

  }

  @Ignore
  @Override
  public void testOrderByLatestDouble()
  {

  }

  @Ignore
  @Override
  public void testPrimitiveLatestInSubqueryGroupBy()
  {

  }

  @Ignore
  @Override
  public void testOrderByEarliestLong()
  {

  }

  @Ignore
  @Override
  public void testLatestAggregators()
  {

  }

  @Ignore
  @Override
  public void testEarliestAggregators()
  {

  }

  @Ignore
  @Override
  public void testFirstLatestAggregatorsSkipNulls()
  {

  }

  @Ignore
  @Override
  public void testLatestVectorAggregators()
  {

  }

  @Ignore
  @Override
  public void testOrderByEarliestDouble()
  {

  }

  @Ignore
  @Override
  public void testLatestAggregatorsNumericNull()
  {

  }

  @Ignore
  @Override
  public void testOrderByLatestLong()
  {

  }

  @Ignore
  @Override
  public void testEarliestAggregatorsNumericNulls()
  {

  }
}
