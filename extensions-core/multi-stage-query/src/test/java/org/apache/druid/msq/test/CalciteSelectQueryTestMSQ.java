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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.CalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

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
