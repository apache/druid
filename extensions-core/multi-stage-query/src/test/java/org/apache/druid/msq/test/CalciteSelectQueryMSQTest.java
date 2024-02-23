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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.CalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Runs {@link CalciteQueryTest} but with MSQ engine
 */
public class CalciteSelectQueryMSQTest extends CalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModules(CalciteMSQTestsHelper.fetchModules(temporaryFolder, TestGroupByBuffers.createDefault()).toArray(new Module[0]));
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
            2,
            10,
            2,
            0,
            0
        );
    final MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
        queryJsonMapper,
        injector,
        new MSQTestTaskActionClient(queryJsonMapper, injector),
        workerMemoryParameters,
        ImmutableList.of()
    );
    return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper);
  }

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate())
        .msqCompatible(msqCompatible);
  }

  @Ignore
  @Override
  public void testCannotInsertWithNativeEngine()
  {

  }

  @Ignore
  @Override
  public void testCannotReplaceWithNativeEngine()
  {

  }

  @Ignore
  @Override
  public void testRequireTimeConditionSimpleQueryNegative()
  {

  }

  @Ignore
  @Override
  public void testRequireTimeConditionSubQueryNegative()
  {

  }

  @Ignore
  @Override
  public void testRequireTimeConditionSemiJoinNegative()
  {

  }

  @Ignore
  @Override
  public void testExactCountDistinctWithFilter()
  {

  }

  @Ignore
  @Override
  public void testUnplannableScanOrderByNonTime()
  {

  }

  @Ignore
  @Override
  public void testUnplannableJoinQueriesInNonSQLCompatibleMode()
  {

  }

  @Ignore
  @Override
  public void testQueryWithMoreThanMaxNumericInFilter()
  {

  }

  @Ignore
  @Override
  public void testUnSupportedNullsFirst()
  {
  }

  @Ignore
  @Override
  public void testUnSupportedNullsLast()
  {
  }

  /**
   * Same query as {@link CalciteQueryTest#testArrayAggQueryOnComplexDatatypes}. ARRAY_AGG is not supported in MSQ currently.
   * Once support is added, this test can be removed and msqCompatible() can be added to the one in CalciteQueryTest.
   */
  @Test
  @Override
  public void testArrayAggQueryOnComplexDatatypes()
  {
    try {
      testQuery("SELECT ARRAY_AGG(unique_dim1) FROM druid.foo", ImmutableList.of(), ImmutableList.of());
      Assert.fail("query execution should fail");
    }
    catch (ISE e) {
      Assert.assertTrue(
          e.getMessage().contains("Cannot handle column [a0] with type [ARRAY<COMPLEX<hyperUnique>>]")
      );
    }
  }

  @Test(timeout = 40000)
  public void testJoinMultipleTablesWithWhereCondition()
  {
    testBuilder()
        .queryContext(
            ImmutableMap.of(
                "sqlJoinAlgorithm", "sortMerge"
            )
        )
        .sql(
            "SELECT f2.dim3,sum(f6.m1 * (1- f6.m2)) FROM"
                + " druid.foo as f5, "
                + " druid.foo as f6,  "
                + " druid.numfoo as f7, "
                + " druid.foo2 as f2, "
                + " druid.numfoo as f3, "
                + " druid.foo as f4, "
                + " druid.numfoo as f1, "
                + " druid.foo2 as f8  "
                + "where true"
                + " and f1.dim1 = f2.dim2 "
                + " and f3.dim1 = f4.dim2 "
                + " and f5.dim1 = f6.dim2 "
                + " and f7.dim2 = f8.dim3 "
                + " and f2.dim1 = f4.dim2 "
                + " and f6.dim1 = f8.dim2 "
                + " and f1.dim1 = f7.dim2 "
                + " and f8.dim2 = 'x' "
                + " and f3.__time >= date '2011-11-11' "
                + " and f3.__time < date '2013-11-11' "
                + "group by 1 "
                + "order by 2 desc limit 1001"
        )
        .run();
  }

  @Override
  public void testFilterParseLongNullable()
  {
    // this isn't really correct in default value mode, the result should be ImmutableList.of(new Object[]{0L})
    // but MSQ is missing default aggregator values in empty group results. this override can be removed when this
    // is fixed
    testBuilder().queryContext(QUERY_CONTEXT_DEFAULT)
                 .sql("select count(*) from druid.foo where parse_long(dim1, 10) is null")
                 .expectedResults(
                     NullHandling.sqlCompatible() ? ImmutableList.of(new Object[]{4L}) : ImmutableList.of()
                 )
                 .run();
  }
}
