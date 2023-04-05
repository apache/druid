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
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.CalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Runs {@link CalciteQueryTest} but with MSQ engine
 */
public class CalciteSelectQueryMSQTest extends CalciteQueryTest
{
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
            2,
            10,
            2,
            0,
            0
        );
    final MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
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
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomVerification(new VerifyMSQSupportedNativeQueriesFactory())
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(false)
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
  public void testUnplannableQueries()
  {

  }

  @Ignore
  @Override
  public void testMaxSubqueryRows()
  {

  }

  @Ignore
  @Override
  public void testQueryWithMoreThanMaxNumericInFilter()
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
    msqCompatible();
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
}
