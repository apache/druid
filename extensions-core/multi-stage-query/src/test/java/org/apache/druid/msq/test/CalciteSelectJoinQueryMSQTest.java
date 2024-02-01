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
import org.apache.calcite.rel.RelRoot;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.CalciteJoinQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

/**
 * Runs {@link CalciteJoinQueryTest} but with MSQ engine.
 */
@RunWith(Enclosed.class)
public abstract class CalciteSelectJoinQueryMSQTest
{
  /**
   * Run all tests with {@link JoinAlgorithm#BROADCAST}.
   */
  public static class BroadcastTest extends Base
  {
    public BroadcastTest()
    {
      super(JoinAlgorithm.BROADCAST);
    }

    @Override
    protected QueryTestBuilder testBuilder()
    {
      return super.testBuilder()
                  .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
    }
  }

  /**
   * Run all tests with {@link JoinAlgorithm#SORT_MERGE}.
   */
  public static class SortMergeTest extends Base
  {
    public SortMergeTest()
    {
      super(JoinAlgorithm.SORT_MERGE);
    }

    @Override
    protected QueryTestBuilder testBuilder()
    {
      // Don't verify native queries for sort-merge join, since the structure is different.
      // (Lots of extra subqueries.)
      return super.testBuilder()
                  .verifyNativeQueries(xs -> false);
    }
  }

  public abstract static class Base extends CalciteJoinQueryTest
  {
    private final JoinAlgorithm joinAlgorithm;


    protected Base(final JoinAlgorithm joinAlgorithm)
    {
      super(joinAlgorithm == JoinAlgorithm.SORT_MERGE);
      this.joinAlgorithm = joinAlgorithm;
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      super.configureGuice(builder);
      builder.addModules(
          CalciteMSQTestsHelper.fetchModules(temporaryFolder, TestGroupByBuffers.createDefault()).toArray(new Module[0])
      );
    }

    @Override
    public SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper queryJsonMapper,
        Injector injector
    )
    {
      final WorkerMemoryParameters workerMemoryParameters =
          WorkerMemoryParameters.createInstance(WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50, 2, 10, 2, 0, 0);
      final MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
          queryJsonMapper,
          injector,
          new MSQTestTaskActionClient(queryJsonMapper, injector),
          workerMemoryParameters,
          ImmutableList.of()
      );
      return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper)
      {
        @Override
        public boolean featureAvailable(EngineFeature feature, PlannerContext plannerContext)
        {
          plannerContext.queryContextMap().put(PlannerContext.CTX_SQL_JOIN_ALGORITHM, joinAlgorithm.toString());
          return super.featureAvailable(feature, plannerContext);
        }

        @Override
        public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
        {
          plannerContext.queryContextMap().put(PlannerContext.CTX_SQL_JOIN_ALGORITHM, joinAlgorithm.toString());
          return super.buildQueryMakerForSelect(relRoot, plannerContext);
        }
      };
    }

    @Override
    protected QueryTestBuilder testBuilder()
    {
      return new QueryTestBuilder(new CalciteTestConfig(true))
          .addCustomRunner(
              new ExtractResultsFactory(
                  () -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
          .skipVectorize(true)
          .msqCompatible(msqCompatible);
    }
  }
}
