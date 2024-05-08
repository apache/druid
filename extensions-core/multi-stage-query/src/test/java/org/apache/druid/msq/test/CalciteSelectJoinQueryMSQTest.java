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
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;

/**
 * Runs {@link CalciteJoinQueryTest} but with MSQ engine.
 */
public class CalciteSelectJoinQueryMSQTest
{
  /**
   * Run all tests with {@link JoinAlgorithm#BROADCAST}.
   */
  @SqlTestFramework.SqlTestFrameWorkModule(BroadcastJoinComponentSupplier.class)
  public static class BroadcastTest extends Base
  {
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
  @SqlTestFramework.SqlTestFrameWorkModule(SortMergeJoinComponentSupplier.class)
  public static class SortMergeTest extends Base
  {
    @Override
    public boolean isSortBasedJoin()
    {
      return true;
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
    @Override
    protected QueryTestBuilder testBuilder()
    {
      return new QueryTestBuilder(new CalciteTestConfig(true))
          .addCustomRunner(
              new ExtractResultsFactory(
                  () -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
          .skipVectorize(true);
    }
  }

  protected static class SortMergeJoinComponentSupplier extends AbstractJoinComponentSupplier
  {
    public SortMergeJoinComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer, JoinAlgorithm.SORT_MERGE);
    }
  }

  protected static class BroadcastJoinComponentSupplier extends AbstractJoinComponentSupplier
  {
    public BroadcastJoinComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer, JoinAlgorithm.BROADCAST);
    }
  }

  protected abstract static class AbstractJoinComponentSupplier extends StandardComponentSupplier
  {
    private JoinAlgorithm joinAlgorithm;

    public AbstractJoinComponentSupplier(TempDirProducer tempFolderProducer, JoinAlgorithm joinAlgorithm)
    {
      super(tempFolderProducer);
      this.joinAlgorithm = joinAlgorithm;
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      super.configureGuice(builder);
      builder.addModules(
          CalciteMSQTestsHelper.fetchModules(tempDirProducer::newTempFolder, TestGroupByBuffers.createDefault()).toArray(new Module[0])
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
      return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper)
      {
        @Override
        public boolean featureAvailable(EngineFeature feature)
        {
          return super.featureAvailable(feature);
        }

        @Override
        public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
        {
          plannerContext.queryContextMap().put(PlannerContext.CTX_SQL_JOIN_ALGORITHM, joinAlgorithm.toString());
          return super.buildQueryMakerForSelect(relRoot, plannerContext);
        }
      };
    }
  }
}
