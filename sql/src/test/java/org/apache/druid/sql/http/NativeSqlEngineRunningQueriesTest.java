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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.RunningQueryRegistry;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalogProvider;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Functional test for the "list running queries" API: runs a real SQL query through {@link SqlResource} and verifies
 * that the query is reported as running while it executes, and that it disappears once it completes.
 */
public class NativeSqlEngineRunningQueriesTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final DruidNode DUMMY_DRUID_NODE = new DruidNode("dummy", "dummy", false, 1, null, true, false);
  private static final ResponseContextConfig RESPONSE_CONTEXT_CONFIG = ResponseContextConfig.newConfig(false);

  @TempDir
  File tempDir;

  private Closer closer;
  private QueryRunnerFactoryConglomerate conglomerate;
  private SpecificSegmentsQuerySegmentWalker walker;

  @BeforeEach
  public void setUp()
  {
    closer = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    walker = CalciteTests.createMockWalker(conglomerate, tempDir);
    closer.register(walker);
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Test
  public void testQueryIsVisibleWhileRunning() throws Exception
  {
    final RunningQueryRegistry registry = new RunningQueryRegistry();
    final AtomicReference<NativeSqlEngine> engineRef = new AtomicReference<>();
    final AtomicInteger runningQueriesSeenDuringExecution = new AtomicInteger(-1);

    // Wraps the real test walker, so that we can observe the API from within query execution.
    final QuerySegmentWalker observingWalker = new QuerySegmentWalker()
    {
      @Override
      public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
      {
        return observe(walker.getQueryRunnerForIntervals(query, intervals));
      }

      @Override
      public <T> QueryRunner<T> getQueryRunnerForSegments(
          final Query<T> query,
          final Iterable<SegmentDescriptor> specs
      )
      {
        return observe(walker.getQueryRunnerForSegments(query, specs));
      }

      private <T> QueryRunner<T> observe(final QueryRunner<T> runner)
      {
        return new QueryRunner<T>()
        {
          @Override
          public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
          {
            runningQueriesSeenDuringExecution.set(
                engineRef.get()
                         .getQueries(
                             false,
                             false,
                             CalciteTests.SUPER_USER_AUTH_RESULT,
                             AuthorizationResult.ALLOW_NO_RESTRICTION
                         )
                         .getQueries()
                         .size()
            );
            return runner.run(queryPlus, responseContext);
          }
        };
      }
    };

    final QueryLifecycleFactory queryLifecycleFactory = new QueryLifecycleFactory(
        conglomerate,
        observingWalker,
        new DefaultGenericQueryMetricsFactory(),
        NoopServiceEmitter.instance(),
        NoopRequestLogger.instance(),
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        new DefaultQueryConfig(Map.of()),
        null,
        registry
    );

    final SqlLifecycleManager lifecycleManager = new SqlLifecycleManager();
    final QueryScheduler scheduler = new QueryScheduler(
        5,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new HiLoQueryLaningStrategy(40),
        new ServerConfig(false)
    );
    final PlannerConfig plannerConfig = PlannerConfig.builder().build();
    final DruidSchemaCatalogProvider schemaProvider = CalciteTests.createMockRootSchemaProvider(
        conglomerate,
        walker,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
    final PlannerFactory plannerFactory = new PlannerFactory(
        schemaProvider,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        new DruidHookDispatcher()
    );
    final SqlToolbox sqlToolbox = new SqlToolbox(
        null,
        plannerFactory,
        NoopServiceEmitter.instance(),
        NoopRequestLogger.instance(),
        scheduler,
        lifecycleManager
    );

    final NativeSqlEngine engine = new NativeSqlEngine(queryLifecycleFactory, JSON_MAPPER, sqlToolbox, registry);
    engineRef.set(engine);

    final SqlResource resource = new SqlResource(
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        lifecycleManager,
        new SqlEngineRegistry(Set.of(engine)),
        new SqlResourceQueryResultPusherFactory(
            JSON_MAPPER,
            new ServerConfig(),
            RESPONSE_CONTEXT_CONFIG,
            DUMMY_DRUID_NODE
        ),
        DefaultQueryConfig.NIL,
        new ServerConfig(),
        QueryFrameworkUtils.TEST_QUERY_CONTEXT_CONFIG
    );

    final MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, CalciteTests.SUPER_USER_AUTH_RESULT);
    req.remoteAddr = "1.2.3.4";
    final MockHttpServletResponse response = MockHttpServletResponse.forRequest(req);

    Assertions.assertNull(
        resource.doPost(
            new SqlQuery(
                "SELECT COUNT(*) AS c FROM druid.foo",
                ResultFormat.OBJECT,
                false,
                false,
                false,
                null,
                null
            ),
            req
        )
    );

    Assertions.assertEquals(200, response.getStatus());

    // While the native query was executing, the API reported exactly one running query.
    Assertions.assertEquals(1, runningQueriesSeenDuringExecution.get());

    // Once the query completed, it is no longer reported.
    Assertions.assertTrue(
        engine.getQueries(false, false, CalciteTests.SUPER_USER_AUTH_RESULT, AuthorizationResult.ALLOW_NO_RESTRICTION)
              .getQueries()
              .isEmpty()
    );
  }
}
