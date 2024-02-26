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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.BrokerSegmentMetadataCacheConfig;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.MetadataSegmentView;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.util.testoperator.CalciteTestOperatorModule;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "numfoo";
  public static final String DATASOURCE4 = "foo4";
  public static final String DATASOURCE5 = "lotsocolumns";
  public static final String BROADCAST_DATASOURCE = "broadcast";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";
  public static final String FORBIDDEN_DESTINATION = "forbiddenDestination";
  public static final String SOME_DATASOURCE = "some_datasource";
  public static final String SOME_DATSOURCE_ESCAPED = "some\\_datasource";
  public static final String SOMEXDATASOURCE = "somexdatasource";
  public static final String USERVISITDATASOURCE = "visits";
  public static final String DRUID_SCHEMA_NAME = "druid";
  public static final String WIKIPEDIA = "wikipedia";
  public static final String WIKIPEDIA_FIRST_LAST = "wikipedia_first_last";

  public static final String TEST_SUPERUSER_NAME = "testSuperuser";
  public static final AuthorizerMapper TEST_AUTHORIZER_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (TEST_SUPERUSER_NAME.equals(authenticationResult.getIdentity())) {
          return Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
            if (FORBIDDEN_DATASOURCE.equals(resource.getName())) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.VIEW:
            if ("forbiddenView".equals(resource.getName())) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.QUERY_CONTEXT:
            return Access.OK;
          case ResourceType.EXTERNAL:
            if (Action.WRITE.equals(action)) {
              if (FORBIDDEN_DESTINATION.equals(resource.getName())) {
                return new Access(false);
              } else {
                return Access.OK;
              }
            }
            return new Access(false);
          default:
            return new Access(false);
        }
      };
    }
  };

  public static final AuthorizerMapper TEST_EXTERNAL_AUTHORIZER_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (TEST_SUPERUSER_NAME.equals(authenticationResult.getIdentity())) {
          return Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
            if (FORBIDDEN_DATASOURCE.equals(resource.getName())) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.VIEW:
            if ("forbiddenView".equals(resource.getName())) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.QUERY_CONTEXT:
          case ResourceType.EXTERNAL:
            return Access.OK;
          default:
            return new Access(false);
        }
      };
    }
  };

  public static final AuthenticatorMapper TEST_AUTHENTICATOR_MAPPER;

  static {
    final Map<String, Authenticator> defaultMap = new HashMap<>();
    defaultMap.put(
        AuthConfig.ALLOW_ALL_NAME,
        new AllowAllAuthenticator()
        {
          @Override
          public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
          {
            return new AuthenticationResult((String) context.get("user"), AuthConfig.ALLOW_ALL_NAME, null, null);
          }
        }
    );
    TEST_AUTHENTICATOR_MAPPER = new AuthenticatorMapper(defaultMap);
  }

  public static final Escalator TEST_AUTHENTICATOR_ESCALATOR;

  static {
    TEST_AUTHENTICATOR_ESCALATOR = new NoopEscalator()
    {

      @Override
      public AuthenticationResult createEscalatedAuthenticationResult()
      {
        return SUPER_USER_AUTH_RESULT;
      }
    };
  }

  public static final AuthenticationResult REGULAR_USER_AUTH_RESULT = new AuthenticationResult(
      AuthConfig.ALLOW_ALL_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null,
      null
  );

  public static final AuthenticationResult SUPER_USER_AUTH_RESULT = new AuthenticationResult(
      TEST_SUPERUSER_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null,
      null
  );

  public static final Injector INJECTOR = QueryStackTests.defaultInjectorBuilder()
      .addModule(new LookylooModule())
      .addModule(new SqlAggregationModule())
      .addModule(new CalciteTestOperatorModule())
      .build();

  private CalciteTests()
  {
    // No instantiation.
  }

  public static NativeSqlEngine createMockSqlEngine(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return new NativeSqlEngine(createMockQueryLifecycleFactory(walker, conglomerate), getJsonMapper());
  }

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return QueryFrameworkUtils.createMockQueryLifecycleFactory(walker, conglomerate);
  }

  public static SqlStatementFactory createSqlStatementFactory(
      final SqlEngine engine,
      final PlannerFactory plannerFactory
  )
  {
    return createSqlStatementFactory(engine, plannerFactory, new AuthConfig());
  }

  public static SqlStatementFactory createSqlStatementFactory(
      final SqlEngine engine,
      final PlannerFactory plannerFactory,
      final AuthConfig authConfig
  )
  {
    return QueryFrameworkUtils.createSqlStatementFactory(engine, plannerFactory, authConfig);
  }

  public static ObjectMapper getJsonMapper()
  {
    return INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir
  )
  {
    return TestDataBuilder.createMockWalker(INJECTOR, conglomerate, tmpDir);
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir,
      final QueryScheduler scheduler
  )
  {
    return TestDataBuilder.createMockWalker(INJECTOR, conglomerate, tmpDir, scheduler);
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir,
      final QueryScheduler scheduler,
      final JoinableFactory joinableFactory
  )
  {
    return TestDataBuilder.createMockWalker(
        INJECTOR,
        conglomerate,
        tmpDir,
        scheduler,
        joinableFactory
    );
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir,
      final QueryScheduler scheduler,
      final JoinableFactoryWrapper joinableFactoryWrapper
  )
  {
    return TestDataBuilder.createMockWalker(
        INJECTOR,
        conglomerate,
        tmpDir,
        scheduler,
        joinableFactoryWrapper
    );
  }

  public static ExprMacroTable createExprMacroTable()
  {
    return INJECTOR.getInstance(ExprMacroTable.class);
  }

  public static JoinableFactoryWrapper createJoinableFactoryWrapper()
  {
    return new JoinableFactoryWrapper(QueryFrameworkUtils.createDefaultJoinableFactory(INJECTOR));
  }

  public static DruidOperatorTable createOperatorTable()
  {
    return QueryFrameworkUtils.createOperatorTable(INJECTOR);
  }

  public static SystemSchema createMockSystemSchema(
      final DruidSchema druidSchema,
      final SpecificSegmentsQuerySegmentWalker walker,
      final AuthorizerMapper authorizerMapper
  )
  {
    final DruidNode coordinatorNode = new DruidNode("test-coordinator", "dummy", false, 8081, null, true, false);
    FakeDruidNodeDiscoveryProvider provider = new FakeDruidNodeDiscoveryProvider(
        ImmutableMap.of(
            NodeRole.COORDINATOR, new FakeDruidNodeDiscovery(ImmutableMap.of(NodeRole.COORDINATOR, coordinatorNode))
        )
    );

    final DruidNode overlordNode = new DruidNode("test-overlord", "dummy", false, 8090, null, true, false);

    final DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        new FakeHttpClient(),
        provider,
        NodeRole.COORDINATOR,
        "/simple/leader"
    ) {
      @Override
      public String findCurrentLeader()
      {
        return coordinatorNode.getHostAndPortToUse();
      }
    };

    final OverlordClient overlordClient = new NoopOverlordClient() {
      @Override
      public ListenableFuture<URI> findCurrentLeader()
      {
        try {
          return Futures.immediateFuture(new URI(overlordNode.getHostAndPortToUse()));
        }
        catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
          @Nullable String state,
          @Nullable String dataSource,
          @Nullable Integer maxCompletedTasks
      )
      {
        List<TaskStatusPlus> tasks = new ArrayList<TaskStatusPlus>();
        tasks.add(createTaskStatus("id1", DATASOURCE1, 10L));
        tasks.add(createTaskStatus("id1", DATASOURCE1, 1L));
        tasks.add(createTaskStatus("id2", DATASOURCE2, 20L));
        tasks.add(createTaskStatus("id2", DATASOURCE2, 2L));
        return Futures.immediateFuture(CloseableIterators.withEmptyBaggage(tasks.iterator()));
      }

      private TaskStatusPlus createTaskStatus(String id, String datasource, Long duration)
      {
        return new TaskStatusPlus(
            id,
            "testGroupId",
            "testType",
            DateTimes.nowUtc(),
            DateTimes.nowUtc(),
            TaskState.RUNNING,
            RunnerTaskState.RUNNING,
            duration,
            TaskLocation.create("testHost", 1010, -1),
            datasource,
            null
        );
      }
    };

    return new SystemSchema(
        druidSchema,
        new MetadataSegmentView(
            druidLeaderClient,
            getJsonMapper(),
            new BrokerSegmentWatcherConfig(),
            BrokerSegmentMetadataCacheConfig.create()
        ),
        new TestTimelineServerView(walker.getSegments()),
        new FakeServerInventoryView(),
        authorizerMapper,
        druidLeaderClient,
        overlordClient,
        provider,
        getJsonMapper()
    );
  }

  public static DruidSchemaCatalog createMockRootSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    return QueryFrameworkUtils.createMockRootSchema(
        INJECTOR,
        conglomerate,
        walker,
        plannerConfig,
        authorizerMapper);
  }

  /**
   * A fake {@link HttpClient} for {@link #createMockSystemSchema}.
   */
  private static class FakeHttpClient implements HttpClient
  {
    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler,
        Duration readTimeout
    )
    {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A fake {@link DruidNodeDiscoveryProvider} for {@link #createMockSystemSchema}.
   */
  private static class FakeDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
  {
    private final Map<NodeRole, FakeDruidNodeDiscovery> nodeDiscoveries;

    public FakeDruidNodeDiscoveryProvider(Map<NodeRole, FakeDruidNodeDiscovery> nodeDiscoveries)
    {
      this.nodeDiscoveries = nodeDiscoveries;
    }

    @Override
    public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
    {
      boolean get = nodeDiscoveries.getOrDefault(nodeRole, new FakeDruidNodeDiscovery())
                                   .getAllNodes()
                                   .stream()
                                   .anyMatch(x -> x.getDruidNode().equals(node));
      return () -> get;
    }

    @Override
    public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
    {
      return nodeDiscoveries.getOrDefault(nodeRole, new FakeDruidNodeDiscovery());
    }
  }

  private static class FakeDruidNodeDiscovery implements DruidNodeDiscovery
  {
    private final Set<DiscoveryDruidNode> nodes;

    FakeDruidNodeDiscovery()
    {
      this.nodes = new HashSet<>();
    }

    FakeDruidNodeDiscovery(Map<NodeRole, DruidNode> nodes)
    {
      this.nodes = Sets.newHashSetWithExpectedSize(nodes.size());
      nodes.forEach((k, v) -> {
        addNode(v, k);
      });
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return nodes;
    }

    void addNode(DruidNode node, NodeRole role)
    {
      final DiscoveryDruidNode discoveryNode = new DiscoveryDruidNode(node, role, ImmutableMap.of());
      this.nodes.add(discoveryNode);
    }

    @Override
    public void registerListener(Listener listener)
    {

    }
  }

  /**
   * A fake {@link ServerInventoryView} for {@link #createMockSystemSchema}.
   */
  private static class FakeServerInventoryView implements FilteredServerInventoryView
  {
    @Nullable
    @Override
    public DruidServer getInventoryValue(String serverKey)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<DruidServer> getInventory()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStarted()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerSegmentCallback(
        Executor exec,
        ServerView.SegmentCallback callback,
        Predicate<Pair<DruidServerMetadata, DataSegment>> filter
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerServerRemovedCallback(
        Executor exec,
        ServerView.ServerRemovedCallback callback
    )
    {
      throw new UnsupportedOperationException();
    }
  }
}
