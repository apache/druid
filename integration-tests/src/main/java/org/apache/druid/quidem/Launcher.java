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

package org.apache.druid.quidem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.druid.cli.GuiceRunnable;
import org.apache.druid.cli.QueryJettyServerInitializer;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.curator.discovery.DiscoveryModule;
import org.apache.druid.guice.AnnouncerModule;
import org.apache.druid.guice.BrokerProcessingModule;
import org.apache.druid.guice.BrokerServiceModule;
import org.apache.druid.guice.CoordinatorDiscoveryModule;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.ExtensionsModule;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.guice.JacksonConfigManagerModule;
import org.apache.druid.guice.JavaScriptModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LegacyBrokerParallelMergeConfigModule;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.LocalDataStorageDruidModule;
import org.apache.druid.guice.MetadataConfigModule;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.guice.ServerViewModule;
import org.apache.druid.guice.StartupLoggingModule;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.http.HttpClientModule;
import org.apache.druid.guice.security.AuthenticatorModule;
import org.apache.druid.guice.security.AuthorizerModule;
import org.apache.druid.guice.security.DruidAuthModule;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.Log4jShutterDownerModule;
import org.apache.druid.initialization.ServerInjectorBuilder;
import org.apache.druid.initialization.TombstoneDataStorageModule;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.rpc.guice.ServiceClientModule;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumModule;
import org.apache.druid.server.BrokerQueryResource;
import org.apache.druid.server.ClientInfoResource;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.http.BrokerResource;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.initialization.AuthorizerMapperModule;
import org.apache.druid.server.initialization.ExternalStorageAccessSecurityModule;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.initialization.jetty.JettyServerModule;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.security.TLSCertificateCheckerModule;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.ConfigurationInstance;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigStore;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.Builder;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.druid.storage.StorageConnectorModule;
import org.apache.druid.timeline.PruneLoadSpec;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.jetty.server.Server;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertNotEquals;

public class Launcher
{

  public static final String URI_PREFIX = "druidtest://";
  public static final String DEFAULT_URI = URI_PREFIX + "/";

  static final SqlTestFrameworkConfigStore CONFIG_STORE = new SqlTestFrameworkConfigStore();
  private static Logger log = new Logger(Launcher.class);

  public Launcher()
  {
  }

  public static SqlTestFrameworkConfig buildConfigfromURIParams(String url) throws SQLException
  {
    Map<String, String> queryParams;
    queryParams = new HashMap<>();
    try {
      List<NameValuePair> params = URLEncodedUtils.parse(new URI(url), StandardCharsets.UTF_8);
      for (NameValuePair pair : params) {
        queryParams.put(pair.getName(), pair.getValue());
      }
      // possible caveat: duplicate entries overwrite earlier ones
    }
    catch (URISyntaxException e) {
      throw new SQLException("Can't decode URI", e);
    }

    return new SqlTestFrameworkConfig(queryParams);
  }

  @Test
  public void runIt() throws Exception
  {
    Launcher.main3(null);
  }

  private static ConfigurationInstance getCI2() throws SQLException, Exception
  {
    SqlTestFrameworkConfig config = buildConfigfromURIParams("druidtest:///");

    ConfigurationInstance ci = CONFIG_STORE.getConfigurationInstance(
        config,
        x -> new AvaticaBasedTestConnectionSupplier2(x)
    );
    return ci;
  }

  static class AvaticaBasedTestConnectionSupplier2 implements QueryComponentSupplier
  {
    private QueryComponentSupplier delegate;

    public AvaticaBasedTestConnectionSupplier2(QueryComponentSupplier delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void gatherProperties(Properties properties)
    {
      delegate.gatherProperties(properties);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      throw new RuntimeException("f");
    }

    @Override
    public void configureGuice(CoreInjectorBuilder builder, List<Module> overrideModules)
    {
      delegate.configureGuice(builder);
      TestRequestLogger testRequestLogger = new TestRequestLogger();

      overrideModules.add(new DiscovertModule());

      builder.addModule(
          binder -> {
            // why need to add this?
//            binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(false));
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(AuthenticatorMapper.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_MAPPER);
//            binder.bind(AuthorizerMapper.class).toInstance(CalciteTests.TEST_AUTHORIZER_MAPPER);
            binder.bind(Escalator.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
            binder.bind(RequestLogger.class).toInstance(testRequestLogger);
            binder.bind(String.class)
                .annotatedWith(DruidSchemaName.class)
                .toInstance(CalciteTests.DRUID_SCHEMA_NAME);
            binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class);
            binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
            binder.bind(QueryScheduler.class)
                .toProvider(QuerySchedulerProvider.class)
                .in(LazySingleton.class);
//            binder.install(new SqlModule.SqlStatementFactoryModule());
            binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>()
            {
            }).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of())));
            binder.bind(CalciteRulesManager.class).toInstance(new CalciteRulesManager(ImmutableSet.of()));
            binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
          }
      );

      if(true) {

        builder.add(
        new Log4jShutterDownerModule(),
        new LifecycleModule(),
          ExtensionsModule.SecondaryModule.class,
            new DruidAuthModule(),
          TLSCertificateCheckerModule.class,
//          EmitterModule.class,
            HttpClientModule.global(),
            HttpClientModule.escalatedGlobal(),
            new HttpClientModule("druid.broker.http", Client.class, true),
            new HttpClientModule("druid.broker.http", EscalatedClient.class, true),
            new CuratorModule(),
            new AnnouncerModule(),
//          new MetricsModule(),
            new SegmentWriteOutMediumModule(),
            new ServerModule(),
            new StorageNodeModule(),
            new JettyServerModule(),
            new ExpressionModule(),
            new NestedDataModule(),
            new DiscoveryModule(),
            new ServerViewModule(),
            new MetadataConfigModule(),
            new DerbyMetadataStorageDruidModule(),
            new JacksonConfigManagerModule(),
            new CoordinatorDiscoveryModule(),
            new LocalDataStorageDruidModule(),
            new TombstoneDataStorageModule(),
            new FirehoseModule(),
            new JavaScriptModule(),
            new AuthenticatorModule(),
//            new AuthenticatorMapperModule(),
//            new EscalatorModule(),
            new AuthorizerModule(),
            new AuthorizerMapperModule(),
            new StartupLoggingModule(),
            new ExternalStorageAccessSecurityModule(),
            new ServiceClientModule(),
            new StorageConnectorModule(),
            new SqlModule()

);
//      builder.addModules();
//        builder.addModules(new CliBroker2().getmodules2().toArray(new Module[0]));

      overrideModules.addAll(brokerModules());

      builder.add(ServerInjectorBuilder.registerNodeRoleModule(ImmutableSet.of()));

      }

    }

    @Override
    public QueryRunnerFactoryConglomerate createCongolmerate(Builder builder, Closer closer)
    {
      return delegate.createCongolmerate(builder, closer);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory, Injector injector)
    {
      return delegate.createQuerySegmentWalker(conglomerate, joinableFactory, injector);
    }

    @Override
    public SqlEngine createEngine(QueryLifecycleFactory qlf, ObjectMapper objectMapper, Injector injector)
    {
      return delegate.createEngine(qlf, objectMapper, injector);
    }

    @Override
    public void configureJsonMapper(ObjectMapper mapper)
    {
      delegate.configureJsonMapper(mapper);
    }

    @Override
    public JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider)
    {
      return delegate.createJoinableFactoryWrapper(lookupProvider);
    }

    @Override
    public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
    {
      delegate.finalizeTestFramework(sqlTestFramework);
    }

    @Override
    public void close() throws IOException
    {
      delegate.close();
    }

    @Override
    public PlannerComponentSupplier getPlannerComponentSupplier()
    {
      return delegate.getPlannerComponentSupplier();
    }
  }


  private static void main3(Object object) throws Exception
 {

    SqlTestFramework framework = getCI2().framework
        ;

    if(true) {
        Lifecycle lifecycle = GuiceRunnable.initLifecycle(framework.injector(), log);

        chk1();
        chkStatus();


        lifecycle.stop();
    }else {

    }

  }

  private static void chk1() throws IOException, InterruptedException
  {
    HttpRequest request = HttpRequest.newBuilder()
       .uri(URI.create("http://localhost:12345/druid/v2/sql"))
       .header("Content-Type", "application/json")
       .POST(BodyPublishers.ofString("{\"query\":\"Select * from foo\"}"))
       .build();
    System.out.println(request);
    HttpClient hc = HttpClient.newHttpClient();
    HttpResponse<String> a = hc.send(request, HttpResponse.BodyHandlers.ofString());
    System.out.println(a);
    assertNotEquals(400, a.statusCode());
  }
  private static void chkStatus() throws IOException, InterruptedException
  {
    HttpRequest request = HttpRequest.newBuilder()
       .uri(URI.create("http://localhost:12345/status"))
       .header("Content-Type", "application/json")
       .GET()
       .build();
    System.out.println(request);
//        request.
    HttpClient hc = HttpClient.newHttpClient();
    HttpResponse<String> a = hc.send(request, HttpResponse.BodyHandlers.ofString());
    System.out.println(a);
    assertNotEquals(400, a.statusCode());

  }


  private static List<? extends Module> brokerModules()
  {
    return ImmutableList.of(
        new LegacyBrokerParallelMergeConfigModule(),
        new BrokerProcessingModule(),
//        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new SegmentWranglerModule(),
        new JoinableFactoryModule(),
        new BrokerServiceModule(),
        binder -> {

          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
              TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME
          );
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);
          binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(true);
          binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(false));

//          binder.bind(CachingClusteredClient.class).in(LazySingleton.class);
//          LifecycleModule.register(binder, BrokerServerView.class);
//          LifecycleModule.register(binder, MetadataSegmentView.class);
          binder.bind(TimelineServerView.class).to(BrokerServerView.class).in(LazySingleton.class);
//
//          JsonConfigProvider.bind(binder, "druid.broker.cache", CacheConfig.class);
//          binder.install(new CacheModule());
//
          JsonConfigProvider.bind(binder, "druid.broker.select", TierSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.select.tier.custom", CustomTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.balancer", ServerSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.retryPolicy", RetryQueryRunnerConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.segment", BrokerSegmentWatcherConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.internal.query.config", InternalQueryConfig.class);

//          binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);

          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

//          binder.bind(BrokerQueryResource.class).in(LazySingleton.class);
          Jerseys.addResource(binder, BrokerQueryResource.class);
//          binder.bind(SubqueryGuardrailHelper.class).toProvider(SubqueryGuardrailHelperProvider.class);
//          binder.bind(QueryCountStatsProvider.class).to(BrokerQueryResource.class).in(LazySingleton.class);
//          binder.bind(SubqueryCountStatsProvider.class).toInstance(new SubqueryCountStatsProvider());
          Jerseys.addResource(binder, BrokerResource.class);
          Jerseys.addResource(binder, ClientInfoResource.class);

          LifecycleModule.register(binder, BrokerQueryResource.class);

//          Jerseys.addResource(binder, HttpServerInventoryViewResource.class);

          LifecycleModule.register(binder, Server.class);
//          binder.bind(SegmentManager.class).in(LazySingleton.class);
//          binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
          binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.BROKER));
//          Jerseys.addResource(binder, HistoricalResource.class);
//          Jerseys.addResource(binder, SegmentListerResource.class);

//          if (isZkEnabled) {
//            LifecycleModule.register(binder, ZkCoordinator.class);
//          }

//          bindAnnouncer(
//              binder,
//              DiscoverySideEffectsProvider.withLegacyAnnouncer()
//          );
          binder.bind(String.class)
          .annotatedWith(DruidSchemaName.class)
          .toInstance(CalciteTests.DRUID_SCHEMA_NAME);

          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));
        }
//        new LookupModule(),
//        new LookylooModule(),
//        new SqlModule()
    );
  }

}
