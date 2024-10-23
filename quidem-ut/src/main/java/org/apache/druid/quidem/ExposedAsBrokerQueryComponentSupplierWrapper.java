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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import org.apache.druid.cli.CliBroker;
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
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.AnnouncerModule;
import org.apache.druid.guice.BrokerProcessingModule;
import org.apache.druid.guice.BrokerServiceModule;
import org.apache.druid.guice.CoordinatorDiscoveryModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.ExtensionsModule;
import org.apache.druid.guice.JacksonConfigManagerModule;
import org.apache.druid.guice.JavaScriptModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.LocalDataStorageDruidModule;
import org.apache.druid.guice.MetadataConfigModule;
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
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.rpc.guice.ServiceClientModule;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumModule;
import org.apache.druid.server.BrokerQueryResource;
import org.apache.druid.server.ClientInfoResource;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.SubqueryGuardrailHelper;
import org.apache.druid.server.SubqueryGuardrailHelperProvider;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.emitter.EmitterModule;
import org.apache.druid.server.http.BrokerResource;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.initialization.AuthorizerMapperModule;
import org.apache.druid.server.initialization.ExternalStorageAccessSecurityModule;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.initialization.jetty.JettyServerModule;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.metrics.SubqueryCountStatsProvider;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.apache.druid.server.security.TLSCertificateCheckerModule;
import org.apache.druid.sql.calcite.schema.BrokerSegmentMetadataCache;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplierDelegate;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.druid.storage.StorageConnectorModule;
import org.apache.druid.timeline.PruneLoadSpec;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;

/**
 * A wrapper class to expose a {@link QueryComponentSupplier} as a Broker service.
 */
public class ExposedAsBrokerQueryComponentSupplierWrapper extends QueryComponentSupplierDelegate
{
  public ExposedAsBrokerQueryComponentSupplierWrapper(QueryComponentSupplier delegate)
  {
    super(delegate);
  }

  @Override
  public void configureGuice(CoreInjectorBuilder builder, List<Module> overrideModules)
  {
    super.configureGuice(builder);

    installForServerModules(builder);
    builder.add(new QueryRunnerFactoryModule());

    overrideModules.addAll(ExposedAsBrokerQueryComponentSupplierWrapper.brokerModules());
    overrideModules.add(new BrokerTestModule());
    builder.add(QuidemCaptureModule.class);
  }

  public static class BrokerTestModule extends AbstractModule
  {
    @Override
    protected void configure()
    {
    }

    @Provides
    @LazySingleton
    public BrokerSegmentMetadataCache provideCache()
    {
      return null;
    }

    @Provides
    @LazySingleton
    public Properties getProps()
    {
      Properties localProps = new Properties();
      localProps.put("druid.enableTlsPort", "false");
      localProps.put("druid.zk.service.enabled", "false");
      localProps.put("druid.plaintextPort", "12345");
      localProps.put("druid.host", "localhost");
      localProps.put("druid.broker.segment.awaitInitializationOnStart", "false");
      return localProps;
    }

    @Provides
    @LazySingleton
    DruidNodeDiscoveryProvider getDruidNodeDiscoveryProvider()
    {
      final DruidNode coordinatorNode = CalciteTests.mockCoordinatorNode();
      return CalciteTests.mockDruidNodeDiscoveryProvider(coordinatorNode);
    }
  }

  /**
   * Closely related to {@link CoreInjectorBuilder#forServer()}
   */
  private void installForServerModules(CoreInjectorBuilder builder)
  {

    builder.add(
        new Log4jShutterDownerModule(),
        new LifecycleModule(),
        ExtensionsModule.SecondaryModule.class,
        new DruidAuthModule(),
        TLSCertificateCheckerModule.class,
        EmitterModule.class,
        HttpClientModule.global(),
        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("druid.broker.http", Client.class, true),
        new HttpClientModule("druid.broker.http", EscalatedClient.class, true),
        new CuratorModule(),
        new AnnouncerModule(),
        new SegmentWriteOutMediumModule(),
        new ServerModule(),
        new StorageNodeModule(),
        new JettyServerModule(),
        new ExpressionModule(),
        new DiscoveryModule(),
        new ServerViewModule(),
        new MetadataConfigModule(),
        new DerbyMetadataStorageDruidModule(),
        new JacksonConfigManagerModule(),
        new CoordinatorDiscoveryModule(),
        new LocalDataStorageDruidModule(),
        new TombstoneDataStorageModule(),
        new JavaScriptModule(),
        new AuthenticatorModule(),
        new AuthorizerModule(),
        new AuthorizerMapperModule(),
        new StartupLoggingModule(),
        new ExternalStorageAccessSecurityModule(),
        new ServiceClientModule(),
        new StorageConnectorModule(),
        new SqlModule(),
        ServerInjectorBuilder.registerNodeRoleModule(ImmutableSet.of())
    );
  }

  /**
   * Closely related to {@link CliBroker#getModules}.
   */
  static List<? extends Module> brokerModules()
  {
    return ImmutableList.of(
        new BrokerProcessingModule(),
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

          binder.bind(TimelineServerView.class).to(BrokerServerView.class).in(LazySingleton.class);

          JsonConfigProvider.bind(binder, "druid.broker.select", TierSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.select.tier.custom", CustomTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.balancer", ServerSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.retryPolicy", RetryQueryRunnerConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.segment", BrokerSegmentWatcherConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.internal.query.config", InternalQueryConfig.class);
          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

          binder.bind(BrokerQueryResource.class).in(LazySingleton.class);
          Jerseys.addResource(binder, BrokerQueryResource.class);
          binder.bind(SubqueryGuardrailHelper.class).toProvider(SubqueryGuardrailHelperProvider.class);
          binder.bind(QueryCountStatsProvider.class).to(BrokerQueryResource.class).in(LazySingleton.class);
          binder.bind(SubqueryCountStatsProvider.class).toInstance(new SubqueryCountStatsProvider());
          Jerseys.addResource(binder, BrokerResource.class);
          Jerseys.addResource(binder, ClientInfoResource.class);

          LifecycleModule.register(binder, BrokerQueryResource.class);

          LifecycleModule.register(binder, Server.class);
          binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.BROKER));

          binder.bind(String.class)
              .annotatedWith(DruidSchemaName.class)
              .toInstance(CalciteTests.DRUID_SCHEMA_NAME);

          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));
        }
    );
  }
}
