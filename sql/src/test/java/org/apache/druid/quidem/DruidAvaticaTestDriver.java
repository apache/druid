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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.avatica.AvaticaMonitor;
import org.apache.druid.sql.avatica.DruidAvaticaJsonHandler;
import org.apache.druid.sql.avatica.DruidMeta;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.ConfigurationInstance;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigInstance;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigStore;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.Builder;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.jetty.server.Server;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class DruidAvaticaTestDriver implements Driver
{
  static {
    new DruidAvaticaTestDriver().register();
  }

  public static final String URI_PREFIX = "druidtest://";
  public static final String DEFAULT_URI = URI_PREFIX + "/";

  static final SqlTestFrameworkConfigStore CONFIG_STORE = new SqlTestFrameworkConfigStore();

  public DruidAvaticaTestDriver()
  {
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException
  {
    if (!acceptsURL(url)) {
      return null;
    }
    SqlTestFrameworkConfigInstance config = buildConfigfromURIParams(url);

    ConfigurationInstance ci = CONFIG_STORE.getConfigurationInstance(
        config,
        tempDirProducer -> new AvaticaBasedTestConnectionSupplier(
            new StandardComponentSupplier(tempDirProducer)
        )
    );

    try {
      AvaticaJettyServer server = ci.framework.injector().getInstance(AvaticaJettyServer.class);
      return server.getConnection(info);
    }
    catch (Exception e) {
      throw new SQLException("Can't create testconnection", e);
    }
  }

  static class AvaticaBasedConnectionModule implements DruidModule, Closeable
  {
    Closer closer = Closer.create();

    @Provides
    @LazySingleton
    public DruidSchemaCatalog getLookupNodeService(QueryRunnerFactoryConglomerate conglomerate,
        SpecificSegmentsQuerySegmentWalker walker, PlannerConfig plannerConfig)
    {
      return CalciteTests.createMockRootSchema(
          conglomerate,
          walker,
          plannerConfig,
          CalciteTests.TEST_AUTHORIZER_MAPPER
      );
    }

    @Provides
    @LazySingleton
    public DruidConnectionExtras getConnectionExtras(ObjectMapper objectMapper)
    {
      return new DruidConnectionExtras.DruidConnectionExtrasImpl(objectMapper);
    }

    @Provides
    @LazySingleton
    public AvaticaJettyServer getAvaticaServer(DruidMeta druidMeta, DruidConnectionExtras druidConnectionExtras) throws Exception
    {
      AvaticaJettyServer avaticaJettyServer = new AvaticaJettyServer(druidMeta, druidConnectionExtras);
      closer.register(avaticaJettyServer);
      return avaticaJettyServer;
    }

    @Override
    public void configure(Binder binder)
    {
    }

    @Override
    public void close() throws IOException
    {
      closer.close();
    }

  }

  static class AvaticaJettyServer implements Closeable
  {
    final DruidMeta druidMeta;
    final Server server;
    final String url;
    final DruidConnectionExtras connectionExtras;

    AvaticaJettyServer(final DruidMeta druidMeta, DruidConnectionExtras druidConnectionExtras) throws Exception
    {
      this.druidMeta = druidMeta;
      server = new Server(0);
      server.setHandler(getAvaticaHandler(druidMeta));
      server.start();
      url = StringUtils.format(
          "jdbc:avatica:remote:url=%s",
          new URIBuilder(server.getURI()).setPath(DruidAvaticaJsonHandler.AVATICA_PATH).build()
      );
      connectionExtras = druidConnectionExtras;
    }

    public Connection getConnection(Properties info) throws SQLException
    {
      Connection realConnection = DriverManager.getConnection(url, info);
      Connection proxyConnection = DynamicComposite.make(
          realConnection,
          Connection.class,
          connectionExtras,
          DruidConnectionExtras.class
      );
      return proxyConnection;
    }

    @Override
    public void close()
    {
      druidMeta.closeAllConnections();
      try {
        server.stop();
      }
      catch (Exception e) {
        throw new RuntimeException("Can't stop server", e);
      }
    }

    protected AbstractAvaticaHandler getAvaticaHandler(final DruidMeta druidMeta)
    {
      return new DruidAvaticaJsonHandler(
          druidMeta,
          new DruidNode("dummy", "dummy", false, 1, null, true, false),
          new AvaticaMonitor()
      );
    }
  }

  static class AvaticaBasedTestConnectionSupplier implements QueryComponentSupplier
  {
    private QueryComponentSupplier delegate;
    private AvaticaBasedConnectionModule connectionModule;

    public AvaticaBasedTestConnectionSupplier(QueryComponentSupplier delegate)
    {
      this.delegate = delegate;
      this.connectionModule = new AvaticaBasedConnectionModule();
    }

    @Override
    public void gatherProperties(Properties properties)
    {
      delegate.gatherProperties(properties);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      delegate.configureGuice(builder);
      TestRequestLogger testRequestLogger = new TestRequestLogger();
      builder.addModule(connectionModule);
      builder.addModule(
          binder -> {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(AuthenticatorMapper.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_MAPPER);
            binder.bind(AuthorizerMapper.class).toInstance(CalciteTests.TEST_AUTHORIZER_MAPPER);
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
            binder.install(new SqlModule.SqlStatementFactoryModule());
            binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>()
            {
            }).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of())));
            binder.bind(CalciteRulesManager.class).toInstance(new CalciteRulesManager(ImmutableSet.of()));
            binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
          }
      );
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
      connectionModule.close();
      delegate.close();
    }

    @Override
    public PlannerComponentSupplier getPlannerComponentSupplier()
    {
      return delegate.getPlannerComponentSupplier();
    }
  }

  protected File createTempFolder(String prefix)
  {
    File tempDir = FileUtils.createTempDir(prefix);
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        try {
          FileUtils.deleteDirectory(tempDir);
        }
        catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    });
    return tempDir;
  }

  public static SqlTestFrameworkConfigInstance buildConfigfromURIParams(String url) throws SQLException
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

    SqlTestFrameworkConfig config = MapToInterfaceHandler.newInstanceFor(SqlTestFrameworkConfig.class, queryParams);
    return new SqlTestFrameworkConfigInstance(config);
  }

  private void register()
  {
    try {
      DriverManager.registerDriver(this);
    }
    catch (SQLException e) {
      System.out.println("Error occurred while registering JDBC driver " + this.getClass().getName() + ": " + e);
    }
  }

  @Override
  public boolean acceptsURL(String url)
  {
    return url.startsWith(URI_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
  {
    throw new RuntimeException("Unimplemented method!");
  }

  @Override
  public int getMajorVersion()
  {
    return 0;
  }

  @Override
  public int getMinorVersion()
  {
    return 0;
  }

  @Override
  public boolean jdbcCompliant()
  {
    return false;
  }

  @Override
  public Logger getParentLogger()
  {
    return Logger.getLogger("");
  }
}
