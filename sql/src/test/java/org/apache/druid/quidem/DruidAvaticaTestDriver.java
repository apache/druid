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
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.avatica.AvaticaMonitor;
import org.apache.druid.sql.avatica.DruidAvaticaJsonHandler;
import org.apache.druid.sql.avatica.DruidMeta;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.ConfigurationInstance;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigStore;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplierDelegate;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.server.Server;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class DruidAvaticaTestDriver implements Driver
{
  static {
    new DruidAvaticaTestDriver().register();
  }

  public static final String SCHEME = "druidtest";
  public static final String URI_PREFIX = SCHEME + "://";
  public static final String DEFAULT_URI = URI_PREFIX + "/";

  static final SqlTestFrameworkConfigStore CONFIG_STORE = new SqlTestFrameworkConfigStore(
      x -> new AvaticaBasedTestConnectionSupplier(x)
  );

  public DruidAvaticaTestDriver()
  {
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException
  {
    if (!acceptsURL(url)) {
      return null;
    }
    try {
      SqlTestFrameworkConfig config = SqlTestFrameworkConfig.fromURL(url);
      ConfigurationInstance ci = CONFIG_STORE.getConfigurationInstance(config);
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
    public DruidConnectionExtras getConnectionExtras(
        ObjectMapper objectMapper,
        DruidHookDispatcher druidHookDispatcher,
        @Named("isExplainSupported") Boolean isExplainSupported
    )
    {
      return new DruidConnectionExtras.DruidConnectionExtrasImpl(objectMapper, druidHookDispatcher, isExplainSupported);
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

  static class AvaticaBasedTestConnectionSupplier extends QueryComponentSupplierDelegate
  {
    private AvaticaBasedConnectionModule connectionModule;

    public AvaticaBasedTestConnectionSupplier(QueryComponentSupplier delegate)
    {
      super(delegate);
      this.connectionModule = new AvaticaBasedConnectionModule();
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      super.configureGuice(builder);
      builder.addModule(connectionModule);
      builder.addModule(
          binder -> {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
          }
      );
    }

    @Override
    public void close() throws IOException
    {
      connectionModule.close();
      super.close();
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
