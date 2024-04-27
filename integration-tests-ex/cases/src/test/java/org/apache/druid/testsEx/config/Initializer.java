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

package org.apache.druid.testsEx.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import org.apache.druid.cli.GuiceRunnable;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.curator.discovery.DiscoveryModule;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.AnnouncerModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LegacyBrokerParallelMergeConfigModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.SQLMetadataStorageDruidModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.http.HttpClientModule;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.jackson.DruidServiceSerializerModifier;
import org.apache.druid.jackson.StringObjectPairList;
import org.apache.druid.jackson.ToStringObjectPairListDeserializer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.core.LoggingEmitterConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.NoopMetadataStorageProvider;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorDriverConfig;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorSslConfig;
import org.apache.druid.metadata.storage.mysql.MySQLMetadataStorageModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.IntegrationTestingConfigProvider;
import org.apache.druid.testing.clients.AdminClient;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.cluster.MetastoreClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * The magic needed to piece together enough of Druid to allow clients to
 * run without server dependencies being pulled in. Used to set up the
 * Guice injector used to inject members into integration tests, while
 * reading configuration from the docker.yaml or similar test
 * configuration file.
 * <p>
 * Much of the work here deals the tedious task of assembling Druid
 * modules, sometimes using copy/past to grab the part that a client
 * wants (such as object deserialization) without the parts that the
 * server needs (and which would introduce the need for unused configuration
 * just to make dependencies work.)
 * <p>
 * See the documentation for these test for the "user view" of this
 * class and its configuration.
 */
public class Initializer
{
  public static final String TEST_CONFIG_PROPERTY = "testConfig";
  public static final String TEST_CONFIG_VAR = "TEST_CONFIG";
  public static final String CLUSTER_RESOURCES = "/cluster/";
  public static final String CLUSTER_CONFIG_RESOURCE = CLUSTER_RESOURCES + "%s/%s.yaml";
  public static final String CLUSTER_CONFIG_DEFAULT = "docker";
  public static final String METASTORE_CONFIG_PROPERTY = "sqlConfig";
  public static final String METASTORE_CONFIG_RESOURCE = "/metastore/%s.sql";
  public static final String METASTORE_CONFIG_DEFAULT = "init";

  private static final Logger log = new Logger(Initializer.class);

  public static String queryFile(Class<?> category, String fileName)
  {
    return CLUSTER_RESOURCES + category.getSimpleName() + "/queries/" + fileName;
  }

  private static class TestModule implements DruidModule
  {
    ResolvedConfig config;

    public TestModule(ResolvedConfig config)
    {
      this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
      binder
          .bind(ResolvedConfig.class)
          .toInstance(config);
      binder
          .bind(IntegrationTestingConfig.class)
          .to(IntegrationTestingConfigEx.class)
          .in(LazySingleton.class);
      binder
          .bind(MetastoreClient.class)
          .in(LazySingleton.class);

      // Dummy DruidNode instance to make Guice happy. This instance is unused.
      DruidNode dummy = new DruidNode("integration-tests", "localhost", false, 9191, null, null, true, false);
      binder
          .bind(DruidNode.class)
          .annotatedWith(Self.class)
          .toInstance(dummy);

      // Required for MSQIndexingModule
      binder.bind(new TypeLiteral<Set<NodeRole>>()
      {
      }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));

      // Reduced form of SQLMetadataStorageDruidModule
      String prop = SQLMetadataStorageDruidModule.PROPERTY;
      String defaultValue = MySQLMetadataStorageModule.TYPE;
      PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageConnector.class), defaultValue);
      PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageProvider.class), defaultValue);
      PolyBind.createChoiceWithDefault(binder, prop, Key.get(SQLMetadataConnector.class), defaultValue);

      // Reduced form of MetadataConfigModule
      // Not actually used here (tests don't create tables), but needed by MySQLConnector constructor
      JsonConfigProvider.bind(binder, MetadataStorageTablesConfig.PROPERTY_BASE, MetadataStorageTablesConfig.class);

      // Build from properties provided in the config
      JsonConfigProvider.bind(
          binder,
          MetadataStorageConnectorConfig.PROPERTY_BASE,
          MetadataStorageConnectorConfig.class
      );
    }

    @Provides
    @TestClient
    public HttpClient getHttpClient(
        IntegrationTestingConfig config,
        Lifecycle lifecycle,
        @Client HttpClient delegate
    )
    {
      return delegate;
    }

    @Provides
    @AdminClient
    public HttpClient getAdminClient(@Client HttpClient delegate)
    {
      BasicCredentials basicCredentials = new BasicCredentials("admin", "priest");
      return new CredentialedHttpClient(basicCredentials, delegate);
    }

    @Provides
    @ManageLifecycle
    public ServiceEmitter getServiceEmitter(ObjectMapper jsonMapper)
    {
      return new ServiceEmitter("", "", new LoggingEmitter(new LoggingEmitterConfig(), jsonMapper));
    }

    // From ServerModule to allow deserialization of DiscoveryDruidNode objects from ZK.
    // We don't want the other dependencies of that module.
    @Override
    public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
    {
      return ImmutableList.of(
          new SimpleModule()
              .addDeserializer(StringObjectPairList.class, new ToStringObjectPairListDeserializer())
              .setSerializerModifier(new DruidServiceSerializerModifier())
      );
    }
  }

  /**
   * Reduced form of MySQLMetadataStorageModule.
   */
  private static class TestMySqlModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
      JsonConfigProvider.bind(binder, "druid.metadata.mysql.ssl", MySQLConnectorSslConfig.class);
      JsonConfigProvider.bind(binder, "druid.metadata.mysql.driver", MySQLConnectorDriverConfig.class);
      String type = MySQLMetadataStorageModule.TYPE;
      PolyBind
          .optionBinder(binder, Key.get(MetadataStorageProvider.class))
          .addBinding(type)
          .to(NoopMetadataStorageProvider.class)
          .in(LazySingleton.class);

      PolyBind
          .optionBinder(binder, Key.get(MetadataStorageConnector.class))
          .addBinding(type)
          .to(MySQLConnector.class)
          .in(LazySingleton.class);

      PolyBind
          .optionBinder(binder, Key.get(SQLMetadataConnector.class))
          .addBinding(type)
          .to(MySQLConnector.class)
          .in(LazySingleton.class);
    }

    @Override
    public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
    {
      return new MySQLMetadataStorageModule().getJacksonModules();
    }
  }

  /**
   * Class used by test to identify test-specific options, load configuration
   * and "inject themselves" with dependencies.
   */
  public static class Builder
  {
    private final String clusterName;
    private String configFile;
    private Object test;
    private List<Module> modules = new ArrayList<>();
    private boolean validateCluster;
    private List<Class<?>> eagerCreation = new ArrayList<>();
    private Map<String, String> envVarBindings = new HashMap<>();
    private Properties testProperties = new Properties();

    public Builder(String clusterName)
    {
      this.clusterName = clusterName;

      // Node discovery is lifecycle managed. If we're using it, we have to
      // create the instance within Guice during setup so it can be lifecycle
      // managed. Using LazySingleon works in a server, but not in test clients,
      // because test clients declare their need of node discovery after the
      // the lifecycle starts.
      eagerInstance(DruidNodeDiscoveryProvider.class);

      // Set properties from environment variables, or hard-coded values
      // previously set in Maven.
      propertyEnvVarBinding("druid.test.config.dockerIp", "DOCKER_IP");
      propertyEnvVarBinding("druid.zk.service.host", "DOCKER_IP");
      property("druid.client.https.trustStorePath", "client_tls/truststore.jks");
      property("druid.client.https.trustStorePassword", "druid123");
      property("druid.client.https.keyStorePath", "client_tls/client.jks");
      property("druid.client.https.certAlias", "druid");
      property("druid.client.https.keyManagerPassword", "druid123");
      property("druid.client.https.keyStorePassword", "druid123");
      propertyEnvVarBinding("druid.metadata.mysql.driver.driverClassName", "MYSQL_DRIVER_CLASSNAME");

      // More env var bindings for properties formerly passed in via
      // a generated config file.
      final String base = IntegrationTestingConfigProvider.PROPERTY_BASE + ".";
      propertyEnvVarBinding(base + "cloudBucket", "DRUID_CLOUD_BUCKET");
      propertyEnvVarBinding(base + "cloudPath", "DRUID_CLOUD_PATH");
      propertyEnvVarBinding(base + "s3AccessKey", "AWS_ACCESS_KEY_ID");
      propertyEnvVarBinding(base + "s3SecretKey", "AWS_SECRET_ACCESS_KEY");
      propertyEnvVarBinding(base + "s3Region", "AWS_REGION");
      propertyEnvVarBinding(base + "azureContainer", "AZURE_CONTAINER");
      propertyEnvVarBinding(base + "azureAccount", "AZURE_ACCOUNT");
      propertyEnvVarBinding(base + "azureKey", "AZURE_KEY");
      propertyEnvVarBinding(base + "googleBucket", "GOOGLE_BUCKET");
      propertyEnvVarBinding(base + "googlePrefix", "GOOGLE_PREFIX");

      // Other defaults
      // druid.global.http.numMaxThreads avoids creating 40+ Netty threads.
      // We only ever use 1.
      property("druid.global.http.numMaxThreads", 3);
      property("druid.broker.http.numMaxThreads", 3);
    }

    /**
     * Load a configuration from the named file. Primarily for
     * debugging to use a one-off, custom configuration file.
     */
    public Builder configFile(String configFile)
    {
      this.configFile = configFile;
      return this;
    }

    /**
     * The test class with members to be injected.
     */
    public Builder test(Object test)
    {
      this.test = test;
      return this;
    }

    /**
     * Druid provides the {@code PolyBind} abstraction and the {@code Lifecycle}
     * abstraction. When used together, we can encounter initialization issues. We won't create
     * and instance of a polymorphic binding until it is first needed, and only then does
     * the instance add itself to the lifecycle. However, if it is a test that acks for
     * the instance, that is too late: the lifecycle has started. A test should call this
     * method to "register" polymorphic lifecycle classes that will be injected later.
     * <p>
     * The builder registers {@code DruidNodeDiscoveryProvider} by default: add any
     * test-specific instances as needed.
     */
    public Builder eagerInstance(Class<?> theClass)
    {
      this.eagerCreation.add(theClass);
      return this;
    }

    /**
     * Optional test-specific modules to load.
     */
    public Builder modules(List<Module> modules)
    {
      this.modules.addAll(modules);
      return this;
    }

    public Builder modules(Module... modules)
    {
      return modules(Arrays.asList(modules));
    }

    /**
     * Validates the cluster before running tests. Ensures that each
     * Druid service reports itself as healthy. Since Druid services
     * depend on ZK and the metadata DB, this indirectly checks their
     * health as well.
     */
    public Builder validateCluster()
    {
      this.validateCluster = true;
      return this;
    }

    /**
     * Set a property value in code. Such values go into the {@link Properties}
     * object in Guice, and act as defaults to properties defined in the config
     * file or via system properties. These properties can also "hard code" items
     * that would normally be user-settable in a server. The value can be of any
     * type: it is converted to a String internally.
     */
    public Builder property(String key, Object value)
    {
      if (value == null) {
        testProperties.remove(key);
      } else {
        testProperties.put(key, value.toString());
      }
      return this;
    }

    /**
     * Bind a property value to an environment variable. Useful if the property
     * is set in the environment via the build system, Maven or other means.
     * Avoids the need to add command-line arguments of the form
     * {@code -Dproperty.name=$ENV_VAR}. Environment variable bindings take
     * precedence over values set via {@link #property(String, Object)}, or
     * the config file, but are lower priority than system properties. The
     * environment variable is used only if set, else it is ignored.
     */
    public Builder propertyEnvVarBinding(String property, String envVar)
    {
      this.envVarBindings.put(property, envVar);
      return this;
    }

    public synchronized Initializer build()
    {
      return new Initializer(this);
    }
  }

  private final ResolvedConfig clusterConfig;
  private final Injector injector;
  private final Lifecycle lifecycle;
  private DruidClusterClient clusterClient;

  private Initializer(Builder builder)
  {
    if (builder.configFile != null) {
      this.clusterConfig = loadConfigFile(builder.clusterName, builder.configFile);
    } else {
      this.clusterConfig = loadConfig(builder.clusterName, builder.configFile);
    }
    this.injector = makeInjector(builder, clusterConfig);

    // Do the injection of test members early, for force lazy singleton
    // instance creation to avoid problems when lifecycle-managed objects
    // are combined with PolyBind.
    if (builder.test != null) {
      this.injector.injectMembers(builder.test);
    }

    // Backup: instantiate any additional instances that might be referenced
    // later outside of injection.
    for (Class<?> eagerClass : builder.eagerCreation) {
      this.injector.getInstance(eagerClass);
    }

    // Now that we've created lifecycle-managed instances, start the lifecycle.
    log.info("Starting lifecycle");
    this.lifecycle = GuiceRunnable.initLifecycle(injector, log);

    // Verify the cluster to ensure it is ready.
    log.info("Creating cluster client");
    this.clusterClient = this.injector.getInstance(DruidClusterClient.class);
    if (builder.validateCluster) {
      clusterClient.validate();
    }

    // Now that the cluster is ready (which implies that the metastore is ready),
    // load any "starter data" into the metastore. Warning: there is a time-lag between
    // when the DB is updated and when Coordinator or Overlord learns about the updates.
    // At present, there is no API to force a cache flush. Caveat emptor.
    prepareDB();
  }

  public static Builder builder(String clusterName)
  {
    return new Builder(clusterName);
  }

  private static ResolvedConfig loadConfig(String category, String configName)
  {
    if (configName == null) {
      configName = System.getProperty(TEST_CONFIG_PROPERTY);
    }
    if (configName == null) {
      configName = System.getenv(TEST_CONFIG_VAR);
    }
    if (configName == null) {
      configName = CLUSTER_CONFIG_DEFAULT;
    }
    String loadName = StringUtils.format(CLUSTER_CONFIG_RESOURCE, category, configName);
    ClusterConfig config = ClusterConfig.loadFromResource(loadName);
    return config.resolve(category);
  }

  private static ResolvedConfig loadConfigFile(String category, String path)
  {
    ClusterConfig config = ClusterConfig.loadFromFile(path);
    return config.resolve(category);
  }

  private static Injector makeInjector(
      Builder builder,
      ResolvedConfig clusterConfig
  )
  {
    Injector startupInjector = new StartupInjectorBuilder()
        .withProperties(properties(builder, clusterConfig))
        .build();
    return new CoreInjectorBuilder(startupInjector)
        .withLifecycle()
        .add(
            // Required by clients
            new EscalatorModule(),
            HttpClientModule.global(),
            HttpClientModule.escalatedGlobal(),
            new HttpClientModule("druid.broker.http", Client.class, true),
            new HttpClientModule("druid.broker.http", EscalatedClient.class, true),
            // For ZK discovery
            new CuratorModule(),
            new AnnouncerModule(),
            new DiscoveryModule(),
            // Dependencies from other modules
            new LegacyBrokerParallelMergeConfigModule(),
            // Dependencies from other modules
            new StorageNodeModule(),
            new MSQExternalDataSourceModule(),

            // Test-specific items, including bits copy/pasted
            // from modules that don't play well in a client setting.
            new TestModule(clusterConfig),
            new TestMySqlModule()
        )
        .addAll(builder.modules)
        .build();
  }

  /**
   * Define test properties similar to how the server does. Property precedence
   * is:
   * <ul>
   * <li>System properties (highest)</li>
   * <li>Environment variable bindings</li>
   * <li>Configuration file</li>
   * <li>Hard-coded values (lowest></li>
   * </ul>
   */
  private static Properties properties(
      Builder builder,
      ResolvedConfig clusterConfig
  )
  {
    Properties finalProperties = new Properties();
    finalProperties.putAll(builder.testProperties);
    finalProperties.putAll(clusterConfig.toProperties());
    for (Entry<String, String> entry : builder.envVarBindings.entrySet()) {
      String value = System.getenv(entry.getValue());
      if (value != null) {
        finalProperties.put(entry.getKey(), value);
      }
    }
    finalProperties.putAll(System.getProperties());
    log.info("Properties:");
    log.info(finalProperties.toString());
    return finalProperties;
  }

  /**
   * Some tests need a known set of metadata in the metadata DB. To avoid the
   * complexity of do the actual actions (such as creating segments), the tests
   * "seed" the database directly. The result is not entirely valid and consistent,
   * but is good enough for the test at hand.
   * <p>
   * <b>WARNING</b>: At present, there is no way to force the Coordinator or
   * Overlord to flush its cache to learn about these new entries. Instead, we have
   * to sleep for the cache timeout period. This solution is unsatisfying, and error-prone.
   */
  private void prepareDB()
  {
    ResolvedMetastore metastoreConfig = clusterConfig.metastore();
    if (metastoreConfig == null) {
      return;
    }
    List<MetastoreStmt> stmts = metastoreConfig.initStmts();
    if (stmts == null || stmts.isEmpty()) {
      return;
    }
    log.info("Preparing database");
    MetastoreClient client = injector.getInstance(MetastoreClient.class);
    for (MetastoreStmt stmt : stmts) {
      client.execute(stmt.toSQL());
    }
    try {
      Thread.sleep(metastoreConfig.initDelaySec() * 1000);
    }
    catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for coordinator to notice DB changes");
    }
    log.info("Database prepared");
  }

  public Injector injector()
  {
    return injector;
  }

  public ResolvedConfig clusterConfig()
  {
    return clusterConfig;
  }

  public MetastoreClient metastoreClient()
  {
    if (clusterConfig.metastore() == null) {
      throw new IAE("Please provide a metastore section in docker.yaml");
    }
    return injector.getInstance(MetastoreClient.class);
  }

  public DruidClusterClient clusterClient()
  {
    return clusterClient;
  }

  public void close()
  {
    lifecycle.stop();
  }
}
