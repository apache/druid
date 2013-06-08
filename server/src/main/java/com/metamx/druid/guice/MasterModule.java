package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.client.ServerInventoryViewConfig;
import com.metamx.druid.client.indexing.IndexingService;
import com.metamx.druid.client.indexing.IndexingServiceClient;
import com.metamx.druid.client.indexing.IndexingServiceSelector;
import com.metamx.druid.client.selector.DiscoverySelector;
import com.metamx.druid.client.selector.Server;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.db.DbTablesConfig;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.druid.master.DruidMasterConfig;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceProvider;
import org.skife.jdbi.v2.IDBI;

/**
 */
public class MasterModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, ZkPathsConfig.class);
    ConfigProvider.bind(binder, ServerInventoryViewConfig.class);
    ConfigProvider.bind(binder, DbConnectorConfig.class);

    JsonConfigProvider.bind(binder, "druid.database.tables", DbTablesConfig.class);

    binder.bind(new TypeLiteral<DiscoverySelector<Server>>(){})
          .annotatedWith(IndexingService.class)
          .to(IndexingServiceSelector.class)
          .in(ManageLifecycle.class);
    binder.bind(IndexingServiceClient.class).in(LazySingleton.class);
  }

  @Provides @ManageLifecycle @IndexingService
  public DiscoverySelector<Server> getIndexingServiceSelector(DruidMasterConfig config, ServiceDiscovery serviceDiscovery)
  {
    final ServiceProvider serviceProvider = serviceDiscovery.serviceProviderBuilder()
                                                            .serviceName(config.getMergerServiceName())
                                                            .build();

    return new IndexingServiceSelector(serviceProvider);
  }

  @Provides @LazySingleton
  public IDBI getDbi(final DbConnector dbConnector, final DbConnectorConfig config, Lifecycle lifecycle)
  {
    if (config.isCreateTables()) {
      lifecycle.addHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              dbConnector.createSegmentTable();
              dbConnector.createRulesTable();
            }

            @Override
            public void stop()
            {

            }
          }
      );
    }

    return dbConnector.getDBI();
  }
}
