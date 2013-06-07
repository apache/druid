package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.client.ServerInventoryViewConfig;
import com.metamx.druid.client.indexing.IndexingServiceClient;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.db.DbTablesConfig;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.http.client.HttpClient;
import org.skife.jdbi.v2.DBI;

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

  }

  @Provides
  public IndexingServiceClient getIndexingServiceClient(HttpClient client)
  {
    // TODO
    return null;
  }

  @Provides @LazySingleton
  public DBI getDbi(final DbConnector dbConnector, final DbConnectorConfig config, Lifecycle lifecycle)
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
