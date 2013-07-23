package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.db.DbTablesConfig;
import org.skife.jdbi.v2.IDBI;

/**
 */
public class DbConnectorModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.db.tables", DbTablesConfig.class);
    JsonConfigProvider.bind(binder, "druid.db.connector", DbConnectorConfig.class);

    binder.bind(DbConnector.class);
  }

  @Provides @LazySingleton
  public IDBI getDbi(final DbConnector dbConnector)
  {
    return dbConnector.getDBI();
  }
}
