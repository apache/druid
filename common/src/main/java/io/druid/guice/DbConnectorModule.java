/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.db.DbConnector;
import io.druid.db.DbConnectorConfig;
import io.druid.db.DbTablesConfig;
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
