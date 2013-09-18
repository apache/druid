/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
