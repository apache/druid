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

package io.druid.db;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import org.apache.derby.drda.NetworkServerControl;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.ConnectionFactory;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.net.InetAddress;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class DerbyConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(DerbyConnector.class);
  private final DBI dbi;

  @Inject
  public DerbyConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);
    this.dbi = new DBI(getConnectionFactory("druidDerbyDb"));
  }

  @Override
  public DBI getDBI() { return dbi; }

  private ConnectionFactory getConnectionFactory(String dbName)
  {
    try {
      NetworkServerControl server = new NetworkServerControl(InetAddress.getByName("localhost"),1527);
      server.start(null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return new DerbyConnectionFactory(dbName);
  }
}
