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

package io.druid.metadata;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DerbyConnectionFactory implements ConnectionFactory
{
  final private String dbName;

  public DerbyConnectionFactory(String dbName) {
    this.dbName = dbName;
  }

  public Connection openConnection() throws SQLException {
    final String nsURL=String.format("jdbc:derby://localhost:1527/%s;create=true", dbName);
    try {
      Class.forName("org.apache.derby.jdbc.ClientDriver");
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return DriverManager.getConnection(nsURL);
  }
}
