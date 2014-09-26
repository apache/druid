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
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DerbyConnectionFactory implements ConnectionFactory
{
  private String dbName;

  public DerbyConnectionFactory(String dbName) {
    this.dbName = dbName;
  }

  public Connection openConnection() throws SQLException {
    String nsURL="jdbc:derby://localhost:1527/"+dbName+";create=true";

    try {
      Class.forName("org.apache.derby.jdbc.ClientDriver");
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }


    Connection conn = DriverManager.getConnection(nsURL);
    return conn;
  }
}
