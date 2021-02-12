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

package org.apache.druid.metadata;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class exists so that {@link MetadataStorageConnectorConfig} is asked for password every time a brand new
 * connection is established with DB. {@link PasswordProvider} impls such as those based on AWS tokens refresh the
 * underlying token periodically since each token is valid for a certain period of time only.
 * So, This class overrides (and largely copies due to lack of extensibility), the methods from base class in order to keep
 * track of connection properties and call {@link MetadataStorageConnectorConfig#getPassword()} everytime a new
 * connection is setup.
 */
public class BasicDataSourceExt extends BasicDataSource
{
  private static final Logger LOGGER = new Logger(BasicDataSourceExt.class);

  private Properties connectionProperties;
  private final MetadataStorageConnectorConfig connectorConfig;

  public BasicDataSourceExt(MetadataStorageConnectorConfig connectorConfig)
  {
    this.connectorConfig = connectorConfig;
    this.connectionProperties = new Properties();
  }

  @Override
  public void addConnectionProperty(String name, String value)
  {
    connectionProperties.put(name, value);
    super.addConnectionProperty(name, value);
  }

  @Override
  public void removeConnectionProperty(String name)
  {
    connectionProperties.remove(name);
    super.removeConnectionProperty(name);
  }

  @Override
  public void setConnectionProperties(String connectionProperties)
  {
    if (connectionProperties == null) {
      throw new NullPointerException("connectionProperties is null");
    }

    String[] entries = connectionProperties.split(";");
    Properties properties = new Properties();
    for (String entry : entries) {
      if (entry.length() > 0) {
        int index = entry.indexOf('=');
        if (index > 0) {
          String name = entry.substring(0, index);
          String value = entry.substring(index + 1);
          properties.setProperty(name, value);
        } else {
          // no value is empty string which is how java.util.Properties works
          properties.setProperty(entry, "");
        }
      }
    }
    this.connectionProperties = properties;
    super.setConnectionProperties(connectionProperties);
  }

  @VisibleForTesting
  public Properties getConnectionProperties()
  {
    return connectionProperties;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() throws SQLException
  {
    Driver driverToUse = getDriver();

    if (driverToUse == null) {
      Class<?> driverFromCCL = null;
      if (getDriverClassName() != null) {
        try {
          try {
            if (getDriverClassLoader() == null) {
              driverFromCCL = Class.forName(getDriverClassName());
            } else {
              driverFromCCL = Class.forName(
                  getDriverClassName(), true, getDriverClassLoader());
            }
          }
          catch (ClassNotFoundException cnfe) {
            driverFromCCL = Thread.currentThread(
            ).getContextClassLoader().loadClass(
                getDriverClassName());
          }
        }
        catch (Exception t) {
          String message = "Cannot load JDBC driver class '" +
                           getDriverClassName() + "'";
          LOGGER.error(t, message);
          throw new SQLException(message, t);
        }
      }

      try {
        if (driverFromCCL == null) {
          driverToUse = DriverManager.getDriver(getUrl());
        } else {
          // Usage of DriverManager is not possible, as it does not
          // respect the ContextClassLoader
          // N.B. This cast may cause ClassCastException which is handled below
          driverToUse = (Driver) driverFromCCL.newInstance();
          if (!driverToUse.acceptsURL(getUrl())) {
            throw new SQLException("No suitable driver", "08001");
          }
        }
      }
      catch (Exception t) {
        String message = "Cannot create JDBC driver of class '" +
                         (getDriverClassName() != null ? getDriverClassName() : "") +
                         "' for connect URL '" + getUrl() + "'";
        LOGGER.error(t, message);
        throw new SQLException(message, t);
      }
    }

    if (driverToUse == null) {
      throw new RE("Failed to find the DB Driver");
    }

    final Driver finalDriverToUse = driverToUse;

    return () -> {
      String user = connectorConfig.getUser();
      if (user != null) {
        connectionProperties.put("user", user);
      } else {
        log("DBCP DataSource configured without a 'username'");
      }

      // Note: This is the main point of this class where we are getting fresh password before setting up
      // every new connection.
      String password = connectorConfig.getPassword();
      if (password != null) {
        connectionProperties.put("password", password);
      } else {
        log("DBCP DataSource configured without a 'password'");
      }

      return finalDriverToUse.connect(connectorConfig.getConnectURI(), connectionProperties);
    };
  }
}
