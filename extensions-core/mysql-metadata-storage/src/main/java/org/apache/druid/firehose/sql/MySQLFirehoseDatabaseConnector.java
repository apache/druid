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

package org.apache.druid.firehose.sql;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import com.mysql.jdbc.NonRegisteringDriver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.utils.Throwables;
import org.skife.jdbi.v2.DBI;

import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;


@JsonTypeName("mysql")
public class MySQLFirehoseDatabaseConnector extends SQLFirehoseDatabaseConnector
{
  private final DBI dbi;
  private final MetadataStorageConnectorConfig connectorConfig;

  @JsonCreator
  public MySQLFirehoseDatabaseConnector(
      @JsonProperty("connectorConfig") MetadataStorageConnectorConfig connectorConfig,
      @JacksonInject JdbcAccessSecurityConfig securityConfig
  )
  {
    this.connectorConfig = connectorConfig;
    final BasicDataSource datasource = getDatasource(connectorConfig, securityConfig);
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("com.mysql.jdbc.Driver");
    this.dbi = new DBI(datasource);
  }

  @JsonProperty
  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  @Override
  public Set<String> findPropertyKeysFromConnectURL(String connectUrl)
  {
    // This method should be in sync with
    // - org.apache.druid.server.lookup.jdbc.JdbcDataFetcher.checkConnectionURL()
    // - org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace.checkConnectionURL()
    Properties properties;
    try {
      NonRegisteringDriver driver = new NonRegisteringDriver();
      properties = driver.parseURL(connectUrl, null);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
    catch (Throwable e) {
      if (Throwables.isThrowable(e, NoClassDefFoundError.class)
          || Throwables.isThrowable(e, ClassNotFoundException.class)) {
        if (e.getMessage().contains("com/mysql/jdbc/NonRegisteringDriver")) {
          throw new RuntimeException(
              "Failed to find MySQL driver class. Please check the MySQL connector version 5.1.48 is in the classpath",
              e
          );
        }
      }
      throw new RuntimeException(e);
    }

    if (properties == null) {
      throw new IAE("Invalid URL format for MySQL: [%s]", connectUrl);
    }
    Set<String> keys = Sets.newHashSetWithExpectedSize(properties.size());
    properties.forEach((k, v) -> keys.add((String) k));
    return keys;
  }
}
