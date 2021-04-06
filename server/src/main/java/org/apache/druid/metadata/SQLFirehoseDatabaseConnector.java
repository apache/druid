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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.utils.ConnectionUriUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public abstract class SQLFirehoseDatabaseConnector
{
  static final int MAX_RETRIES = 10;

  public <T> T retryWithHandle(
      HandleCallback<T> callback,
      Predicate<Throwable> myShouldRetry
  )
  {
    try {
      return RetryUtils.retry(() -> getDBI().withHandle(callback), myShouldRetry, MAX_RETRIES);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public final boolean isTransientException(Throwable e)
  {
    return e != null && (e instanceof RetryTransactionException
                         || e instanceof SQLTransientException
                         || e instanceof SQLRecoverableException
                         || e instanceof UnableToObtainConnectionException
                         || e instanceof UnableToExecuteStatementException
                         || (e instanceof SQLException && isTransientException(e.getCause()))
                         || (e instanceof DBIException && isTransientException(e.getCause())));
  }

  protected BasicDataSource getDatasource(
      MetadataStorageConnectorConfig connectorConfig,
      JdbcAccessSecurityConfig securityConfig
  )
  {
    // We validate only the connection URL here as all properties will be read from only the URL except
    // users and password. If we want to allow another way to specify user properties such as using
    // MetadataStorageConnectorConfig.getDbcpProperties(), those properties should be validated as well.
    validateConfigs(connectorConfig.getConnectURI(), securityConfig);
    BasicDataSource dataSource = new BasicDataSourceExt(connectorConfig);
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    dataSource.setUrl(uri);
    dataSource.setTestOnBorrow(true);
    dataSource.setValidationQuery(getValidationQuery());

    return dataSource;
  }

  private void validateConfigs(String urlString, JdbcAccessSecurityConfig securityConfig)
  {
    if (Strings.isNullOrEmpty(urlString)) {
      throw new IllegalArgumentException("connectURI cannot be null or empty");
    }
    if (!securityConfig.isEnforceAllowedProperties()) {
      // You don't want to do anything with properties.
      return;
    }
    final Set<String> propertyKeyFromConnectURL = findPropertyKeysFromConnectURL(urlString);
    ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
        propertyKeyFromConnectURL,
        securityConfig.getSystemPropertyPrefixes(),
        securityConfig.getAllowedProperties()
    );
  }

  public String getValidationQuery()
  {
    return "SELECT 1";
  }

  public abstract DBI getDBI();

  /**
   * Extract property keys from the given JDBC URL.
   */
  public abstract Set<String> findPropertyKeysFromConnectURL(String connectUri);
}
