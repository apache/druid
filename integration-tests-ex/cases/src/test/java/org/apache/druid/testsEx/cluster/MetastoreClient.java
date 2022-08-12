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

package org.apache.druid.testsEx.cluster;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorDriverConfig;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorSslConfig;
import org.apache.druid.testsEx.config.ResolvedConfig;
import org.apache.druid.testsEx.config.ResolvedMetastore;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Simple test-time client to the MySQL metastore.
 * <p>
 * Used to verify that the DB is up and available. The JDBC
 * connection can be used to query the metadata DB for tests.
 * <p>
 * Also handles running metastore setup queries on test
 * startup. Write such queries to be idempotent: REPLACE
 * rather than INSERT, for example.
 */
public class MetastoreClient
{
  // See SQLMetadataConnector.getValidationQuery()
  // That instance isn't available here, so we punt.
  public static String VALIDATION_QUERY = "SELECT 1";

  private final ResolvedConfig clusterConfig;
  private final ResolvedMetastore config;
  private DBI dbi;
  private Handle handle;

  public MetastoreClient(ResolvedConfig config)
  {
    this.clusterConfig = config;
    this.config = config.requireMetastore();
    prepare();
    validate();
  }

  private void prepare()
  {
    // This approach is rather overkill and is MySQL-specific.
    // It does have the advantage of exercising the actual Druid code.
    MetadataStorageConnectorConfig msConfig = clusterConfig.toMetadataConfig();
    MySQLConnectorDriverConfig driverConfig = config.toDriverConfig();
    MySQLConnectorSslConfig sslConfig = new MySQLConnectorSslConfig();
    dbi = MySQLConnector.createDBI(msConfig, driverConfig, sslConfig, VALIDATION_QUERY);
    handle = dbi.open();
  }

  private void validate()
  {
    boolean ok = execute(VALIDATION_QUERY);
    if (!ok) {
      throw new ISE("Metadata store validation failed");
    }
  }

  public Connection connection()
  {
    return handle.getConnection();
  }

  public boolean execute(String sql)
  {
    try {
      return connection().prepareStatement(sql).execute();
    }
    catch (SQLException e) {
      throw new ISE(e, "Metadata query failed");
    }
  }

  public void close()
  {
    handle.close();
    handle = null;
  }
}
