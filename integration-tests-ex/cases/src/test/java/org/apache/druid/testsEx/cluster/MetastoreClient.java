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
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.inject.Inject;

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

  private DBI dbi;
  private Handle handle;

  @Inject
  public MetastoreClient(MetadataStorageConnector connector)
  {
    SQLMetadataConnector sqlConnector = (SQLMetadataConnector) connector;
    this.dbi = sqlConnector.getDBI();
    this.handle = dbi.open();
  }

  public void validate()
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
