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

package org.apache.druid.testing.embedded.psql;

import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgreSQLMetadataResourceTest
{
  @Test
  @Timeout(60)
  public void testPostgreSQLMetdataStore() throws Exception
  {
    final PostgreSQLMetadataResource resource = new PostgreSQLMetadataResource();

    resource.start();
    assertTrue(resource.isRunning());

    // Test database connection
    resource.onStarted(EmbeddedDruidCluster.empty());
    final String uri = resource.getConnectURI();
    final String userName = resource.getUsername();
    final String password = resource.getPassword();

    try (Connection connection = DriverManager.getConnection(uri, userName, password);
         Statement statement = connection.createStatement()) {
      final ResultSet resultSet = statement.executeQuery("SELECT CURRENT_DATABASE()");
      assertTrue(resultSet.next());
      assertEquals(resource.getDatabaseName(), resultSet.getString(1));
    }

    resource.stop();
    assertFalse(resource.isRunning());
  }
}
