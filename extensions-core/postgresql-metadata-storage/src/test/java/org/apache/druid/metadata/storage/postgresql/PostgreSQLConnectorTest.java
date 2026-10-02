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

package org.apache.druid.metadata.storage.postgresql;

import com.google.common.base.Suppliers;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;

public class PostgreSQLConnectorTest
{
  private CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  public void initPostgreSQLConnectorTest(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
  }

  public static Object[][] constructorFeeder()
  {
    return new Object[][]{
        {CentralizedDatasourceSchemaConfig.enabled(false)},
        {CentralizedDatasourceSchemaConfig.enabled(true)}
    };
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIsTransientException(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initPostgreSQLConnectorTest(centralizedDatasourceSchemaConfig);
    PostgreSQLConnector connector = new PostgreSQLConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase(null)),
        new PostgreSQLConnectorConfig(),
        new PostgreSQLTablesConfig(),
        centralizedDatasourceSchemaConfig
    );

    Assertions.assertTrue(connector.isTransientException(new SQLException("bummer, connection problem", "08DIE")));
    Assertions.assertTrue(connector.isTransientException(new SQLException("bummer, too many things going on", "53RES")));
    Assertions.assertFalse(connector.isTransientException(new SQLException("oh god, no!", "58000")));
    Assertions.assertFalse(connector.isTransientException(new SQLException("help!")));
    Assertions.assertFalse(connector.isTransientException(new SQLException()));
    Assertions.assertFalse(connector.isTransientException(new Exception("I'm not happy")));
    Assertions.assertFalse(connector.isTransientException(new Throwable("I give up")));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIsUniqueConstraintViolation(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initPostgreSQLConnectorTest(centralizedDatasourceSchemaConfig);
    PostgreSQLConnector connector = new PostgreSQLConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase(null)),
        new PostgreSQLConnectorConfig(),
        new PostgreSQLTablesConfig(),
        centralizedDatasourceSchemaConfig
    );

    // PostgreSQL unique_violation SQL state (23505)
    Assertions.assertTrue(connector.isUniqueConstraintViolation(
        new SQLException("duplicate key value violates unique constraint", "23505")
    ));

    // Different SQL state should return false
    Assertions.assertFalse(connector.isUniqueConstraintViolation(
        new SQLException("some other error", "42P01")
    ));

    // SQLException wrapped in another exception (tests cause chain traversal)
    Assertions.assertTrue(connector.isUniqueConstraintViolation(
        new RuntimeException(new SQLException("duplicate key", "23505"))
    ));

    // Non-SQLException exception
    Assertions.assertFalse(connector.isUniqueConstraintViolation(new Exception("not a SQLException")));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testLimitClause(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initPostgreSQLConnectorTest(centralizedDatasourceSchemaConfig);
    PostgreSQLConnector connector = new PostgreSQLConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase(null)),
        new PostgreSQLConnectorConfig(),
        new PostgreSQLTablesConfig(),
        centralizedDatasourceSchemaConfig
    );
    Assertions.assertEquals("LIMIT 100", connector.limitClause(100));
  }
}
