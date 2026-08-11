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

package org.apache.druid.metadata.storage.mysql;

import com.google.common.base.Supplier;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;

public class MySQLConnectorTest
{
  private static final MySQLConnectorDriverConfig MYSQL_DRIVER_CONFIG = new MySQLConnectorDriverConfig();
  private static final MySQLConnectorDriverConfig MARIADB_DRIVER_CONFIG = new MySQLConnectorDriverConfig()
  {
    @Override
    public String getDriverClassName()
    {
      return "org.mariadb.jdbc.Driver";
    }
  };
  private static final Supplier<MetadataStorageConnectorConfig> CONNECTOR_CONFIG_SUPPLIER =
      MetadataStorageConnectorConfig::new;
  private static final Supplier<MetadataStorageTablesConfig> TABLES_CONFIG_SUPPLIER =
      () -> MetadataStorageTablesConfig.fromBase(null);

  private CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  public void initMySQLConnectorTest(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
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
  public void testIsExceptionTransientMySql(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initMySQLConnectorTest(centralizedDatasourceSchemaConfig);
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MYSQL_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );
    Assertions.assertTrue(
        connector.connectorIsTransientException(new SQLException("some transient failure", "s0", 1317))
    );
    Assertions.assertFalse(
        connector.connectorIsTransientException(new SQLException("totally realistic test data", "s0", 1337))
    );
    Assertions.assertTrue(
        connector.connectorIsTransientException(new SQLTransientConnectionException("transient"))
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIsExceptionTransientNoMySqlClazz(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initMySQLConnectorTest(centralizedDatasourceSchemaConfig);
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MARIADB_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );
    // no vendor specific for MariaDb, so should always be false
    Assertions.assertFalse(connector.connectorIsTransientException(new SQLTransientException()));
    Assertions.assertFalse(
        connector.connectorIsTransientException(new SQLException("some transient failure", "s0", 1317))
    );
    Assertions.assertFalse(
        connector.connectorIsTransientException(new SQLException("totally realistic test data", "s0", 1337))
    );
    Assertions.assertFalse(
        connector.connectorIsTransientException(new SQLTransientConnectionException("transient"))
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIsRootCausePacketTooBigException(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initMySQLConnectorTest(centralizedDatasourceSchemaConfig);
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MYSQL_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );

    // The test method should return true only for
    // mariadb.MaxAllowedPacketException or mysql.PacketTooBigException.
    // Verifying this requires creating a mock Class object, but Class is final
    // and has only a private constructor. It would be overkill to try to mock it.

    // Verify some of the false cases
    Assertions.assertFalse(
        connector.isRootCausePacketTooBigException(new SQLException())
    );
    Assertions.assertFalse(
        connector.isRootCausePacketTooBigException(new SQLTransientException())
    );
    Assertions.assertFalse(
        connector.isRootCausePacketTooBigException(new SQLTransientException())
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIsUniqueConstraintViolation(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initMySQLConnectorTest(centralizedDatasourceSchemaConfig);
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MYSQL_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );

    // MySQL integrity_constraint_violation SQL state (23000)
    Assertions.assertTrue(connector.isUniqueConstraintViolation(
        new SQLException("Duplicate entry 'value' for key 'PRIMARY'", "23000")
    ));

    // Different SQL state should return false
    Assertions.assertFalse(connector.isUniqueConstraintViolation(
        new SQLException("some other error", "42S02")
    ));

    // SQLException wrapped in another exception (tests cause chain traversal)
    Assertions.assertTrue(connector.isUniqueConstraintViolation(
        new RuntimeException(new SQLException("Duplicate entry", "23000"))
    ));

    // Non-SQLException exception
    Assertions.assertFalse(connector.isUniqueConstraintViolation(new Exception("not a SQLException")));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testLimitClause(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    initMySQLConnectorTest(centralizedDatasourceSchemaConfig);
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MYSQL_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );
    Assertions.assertEquals("LIMIT 100", connector.limitClause(100));
  }
}
