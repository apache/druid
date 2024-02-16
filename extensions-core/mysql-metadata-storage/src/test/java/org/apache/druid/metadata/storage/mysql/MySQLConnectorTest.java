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
import com.mysql.jdbc.exceptions.MySQLTransactionRollbackException;
import com.mysql.jdbc.exceptions.MySQLTransientException;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
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

  public MySQLConnectorTest(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
  {
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    constructors.add(new Object[]{CentralizedDatasourceSchemaConfig.create()});
    CentralizedDatasourceSchemaConfig config = new CentralizedDatasourceSchemaConfig();
    config.setEnabled(true);
    constructors.add(new Object[]{config});
    return constructors;
  }

  @Test
  public void testIsExceptionTransientMySql()
  {
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MYSQL_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );
    Assert.assertTrue(connector.connectorIsTransientException(new MySQLTransientException()));
    Assert.assertTrue(connector.connectorIsTransientException(new MySQLTransactionRollbackException()));
    Assert.assertTrue(
        connector.connectorIsTransientException(new SQLException("some transient failure", "s0", 1317))
    );
    Assert.assertFalse(
        connector.connectorIsTransientException(new SQLException("totally realistic test data", "s0", 1337))
    );
    // this method does not specially handle normal transient exceptions either, since it is not vendor specific
    Assert.assertFalse(
        connector.connectorIsTransientException(new SQLTransientConnectionException("transient"))
    );
  }

  @Test
  public void testIsExceptionTransientNoMySqlClazz()
  {
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MARIADB_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );
    // no vendor specific for MariaDb, so should always be false
    Assert.assertFalse(connector.connectorIsTransientException(new MySQLTransientException()));
    Assert.assertFalse(
        connector.connectorIsTransientException(new SQLException("some transient failure", "s0", 1317))
    );
    Assert.assertFalse(
        connector.connectorIsTransientException(new SQLException("totally realistic test data", "s0", 1337))
    );
    Assert.assertFalse(
        connector.connectorIsTransientException(new SQLTransientConnectionException("transient"))
    );
  }

  @Test
  public void testIsRootCausePacketTooBigException()
  {
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
    Assert.assertFalse(
        connector.isRootCausePacketTooBigException(new SQLException())
    );
    Assert.assertFalse(
        connector.isRootCausePacketTooBigException(new SQLTransientException())
    );
    Assert.assertFalse(
        connector.isRootCausePacketTooBigException(new MySQLTransientException())
    );
  }

  @Test
  public void testLimitClause()
  {
    MySQLConnector connector = new MySQLConnector(
        CONNECTOR_CONFIG_SUPPLIER,
        TABLES_CONFIG_SUPPLIER,
        new MySQLConnectorSslConfig(),
        MYSQL_DRIVER_CONFIG,
        centralizedDatasourceSchemaConfig
    );
    Assert.assertEquals("LIMIT 100", connector.limitClause(100));
  }
}
