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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class PostgreSQLConnectorTest
{
  private CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  public PostgreSQLConnectorTest(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
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
  public void testIsTransientException()
  {
    PostgreSQLConnector connector = new PostgreSQLConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase(null)),
        new PostgreSQLConnectorConfig(),
        new PostgreSQLTablesConfig(),
        centralizedDatasourceSchemaConfig
    );

    Assert.assertTrue(connector.isTransientException(new SQLException("bummer, connection problem", "08DIE")));
    Assert.assertTrue(connector.isTransientException(new SQLException("bummer, too many things going on", "53RES")));
    Assert.assertFalse(connector.isTransientException(new SQLException("oh god, no!", "58000")));
    Assert.assertFalse(connector.isTransientException(new SQLException("help!")));
    Assert.assertFalse(connector.isTransientException(new SQLException()));
    Assert.assertFalse(connector.isTransientException(new Exception("I'm not happy")));
    Assert.assertFalse(connector.isTransientException(new Throwable("I give up")));
  }

  @Test
  public void testLimitClause()
  {
    PostgreSQLConnector connector = new PostgreSQLConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase(null)),
        new PostgreSQLConnectorConfig(),
        new PostgreSQLTablesConfig(),
        centralizedDatasourceSchemaConfig
    );
    Assert.assertEquals("LIMIT 100", connector.limitClause(100));
  }
}
