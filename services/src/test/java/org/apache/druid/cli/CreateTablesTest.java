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

package org.apache.druid.cli;

import com.google.inject.Injector;
import org.apache.druid.metadata.JUnit5TestDerbyConnector;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.testing.junit5.JUnit5Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.util.Locale;

public class CreateTablesTest
{
  @RegisterExtension
  public final JUnit5TestDerbyConnector derbyConnectorRule
      = new JUnit5TestDerbyConnector();

  private TestDerbyConnector connector;

  @BeforeEach
  public void setup()
  {
    this.connector = derbyConnectorRule.getConnector();
  }

  @Test
  public void testRunCreatesAllTables()
  {
    final MetadataStorageTablesConfig config = derbyConnectorRule.metadataTablesConfigSupplier().get();
    JUnit5Assertions.assertNotNull(config);

    // Verify that tables do not exist before starting
    JUnit5Assertions.assertFalse(tableExists(config.getDataSourceTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getSegmentsTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getPendingSegmentsTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getUpgradeSegmentsTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getConfigTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getRulesTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getAuditTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getSupervisorTable()));
    JUnit5Assertions.assertFalse(tableExists(config.getTaskLockTable()));

    // Run CreateTables
    CreateTables createTables = new CreateTables()
    {
      @Override
      public Injector makeInjector()
      {
        Injector injector = Mockito.mock(Injector.class);
        Mockito.when(injector.getInstance(MetadataStorageConnector.class)).thenReturn(connector);
        return injector;
      }
    };
    createTables.run();

    // Verify that tables have now been created
    JUnit5Assertions.assertTrue(tableExists(config.getDataSourceTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getSegmentsTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getPendingSegmentsTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getUpgradeSegmentsTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getConfigTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getRulesTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getAuditTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getSupervisorTable()));
    JUnit5Assertions.assertTrue(tableExists(config.getTaskLockTable()));
  }

  private boolean tableExists(String tableName)
  {
    return connector.retryWithHandle(
        handle -> connector.tableExists(handle, tableName.toUpperCase(Locale.ENGLISH))
    );
  }

}
