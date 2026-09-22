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
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.util.Locale;

public class CreateTablesTest
{
  @RegisterExtension
  public static final TestDerbyConnector.DerbyConnectorRule5 DERBY_CONNECTOR_RULE
      = new TestDerbyConnector.DerbyConnectorRule5();

  private TestDerbyConnector connector;

  @BeforeEach
  public void setup()
  {
    this.connector = DERBY_CONNECTOR_RULE.getConnector();
  }

  @Test
  public void testRunCreatesAllTables()
  {
    final MetadataStorageTablesConfig config = DERBY_CONNECTOR_RULE.metadataTablesConfigSupplier().get();
    Assertions.assertNotNull(config);

    // Verify that tables do not exist before starting
    Assertions.assertFalse(tableExists(config.getDataSourceTable()));
    Assertions.assertFalse(tableExists(config.getSegmentsTable()));
    Assertions.assertFalse(tableExists(config.getPendingSegmentsTable()));
    Assertions.assertFalse(tableExists(config.getUpgradeSegmentsTable()));
    Assertions.assertFalse(tableExists(config.getConfigTable()));
    Assertions.assertFalse(tableExists(config.getRulesTable()));
    Assertions.assertFalse(tableExists(config.getAuditTable()));
    Assertions.assertFalse(tableExists(config.getSupervisorTable()));
    Assertions.assertFalse(tableExists(config.getTaskLockTable()));
    Assertions.assertFalse(tableExists(config.getIndexingStatesTable()));

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
    Assertions.assertTrue(tableExists(config.getDataSourceTable()));
    Assertions.assertTrue(tableExists(config.getSegmentsTable()));
    Assertions.assertTrue(tableExists(config.getPendingSegmentsTable()));
    Assertions.assertTrue(tableExists(config.getUpgradeSegmentsTable()));
    Assertions.assertTrue(tableExists(config.getConfigTable()));
    Assertions.assertTrue(tableExists(config.getRulesTable()));
    Assertions.assertTrue(tableExists(config.getAuditTable()));
    Assertions.assertTrue(tableExists(config.getSupervisorTable()));
    Assertions.assertTrue(tableExists(config.getTaskLockTable()));
    Assertions.assertTrue(tableExists(config.getIndexingStatesTable()));
  }

  private boolean tableExists(String tableName)
  {
    return connector.retryWithHandle(
        handle -> connector.tableExists(handle, tableName.toUpperCase(Locale.ENGLISH))
    );
  }

}
