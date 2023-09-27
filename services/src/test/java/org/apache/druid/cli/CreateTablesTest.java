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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Locale;

public class CreateTablesTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;

  @Before
  public void setup()
  {
    this.connector = derbyConnectorRule.getConnector();
  }

  @Test
  public void testRunCreatesAllTables()
  {
    final MetadataStorageTablesConfig config = derbyConnectorRule.metadataTablesConfigSupplier().get();
    Assert.assertNotNull(config);

    // Verify that tables do not exist before starting
    Assert.assertFalse(tableExists(config.getDataSourceTable()));
    Assert.assertFalse(tableExists(config.getSegmentsTable()));
    Assert.assertFalse(tableExists(config.getPendingSegmentsTable()));
    Assert.assertFalse(tableExists(config.getUpgradeSegmentsTable()));
    Assert.assertFalse(tableExists(config.getConfigTable()));
    Assert.assertFalse(tableExists(config.getRulesTable()));
    Assert.assertFalse(tableExists(config.getAuditTable()));
    Assert.assertFalse(tableExists(config.getSupervisorTable()));
    Assert.assertFalse(tableExists(config.getTaskLockTable()));

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
    Assert.assertTrue(tableExists(config.getDataSourceTable()));
    Assert.assertTrue(tableExists(config.getSegmentsTable()));
    Assert.assertTrue(tableExists(config.getPendingSegmentsTable()));
    Assert.assertTrue(tableExists(config.getUpgradeSegmentsTable()));
    Assert.assertTrue(tableExists(config.getConfigTable()));
    Assert.assertTrue(tableExists(config.getRulesTable()));
    Assert.assertTrue(tableExists(config.getAuditTable()));
    Assert.assertTrue(tableExists(config.getSupervisorTable()));
    Assert.assertTrue(tableExists(config.getTaskLockTable()));
  }

  private boolean tableExists(String tableName)
  {
    return connector.retryWithHandle(
        handle -> connector.tableExists(handle, tableName.toUpperCase(Locale.ENGLISH))
    );
  }

}
