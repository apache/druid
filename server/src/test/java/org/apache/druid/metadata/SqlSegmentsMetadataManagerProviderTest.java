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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Locale;

public class SqlSegmentsMetadataManagerProviderTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Test
  public void testLifecycleStartCreatesSegmentTables() throws Exception
  {
    final TestDerbyConnector connector = derbyConnectorRule.getConnector();
    final SegmentsMetadataManagerConfig config = new SegmentsMetadataManagerConfig();
    final Lifecycle lifecycle = new Lifecycle();
    final SegmentSchemaCache segmentSchemaCache = new SegmentSchemaCache(new NoopServiceEmitter());
    SqlSegmentsMetadataManagerProvider provider = new SqlSegmentsMetadataManagerProvider(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector,
        lifecycle,
        segmentSchemaCache,
        CentralizedDatasourceSchemaConfig.create()
    );
    SegmentsMetadataManager manager = provider.get();
    Assert.assertTrue(manager instanceof SqlSegmentsMetadataManager);

    final MetadataStorageTablesConfig storageConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    final String segmentsTable = storageConfig.getSegmentsTable();
    final String upgradeSegmentsTable = storageConfig.getUpgradeSegmentsTable();

    // Verify that the tables do not exist yet
    Assert.assertFalse(tableExists(segmentsTable, connector));
    Assert.assertFalse(tableExists(upgradeSegmentsTable, connector));

    lifecycle.start();

    // Verify that tables have now been created
    Assert.assertTrue(tableExists(segmentsTable, connector));
    Assert.assertTrue(tableExists(upgradeSegmentsTable, connector));

    lifecycle.stop();
  }

  private boolean tableExists(String tableName, TestDerbyConnector connector)
  {
    return connector.retryWithHandle(
        handle -> connector.tableExists(handle, tableName.toUpperCase(Locale.ENGLISH))
    );
  }
}
