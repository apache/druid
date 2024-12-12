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

package org.apache.druid.catalog.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.CatalogException.DuplicateKeyException;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Builds on the generic {@link CatalogSyncTest} to focus on cache-specific
 * operations.
 */
public class CatalogCacheTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;
  private ObjectMapper jsonMapper;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
    jsonMapper = new ObjectMapper();
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  /**
   * Test overall cache lifecycle. Detailed checks of contents is done
   * in {@link CatalogSyncTest} and is not repeated here.
   */
  @Test
  public void testLifecycle() throws DuplicateKeyException
  {
    // Create entries with no listener.
    TableMetadata table1 = TableBuilder.datasource("table1", "P1D")
        .timeColumn()
        .column("a", Columns.STRING)
        .build();
    storage.validate(table1);
    storage.tables().create(table1);

    // Create a listener. Starts with the cache empty
    CachedMetadataCatalog cache1 = new CachedMetadataCatalog(storage, storage.schemaRegistry(), jsonMapper);
    storage.register(cache1);
    assertTrue(cache1.tableNames(TableId.DRUID_SCHEMA).isEmpty());

    // Load table on demand.
    assertNotNull(cache1.getTable(table1.id()));
    assertEquals(1, cache1.tableNames(TableId.DRUID_SCHEMA).size());

    // Flush to empty the cache.
    cache1.flush();
    assertTrue(cache1.tableNames(TableId.DRUID_SCHEMA).isEmpty());

    // Resync to reload the cache.
    cache1.resync();
    assertEquals(1, cache1.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache1.getTable(table1.id()));

    // Add a table: cache is updated.
    TableMetadata table2 = TableBuilder.datasource("table2", "P1D")
        .timeColumn()
        .column("dim", Columns.STRING)
        .column("measure", Columns.LONG)
        .build();
    storage.validate(table2);
    storage.tables().create(table2);
    assertEquals(2, cache1.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache1.getTable(table2.id()));

    // Second listener. Starts with the cache empty.
    CachedMetadataCatalog cache2 = new CachedMetadataCatalog(storage, storage.schemaRegistry(), jsonMapper);
    storage.register(cache2);
    assertTrue(cache2.tableNames(TableId.DRUID_SCHEMA).isEmpty());

    // Second listener resyncs.
    cache2.resync();
    assertEquals(2, cache2.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache2.getTable(table1.id()));
    assertNotNull(cache2.getTable(table2.id()));

    // Add a third table: both caches updated.
    TableMetadata table3 = TableBuilder.datasource("table3", "PT1H")
        .timeColumn()
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    storage.validate(table3);
    storage.tables().create(table3);
    assertEquals(3, cache1.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache1.getTable(table3.id()));
    assertEquals(3, cache2.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache2.getTable(table3.id()));

    // Another resync puts us back where we are.
    cache1.flush();
    assertTrue(cache1.tableNames(TableId.DRUID_SCHEMA).isEmpty());
    cache1.resync();
    assertEquals(3, cache1.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache1.getTable(table3.id()));

    cache2.resync();
    assertEquals(3, cache2.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache2.getTable(table3.id()));

    // Third cache, managed by the receiver.
    CachedMetadataCatalog cache3 = new CachedMetadataCatalog(storage, storage.schemaRegistry(), jsonMapper);
    storage.register(cache3);
    CatalogUpdateReceiver receiver = new CatalogUpdateReceiver(cache3);
    receiver.start();
    assertEquals(3, cache3.tableNames(TableId.DRUID_SCHEMA).size());
    assertNotNull(cache3.getTable(table3.id()));
  }
}
