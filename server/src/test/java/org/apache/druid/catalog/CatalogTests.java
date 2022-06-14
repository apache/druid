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

package org.apache.druid.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule;
import org.apache.druid.metadata.catalog.CatalogManager;
import org.apache.druid.metadata.catalog.SQLCatalogManager;

import java.util.Arrays;

public class CatalogTests
{
  public static InputFormat csvFormat()
  {
    return new CsvInputFormat(
        Arrays.asList("x", "y", "z"),
        null,  // listDelimiter
        false, // hasHeaderRow
        false, // findColumnsFromHeader
        0      // skipHeaderRows
        );
  }

  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public static class DbFixture
  {
    public CatalogManager manager;
    public CatalogStorage storage;

    public DbFixture(DerbyConnectorRule derbyConnectorRule)
    {
      MetastoreManager metastoreMgr = new MetastoreManagerImpl(
          JSON_MAPPER,
          derbyConnectorRule.getConnector(),
          () -> derbyConnectorRule.getMetadataConnectorConfig(),
          derbyConnectorRule.metadataTablesConfigSupplier()
          );
      manager = new SQLCatalogManager(metastoreMgr);
      manager.start();
      storage = new CatalogStorage(manager, DummyRequest.AUTH_MAPPER);
    }

    public void tearDown()
    {
      if (manager != null) {
        manager.stop();
        manager = null;
      }
    }
  }

  public static void tearDown(DbFixture fixture)
  {
    if (fixture != null) {
      fixture.tearDown();
    }
  }

}
