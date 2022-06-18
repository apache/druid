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

import org.apache.druid.catalog.SchemaRegistry.SchemaSpec;

import javax.inject.Inject;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Metadata catalog which reads from the catalog storage. No caching.
 * For testing, and as the Coordinator-side implementation of the remote
 * synchronization protocol.
 */
public class LocalMetadataCatalog implements MetadataCatalog
{
  private final CatalogSource catalog;
  private final SchemaRegistry schemaRegistry;

  @Inject
  public LocalMetadataCatalog(
      CatalogSource catalog,
      SchemaRegistry schemaRegistry
  )
  {
    this.catalog = catalog;
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public TableMetadata resolveTable(TableId tableId)
  {
    return catalog.table(tableId);
  }

  @Override
  public List<TableMetadata> tables(String schemaName)
  {
    SchemaSpec schema = schemaRegistry.schema(schemaName);
    if (schema == null || !schema.writable()) {
      return Collections.emptyList();
    }
    return catalog.tablesForSchema(schemaName);
  }

  @Override
  public Set<String> tableNames(String schemaName)
  {
    SchemaSpec schema = schemaRegistry.schema(schemaName);
    if (schema == null || !schema.writable()) {
      return Collections.emptySet();
    }
    List<TableMetadata> catalogTables = catalog.tablesForSchema(schemaName);
    Set<String> tables = new HashSet<>();
    for (TableMetadata table : catalogTables) {
      tables.add(table.name());
    }
    return tables;
  }
}
