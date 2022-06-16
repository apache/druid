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

import org.apache.druid.catalog.MetadataCatalog.CatalogListener;
import org.apache.druid.catalog.SchemaRegistry.SchemaSpec;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caching version of the metadata catalog. Draws information
 * from a base catalog. Fetches from the base if:
 * <ul>
 * <li>A table is requested that has not yet been requested.
 * Once requested, the entry is cached, even if the table does
 * not exist in the base catalog.</li>
 * <li>The contents of a schema are requested, and have not yet
 * been fetched.</li>
 * </ul>
 *
 * Both tables and schemas are cached. In particular, if a table or
 * schema is requested, and does not exist in the base catalog, then
 * that schema is marked as not existing and won't be fetched again.
 *
 * The cache is updated via an update facility which either flushes
 * the cache (crude) or listens to the base catalog for updates and
 * populates the cache with updates. For a local cache, the DB layer
 * provides the updates. For a remote cache, the DB host pushes updates.
 */
public class CachedMetadataCatalog implements MetadataCatalog, CatalogListener
{
  public static final int NOT_FETCHED = -1;
  public static final int UNDEFINED = 0;

  private static class TableEntry
  {
    private final TableMetadata table;

    protected TableEntry(SchemaSpec schema, TableMetadata table)
    {
      this.table = table;
    }

    protected long version()
    {
      return table == null ? UNDEFINED : table.updateTime();
    }
  }

  private class SchemaEntry
  {
    private final SchemaSpec schema;
    private long version = NOT_FETCHED;
    private final ConcurrentHashMap<String, TableEntry> cache = new ConcurrentHashMap<>();

    protected SchemaEntry(SchemaSpec schema)
    {
      this.schema = schema;
    }

    protected TableMetadata resolveTable(TableId tableId)
    {
      TableEntry entry = cache.computeIfAbsent(
          tableId.name(),
          key -> new TableEntry(schema, base.table(tableId))
          );
      return entry.table;
    }

    public synchronized List<TableMetadata> tables()
    {
      if (version == UNDEFINED) {
        return Collections.emptyList();
      }
      if (version == NOT_FETCHED) {
        List<TableMetadata> catalogTables = base.tablesForSchema(schema.name());
        for (TableMetadata table : catalogTables) {
          update(table);
        }
      }
      List<TableMetadata> orderedTables = new ArrayList<>();

      // Get the list of actual tables; excluding any cached "misses".
      cache.forEach((k, v) -> {
        if (v.table != null) {
          orderedTables.add(v.table);
        }
      });
      orderedTables.sort((e1, e2) -> e1.id().name().compareTo(e2.id().name()));
      return orderedTables;
    }

    public synchronized void update(TableMetadata table)
    {
      cache.compute(
          table.name(),
          (k, v) -> v == null || v.version() < table.updateTime()
                ? new TableEntry(schema, table)
                : v
      );
      version = Math.max(version, table.updateTime());
    }

    public void remove(String name)
    {
      cache.remove(name);
    }

    public Set<String> tableNames()
    {
      Set<String> tables = new HashSet<>();
      cache.forEach((k, v) -> {
        if (v.table != null) {
          tables.add(k);
        }
      });
      return tables;
    }
  }

  private final ConcurrentHashMap<String, SchemaEntry> schemaCache = new ConcurrentHashMap<>();
  private final CatalogSource base;
  private final SchemaRegistry schemaRegistry;

  @Inject
  public CachedMetadataCatalog(
      CatalogSource catalog,
      SchemaRegistry schemaRegistry
  )
  {
    this.base = catalog;
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public TableMetadata resolveTable(TableId tableId)
  {
    SchemaEntry schemaEntry = entryFor(tableId.schema());
    return schemaEntry == null ? null : schemaEntry.resolveTable(tableId);
  }

  @Override
  public List<TableMetadata> tables(String schemaName)
  {
    SchemaEntry schemaEntry = entryFor(schemaName);
    return schemaEntry == null ? null : schemaEntry.tables();
  }

  @Override
  public void updated(TableMetadata table)
  {
    SchemaEntry schemaEntry = entryFor(table.dbSchema());
    if (schemaEntry != null) {
      schemaEntry.update(table);
    }
  }

  @Override
  public void deleted(TableId tableId)
  {
    SchemaEntry schemaEntry = entryFor(tableId.schema());
    if (schemaEntry != null) {
      schemaEntry.remove(tableId.name());
    }
  }

  @Override
  public Set<String> tableNames(String schemaName)
  {
    SchemaEntry schemaEntry = entryFor(schemaName);
    return schemaEntry == null ? Collections.emptySet() : schemaEntry.tableNames();
  }

  public void flush()
  {
    schemaCache.clear();
  }

  private SchemaEntry entryFor(String schemaName)
  {
    return schemaCache.computeIfAbsent(
        schemaName,
        k -> {
          SchemaSpec schema = schemaRegistry.schema(k);
          return schema == null ? null : new SchemaEntry(schema);
        });
  }
}
