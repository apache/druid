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
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.SchemaRegistry;
import org.apache.druid.catalog.model.SchemaRegistry.SchemaSpec;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;

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
 * <p>
 * The cache is updated via an update facility which either flushes
 * the cache (crude) or listens to the base catalog for updates and
 * populates the cache with updates. For a local cache, the DB layer
 * provides the updates. For a remote cache, the DB host pushes updates.
 */
public class CachedMetadataCatalog implements MetadataCatalog, CatalogUpdateListener
{
  private static final Logger LOG = new Logger(CachedMetadataCatalog.class);

  /**
   * Indicates that the schema entry has not yet been fetched from the catalog.
   */
  public static final int NOT_FETCHED = -1;

  /**
   * Indicates that the schema was requested from the catalog, and the catalog
   * reported that no such schema exists. Saves pinging the catalog over and
   * over for an undefined schema. The catalog will let us know if the schema
   * becomes defined (which it won't because, at present, schemas are hard-coded
   * to a fixed set.)
   */
  public static final int UNDEFINED = 0;

  /**
   * Cache entry. Normally wraps a catalog table entry. Can also wrap a null
   * entry which says that we tried to resolve the table, but there is no such
   * entry, and there is no need to check again.
   */
  private static class TableEntry
  {
    private final TableMetadata table;

    protected TableEntry(TableMetadata table)
    {
      this.table = table;
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

    /**
     * High-frequency by-name table lookup called for every table in every SQL query.
     */
    protected TableMetadata resolveTable(TableId tableId)
    {
      TableEntry entry = cache.computeIfAbsent(
          tableId.name(),
          key -> new TableEntry(base.table(tableId))
          );
      return entry.table;
    }

    /**
     * Low-frequency list of tables sorted by name.
     */
    public List<TableMetadata> tables()
    {
      if (version == UNDEFINED) {
        return Collections.emptyList();
      }
      if (version == NOT_FETCHED) {
        synchronized (this) {
          List<TableMetadata> catalogTables = base.tablesForSchema(schema.name());
          for (TableMetadata table : catalogTables) {
            cache.put(table.id().name(), new TableEntry(table));
          }
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

    public synchronized void update(UpdateEvent event)
    {
      TableMetadata table = event.table;
      final String name = table.id().name();
      switch (event.type) {
        case CREATE:
          cache.compute(
              name,
              (k, v) -> computeCreate(v, table)
          );
          break;
        case UPDATE:
          cache.compute(
              name,
              (k, v) -> computeUpdate(v, table)
          );
          break;
        case DELETE:
          cache.remove(name);
          break;
        case COLUMNS_UPDATE:
          cache.compute(
              name,
              (k, v) -> computeColumnsUpdate(v, table)
          );
          break;
        case PROPERTY_UPDATE:
          cache.compute(
              name,
              (k, v) -> computePropertiesUpdate(v, table)
          );
          break;
        default:
          // Don't know what to do
          return;
      }
      version = Math.max(version, table.updateTime());
    }

    private TableEntry computeCreate(TableEntry entry, TableMetadata update)
    {
      if (entry != null && entry.table != null) {
        LOG.warn("Received creation event for existing entry: %s", update.id().sqlName());
        return computeUpdate(entry, update);
      }
      return new TableEntry(update);
    }

    private TableEntry computeUpdate(TableEntry entry, TableMetadata update)
    {
      if (!checkExists(entry, update)) {
        return new TableEntry(update);
      }
      if (!checkVersion(entry, update)) {
        return entry;
      }
      return new TableEntry(update);
    }

    private boolean checkExists(TableEntry entry, TableMetadata update)
    {
      if (entry == null || entry.table == null) {
        LOG.error("Reveived update for missing cache entry: %s", update.id().sqlName());
        // TODO: force resync
        return false;
      }
      return true;
    }

    private TableEntry computeColumnsUpdate(TableEntry entry, TableMetadata update)
    {
      if (!checkExists(entry, update)) {
        return new TableEntry(null);
      }
      if (!checkResolved(entry, update, "columns")) {
        return entry;
      }
      if (!checkVersion(entry, update)) {
        return entry;
      }
      return new TableEntry(entry.table.withColumns(update));
    }

    private TableEntry computePropertiesUpdate(TableEntry entry, TableMetadata update)
    {
      if (!checkExists(entry, update)) {
        return new TableEntry(null);
      }
      if (!checkResolved(entry, update, "properties")) {
        return entry;
      }
      if (!checkVersion(entry, update)) {
        return entry;
      }
      return new TableEntry(entry.table.withProperties(update));
    }

    private boolean checkResolved(TableEntry entry, TableMetadata update, String action)
    {
      if (entry.table == null) {
        LOG.error("Received %s update for unresolved table: %s",
            action,
            update.id().sqlName()
        );
        // TODO: force resync
        return false;
      }
      return true;
    }

    private boolean checkVersion(TableEntry entry, TableMetadata update)
    {
      if (entry.table.updateTime() > update.updateTime()) {
        LOG.warn(
            "Received out-of-order update for table: %s. Cache: %d, update:%d",
            update.id().sqlName(),
            entry.table.updateTime(),
            update.updateTime()
        );
        // TODO: force resync
        return false;
      }
      return true;
    }

    public synchronized Set<String> tableNames()
    {
      Set<String> tables = new HashSet<>();
      cache.forEach((k, v) -> {
        if (v.table != null) {
          tables.add(k);
        }
      });
      return tables;
    }

    /**
     * Populate the cache by asking the catalog source for all tables for
     * this schema.
     */
    public synchronized void resync(CatalogSource source)
    {
      List<TableMetadata> tables = source.tablesForSchema(schema.name());
      cache.clear();
      for (TableMetadata table : tables) {
        cache.compute(
            table.id().name(),
            (k, v) -> computeCreate(v, table)
        );
      }
    }
  }

  private final ConcurrentHashMap<String, SchemaEntry> schemaCache = new ConcurrentHashMap<>();
  private final CatalogSource base;
  private final SchemaRegistry schemaRegistry;
  private final TableDefnRegistry tableRegistry;

  @Inject
  public CachedMetadataCatalog(
      CatalogSource catalog,
      SchemaRegistry schemaRegistry,
      @Json ObjectMapper jsonMapper
  )
  {
    this.base = catalog;
    this.schemaRegistry = schemaRegistry;
    this.tableRegistry = new TableDefnRegistry(jsonMapper);
  }

  @Override
  public TableMetadata getTable(TableId tableId)
  {
    SchemaEntry schemaEntry = entryFor(tableId.schema());
    return schemaEntry == null ? null : schemaEntry.resolveTable(tableId);
  }

  @Override
  public ResolvedTable resolveTable(TableId tableId)
  {
    TableMetadata table = getTable(tableId);
    return table == null ? null : tableRegistry.resolve(table.spec());
  }

  @Override
  public List<TableMetadata> tables(String schemaName)
  {
    SchemaEntry schemaEntry = entryFor(schemaName);
    return schemaEntry == null ? null : schemaEntry.tables();
  }

  @Override
  public void updated(UpdateEvent event)
  {
    SchemaEntry schemaEntry = entryFor(event.table.id().schema());
    if (schemaEntry != null) {
      schemaEntry.update(event);
    }
  }

  /**
   * Get the list of table names <i>in the cache</i>. Does not attempt to
   * lazy load the list since doing so is costly: we don't know when it
   * might be out of date. Rely on priming the cache, and update notifications
   * to keep the list accurate.
   */
  @Override
  public Set<String> tableNames(String schemaName)
  {
    SchemaEntry schemaEntry = entryFor(schemaName);
    return schemaEntry == null ? Collections.emptySet() : schemaEntry.tableNames();
  }

  /**
   * Clear the cache. Primarily for testing.
   */
  @Override
  public void flush()
  {
    LOG.info("Flush requested");
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

  /**
   * Discard any existing cached tables and reload directly from the
   * catalog source. Manages the two schemas which the catalog manages.
   * If the catalog were to manage others, add those here as well.
   * Done both at Broker startup, and on demand for testing.
   */
  @Override
  public void resync()
  {
    LOG.info("Resync requested");
    entryFor(TableId.DRUID_SCHEMA).resync(base);
    entryFor(TableId.EXTERNAL_SCHEMA).resync(base);
  }
}
