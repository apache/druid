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

package org.apache.druid.catalog.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.CatalogException.NotFoundException;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.SchemaRegistry;
import org.apache.druid.catalog.model.SchemaRegistry.SchemaSpec;
import org.apache.druid.catalog.model.SchemaRegistryImpl;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.storage.sql.CatalogManager;
import org.apache.druid.catalog.sync.CatalogUpdateListener;
import org.apache.druid.catalog.sync.MetadataCatalog.CatalogSource;
import org.apache.druid.catalog.sync.MetadataCatalog.CatalogUpdateProvider;
import org.apache.druid.guice.annotations.Json;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;

/**
 * Facade over the three internal components used to manage the metadata
 * catalog from the REST API:
 * <ul>
 * <li>Schema registry: the hard-coded set of schemas which Druid supports.
 * (User does not support user-defined schemas.)</li>
 * <li>Table definition registry: the set of table types which Druid
 * supports: datasources, external tables, etc.</li>
 * <li>Catalog manager: database storage for the user's table specifications
 * within the catalog.</li>
 * </ul>
 */
public class CatalogStorage implements CatalogUpdateProvider, CatalogSource
{
  protected final SchemaRegistry schemaRegistry;
  protected final TableDefnRegistry tableRegistry;
  protected final CatalogManager catalogMgr;

  @Inject
  public CatalogStorage(
      final CatalogManager catalogMgr,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.schemaRegistry = new SchemaRegistryImpl();
    this.tableRegistry = new TableDefnRegistry(jsonMapper);
    this.catalogMgr = catalogMgr;
  }

  public CatalogManager tables()
  {
    return catalogMgr;
  }

  public SchemaRegistry schemaRegistry()
  {
    return schemaRegistry;
  }

  public ObjectMapper jsonMapper()
  {
    return tableRegistry.jsonMapper();
  }

  public SchemaSpec resolveSchema(String dbSchema)
  {
    return schemaRegistry.schema(dbSchema);
  }

  @Override
  public void register(CatalogUpdateListener listener)
  {
    tables().register(listener);
  }

  @Override
  public List<TableMetadata> tablesForSchema(String dbSchema)
  {
    return tables().tablesInSchema(dbSchema);
  }

  @Override
  public @Nullable TableMetadata table(TableId id)
  {
    try {
      return tables().read(id);
    }
    catch (NotFoundException e) {
      return null;
    }
  }

  public void validate(TableMetadata table)
  {
    table.validate();
    tableRegistry.resolve(table.spec()).validate();
  }

  public TableDefnRegistry tableRegistry()
  {
    return tableRegistry;
  }

  @Override
  public @Nullable ResolvedTable resolveTable(TableId id)
  {
    TableMetadata table = table(id);
    return table == null ? null : tableRegistry.resolve(table.spec());
  }
}
