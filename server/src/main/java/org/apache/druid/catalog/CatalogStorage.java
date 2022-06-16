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
import org.apache.druid.catalog.MetadataCatalog.CatalogSource;
import org.apache.druid.catalog.MetadataCatalog.CatalogUpdateProvider;
import org.apache.druid.catalog.SchemaRegistry.SchemaDefn;
import org.apache.druid.metadata.catalog.CatalogManager;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.inject.Inject;

import java.util.List;

/**
 * Facade over the three internal components used to manage the metadata
 * catalog from the REST API.
 */
public class CatalogStorage implements CatalogUpdateProvider, CatalogSource
{
  public static class ListenerAdapter implements CatalogManager.Listener
  {
    private final CatalogListener dest;

    public ListenerAdapter(CatalogListener dest)
    {
      this.dest = dest;
    }

    @Override
    public void added(TableMetadata table)
    {
      dest.updated(table);
    }

    @Override
    public void updated(TableMetadata table)
    {
      dest.updated(table);
    }

    @Override
    public void deleted(TableId id)
    {
      dest.deleted(id);
    }
  }

  protected final SchemaRegistry schemaRegistry;
  protected final CatalogManager catalogMgr;
  protected final CatalogAuthorizer authorizer;

  @Inject
  public CatalogStorage(
      CatalogManager catalogMgr,
      AuthorizerMapper authorizerMapper
  )
  {
    this.schemaRegistry = new SchemaRegistryImpl();
    this.catalogMgr = catalogMgr;
    this.authorizer = new CatalogAuthorizer(authorizerMapper);
  }

  public CatalogAuthorizer authorizer()
  {
    return authorizer;
  }

  public CatalogManager tables()
  {
    return catalogMgr;
  }

  public SchemaRegistry schemaRegistry()
  {
    return schemaRegistry;
  }

  public SchemaDefn resolveSchema(String dbSchema)
  {
    return schemaRegistry.schema(dbSchema);
  }

  @Override
  public void register(CatalogListener listener)
  {
    tables().register(new ListenerAdapter(listener));
  }

  @Override
  public List<TableMetadata> tablesForSchema(String dbSchema)
  {
    return tables().listDetails(dbSchema);
  }

  @Override
  public TableMetadata table(TableId id)
  {
    return tables().read(id);
  }
}
