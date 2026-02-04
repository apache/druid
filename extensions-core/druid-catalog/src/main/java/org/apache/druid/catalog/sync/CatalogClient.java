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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.catalog.http.CatalogResource;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.http.ServletResourceUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;

/**
 * Used by Brokers and Overlord to sync the catalog by querying the Coordinator.
 * <p>
 * Modeled after other {@link ServiceClient}-based classes such as {@code CoordinatorClient}.
 */
public class CatalogClient implements CatalogSource
{
  public static final String SCHEMA_SYNC_PATH = CatalogResource.ROOT_PATH + CatalogResource.SCHEMA_SYNC;
  public static final String TABLE_SYNC_PATH = CatalogResource.ROOT_PATH + CatalogResource.TABLE_SYNC;
  private static final String TABLE_CREATE_PATH = CatalogResource.ROOT_PATH + "/schemas/{schema}/tables/{name}";

  private final ServiceClient serviceClient;
  private final ObjectMapper jsonMapper;
  private final TableDefnRegistry tableRegistry;

  @Inject
  public CatalogClient(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator,
      ObjectMapper jsonMapper
  )
  {
    this.jsonMapper = jsonMapper;
    this.tableRegistry = new TableDefnRegistry(jsonMapper);
    this.serviceClient = clientFactory.makeClient(
        NodeRole.COORDINATOR.getJsonName(),
        serviceLocator,
        StandardRetryPolicy.builder().maxAttempts(6).build()
    );
  }

  @Override
  public List<TableMetadata> tablesForSchema(String dbSchema)
  {
    List<TableMetadata> results = getResult(fetchTablesForSchema(dbSchema));

    // Not found for a list is an empty list.
    return results == null ? Collections.emptyList() : results;
  }

  @Override
  public TableMetadata table(TableId id)
  {
    return getResult(fetchTableForId(id));
  }

  @Override
  public ResolvedTable resolveTable(TableId id)
  {
    TableMetadata table = table(id);
    return table == null ? null : tableRegistry.resolve(table.spec());
  }

  /**
   * Creates a table for the given {@link TableId} and {@link TableSpec}.
   * If a table already exists for this id, it is overwritten.
   * <p>
   * This method is currently used only in tests.
   */
  public void createTable(TableId tableId, TableSpec tableSpec)
  {
    getResult(postCreateTable(tableId, tableSpec));
  }

  /**
   * Posts the given table spec to the Coordinator.
   * <p>
   * API: {@code POST /druid/coordinator/v1/catalog/schemas/{schema}/tables/{name}}
   */
  private ListenableFuture<Void> postCreateTable(TableId tableId, TableSpec tableSpec)
  {
    String path = StringUtils.replace(TABLE_CREATE_PATH, "{schema}", StringUtils.urlEncode(tableId.schema()));
    path = StringUtils.replace(path, "{name}", StringUtils.urlEncode(tableId.name()));

    return serviceClient.asyncRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, tableSpec),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  /**
   * Fetches the metadata of a table from the Coordinator.
   * <p>
   * API: {@code GET /druid/coordinator/v1/catalog/sync/schemas/{schema}/{name}}
   */
  private ListenableFuture<TableMetadata> fetchTableForId(TableId id)
  {
    String path = StringUtils.replace(TABLE_SYNC_PATH, "{schema}", StringUtils.urlEncode(id.schema()));
    path = StringUtils.replace(path, "{name}", StringUtils.urlEncode(id.name()));

    return FutureUtils.transform(
        serviceClient.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), TableMetadata.class)
    );
  }

  /**
   * Fetches the metadata of all tables in a schema from the Coordinator.
   * <p>
   * API: {@code GET /druid/coordinator/v1/catalog/sync/schemas/{schema}}
   */
  private ListenableFuture<List<TableMetadata>> fetchTablesForSchema(String schemaName)
  {
    final String path = StringUtils.replace(
        SCHEMA_SYNC_PATH,
        "{schema}",
        StringUtils.urlEncode(schemaName)
    );

    return FutureUtils.transform(
        serviceClient.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  /**
   * Gets the result from the given future.
   *
   * @return null if the API returns a 404 not found status.
   */
  @Nullable
  private <T> T getResult(ListenableFuture<T> future)
  {
    try {
      return FutureUtils.getUnchecked(future, true);
    }
    catch (Exception e) {
      return ServletResourceUtils.getDefaultValueIfCauseIs404ElseThrow(e, () -> null);
    }
  }
}
