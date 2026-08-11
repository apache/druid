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
import com.google.common.base.Throwables;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.http.TableEditRequest;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.DatasourceBaseTableMetadata;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.sql.calcite.planner.CatalogTableWriter;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * Applies catalog DDL by calling the Coordinator, which owns catalog metadata.
 * <p>
 * The Coordinator is authoritative for validation, so the errors it reports are unwrapped and presented as the
 * statement's own error rather than as a failed HTTP call.
 */
public class CatalogSqlTableWriter implements CatalogTableWriter
{
  private static final Logger LOG = new Logger(CatalogSqlTableWriter.class);

  private final CatalogClient client;
  private final CachedMetadataCatalog cache;
  private final ObjectMapper jsonMapper;

  @Inject
  public CatalogSqlTableWriter(
      final CatalogClient client,
      final CachedMetadataCatalog cache,
      final ObjectMapper jsonMapper
  )
  {
    this.client = client;
    this.cache = cache;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void createTable(TableId tableId, TableSpec spec, boolean ifNotExists, boolean replace)
  {
    execute(tableId, () -> client.createTable(tableId, spec, ifNotExists, replace));
  }

  @Override
  public void addColumns(TableId tableId, List<ColumnSpec> columns)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.AddColumns(columns)));
  }

  @Override
  public void alterColumns(TableId tableId, List<ColumnSpec> columns)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.AlterColumns(columns)));
  }

  @Override
  public void dropColumns(TableId tableId, List<String> columns)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.DropColumns(columns)));
  }

  @Override
  public void updateProperties(TableId tableId, Map<String, Object> properties)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.UpdateProperties(properties)));
  }

  @Override
  public void addProjection(TableId tableId, DatasourceProjectionMetadata projection, boolean ifNotExists)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.AddProjection(projection, ifNotExists)));
  }

  @Override
  public void dropProjection(TableId tableId, String projectionName, boolean ifExists)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.DropProjection(projectionName, ifExists)));
  }

  @Override
  public void setBaseTable(TableId tableId, DatasourceBaseTableMetadata baseTable, boolean ifNotExists)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.SetBaseTable(baseTable, ifNotExists)));
  }

  @Override
  public void dropBaseTable(TableId tableId, boolean ifExists)
  {
    execute(tableId, () -> client.editTable(tableId, new TableEditRequest.DropBaseTable(ifExists)));
  }

  @Nullable
  @Override
  public TableMetadata readTable(TableId tableId)
  {
    return client.table(tableId);
  }

  /**
   * Run a catalog write, translate any failure into a statement error, then refresh this Broker's cache.
   * <p>
   * The refresh matters because the Coordinator's update notification is asynchronous: without it, a CREATE TABLE
   * followed immediately by an INSERT on the same connection could plan against the pre-DDL schema. Other Brokers
   * still converge through the normal notification and polling path.
   */
  private void execute(TableId tableId, Runnable operation)
  {
    try {
      operation.run();
    }
    catch (Exception e) {
      throw translateError(tableId, e);
    }
    refreshCache(tableId);
  }

  private void refreshCache(TableId tableId)
  {
    try {
      final TableMetadata table = client.table(tableId);
      cache.updated(
          new UpdateEvent(
              table == null ? UpdateEvent.EventType.DELETE : UpdateEvent.EventType.UPDATE,
              table == null ? TableMetadata.empty(tableId) : table
          )
      );
    }
    catch (Exception e) {
      // The write succeeded, so failing the statement here would be misleading. The cache converges on the next
      // notification or poll; the only cost is that this Broker may briefly plan against the older spec.
      LOG.warn(e, "Could not refresh the catalog cache for table[%s] after a DDL statement", tableId);
    }
  }

  /**
   * Surface the Coordinator's own error message. Catalog validation lives there, so its wording is what explains
   * why the statement was rejected; wrapping it in "server error" would bury it.
   */
  private DruidException translateError(TableId tableId, Exception e)
  {
    final HttpResponseException httpError = findHttpResponseException(e);
    if (httpError != null) {
      final String message = errorMessage(httpError);
      if (message != null) {
        return DruidException.forPersona(DruidException.Persona.USER)
                             .ofCategory(DruidException.Category.INVALID_INPUT)
                             .build("%s", message);
      }
    }
    if (e instanceof DruidException) {
      return (DruidException) e;
    }
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                         .build(e, "Could not update the catalog entry for table[%s]", tableId.name());
  }

  @Nullable
  private String errorMessage(HttpResponseException e)
  {
    final String content = e.getResponse().getContent();
    if (content == null || content.isEmpty()) {
      return null;
    }
    try {
      final Map<String, Object> payload = jsonMapper.readValue(content, Map.class);
      final Object message = payload.get(CatalogException.ERR_MSG_KEY);
      return message == null ? null : message.toString();
    }
    catch (Exception ignored) {
      // Not a catalog error payload; fall back to the generic message.
      return null;
    }
  }

  @Nullable
  private static HttpResponseException findHttpResponseException(Throwable t)
  {
    for (Throwable cause : Throwables.getCausalChain(t)) {
      if (cause instanceof HttpResponseException) {
        return (HttpResponseException) cause;
      }
    }
    return null;
  }
}
