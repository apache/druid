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

package org.apache.druid.testsEx.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.catalog.http.CatalogResource;
import org.apache.druid.catalog.http.HideColumns;
import org.apache.druid.catalog.http.MoveColumn;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.List;

public class CatalogClient
{
  public static class VersionResponse
  {
    public final long version;

    @JsonCreator
    public VersionResponse(
        @JsonProperty("version") long version
    )
    {
      this.version = version;
    }
  }

  private final DruidClusterClient clusterClient;

  public CatalogClient(final DruidClusterClient clusterClient)
  {
    this.clusterClient = clusterClient;
  }

  public long createTable(TableMetadata table)
  {
    return createTable(table, null);
  }

  public long createTable(TableMetadata table, String action)
  {
    // Use action=
    String url = StringUtils.format(
        "%s%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        table.id().schema(),
        table.id().name()
    );
    if (action != null) {
      url += "?action=" + action;
    }
    VersionResponse response = clusterClient.post(url, table.spec(), VersionResponse.class);
    return response.version;
  }

  public long updateTable(TableId tableId, TableSpec tableSpec, long version)
  {
    String url = StringUtils.format(
        "%s%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    if (version > 0) {
      url += "?version=" + version;
    }
    VersionResponse response = clusterClient.put(url, tableSpec, VersionResponse.class);
    return response.version;
  }

  public TableMetadata readTable(TableId tableId)
  {
    String url = StringUtils.format(
        "%s%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return clusterClient.getAs(url, TableMetadata.class);
  }

  public void dropTable(TableId tableId)
  {
    String url = StringUtils.format(
        "%s%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    clusterClient.send(HttpMethod.DELETE, url);
  }

  public long moveColumn(TableId tableId, MoveColumn cmd)
  {
    String url = StringUtils.format(
        "%s%s/tables/%s/%s/moveColumn",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    VersionResponse response = clusterClient.post(url, cmd, VersionResponse.class);
    return response.version;
  }

  public long hideColumns(TableId tableId, HideColumns cmd)
  {
    String url = StringUtils.format(
        "%s%s/tables/%s/%s/hideColumns",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    VersionResponse response = clusterClient.post(url, cmd, VersionResponse.class);
    return response.version;
  }

  public long dropColumns(TableId tableId, List<String> columns)
  {
    String url = StringUtils.format(
        "%s%s/tables/%s/%s/dropColumns",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    VersionResponse response = clusterClient.post(url, columns, VersionResponse.class);
    return response.version;
  }

  public List<String> listSchemas()
  {
    String url = StringUtils.format(
        "%s%s/list/schemas/names",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH
    );
    return clusterClient.getAs(url, new TypeReference<List<String>>() { });
  }

  public List<TableId> listTables()
  {
    String url = StringUtils.format(
        "%s%s/list/tables/names",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH
    );
    return clusterClient.getAs(url, new TypeReference<List<TableId>>() { });
  }

  public List<String> listTableNamesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s%s/schemas/%s/names",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return clusterClient.getAs(url, new TypeReference<List<String>>() { });
  }

  public List<TableMetadata> listTablesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s%s/schemas/%s/tables",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return clusterClient.getAs(url, new TypeReference<List<TableMetadata>>() { });
  }
}
