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
import org.apache.druid.catalog.http.TableEditRequest;
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

  public long createTable(TableMetadata table, boolean overwrite)
  {
    // Use action=
    String url = StringUtils.format(
        "%s%s/schemas/%s/tables/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        table.id().schema(),
        table.id().name()
    );
    if (overwrite) {
      url += "?overwrite=true";
    }
    VersionResponse response = clusterClient.post(url, table.spec(), VersionResponse.class);
    return response.version;
  }

  public long updateTable(TableId tableId, TableSpec tableSpec, long version)
  {
    String url = StringUtils.format(
        "%s%s/schemas/%s/tables/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    if (version > 0) {
      url += "?version=" + version;
    }
    VersionResponse response = clusterClient.post(url, tableSpec, VersionResponse.class);
    return response.version;
  }

  public TableMetadata readTable(TableId tableId)
  {
    String url = StringUtils.format(
        "%s%s/schemas/%s/tables/%s",
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
        "%s%s/schemas/%s/tables/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    clusterClient.send(HttpMethod.DELETE, url);
  }

  public long editTable(TableId tableId, TableEditRequest cmd)
  {
    String url = StringUtils.format(
        "%s%s/schemas/%s/tables/%s/edit",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    VersionResponse response = clusterClient.post(url, cmd, VersionResponse.class);
    return response.version;
  }

  public List<String> listSchemas()
  {
    String url = StringUtils.format(
        "%s%s/schemas?format=name",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH
    );
    return clusterClient.getAs(url, new TypeReference<List<String>>() { });
  }

  public List<TableId> listTables()
  {
    String url = StringUtils.format(
        "%s%s/schemas?format=path",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH
    );
    return clusterClient.getAs(url, new TypeReference<List<TableId>>() { });
  }

  public List<String> listTableNamesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s%s/schemas/%s/tables?format=name",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return clusterClient.getAs(url, new TypeReference<List<String>>() { });
  }

  public List<TableMetadata> listTablesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s%s/schemas/%s/tables?format=metadata",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return clusterClient.getAs(url, new TypeReference<List<TableMetadata>>() { });
  }
}
