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

package org.apache.druid.testing.embedded.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.catalog.http.CatalogResource;
import org.apache.druid.catalog.http.TableEditRequest;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.List;

public class TestCatalogClient
{
  public static class VersionResponse
  {
    private final long version;

    @JsonCreator
    public VersionResponse(
        @JsonProperty("version") long version
    )
    {
      this.version = version;
    }
  }

  private final EmbeddedDruidCluster cluster;

  public TestCatalogClient(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }

  public long createTable(TableMetadata table, boolean overwrite)
  {
    final String url = StringUtils.format(
        "%s/schemas/%s/tables/%s%s",
        CatalogResource.ROOT_PATH,
        table.id().schema(),
        table.id().name(),
        overwrite ? "?overwrite=true" : ""
    );

    return postTableSpec(url, table.spec());
  }

  public long updateTable(TableId tableId, TableSpec tableSpec, long version)
  {
    String url = StringUtils.format(
        "%s/schemas/%s/tables/%s%s",
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name(),
        version > 0 ? "?version=" + version : ""
    );
    return postTableSpec(url, tableSpec);
  }

  public TableMetadata readTable(TableId tableId)
  {
    String url = StringUtils.format(
        "%s/schemas/%s/tables/%s",
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return getFromCoordinator(url, new TypeReference<>() {});
  }

  public void dropTable(TableId tableId)
  {
    String url = StringUtils.format(
        "%s/schemas/%s/tables/%s",
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    serviceClient().onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.DELETE, url),
        null
    );
  }

  public long editTable(TableId tableId, TableEditRequest cmd)
  {
    String url = StringUtils.format(
        "%s/schemas/%s/tables/%s/edit",
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return postTableSpec(url, cmd);
  }

  public List<String> listSchemas()
  {
    String url = StringUtils.format(
        "%s/schemas?format=name",
        CatalogResource.ROOT_PATH
    );
    return getFromCoordinator(url, new TypeReference<>() {});
  }

  public List<TableId> listTables()
  {
    String url = StringUtils.format(
        "%s/schemas?format=path",
        CatalogResource.ROOT_PATH
    );
    return getFromCoordinator(url, new TypeReference<>() {});
  }

  public List<String> listTableNamesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s/schemas/%s/tables?format=name",
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return getFromCoordinator(url, new TypeReference<>() {});
  }

  public List<TableMetadata> listTablesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s/schemas/%s/tables?format=metadata",
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return getFromCoordinator(url, new TypeReference<>() {});
  }
  
  private EmbeddedServiceClient serviceClient()
  {
    return cluster.callApi().serviceClient();
  }

  private <T> T getFromCoordinator(String url, TypeReference<T> typeReference)
  {
    return serviceClient().onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.GET, url),
        typeReference
    );
  }

  private long postTableSpec(String url, Object payload)
  {
    VersionResponse response = serviceClient().onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.POST, url).jsonContent(mapper, payload),
        new TypeReference<>() {}
    );
    return response == null ? 0 : response.version;
  }
}
