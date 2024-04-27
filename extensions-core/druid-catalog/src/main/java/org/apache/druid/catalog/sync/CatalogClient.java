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
import org.apache.druid.catalog.http.CatalogResource;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.sync.MetadataCatalog.CatalogSource;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Guice-injected client for the catalog update sync process. Requests
 * tables and schemas from the catalog component on the Coordinator.
 *
 * This class handles any recoverable error case. If this class throws
 * an exception, then something went very wrong and there is little the
 * caller can do to make things better. All the caller can do is try
 * again later and hope things improve.
 */
public class CatalogClient implements CatalogSource
{
  public static final String SCHEMA_SYNC_PATH = CatalogResource.ROOT_PATH + CatalogResource.SCHEMA_SYNC;
  public static final String TABLE_SYNC_PATH = CatalogResource.ROOT_PATH + CatalogResource.TABLE_SYNC;
  private static final TypeReference<List<TableMetadata>> LIST_OF_TABLE_METADATA_TYPE = new TypeReference<List<TableMetadata>>()
  {
  };
  // Not strictly needed as a TypeReference, but doing so makes the code simpler.
  private static final TypeReference<TableMetadata> TABLE_METADATA_TYPE = new TypeReference<TableMetadata>()
  {
  };

  private final DruidLeaderClient coordClient;
  private final ObjectMapper smileMapper;
  private final TableDefnRegistry tableRegistry;

  @Inject
  public CatalogClient(
      @Coordinator DruidLeaderClient coordClient,
      @Smile ObjectMapper smileMapper,
      @Json ObjectMapper jsonMapper
  )
  {
    this.coordClient = coordClient;
    this.smileMapper = smileMapper;
    this.tableRegistry = new TableDefnRegistry(jsonMapper);
  }

  @Override
  public List<TableMetadata> tablesForSchema(String dbSchema)
  {
    String url = StringUtils.replace(SCHEMA_SYNC_PATH, "{schema}", dbSchema);
    List<TableMetadata> results = send(url, LIST_OF_TABLE_METADATA_TYPE);

    // Not found for a list is an empty list.
    return results == null ? Collections.emptyList() : results;
  }

  @Override
  public TableMetadata table(TableId id)
  {
    String url = StringUtils.replace(TABLE_SYNC_PATH, "{schema}", id.schema());
    url = StringUtils.replace(url, "{name}", id.name());
    return send(url, TABLE_METADATA_TYPE);
  }

  @Override
  public ResolvedTable resolveTable(TableId id)
  {
    TableMetadata table = table(id);
    return table == null ? null : tableRegistry.resolve(table.spec());
  }

  /**
   * Send the update. Exceptions are "unexpected": they should never occur in a
   * working system. If they occur, something is broken.
   *
   * @return the requested update, or null if the item was not found in the
   * catalog.
   */
  private <T> T send(String url, TypeReference<T> typeRef)
  {
    final Request request;
    try {
      request = coordClient.makeRequest(HttpMethod.GET, url)
          .addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON);
    }
    catch (IOException e) {
      throw new ISE("Cannot create catalog sync request");
    }
    final StringFullResponseHolder responseHolder;
    try {
      responseHolder = coordClient.go(request);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to send catalog sync");
    }
    catch (InterruptedException e1) {
      // Treat as a not-found: the only way this exception should occur
      // is during shutdown.
      return null;
    }
    if (responseHolder.getStatus().getCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      // Not found means the item disappeared. Returning null means "not found".
      return null;
    }
    if (responseHolder.getStatus().getCode() != HttpResponseStatus.OK.getCode()) {
      throw new ISE("Unexpected status from catalog sync: " + responseHolder.getStatus());
    }
    try {
      return smileMapper.readValue(responseHolder.getContent(), typeRef);
    }
    catch (IOException e) {
      throw new ISE(e, "Could not decode the JSON response from catalog sync.");
    }
  }
}
