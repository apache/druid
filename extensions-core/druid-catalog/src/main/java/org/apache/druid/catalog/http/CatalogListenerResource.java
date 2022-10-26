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

package org.apache.druid.catalog.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.sync.CatalogUpdateNotifier;
import org.apache.druid.catalog.sync.MetadataCatalog.CatalogListener;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;

/**
 * Resource on the Broker to listen to catalog update events from the
 * Coordinator. Since this is an internal API, it supports the efficient
 * Smile encoding as well as the JSON encoding. (JSON is more convenient
 * for debugging as it can be easily created or interpreted.)
 */
@Path(CatalogListenerResource.BASE_URL)
public class CatalogListenerResource
{
  public static final String BASE_URL = "/druid/broker/v1/catalog";
  public static final String SYNC_URL = "/sync";
  private static final Logger log = new Logger(CatalogListenerResource.class);


  private final CatalogListener listener;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper smileMapper;
  private final ObjectMapper jsonMapper;

  @Inject
  public CatalogListenerResource(
      final CatalogListener listener,
      @Smile final ObjectMapper smileMapper,
      @Json final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.listener = listener;
    this.authorizerMapper = authorizerMapper;
    this.smileMapper = smileMapper;
    this.jsonMapper = jsonMapper;
  }

  /**
   * Event sent from the Coordinator to indicate that a table has changed
   * (or been deleted.)
   */
  @POST
  @Path(SYNC_URL)
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @ResourceFilters(ConfigResourceFilter.class)
  public Response syncTable(
      final InputStream inputStream,
      @Context final HttpServletRequest req
  )
  {
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType);
    final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
    TableMetadata tableSpec;
    try {
      tableSpec = mapper.readValue(inputStream, TableMetadata.class);
    }
    catch (IOException e) {
      log.error(e, "Bad catalog sync request received!");
      return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
    }
    TableSpec spec = tableSpec.spec();
    if (CatalogUpdateNotifier.TOMBSTONE_TABLE_TYPE.equals(spec.type())) {
      listener.deleted(tableSpec.id());
    } else {
      listener.updated(tableSpec);
    }
    return Response.status(Response.Status.ACCEPTED).build();
  }
}
