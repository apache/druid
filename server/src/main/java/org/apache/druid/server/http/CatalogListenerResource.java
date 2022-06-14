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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import org.apache.druid.catalog.MetadataCatalog.CatalogListener;
import org.apache.druid.catalog.TableDefn;
import org.apache.druid.catalog.TableSpec;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

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

@Path(CatalogListenerResource.BASE_URL)
public class CatalogListenerResource
{
  public static final String BASE_URL = "/druid/broker/v1/catalog";
  public static final String SYNC_URL = "/sync";

  private final CatalogListener listener;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper smileMapper;
  private final ObjectMapper jsonMapper;

  @Inject
  public CatalogListenerResource(
      final CatalogListener listener,
      @Smile final ObjectMapper smileMapper,
      @Json final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper)
  {
    this.listener = listener;
    this.authorizerMapper = authorizerMapper;
    this.smileMapper = smileMapper;
    this.jsonMapper = jsonMapper;
  }

  @POST
  @Path(SYNC_URL)
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response syncTable(
      final InputStream inputStream,
      @Context final HttpServletRequest req)
  {
    Response resp = checkAuth(req);
    if (resp != null) {
      return resp;
    }
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType);
    final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
    TableSpec tableSpec;
    try {
      tableSpec = mapper.readValue(inputStream, TableSpec.class);
    }
    catch (IOException e) {
      return Response.serverError().entity(e.getMessage()).build();
    }
    TableDefn defn = tableSpec.defn();
    if (defn instanceof TableDefn.Tombstone) {
      listener.deleted(tableSpec.id());
    } else {
      listener.updated(tableSpec);
    }
    return Response.status(Response.Status.ACCEPTED).build();
  }

  private Response checkAuth(final HttpServletRequest request)
  {
    final ResourceAction resourceAction = new ResourceAction(
        new Resource("CONFIG", ResourceType.CONFIG),
        Action.WRITE
    );

    final Access authResult = AuthorizationUtils.authorizeResourceAction(
        request,
        resourceAction,
        authorizerMapper
    );

    if (authResult.isAllowed()) {
      return null;
    }
    return Response.status(Response.Status.FORBIDDEN)
                  .type(MediaType.TEXT_PLAIN)
                  .entity(StringUtils.format("Access-Check-Result: %s", authResult.toString()))
                  .build();
  }
}
