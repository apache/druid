/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.listener.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.druid.common.utils.ServletResourceUtils;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.logger.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;

/**
 * This is a simple announcement resource that handles simple items that have a POST to an announcement endpoint, a
 * GET of something in that endpoint with an ID, and a DELETE to that endpoint with an ID.
 *
 * The idea of this resource is simply to have a simple endpoint for basic POJO handling assuming the POJO has an ID
 * which distinguishes it from others of its kind.
 *
 * This resource is expected to NOT block for POSTs, and is instead expected to make a best effort at returning
 * as quickly as possible. Thus, returning ACCEPTED instead of OK is normal for POST methods here.
 *
 * Items tagged with a particular ID for an announcement listener are updated by a POST to the announcement listener's
 * path "/{announcement}"
 *
 * Discovery of who can listen to particular announcement keys is not part of this class and should be handled
 * by ListenerResourceAnnouncer
 */
public abstract class ListenerResource
{
  public static final String BASE_PATH = "/druid/listen/v1";
  private static final Logger LOG = new Logger(ListenerResource.class);

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final ListenerHandler handler;

  public ListenerResource(
      final @Json ObjectMapper jsonMapper,
      final @Smile ObjectMapper smileMapper,
      final ListenerHandler handler
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.smileMapper = Preconditions.checkNotNull(smileMapper, "smileMapper");
    this.handler = Preconditions.checkNotNull(handler, "listener handler");
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response serviceAnnouncementPOSTAll(
      final InputStream inputStream,
      final @Context HttpServletRequest req // used only to get request content-type
  )
  {
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType);
    final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
    try {
      return handler.handlePOSTAll(inputStream, mapper);
    }
    catch (Exception e) {
      LOG.error(e, "Exception in handling POSTAll request");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getAll()
  {
    try {
      return handler.handleGETAll();
    }
    catch (Exception e) {
      LOG.error(e, "Exception in handling GETAll request");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @Path("/{id}")
  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response serviceAnnouncementGET(
      final @PathParam("id") String id
  )
  {
    if (Strings.isNullOrEmpty(id)) {
      return makeNullIdResponse();
    }
    try {
      return handler.handleGET(id);
    }
    catch (Exception e) {
      LOG.error(e, "Exception in handling GET request for [%s]", id);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @Path("/{id}")
  @DELETE
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response serviceAnnouncementDELETE(
      final @PathParam("id") String id
  )
  {
    if (Strings.isNullOrEmpty(id)) {
      return makeNullIdResponse();
    }
    try {
      return handler.handleDELETE(id);
    }
    catch (Exception e) {
      LOG.error(e, "Exception in handling DELETE request for [%s]", id);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @Path("/{id}")
  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response serviceAnnouncementPOST(
      final @PathParam("id") String id,
      final InputStream inputStream,
      final @Context HttpServletRequest req // used only to get request content-type
  )
  {
    if (Strings.isNullOrEmpty(id)) {
      return makeNullIdResponse();
    }
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType);
    final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
    try {
      return handler.handlePOST(inputStream, mapper, id);
    }
    catch (Exception e) {
      LOG.error(e, "Exception in handling POST request for ID [%s]", id);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  public static Response makeNullIdResponse()
  {
    return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(ServletResourceUtils.sanitizeException(new IllegalArgumentException("Cannot have null or empty id")))
        .build();
  }
}
