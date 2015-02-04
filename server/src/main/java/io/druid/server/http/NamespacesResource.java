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

package io.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.extraction.namespace.ExtractionNamespaceUpdate;
import io.druid.server.namespace.NamespacedExtractionModule;

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
import java.io.IOException;
import java.io.InputStream;

/**
 * Contains information about namespaces
 */
@Path("/druid/coordinator/v1/namespaces")
public class NamespacesResource
{
  private static final Logger log = new Logger(NamespacesResource.class);
  final NamespacedExtractionModule.NamespacedKeeper keeper;
  final ObjectMapper smileMapper, jsonMapper;

  @Inject
  public NamespacesResource(
      NamespacedExtractionModule.NamespacedKeeper keeper,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper
  )
  {
    this.keeper = keeper;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getNamespaces()
  {
    Response.ResponseBuilder builder = Response.ok();
    try {
      builder.entity(keeper.listNamespaces());
    }catch (Exception e) {
      log.error(e, "Error getting list of namespaces");
      return Response.serverError().entity(
          ImmutableMap.<String, String>of(
              "error",
              Strings.isNullOrEmpty(e.getMessage()) ? "null error" : e.getMessage()
          )
      ).build();
    }
    return builder.build();
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response newNamespace(
      InputStream inputStream,
      @Context final HttpServletRequest req // used only to get request content-type
  )
  {
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType);
    final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;

    Response.ResponseBuilder builder = Response.status(Response.Status.ACCEPTED);


    final ExtractionNamespaceUpdate update;
    try {
      update = mapper.readValue(inputStream, ExtractionNamespaceUpdate.class);
    }
    catch (IOException e) {
      log.error(e, "Error parsing new namespace");
      return Response.status(Response.Status.BAD_REQUEST).entity(
          ImmutableMap.<String, String>of(
              "error",
              Strings.isNullOrEmpty(e.getMessage()) ? "null error" : e.getMessage()
          )
      ).build();
    }

    try {
      keeper.newUpdate(update);
    }catch(Exception e){
      log.error(e, "Error creating new namespace");
      return Response.serverError().entity(
          ImmutableMap.<String, String>of(
              "error",
              Strings.isNullOrEmpty(e.getMessage()) ? "null error" : e.getMessage()
          )
      ).build();
    }
    builder.entity(update);
    return builder.build();
  }

  @DELETE
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{namespace}")
  public Response deleteNamespace(@PathParam("namespace") String namespace){
    if(Strings.isNullOrEmpty(namespace)){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("error", "Must specify namespace")).build();
    }
    try{
      keeper.deleteNamespace(namespace);
    }catch(Exception e){
      log.error(e, "Error deleting namespace [%s]", namespace);
      if(e instanceof IAE){
        // Usually because namespace doesn't exist
        return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("error", e.getMessage())).build();
      }
      return Response.serverError().entity(ImmutableMap.of("error", e.getMessage() == null ? "null error" : e.getMessage())).build();
    }
    return Response.status(Response.Status.ACCEPTED).build();
  }
}
