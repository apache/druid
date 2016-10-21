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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.inject.Inject;

import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.lookup.cache.LookupCoordinatorManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Contains information about lookups exposed through the coordinator
 */
@Path("/druid/coordinator/v1/lookups")
public class LookupCoordinatorResource
{
  private static final Logger LOG = new Logger(LookupCoordinatorResource.class);
  private final LookupCoordinatorManager lookupCoordinatorManager;
  private final ObjectMapper smileMapper;
  private final ObjectMapper jsonMapper;

  @Inject
  public LookupCoordinatorResource(
      final LookupCoordinatorManager lookupCoordinatorManager,
      final @Smile ObjectMapper smileMapper,
      final @Json ObjectMapper jsonMapper
  )
  {
    this.smileMapper = smileMapper;
    this.jsonMapper = jsonMapper;
    this.lookupCoordinatorManager = lookupCoordinatorManager;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getTiers(
      @DefaultValue("false") @QueryParam("discover") boolean discover
  )
  {
    try {
      if (discover) {
        return Response.ok().entity(lookupCoordinatorManager.discoverTiers()).build();
      }
      final Map<String, Map<String, Map<String, Object>>> knownLookups = lookupCoordinatorManager.getKnownLookups();
      if (knownLookups == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok().entity(knownLookups.keySet()).build();
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error getting list of lookups");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response updateAllLookups(
      InputStream in,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    try {
      final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(req.getContentType());
      final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
      final Map<String, Map<String, Map<String, Object>>> map;
      try {
        map = mapper.readValue(in, new TypeReference<Map<String, Map<String, Map<String, Object>>>>()
        {
        });
      }
      catch (IOException e) {
        return Response.status(Response.Status.BAD_REQUEST).entity(ServletResourceUtils.sanitizeException(e)).build();
      }
      if (lookupCoordinatorManager.updateLookups(map, new AuditInfo(author, comment, req.getRemoteAddr()))) {
        return Response.status(Response.Status.ACCEPTED).entity(map).build();
      } else {
        throw new RuntimeException("Unknown error updating configuration");
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error creating new lookups");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @DELETE
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{tier}/{lookup}")
  public Response deleteLookup(
      @PathParam("tier") String tier,
      @PathParam("lookup") String lookup,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    try {
      if (Strings.isNullOrEmpty(tier)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`tier` required")))
                       .build();
      }

      if (Strings.isNullOrEmpty(lookup)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new IAE("`lookup` required")))
                       .build();
      }

      if (lookupCoordinatorManager.deleteLookup(tier, lookup, new AuditInfo(author, comment, req.getRemoteAddr()))) {
        return Response.status(Response.Status.ACCEPTED).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error deleting lookup [%s]", lookup);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{tier}/{lookup}")
  public Response createOrUpdateLookup(
      @PathParam("tier") String tier,
      @PathParam("lookup") String lookup,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      InputStream in,
      @Context HttpServletRequest req
  )
  {
    try {
      if (Strings.isNullOrEmpty(tier)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`tier` required")))
                       .build();
      }

      if (Strings.isNullOrEmpty(lookup)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new IAE("`lookup` required")))
                       .build();
      }
      final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(req.getContentType());
      final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
      final Map<String, Object> lookupSpec;
      try {
        lookupSpec = mapper.readValue(in, new TypeReference<Map<String, Object>>()
        {
        });
      }
      catch (IOException e) {
        return Response.status(Response.Status.BAD_REQUEST).entity(ServletResourceUtils.sanitizeException(e)).build();
      }
      if (lookupCoordinatorManager.updateLookup(
          tier,
          lookup,
          lookupSpec,
          new AuditInfo(author, comment, req.getRemoteAddr())
      )) {
        return Response.status(Response.Status.ACCEPTED).build();
      } else {
        throw new RuntimeException("Unknown error updating configuration");
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error updating tier [%s] lookup [%s]", tier, lookup);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{tier}/{lookup}")
  public Response getSpecificLookup(
      @PathParam("tier") String tier,
      @PathParam("lookup") String lookup
  )
  {
    try {
      if (Strings.isNullOrEmpty(tier)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`tier` required")))
                       .build();
      }
      if (Strings.isNullOrEmpty(lookup)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`lookup` required")))
                       .build();
      }
      final Map<String, Object> map = lookupCoordinatorManager.getLookup(tier, lookup);
      if (map == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.sanitizeException(new RE("lookup [%s] not found", lookup)))
                       .build();
      }
      return Response.ok().entity(map).build();
    }
    catch (Exception e) {
      LOG.error(e, "Error getting lookup [%s]", lookup);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{tier}")
  public Response getSpecificTier(
      @PathParam("tier") String tier
  )
  {
    try {
      if (Strings.isNullOrEmpty(tier)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`tier` required")))
                       .build();
      }
      final Map<String, Map<String, Map<String, Object>>> map = lookupCoordinatorManager.getKnownLookups();
      if (map == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.sanitizeException(new RE("No lookups found")))
                       .build();
      }
      final Map<String, Map<String, Object>> tierLookups = map.get(tier);
      if (tierLookups == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.sanitizeException(new RE("Tier [%s] not found", tier)))
                       .build();
      }
      return Response.ok().entity(tierLookups.keySet()).build();
    }
    catch (Exception e) {
      LOG.error(e, "Error getting tier [%s]", tier);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }
}
