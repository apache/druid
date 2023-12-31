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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.common.utils.ServletResourceUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.apache.druid.server.security.AuthorizationUtils;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains information about lookups exposed through the coordinator
 */
@Path("/druid/coordinator/v1/lookups")
@ResourceFilters(ConfigResourceFilter.class)
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
  @Path("/config")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getTiers(
      @DefaultValue("false") @QueryParam("discover") boolean discover
  )
  {
    try {
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> knownLookups =
          lookupCoordinatorManager.getKnownLookups();
      if (discover) {
        final Set<String> discovered = new HashSet<>(lookupCoordinatorManager.discoverTiers());
        if (knownLookups != null) {
          discovered.addAll(knownLookups.keySet());
        }
        return Response.ok().entity(discovered).build();
      }

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

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/config/all")
  public Response getAllLookupSpecs()
  {
    try {
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> knownLookups = lookupCoordinatorManager
          .getKnownLookups();
      if (knownLookups == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok().entity(knownLookups).build();
      }
    }
    catch (Exception ex) {
      LOG.error(ex, "Error getting lookups status");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
  }

  @POST
  @Path("/config")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response updateAllLookups(
      InputStream in,
      @Context HttpServletRequest req
  )
  {
    try {
      final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(req.getContentType());
      final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> map;
      try {
        map = mapper.readValue(in, new TypeReference<Map<String, Map<String, LookupExtractorFactoryMapContainer>>>()
        {
        });
      }
      catch (IOException e) {
        return Response.status(Response.Status.BAD_REQUEST).entity(ServletResourceUtils.sanitizeException(e)).build();
      }
      if (lookupCoordinatorManager.updateLookups(map, AuthorizationUtils.buildAuditInfo(req))) {
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
  @Path("/config/{tier}")
  public Response deleteTier(
      @PathParam("tier") String tier,
      @Context HttpServletRequest req
  )
  {
    try {
      if (Strings.isNullOrEmpty(tier)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`tier` required")))
                       .build();
      }

      if (lookupCoordinatorManager.deleteTier(tier, AuthorizationUtils.buildAuditInfo(req))) {
        return Response.status(Response.Status.ACCEPTED).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error deleting tier [%s]", tier);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @DELETE
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/config/{tier}/{lookup}")
  public Response deleteLookup(
      @PathParam("tier") String tier,
      @PathParam("lookup") String lookup,
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

      if (lookupCoordinatorManager.deleteLookup(tier, lookup, AuthorizationUtils.buildAuditInfo(req))) {
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
  @Path("/config/{tier}/{lookup}")
  public Response createOrUpdateLookup(
      @PathParam("tier") String tier,
      @PathParam("lookup") String lookup,
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
      final LookupExtractorFactoryMapContainer lookupSpec;
      try {
        lookupSpec = mapper.readValue(in, LookupExtractorFactoryMapContainer.class);
      }
      catch (IOException e) {
        return Response.status(Response.Status.BAD_REQUEST).entity(ServletResourceUtils.sanitizeException(e)).build();
      }
      if (lookupCoordinatorManager.updateLookup(
          tier,
          lookup,
          lookupSpec,
          AuthorizationUtils.buildAuditInfo(req)
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
  @Path("/config/{tier}/{lookup}")
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
      final LookupExtractorFactoryMapContainer map = lookupCoordinatorManager.getLookup(tier, lookup);
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
  @Path("/config/{tier}")
  public Response getSpecificTier(
      @PathParam("tier") String tier,
      @DefaultValue("false") @QueryParam("detailed") boolean detailed

  )
  {
    try {
      if (Strings.isNullOrEmpty(tier)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`tier` required")))
                       .build();
      }
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> map = lookupCoordinatorManager.getKnownLookups();
      if (map == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.sanitizeException(new RE("No lookups found")))
                       .build();
      }
      final Map<String, LookupExtractorFactoryMapContainer> tierLookups = map.get(tier);
      if (tierLookups == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.sanitizeException(new RE("Tier [%s] not found", tier)))
                       .build();
      }
      if (detailed) {
        return Response.ok().entity(tierLookups).build();
      } else {
        return Response.ok().entity(tierLookups.keySet()).build();
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error getting tier [%s]", tier);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/status")
  public Response getAllLookupsStatus(
      @QueryParam("detailed") boolean detailed
  )
  {
    try {
      Map<String, Map<String, LookupExtractorFactoryMapContainer>> configuredLookups = lookupCoordinatorManager
          .getKnownLookups();
      if (configuredLookups == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.jsonize("No lookups found"))
                       .build();
      }

      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupsStateOnNodes = lookupCoordinatorManager
          .getLastKnownLookupsStateOnNodes();

      Map<String, Map<String, LookupStatus>> result = new HashMap<>();

      for (Map.Entry<String, Map<String, LookupExtractorFactoryMapContainer>> tierEntry : configuredLookups.entrySet()) {
        String tier = tierEntry.getKey();
        Map<String, LookupStatus> lookupStatusMap = new HashMap<>();
        result.put(tier, lookupStatusMap);
        Collection<HostAndPort> hosts = lookupCoordinatorManager.discoverNodesInTier(tier);

        for (Map.Entry<String, LookupExtractorFactoryMapContainer> lookupsEntry : tierEntry.getValue().entrySet()) {
          lookupStatusMap.put(
              lookupsEntry.getKey(),
              getLookupStatus(
                  lookupsEntry.getKey(),
                  lookupsEntry.getValue(),
                  hosts,
                  lookupsStateOnNodes,
                  detailed
              )
          );
        }
      }

      return Response.ok(result).build();
    }
    catch (Exception ex) {
      LOG.error(ex, "Error getting lookups status");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/status/{tier}")
  public Response getLookupStatusForTier(
      @PathParam("tier") String tier,
      @QueryParam("detailed") boolean detailed
  )
  {
    try {
      Map<String, Map<String, LookupExtractorFactoryMapContainer>> configuredLookups = lookupCoordinatorManager
          .getKnownLookups();
      if (configuredLookups == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.jsonize("No lookups found"))
                       .build();
      }

      Map<String, LookupExtractorFactoryMapContainer> tierLookups = configuredLookups.get(tier);
      if (tierLookups == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.jsonize("No lookups found for tier [%s].", tier))
                       .build();
      }


      Map<String, LookupStatus> lookupStatusMap = new HashMap<>();
      Collection<HostAndPort> hosts = lookupCoordinatorManager.discoverNodesInTier(tier);

      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupsStateOnNodes = lookupCoordinatorManager
          .getLastKnownLookupsStateOnNodes();

      for (Map.Entry<String, LookupExtractorFactoryMapContainer> lookupsEntry : tierLookups.entrySet()) {
        lookupStatusMap.put(
            lookupsEntry.getKey(),
            getLookupStatus(lookupsEntry.getKey(), lookupsEntry.getValue(), hosts, lookupsStateOnNodes, detailed)
        );
      }

      return Response.ok(lookupStatusMap).build();
    }
    catch (Exception ex) {
      LOG.error(ex, "Error getting lookups status for tier [%s].", tier);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/status/{tier}/{lookup}")
  public Response getSpecificLookupStatus(
      @PathParam("tier") String tier,
      @PathParam("lookup") String lookup,
      @QueryParam("detailed") boolean detailed
  )
  {
    try {
      Map<String, Map<String, LookupExtractorFactoryMapContainer>> configuredLookups = lookupCoordinatorManager
          .getKnownLookups();
      if (configuredLookups == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.jsonize("No lookups found"))
                       .build();
      }

      Map<String, LookupExtractorFactoryMapContainer> tierLookups = configuredLookups.get(tier);
      if (tierLookups == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.jsonize("No lookups found for tier [%s].", tier))
                       .build();
      }

      LookupExtractorFactoryMapContainer lookupDef = tierLookups.get(lookup);
      if (lookupDef == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.jsonize("Lookup [%s] not found for tier [%s].", lookup, tier))
                       .build();
      }

      return Response.ok(
          getLookupStatus(
              lookup,
              lookupDef,
              lookupCoordinatorManager.discoverNodesInTier(tier),
              lookupCoordinatorManager.getLastKnownLookupsStateOnNodes(),
              detailed
          )
      ).build();
    }
    catch (Exception ex) {
      LOG.error(ex, "Error getting lookups status for tier [%s] and lookup [%s].", tier, lookup);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
  }

  @VisibleForTesting
  LookupStatus getLookupStatus(
      String lookupName,
      LookupExtractorFactoryMapContainer lookupDef,
      Collection<HostAndPort> nodes,
      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lastKnownLookupsState,
      boolean detailed
  )
  {
    boolean isReady = true;
    List<HostAndPort> pendingHosts = detailed ? new ArrayList<>() : null;

    for (HostAndPort node : nodes) {
      LookupsState<LookupExtractorFactoryMapContainer> hostState = lastKnownLookupsState.get(node);
      LookupExtractorFactoryMapContainer loadedOnHost = hostState != null
                                                        ? hostState.getCurrent().get(lookupName)
                                                        : null;
      if (loadedOnHost == null || lookupDef.replaces(loadedOnHost)) {
        isReady = false;
        if (detailed) {
          pendingHosts.add(node);
        } else {
          break;
        }
      }
    }

    return new LookupStatus(isReady, pendingHosts);
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/nodeStatus")
  public Response getAllNodesStatus(
      @QueryParam("discover") boolean discover,
      @QueryParam("detailed") @Nullable Boolean detailed
  )
  {
    boolean full = detailed == null || detailed;

    try {
      Collection<String> tiers;
      if (discover) {
        tiers = lookupCoordinatorManager.discoverTiers();
      } else {
        Map<String, Map<String, LookupExtractorFactoryMapContainer>> configuredLookups = lookupCoordinatorManager
            .getKnownLookups();
        if (configuredLookups == null) {
          return Response.status(Response.Status.NOT_FOUND)
                         .entity(ServletResourceUtils.jsonize("No lookups configured."))
                         .build();
        }
        tiers = configuredLookups.keySet();
      }

      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupsStateOnHosts =
          lookupCoordinatorManager.getLastKnownLookupsStateOnNodes();

      final Map result;
      if (full) {
        result = getDetailedAllNodeStatus(lookupsStateOnHosts, tiers);
      } else {
        // lookups to load per host by version
        result = getSimpleAllNodeStatus(lookupsStateOnHosts, tiers);
      }
      return Response.ok(result).build();
    }
    catch (Exception ex) {
      LOG.error(ex, "Error getting node status.");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/nodeStatus/{tier}")
  public Response getNodesStatusInTier(
      @PathParam("tier") String tier
  )
  {
    try {
      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupsStateOnHosts = lookupCoordinatorManager
          .getLastKnownLookupsStateOnNodes();

      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> tierNodesStatus = new HashMap<>();

      Collection<HostAndPort> nodes = lookupCoordinatorManager.discoverNodesInTier(tier);

      for (HostAndPort node : nodes) {
        LookupsState<LookupExtractorFactoryMapContainer> lookupsState = lookupsStateOnHosts.get(node);
        if (lookupsState == null) {
          tierNodesStatus.put(node, new LookupsState<>(null, null, null));
        } else {
          tierNodesStatus.put(node, lookupsState);
        }
      }

      return Response.ok(tierNodesStatus).build();
    }
    catch (Exception ex) {
      LOG.error(ex, "Error getting node status for tier [%s].", tier);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/nodeStatus/{tier}/{hostAndPort}")
  public Response getSpecificNodeStatus(
      @PathParam("tier") String tier,
      @PathParam("hostAndPort") HostAndPort hostAndPort
  )
  {
    try {
      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupsStateOnHosts = lookupCoordinatorManager
          .getLastKnownLookupsStateOnNodes();

      LookupsState<LookupExtractorFactoryMapContainer> lookupsState = lookupsStateOnHosts.get(hostAndPort);
      if (lookupsState == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.jsonize("Node [%s] status is unknown.", hostAndPort))
                       .build();
      } else {
        return Response.ok(lookupsState).build();
      }

    }
    catch (Exception ex) {
      LOG.error(ex, "Error getting node status for [%s].", hostAndPort);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
  }



  /**
   * Build 'simple' lookup cluster status, broken down by tier, host, and then the {@link LookupsState} with
   * the lookup name and version ({@link LookupExtractorFactoryMapContainer#version})
   */
  private Map<String, Map<HostAndPort, LookupsState<String>>> getSimpleAllNodeStatus(
      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupsStateOnHosts,
      Collection<String> tiers
  )
  {
    Map<String, Map<HostAndPort, LookupsState<String>>> results = new HashMap<>();
    for (String tier : tiers) {
      Map<HostAndPort, LookupsState<String>> tierNodesStatus = new HashMap<>();
      results.put(tier, tierNodesStatus);

      Collection<HostAndPort> nodes = lookupCoordinatorManager.discoverNodesInTier(tier);

      for (HostAndPort node : nodes) {
        LookupsState<LookupExtractorFactoryMapContainer> lookupsState = lookupsStateOnHosts.get(node);
        if (lookupsState == null) {
          tierNodesStatus.put(node, new LookupsState<>(null, null, null));
        } else {
          Map<String, String> current = lookupsState.getCurrent()
                                                    .entrySet()
                                                    .stream()
                                                    .collect(
                                                        Collectors.toMap(
                                                            Map.Entry::getKey,
                                                            e -> e.getValue().getVersion()
                                                        )
                                                    );
          Map<String, String> toLoad = lookupsState.getToLoad()
                                                   .entrySet()
                                                   .stream()
                                                   .collect(
                                                       Collectors.toMap(
                                                           Map.Entry::getKey,
                                                           e -> e.getValue().getVersion()
                                                       )
                                                   );
          tierNodesStatus.put(node, new LookupsState<>(current, toLoad, lookupsState.getToDrop()));
        }
      }
    }
    return results;
  }

  /**
   * Build 'detailed' lookup cluster status, broken down by tier, host, and then the full {@link LookupsState} with
   * complete {@link LookupExtractorFactoryMapContainer} information.
   */
  private Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> getDetailedAllNodeStatus(
      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupsStateOnHosts,
      Collection<String> tiers
  )
  {
    Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> result = new HashMap<>();
    for (String tier : tiers) {
      Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> tierNodesStatus = new HashMap<>();
      result.put(tier, tierNodesStatus);

      Collection<HostAndPort> nodes = lookupCoordinatorManager.discoverNodesInTier(tier);

      for (HostAndPort node : nodes) {
        LookupsState<LookupExtractorFactoryMapContainer> lookupsState = lookupsStateOnHosts.get(node);
        if (lookupsState == null) {
          tierNodesStatus.put(node, new LookupsState<>(null, null, null));
        } else {
          tierNodesStatus.put(node, lookupsState);
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  static class LookupStatus
  {
    @JsonProperty
    private boolean loaded;

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<HostAndPort> pendingNodes;

    public LookupStatus(boolean loaded, List<HostAndPort> pendingHosts)
    {
      this.loaded = loaded;
      this.pendingNodes = pendingHosts;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LookupStatus that = (LookupStatus) o;
      return Objects.equals(loaded, that.loaded) &&
             Objects.equals(pendingNodes, that.pendingNodes);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(loaded, pendingNodes);
    }
  }
}
