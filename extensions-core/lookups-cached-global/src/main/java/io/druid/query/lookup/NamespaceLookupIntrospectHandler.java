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

package io.druid.query.lookup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.java.util.common.Pair;
import io.druid.query.lookup.namespace.KeyValueMap;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

public class NamespaceLookupIntrospectHandler implements LookupIntrospectHandler
{
  private final NamespaceLookupExtractorFactory factory;
  private final String extractorID;
  private final NamespaceExtractionCacheManager manager;
  public NamespaceLookupIntrospectHandler(
      NamespaceLookupExtractorFactory factory,
      NamespaceExtractionCacheManager manager,
      String extractorID
  ) {
    this.factory = factory;
    this.extractorID = extractorID;
    this.manager = manager;
  }

  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDefault()
  {
    return getMap(KeyValueMap.DEFAULT_MAPNAME);
  }

  @GET
  @Path("/keys")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDefaultKeys()
  {
    return getKeys(KeyValueMap.DEFAULT_MAPNAME);
  }

  @GET
  @Path("/values")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDefaultValues()
  {
    return getValues(KeyValueMap.DEFAULT_MAPNAME);
  }

  @GET
  @Path("/maps")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMaps(
  )
  {
    try {
      return Response.ok(getIdCleanedMaps().keySet()).build();
    }
    catch (Exception e) {
      return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Path("/maps/{mapName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMap(
      @PathParam("mapName") final String mapName
  )
  {
    try {
      return Response.ok(getIdCleanedMaps().get(mapName)).build();
    }
    catch (Exception e) {
      return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Path("/maps/{mapName}/keys")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeys(
      @PathParam("mapName") final String mapName
  )
  {
    try {
      Map<String, String> map = getIdCleanedMaps().get(mapName);
      if (map == null) {
        return Response.noContent().build();
      }

      return Response.ok(map.keySet()).build();
    }
    catch (Exception e) {
      return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Path("/maps/{mapName}/values")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getValues(
      @PathParam("mapName") final String mapName
  )
  {
    try {
      Map<String, String> map = getIdCleanedMaps().get(mapName);
      if (map == null) {
        return Response.noContent().build();
      }

      return Response.ok(map.values()).build();
    }
    catch (Exception e) {
      return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Path("/version")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getVersion()
  {
    final String version = manager.getVersion(extractorID);
    if (null == version) {
      // Handle race between delete and this method being called
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      return Response.ok(ImmutableMap.of("version", version)).build();
    }
  }

  private Map<String, Map<String, String>> getIdCleanedMaps()
  {
    Map<Pair, Map<String, String>> mapMap = factory.getAllMaps();
    Map<String, Map<String, String>> retMap = Maps.newHashMap();
    for (Pair key :mapMap.keySet()) {
      String mapName = (String) key.rhs;
      retMap.put(mapName, mapMap.get(key));
    }
    return retMap;
  }
}
