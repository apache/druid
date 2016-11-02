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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.java.util.common.ISE;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.commons.collections.keyvalue.MultiKey;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  @Path("/keys")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeys()
  {
    try {
      Set<MultiKey> keySet = getLatest().keySet();
      List<String> cleanedKeyList = Lists.transform(
          Lists.newArrayList(keySet),
          new Function<MultiKey, String>() {
            @Override
            public String apply(MultiKey key) {
              return (String) key.getKey(1);
            }
          }
      );
      return Response.ok(Sets.newHashSet(cleanedKeyList)).build();
    }
    catch (ISE e) {
      return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @GET
  @Path("/values")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getValues()
  {
    try {
      return Response.ok(getLatest().values()).build();
    }
    catch (ISE e) {
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

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMap()
  {
    try {
      Map<MultiKey, Map<String, String>> mapMap = getLatest();
      Map<String, Map<String, String>> retMap = Maps.newHashMap();
      for (MultiKey key :mapMap.keySet()) {
        String mapName = (String) key.getKey(1);
        retMap.put(mapName, mapMap.get(key));
      }
      return Response.ok(retMap).build();
    }
    catch (ISE e) {
      return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  private Map<MultiKey, Map<String, String>> getLatest()
  {
    return factory.getAllMaps();
  }
}
