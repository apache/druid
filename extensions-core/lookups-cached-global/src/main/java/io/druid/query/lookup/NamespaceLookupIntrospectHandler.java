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

import io.druid.common.utils.ServletResourceUtils;
import io.druid.java.util.common.ISE;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
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
  @Path("/keys")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeys()
  {
    try {
      return Response.ok(getLatest().keySet()).build();
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
      return Response.ok(getLatest()).build();
    }
    catch (ISE e) {
      return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  private Map<String, String> getLatest()
  {
    return ((MapLookupExtractor) factory.get()).getMap();
  }
}
