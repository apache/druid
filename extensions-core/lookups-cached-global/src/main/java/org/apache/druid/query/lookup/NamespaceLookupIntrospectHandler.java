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

package org.apache.druid.query.lookup;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.server.initialization.jetty.HttpResponses;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

public class NamespaceLookupIntrospectHandler implements LookupIntrospectHandler
{
  private final NamespaceLookupExtractorFactory factory;

  public NamespaceLookupIntrospectHandler(NamespaceLookupExtractorFactory factory)
  {
    this.factory = factory;
  }

  @GET
  @Path("/keys")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getKeys()
  {
    try {
      return HttpResponses.OK.entity(getLatest().keySet());
    }
    catch (ISE e) {
      return HttpResponses.NOT_FOUND.error(e);
    }
  }

  @GET
  @Path("/values")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getValues()
  {
    try {
      return HttpResponses.OK.entity(getLatest().values());
    }
    catch (ISE e) {
      return HttpResponses.NOT_FOUND.error(e);
    }
  }

  @GET
  @Path("/version")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getVersion()
  {
    final CacheScheduler.CacheState cacheState = factory.entry.getCacheState();
    if (cacheState instanceof CacheScheduler.NoCache) {
      return HttpResponses.NOT_FOUND.empty();
    } else {
      String version = ((CacheScheduler.VersionedCache) cacheState).getVersion();
      return HttpResponses.OK.entity(ImmutableMap.of("version", version));
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMap()
  {
    try {
      return HttpResponses.OK.entity(getLatest());
    }
    catch (ISE e) {
      return HttpResponses.NOT_FOUND.error(e);
    }
  }

  private Map<String, String> getLatest()
  {
    return ((MapLookupExtractor) factory.get()).getMap();
  }
}
