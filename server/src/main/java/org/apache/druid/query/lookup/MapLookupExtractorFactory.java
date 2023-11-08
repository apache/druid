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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.query.extraction.MapLookupExtractor;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@JsonTypeName("map")
public class MapLookupExtractorFactory implements LookupExtractorFactory
{
  @JsonProperty
  private final Map<String, String> map;
  @JsonProperty
  private final boolean isOneToOne;
  private final MapLookupExtractor lookupExtractor;
  private final LookupIntrospectHandler lookupIntrospectHandler;

  @JsonCreator
  public MapLookupExtractorFactory(
      @JsonProperty("map") Map<String, String> map,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {
    this.map = Preconditions.checkNotNull(map, "map cannot be null");
    this.isOneToOne = isOneToOne;
    this.lookupExtractor = new MapLookupExtractor(map, isOneToOne);
    this.lookupIntrospectHandler = new MapLookupIntrospectionHandler(this.map);
  }

  @Override
  public boolean start()
  {
    return true;
  }

  @Override
  public boolean close()
  {
    return true;
  }

  /**
   * For MapLookups, the replaces consideration is very easy, it simply considers if the other is the same as this one
   *
   * @param other Some other LookupExtractorFactory which might need replaced
   *
   * @return true - should replace,   false - should not replace
   */
  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    return !equals(other);
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return lookupIntrospectHandler;
  }

  @Override
  public void awaitInitialization()
  {

  }

  @Override
  public boolean isInitialized()
  {
    return true;
  }
  @Override
  public LookupExtractor get()
  {
    return lookupExtractor;
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

    MapLookupExtractorFactory that = (MapLookupExtractorFactory) o;

    if (isOneToOne != that.isOneToOne) {
      return false;
    }
    return map.equals(that.map);

  }

  @Override
  public int hashCode()
  {
    int result = map.hashCode();
    result = 31 * result + (isOneToOne ? 1 : 0);
    return result;
  }

  public static class MapLookupIntrospectionHandler implements LookupIntrospectHandler
  {
    private final Map<String, String> map;

    public MapLookupIntrospectionHandler(Map<String, String> map)
    {
      this.map = map;
    }

    @GET
    @Path("/keys")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getKeys()
    {
      return Response.ok(map.keySet().toString()).build();
    }

    @GET
    @Path("/values")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getValues()
    {
      return Response.ok(map.values().toString()).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMap()
    {
      return Response.ok(map).build();
    }
  }
}
