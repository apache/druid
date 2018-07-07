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

package io.druid.server.http;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.InventoryView;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.guava.Comparators;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizerMapper;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
@Path("/druid/coordinator/v1/intervals")
public class IntervalsResource
{
  private final InventoryView serverInventoryView;
  private final AuthConfig authConfig;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public IntervalsResource(
      InventoryView serverInventoryView,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.authConfig = authConfig;
    this.authorizerMapper = authorizerMapper;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getIntervals(@Context final HttpServletRequest req)
  {
    final Comparator<Interval> comparator = Comparators.inverse(Comparators.intervalsByStartThenEnd());
    final Set<ImmutableDruidDataSource> datasources = InventoryViewUtils.getSecuredDataSources(
        req,
        serverInventoryView,
        authorizerMapper
    );

    final Map<Interval, Map<String, Map<String, Object>>> retVal = Maps.newTreeMap(comparator);
    for (ImmutableDruidDataSource dataSource : datasources) {
      for (DataSegment dataSegment : dataSource.getSegments()) {
        Map<String, Map<String, Object>> interval = retVal.get(dataSegment.getInterval());
        if (interval == null) {
          Map<String, Map<String, Object>> tmp = Maps.newHashMap();
          retVal.put(dataSegment.getInterval(), tmp);
        }
        setProperties(retVal, dataSource, dataSegment);
      }
    }

    return Response.ok(retVal).build();
  }

  @GET
  @Path("/{interval}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSpecificIntervals(
      @PathParam("interval") String interval,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full,
      @Context final HttpServletRequest req
  )
  {
    final Interval theInterval = Intervals.of(interval.replace("_", "/"));
    final Set<ImmutableDruidDataSource> datasources = InventoryViewUtils.getSecuredDataSources(
        req,
        serverInventoryView,
        authorizerMapper
    );

    final Comparator<Interval> comparator = Comparators.inverse(Comparators.intervalsByStartThenEnd());

    if (full != null) {
      final Map<Interval, Map<String, Map<String, Object>>> retVal = Maps.newTreeMap(comparator);
      for (ImmutableDruidDataSource dataSource : datasources) {
        for (DataSegment dataSegment : dataSource.getSegments()) {
          if (theInterval.contains(dataSegment.getInterval())) {
            Map<String, Map<String, Object>> dataSourceInterval = retVal.get(dataSegment.getInterval());
            if (dataSourceInterval == null) {
              Map<String, Map<String, Object>> tmp = Maps.newHashMap();
              retVal.put(dataSegment.getInterval(), tmp);
            }
            setProperties(retVal, dataSource, dataSegment);
          }
        }
      }

      return Response.ok(retVal).build();
    }

    if (simple != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newHashMap();
      for (ImmutableDruidDataSource dataSource : datasources) {
        for (DataSegment dataSegment : dataSource.getSegments()) {
          if (theInterval.contains(dataSegment.getInterval())) {
            Map<String, Object> properties = retVal.get(dataSegment.getInterval());
            if (properties == null) {
              properties = Maps.newHashMap();
              properties.put("size", dataSegment.getSize());
              properties.put("count", 1);

              retVal.put(dataSegment.getInterval(), properties);
            } else {
              properties.put("size", MapUtils.getLong(properties, "size", 0L) + dataSegment.getSize());
              properties.put("count", MapUtils.getInt(properties, "count", 0) + 1);
            }
          }
        }
      }

      return Response.ok(retVal).build();
    }

    final Map<String, Object> retVal = Maps.newHashMap();
    for (ImmutableDruidDataSource dataSource : datasources) {
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (theInterval.contains(dataSegment.getInterval())) {
          retVal.put("size", MapUtils.getLong(retVal, "size", 0L) + dataSegment.getSize());
          retVal.put("count", MapUtils.getInt(retVal, "count", 0) + 1);
        }
      }
    }

    return Response.ok(retVal).build();
  }

  private void setProperties(
      final Map<Interval, Map<String, Map<String, Object>>> retVal,
      ImmutableDruidDataSource dataSource, DataSegment dataSegment
  )
  {
    Map<String, Object> properties = retVal.get(dataSegment.getInterval()).get(dataSource.getName());
    if (properties == null) {
      properties = Maps.newHashMap();
      properties.put("size", dataSegment.getSize());
      properties.put("count", 1);

      retVal.get(dataSegment.getInterval()).put(dataSource.getName(), properties);
    } else {
      properties.put("size", MapUtils.getLong(properties, "size", 0L) + dataSegment.getSize());
      properties.put("count", MapUtils.getInt(properties, "count", 0) + 1);
    }
  }
}
