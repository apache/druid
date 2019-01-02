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

package org.apache.druid.extensions.watermarking.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/druid/watermarking/v1/keeper")
@ResourceFilters(WatermarkStateResourceFilter.class)
public class WatermarkKeeperResource
{
  private final WatermarkSource source;
  private final WatermarkSink sink;

  @Inject
  public WatermarkKeeperResource(
      WatermarkSource source,
      WatermarkSink sink
  )
  {
    this.source = source;
    this.sink = sink;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response getAllThings()
  {
    final Collection<String> datasources = source.getDatasources();
    if (datasources == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    final Map<String, Map<String, DateTime>> results = new HashMap<>();
    for (String datasource : datasources) {
      final Map<String, DateTime> watermarks = source.getValues(datasource);
      if (watermarks != null) {
        results.put(datasource, watermarks);
      }
    }
    return Response.ok(results).build();
  }


  @GET
  @Path("/datasources")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response getDatasources()
  {
    return Response.ok(source.getDatasources()).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response getDatasource(
      @PathParam("dataSourceName") String dataSourceName
  )
  {
    final Map<String, DateTime> values = source.getValues(dataSourceName);

    if (values != null) {
      HashMap<String, DateTime> body = new HashMap<>(values);
      return Response.ok().entity(body).build();
    }
    return Response.noContent().build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/{watermarkType}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response getDatasourceKey(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("watermarkType") String watermarkType
  )
  {
    final DateTime keyValue = source.getValue(dataSourceName, watermarkType);
    if (keyValue != null) {
      Map<String, DateTime> body = ImmutableMap.of(watermarkType, keyValue);
      return Response.ok().entity(body).build();
    }
    return Response.noContent().build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/{watermarkType}/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response getDatasourceHistory(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("watermarkType") String watermarkType,
      @QueryParam("startTime") String start,
      @QueryParam("endTime") String end
  )
  {
    DateTime startTime;
    DateTime endTime;

    try {
      startTime = start == null ? DateTimes.utc(0) : DateTimes.of(start);
      endTime = end == null ? DateTimes.nowUtc() : DateTimes.of(end);
    }
    catch (IllegalArgumentException iae) {
      return Response.status(Response.Status.BAD_REQUEST).entity(iae.getMessage()).build();
    }
    final List<Pair<DateTime, DateTime>> history =
        source.getValueHistory(
            dataSourceName,
            watermarkType,
            new Interval(startTime, endTime)
        );
    if (history == null) {
      return Response.noContent().build();
    }
    return Response
        .ok()
        .entity(
            history.stream()
                   .map(dateTimeDateTimePair -> ImmutableMap.of(
                       "timestamp", dateTimeDateTimePair.lhs,
                       "inserted", dateTimeDateTimePair.rhs
                   ))
                   .collect(Collectors.toList())
        ).build();
  }

  @POST
  @Path("/datasources/{dataSourceName}/{watermarkType}")
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response updateDatasourceKey(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("watermarkType") String watermarkType,
      @QueryParam("timestamp") String timestamp
  )
  {
    final DateTime update;
    try {
      Preconditions.checkNotNull(timestamp, "timestamp");
      update = DateTimes.of(timestamp);
    }
    catch (IllegalArgumentException | NullPointerException iae) {
      return Response.status(Response.Status.BAD_REQUEST).entity(iae.getMessage()).build();
    }
    sink.update(dataSourceName, watermarkType, update.minuteOfDay().roundCeilingCopy());
    return Response.ok().build();
  }
}
