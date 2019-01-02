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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadata;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollector;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollectorServerView;
import org.apache.druid.extensions.watermarking.gaps.GapDetectorFactory;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.watermarks.WatermarkCursorFactory;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/druid/watermarking/v1/collector")
@ResourceFilters({WatermarkStateResourceFilter.class})
public class WatermarkCollectorResource extends WatermarkKeeperResource
{
  private final TimelineMetadataCollectorServerView serverView;
  private final WatermarkSource watermarkSource;
  private final WatermarkSink watermarkSink;
  private final Map<String, WatermarkCursorFactory> cursorFactories;
  private final Map<String, GapDetectorFactory> gapDetectorFactories;

  @Inject
  public WatermarkCollectorResource(
      TimelineMetadataCollectorServerView monitorServerView,
      WatermarkSource source,
      WatermarkSink sink,
      Map<String, WatermarkCursorFactory> lookup,
      Map<String, GapDetectorFactory> gapDetectors
  )
  {
    super(source, sink);
    this.serverView = monitorServerView;
    this.watermarkSource = source;
    this.watermarkSink = sink;
    this.cursorFactories = lookup;
    this.gapDetectorFactories = gapDetectors;
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus()
  {
    final boolean initalized = serverView.isInitialized();
    final Map<String, Boolean> map = ImmutableMap.of(
        "inventoryInitialized",
        initalized
    );
    if (initalized) {
      return Response.ok(map).build();
    } else {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(map).build();
    }
  }

  @GET
  @Path("/watermarks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWatermarks()
  {
    Map<String, Map<String, DateTime>> loadedWatermarks = null;
    if (serverView.isInitialized()) {
      loadedWatermarks =
          serverView
              .getDataSources()
              .stream()
              .collect(Collectors.toMap(
                  dsName -> dsName,
                  dsName -> {
                    Map<String, DateTime> values = watermarkSource.getValues(
                        dsName);
                    if (values != null) {
                      return new HashMap<>(values);
                    }
                    return new HashMap<>();
                  }
              ));
    }
    return Response.ok(ImmutableMap.of(
        "inventoryInitialized",
        serverView.isInitialized(),
        "watermarks",
        loadedWatermarks != null ? loadedWatermarks : new HashMap<>()
    )).build();
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus()
  {
    final boolean initalized = serverView.isInitialized();
    final Map<String, TimelineStatus> loadStatus;
    if (initalized) {
      loadStatus =
          serverView.getDataSources().stream().collect(Collectors.toMap(
              dsName -> dsName,
              dsName -> {
                final Map<String, DateTime> values = watermarkSource.getValues(dsName);

                final TimelineMetadataCollector<List<Interval>> gapDetector =
                    new TimelineMetadataCollector<>(
                        serverView,
                        ImmutableList.copyOf(gapDetectorFactories.values())
                    );
                final List<TimelineMetadata<List<Interval>>> results =
                    gapDetector.fold(dsName, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

                final Map<String, List<Interval>> dsGaps = new HashMap<>();

                if (results.size() > 0) {
                  for (TimelineMetadata<List<Interval>> gap : results) {
                    if (gap.getMetadata().size() > 0) {
                      dsGaps.put(gap.getType(), gap.getMetadata());
                    }
                  }
                }


                final Map<String, DateTime> dsWatermarks;
                if (values != null) {
                  dsWatermarks = new HashMap<>(values);
                } else {
                  dsWatermarks = new HashMap<>();
                }

                return new TimelineStatus(dsWatermarks, dsGaps);
              }
          ));
    } else {
      loadStatus = ImmutableMap.of();
    }

    final Map<String, Object> entry = ImmutableMap.of(
        "inventoryInitialized",
        serverView.isInitialized(),
        "datasources",
        loadStatus
    );
    final Response.Status status = initalized ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE;
    return Response.status(status).entity(entry).build();
  }

  @GET
  @Path("/datasources")
  @Produces(MediaType.APPLICATION_JSON)
  @Override
  public Response getDatasources()
  {
    if (!serverView.isInitialized()) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(ImmutableList.of()).build();
    }
    return Response.ok(ImmutableMap.of(
        "inventoryInitialized",
        serverView.isInitialized(),
        "datasources",
        serverView.getDataSources()
    )).build();
  }


  @GET
  @Path("/datasources/{dataSourceName}/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatasourceStatus(
      @PathParam("dataSourceName") String dsName
  )
  {
    final TimelineStatus loadStatus;
    if (serverView.isInitialized()) {
      Map<String, DateTime> values = watermarkSource.getValues(dsName);

      TimelineMetadataCollector<List<Interval>> gapDetector =
          new TimelineMetadataCollector<>(
              serverView,
              ImmutableList.copyOf(gapDetectorFactories.values())
          );
      List<TimelineMetadata<List<Interval>>> results =
          gapDetector.fold(dsName, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

      Map<String, List<Interval>> dsGaps = new HashMap<>();
      Map<String, DateTime> dsWatermarks;

      if (results.size() > 0) {
        for (TimelineMetadata<List<Interval>> gap : results) {
          if (gap.getMetadata().size() > 0) {
            dsGaps.put(gap.getType(), gap.getMetadata());
          }
        }
      }

      if (values != null) {
        dsWatermarks = new HashMap<>(values);
      } else {
        dsWatermarks = new HashMap<>();
      }

      loadStatus = new TimelineStatus(dsWatermarks, dsGaps);
    } else {
      loadStatus = new TimelineStatus(ImmutableMap.of(), ImmutableMap.of());
    }

    return Response.ok(ImmutableMap.of(
        "inventoryInitialized",
        serverView.isInitialized(),
        dsName,
        loadStatus
    )).build();
  }


  @POST
  @Path("/datasources/{dataSourceName}/{watermarkType}")
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  @Override
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

    // todo: current behavior is to prefer updating through cursor and falling back to update directly - fail instead?
    if (cursorFactories.containsKey(watermarkType)) {
      if (!cursorFactories.get(watermarkType).canOverride()) {
        return Response.status(Response.Status.BAD_REQUEST).entity(
            StringUtils.format("Watermark %s cannot be manually overridden", watermarkType)
        ).build();
      }
      cursorFactories.get(watermarkType).manualOverride(dataSourceName, update);
    } else {
      watermarkSink.update(dataSourceName, watermarkType, update.minuteOfDay().roundCeilingCopy());
    }
    return Response.ok().build();
  }


  @POST
  @Path("/datasources/{dataSourceName}/{watermarkType}/rollback")
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response rollback(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("watermarkType") String watermarkType,
      @QueryParam("timestamp") String timestamp
  )
  {
    final DateTime rollback;
    try {
      Preconditions.checkNotNull(timestamp, "timestamp");
      rollback = DateTimes.of(timestamp);
    }
    catch (IllegalArgumentException | NullPointerException iae) {
      return Response.status(Response.Status.BAD_REQUEST).entity(iae.getMessage()).build();
    }

    watermarkSink.rollback(dataSourceName, watermarkType, rollback);

    return Response.ok().build();
  }

  @POST
  @Path("/datasources/{dataSourceName}/{watermarkType}/purge")
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response purge(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("watermarkType") String watermarkType,
      @QueryParam("timestamp") String timestamp
  )
  {
    final DateTime purgePoint;
    try {
      Preconditions.checkNotNull(timestamp, "timestamp");
      purgePoint = DateTimes.of(timestamp);
    }
    catch (IllegalArgumentException | NullPointerException iae) {
      return Response.status(Response.Status.BAD_REQUEST).entity(iae.getMessage()).build();
    }

    watermarkSink.purgeHistory(dataSourceName, watermarkType, purgePoint);

    return Response.ok().build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/gaps")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response getGaps(
      @PathParam("dataSourceName") String dataSourceName
  )
  {
    final TimelineMetadataCollector<List<Interval>> gapDetector =
        new TimelineMetadataCollector<>(
            serverView,
            ImmutableList.copyOf(gapDetectorFactories.values())
        );

    final List<TimelineMetadata<List<Interval>>> results =
        gapDetector.fold(dataSourceName, new Interval(DateTimes.utc(0), DateTimes.nowUtc()), true);

    final Map<String, List<Interval>> response = new HashMap<>();

    if (results.size() > 0) {
      for (TimelineMetadata<List<Interval>> gap : results) {
        response.put(gap.getType(), gap.getMetadata());
      }

      return Response.ok().entity(response).build();
    }
    return Response.noContent().build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/gaps/{gapType}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(WatermarkDatasourceResourceFilter.class)
  public Response getGaps(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("gapType") String gapType
  )
  {
    final GapDetectorFactory factory = gapDetectorFactories.get(gapType);
    if (factory == null) {
      return Response.noContent().build();
    }
    final TimelineMetadataCollector<List<Interval>> gapDetector =
        new TimelineMetadataCollector<>(
            serverView,
            ImmutableList.of(gapDetectorFactories.get(gapType))
        );

    final List<TimelineMetadata<List<Interval>>> results = gapDetector
        .fold(
            dataSourceName,
            new Interval(DateTimes.utc(JodaUtils.MIN_INSTANT), DateTimes.utc(JodaUtils.MAX_INSTANT)),
            true
        );

    if (results.isEmpty()) {
      return Response.noContent().build();
    }

    final Map<String, List<Interval>> response = results
        .stream()
        .collect(
            Collectors.toMap(
                TimelineMetadata::getType,
                TimelineMetadata::getMetadata
            )
        );
    return Response.ok().entity(response).build();
  }

  private class TimelineStatus
  {
    @JsonProperty
    public Map<String, DateTime> watermarks;
    @JsonProperty
    public Map<String, List<Interval>> gaps;

    public TimelineStatus(Map<String, DateTime> watermarks, Map<String, List<Interval>> gaps)
    {
      this.watermarks = watermarks;
      this.gaps = gaps;
    }
  }
}
