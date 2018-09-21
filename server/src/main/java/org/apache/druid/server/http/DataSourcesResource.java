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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.SegmentLoadInfo;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataSegmentManager;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 */
@Path("/druid/coordinator/v1/datasources")
public class DataSourcesResource
{
  private static final Logger log = new Logger(DataSourcesResource.class);

  private final CoordinatorServerView serverInventoryView;
  private final MetadataSegmentManager databaseSegmentManager;
  private final IndexingServiceClient indexingServiceClient;
  private final AuthConfig authConfig;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public DataSourcesResource(
      CoordinatorServerView serverInventoryView,
      MetadataSegmentManager databaseSegmentManager,
      @Nullable IndexingServiceClient indexingServiceClient,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.databaseSegmentManager = databaseSegmentManager;
    this.indexingServiceClient = indexingServiceClient;
    this.authConfig = authConfig;
    this.authorizerMapper = authorizerMapper;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueryableDataSources(
      @QueryParam("full") String full,
      @QueryParam("simple") String simple,
      @Context final HttpServletRequest req
  )
  {
    Response.ResponseBuilder builder = Response.ok();
    final Set<ImmutableDruidDataSource> datasources = InventoryViewUtils.getSecuredDataSources(
        req,
        serverInventoryView,
        authorizerMapper
    );

    final Object entity;

    if (full != null) {
      entity = datasources;
    } else if (simple != null) {
      entity = datasources.stream()
                          .map(this::makeSimpleDatasource)
                          .collect(Collectors.toList());
    } else {
      entity = datasources.stream()
                          .map(ImmutableDruidDataSource::getName)
                          .collect(Collectors.toList());
    }

    return builder.entity(entity).build();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getTheDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("full") final String full
  )
  {
    final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);

    if (dataSource == null) {
      return Response.noContent().build();
    }

    if (full != null) {
      return Response.ok(dataSource).build();
    }

    return Response.ok(getSimpleDatasource(dataSourceName)).build();
  }

  @POST
  @Path("/{dataSourceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response enableDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    if (!databaseSegmentManager.enableDataSource(dataSourceName)) {
      return Response.noContent().build();
    }

    return Response.ok().build();
  }

  /* When this method is removed, a new method needs to be introduced corresponding to
    the end point "DELETE /druid/coordinator/v1/datasources/{dataSourceName}" (with no query parameters).
    Ultimately we want to have no method with kill parameter -
    DELETE `{dataSourceName}` will be used to disable datasource and
    DELETE `{dataSourceName}/intervals/{interval}` will be used to nuke segments
  */
  @DELETE
  @Deprecated
  @Path("/{dataSourceName}")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("kill") final String kill,
      @QueryParam("interval") final String interval
  )
  {
    if (indexingServiceClient == null) {
      return Response.ok(ImmutableMap.of("error", "no indexing service found")).build();
    }

    if (kill != null && Boolean.valueOf(kill)) {
      try {
        indexingServiceClient.killSegments(dataSourceName, Intervals.of(interval));
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(
                           ImmutableMap.of(
                               "error",
                               "Exception occurred. Probably the interval is invalid",
                               "message",
                               e.toString()
                           )
                       )
                       .build();
      }
      catch (Exception e) {
        return Response.serverError().entity(
            ImmutableMap.of(
                "error",
                "Exception occurred. Are you sure you have an indexing service?",
                "message",
                e.toString()
            )
        )
                       .build();
      }
    } else {
      if (!databaseSegmentManager.removeDataSource(dataSourceName)) {
        return Response.noContent().build();
      }
    }

    return Response.ok().build();
  }

  @DELETE
  @Path("/{dataSourceName}/intervals/{interval}")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteDataSourceSpecificInterval(
      @PathParam("dataSourceName") final String dataSourceName,
      @PathParam("interval") final String interval
  )
  {
    if (indexingServiceClient == null) {
      return Response.ok(ImmutableMap.of("error", "no indexing service found")).build();
    }
    final Interval theInterval = Intervals.of(interval.replace("_", "/"));
    try {
      indexingServiceClient.killSegments(dataSourceName, theInterval);
    }
    catch (Exception e) {
      return Response.serverError()
                     .entity(ImmutableMap.of(
                         "error",
                         "Exception occurred. Are you sure you have an indexing service?",
                         "message",
                         e.toString()
                     ))
                     .build();
    }
    return Response.ok().build();
  }

  @GET
  @Path("/{dataSourceName}/intervals")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceIntervals(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    if (simple == null && full == null) {
      final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
      if (dataSource == null) {
        return Response.noContent().build();
      }
      final Comparator<Interval> comparator = Comparators.intervalsByStartThenEnd().reversed();
      Set<Interval> intervals = new TreeSet<>(comparator);
      dataSource.getSegments().forEach(segment -> intervals.add(segment.getInterval()));
      return Response.ok(intervals).build();
    }
    return getSegmentDataSourceIntervals(dataSourceName, full != null, interval -> true);
  }

  @GET
  @Path("/{dataSourceName}/intervals/{interval}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSpecificInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    final Interval theInterval = Intervals.of(interval.replace("_", "/"));
    if (simple == null && full == null) {
      final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
      if (dataSource == null) {
        return Response.noContent().build();
      }
      final Set<SegmentId> segmentIds = Sets.newTreeSet();
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (theInterval.contains(dataSegment.getInterval())) {
          segmentIds.add(dataSegment.getId());
        }
      }
      return Response.ok(segmentIds).build();
    }
    return getSegmentDataSourceIntervals(dataSourceName, full != null, theInterval::contains);
  }

  /**
   * The property names belong to the public HTTP JSON API.
   */
  @PublicApi
  enum SimpleProperties
  {
    size,
    count
  }

  private Response getSegmentDataSourceIntervals(
      String dataSourceName,
      boolean full,
      Predicate<Interval> intervalFilter
  )
  {
    final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);

    if (dataSource == null) {
      return Response.noContent().build();
    }

    final Comparator<Interval> comparator = Comparators.intervalsByStartThenEnd().reversed();

    if (full) {
      final Map<Interval, Map<SegmentId, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (intervalFilter.test(dataSegment.getInterval())) {
          Map<SegmentId, Object> segments = retVal.computeIfAbsent(dataSegment.getInterval(), i -> new HashMap<>());

          Pair<DataSegment, Set<String>> val = getServersWhereSegmentIsServed(dataSegment.getId());

          if (val != null) {
            segments.put(dataSegment.getId(), ImmutableMap.of("metadata", val.lhs, "servers", val.rhs));
          }
        }
      }

      return Response.ok(retVal).build();
    } else {
      final Map<Interval, Map<SimpleProperties, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (intervalFilter.test(dataSegment.getInterval())) {
          Map<SimpleProperties, Object> properties =
              retVal.computeIfAbsent(dataSegment.getInterval(), i -> new EnumMap<>(SimpleProperties.class));
          properties.merge(SimpleProperties.size, dataSegment.getSize(), (a, b) -> (Long) a + (Long) b);
          properties.merge(SimpleProperties.count, 1, (a, b) -> (Integer) a + (Integer) b);
        }
      }

      return Response.ok(retVal).build();
    }
  }

  @GET
  @Path("/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full
  )
  {
    ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.noContent().build();
    }

    Response.ResponseBuilder builder = Response.ok();
    if (full != null) {
      return builder.entity(dataSource.getSegments()).build();
    }

    return builder.entity(Iterables.transform(dataSource.getSegments(), DataSegment::getId)).build();
  }

  @GET
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.noContent().build();
    }

    for (SegmentId possibleSegmentId : SegmentId.iteratePossibleParsingsWithDataSource(dataSourceName, segmentId)) {
      Pair<DataSegment, Set<String>> retVal = getServersWhereSegmentIsServed(possibleSegmentId);
      if (retVal != null) {
        return Response.ok(ImmutableMap.of("metadata", retVal.lhs, "servers", retVal.rhs)).build();
      }
    }
    return Response.noContent().build();
  }

  @DELETE
  @Path("/{dataSourceName}/segments/{segmentId}")
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response deleteDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (databaseSegmentManager.removeSegment(dataSourceName, segmentId)) {
      return Response.ok().build();
    }
    return Response.noContent().build();
  }

  @POST
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response enableDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (!databaseSegmentManager.enableSegment(segmentId)) {
      return Response.noContent().build();
    }

    return Response.ok().build();
  }

  @GET
  @Path("/{dataSourceName}/tiers")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceTiers(@PathParam("dataSourceName") String dataSourceName)
  {
    Set<String> retVal = Sets.newHashSet();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      if (druidServer.getDataSource(dataSourceName) != null) {
        retVal.add(druidServer.getTier());
      }
    }

    return Response.ok(retVal).build();
  }

  @Nullable
  private ImmutableDruidDataSource getDataSource(final String dataSourceName)
  {
    List<ImmutableDruidDataSource> dataSources = serverInventoryView
        .getInventory()
        .stream()
        .map(server -> server.getDataSource(dataSourceName))
        .filter(Objects::nonNull)
        .map(DruidDataSource::toImmutableDruidDataSource)
        .collect(Collectors.toList());

    if (dataSources.isEmpty()) {
      return null;
    }

    final SortedMap<SegmentId, DataSegment> segmentMap = new TreeMap<>();
    for (ImmutableDruidDataSource dataSource : dataSources) {
      Iterable<DataSegment> segments = dataSource.getSegments();
      for (DataSegment segment : segments) {
        segmentMap.put(segment.getId(), segment);
      }
    }

    return new ImmutableDruidDataSource(dataSourceName, Collections.emptyMap(), segmentMap);
  }

  @Nullable
  private Pair<DataSegment, Set<String>> getServersWhereSegmentIsServed(SegmentId segmentId)
  {
    DataSegment theSegment = null;
    Set<String> servers = Sets.newHashSet();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      DataSegment currSegment = druidServer.getSegment(segmentId);
      if (currSegment != null) {
        theSegment = currSegment;
        servers.add(druidServer.getHost());
      }
    }

    if (theSegment == null) {
      return null;
    }

    return new Pair<>(theSegment, servers);
  }

  private Map<String, Object> makeSimpleDatasource(ImmutableDruidDataSource input)
  {
    return new ImmutableMap.Builder<String, Object>()
        .put("name", input.getName())
        .put("properties", getSimpleDatasource(input.getName()))
        .build();
  }

  private Map<String, Map<String, Object>> getSimpleDatasource(String dataSourceName)
  {
    Map<String, Object> tiers = Maps.newHashMap();
    Map<String, Object> segments = Maps.newHashMap();
    Map<String, Map<String, Object>> retVal = ImmutableMap.of(
        "tiers", tiers,
        "segments", segments
    );
    Set<SegmentId> totalDistinctSegments = Sets.newHashSet();
    Map<String, HashSet<Object>> tierDistinctSegments = Maps.newHashMap();

    long totalSegmentSize = 0;
    DateTime minTime = DateTimes.MAX;
    DateTime maxTime = DateTimes.MIN;
    String tier;
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      DruidDataSource druidDataSource = druidServer.getDataSource(dataSourceName);
      tier = druidServer.getTier();

      if (druidDataSource == null) {
        continue;
      }

      if (!tierDistinctSegments.containsKey(tier)) {
        tierDistinctSegments.put(tier, Sets.newHashSet());
      }

      long dataSourceSegmentSize = 0;
      for (DataSegment dataSegment : druidDataSource.getSegments()) {
        // tier segments stats
        if (!tierDistinctSegments.get(tier).contains(dataSegment.getId())) {
          dataSourceSegmentSize += dataSegment.getSize();
          tierDistinctSegments.get(tier).add(dataSegment.getId());
        }
        // total segments stats
        if (totalDistinctSegments.add(dataSegment.getId())) {
          totalSegmentSize += dataSegment.getSize();

          minTime = DateTimes.min(minTime, dataSegment.getInterval().getStart());
          maxTime = DateTimes.max(maxTime, dataSegment.getInterval().getEnd());
        }
      }

      // tier stats
      Map<String, Object> tierStats = (Map) tiers.get(tier);
      if (tierStats == null) {
        tierStats = Maps.newHashMap();
        tiers.put(druidServer.getTier(), tierStats);
      }
      tierStats.put("segmentCount", tierDistinctSegments.get(tier).size());

      long segmentSize = MapUtils.getLong(tierStats, "size", 0L);
      tierStats.put("size", segmentSize + dataSourceSegmentSize);
    }

    segments.put("count", totalDistinctSegments.size());
    segments.put("size", totalSegmentSize);
    segments.put("minTime", minTime);
    segments.put("maxTime", maxTime);
    return retVal;
  }

  /**
   * Provides serverView for a datasource and Interval which gives details about servers hosting segments for an interval
   * Used by the realtime tasks to fetch a view of the interval they are interested in.
   */
  @GET
  @Path("/{dataSourceName}/intervals/{interval}/serverview")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSpecificInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval,
      @QueryParam("partial") final boolean partial
  )
  {
    TimelineLookup<String, SegmentLoadInfo> timeline = serverInventoryView.getTimeline(
        new TableDataSource(dataSourceName)
    );
    final Interval theInterval = Intervals.of(interval.replace("_", "/"));
    if (timeline == null) {
      log.debug("No timeline found for datasource[%s]", dataSourceName);
      return Response.ok(Lists.<ImmutableSegmentLoadInfo>newArrayList()).build();
    }

    Iterable<TimelineObjectHolder<String, SegmentLoadInfo>> lookup = timeline.lookupWithIncompletePartitions(theInterval);
    FunctionalIterable<ImmutableSegmentLoadInfo> retval = FunctionalIterable
        .create(lookup).transformCat(
            (TimelineObjectHolder<String, SegmentLoadInfo> input) ->
                Iterables.transform(
                    input.getObject(),
                    (PartitionChunk<SegmentLoadInfo> chunk) ->
                        chunk.getObject().toImmutableSegmentLoadInfo()
                )
        );
    return Response.ok(retval).build();
  }
}
