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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.commons.lang.StringUtils;
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
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataSegmentManager;
import org.apache.druid.metadata.UnknownSegmentIdException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
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
import java.util.ArrayList;
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
  private final MetadataSegmentManager segmentsMetadata;
  private final MetadataRuleManager metadataRuleManager;
  private final IndexingServiceClient indexingServiceClient;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public DataSourcesResource(
      CoordinatorServerView serverInventoryView,
      MetadataSegmentManager segmentsMetadata,
      MetadataRuleManager metadataRuleManager,
      @Nullable IndexingServiceClient indexingServiceClient,
      AuthorizerMapper authorizerMapper
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.segmentsMetadata = segmentsMetadata;
    this.metadataRuleManager = metadataRuleManager;
    this.indexingServiceClient = indexingServiceClient;
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
    final Set<ImmutableDruidDataSource> datasources =
        InventoryViewUtils.getSecuredDataSources(req, serverInventoryView, authorizerMapper);

    final Object entity;

    if (full != null) {
      entity = datasources;
    } else if (simple != null) {
      entity = datasources.stream().map(this::makeSimpleDatasource).collect(Collectors.toList());
    } else {
      entity = datasources.stream().map(ImmutableDruidDataSource::getName).collect(Collectors.toList());
    }

    return builder.entity(entity).build();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("full") final String full
  )
  {
    final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);

    if (dataSource == null) {
      return logAndCreateDataSourceNotFoundResponse(dataSourceName);
    }

    if (full != null) {
      return Response.ok(dataSource).build();
    }

    return Response.ok(getSimpleDatasource(dataSourceName)).build();
  }

  private interface MarkSegments
  {
    int markSegments() throws UnknownSegmentIdException;
  }

  @POST
  @Path("/{dataSourceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markAsUsedAllNonOvershadowedSegments(@PathParam("dataSourceName") final String dataSourceName)
  {
    MarkSegments markSegments = () -> segmentsMetadata.markAsUsedAllNonOvershadowedSegmentsInDataSource(dataSourceName);
    return doMarkSegments("markAsUsedAllNonOvershadowedSegments", dataSourceName, markSegments);
  }

  @POST
  @Path("/{dataSourceName}/markUsed")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markAsUsedNonOvershadowedSegments(
      @PathParam("dataSourceName") String dataSourceName,
      MarkDataSourceSegmentsPayload payload
  )
  {
    MarkSegments markSegments = () -> {
      final Interval interval = payload.getInterval();
      if (interval != null) {
        return segmentsMetadata.markAsUsedNonOvershadowedSegmentsInInterval(dataSourceName, interval);
      } else {
        final Set<String> segmentIds = payload.getSegmentIds();
        return segmentsMetadata.markAsUsedNonOvershadowedSegments(dataSourceName, segmentIds);
      }
    };
    return doMarkSegmentsWithPayload("markAsUsedNonOvershadowedSegments", dataSourceName, payload, markSegments);
  }

  @POST
  @Path("/{dataSourceName}/markUnused")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response markSegmentsAsUnused(
      @PathParam("dataSourceName") final String dataSourceName,
      final MarkDataSourceSegmentsPayload payload
  )
  {
    MarkSegments markSegments = () -> {
      final Interval interval = payload.getInterval();
      if (interval != null) {
        return segmentsMetadata.markAsUnusedSegmentsInInterval(dataSourceName, interval);
      } else {
        final Set<String> segmentIds = payload.getSegmentIds();
        return segmentsMetadata.markSegmentsAsUnused(dataSourceName, segmentIds);
      }
    };
    return doMarkSegmentsWithPayload("markSegmentsAsUnused", dataSourceName, payload, markSegments);
  }

  private Response doMarkSegmentsWithPayload(
      String method,
      String dataSourceName,
      MarkDataSourceSegmentsPayload payload,
      MarkSegments markSegments
  )
  {
    if (payload == null || !payload.isValid()) {
      log.warn("Invalid request payload: [%s]", payload);
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity("Invalid request payload, either interval or segmentIds array must be specified")
          .build();
    }

    final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return logAndCreateDataSourceNotFoundResponse(dataSourceName);
    }

    return doMarkSegments(method, dataSourceName, markSegments);
  }

  private static Response logAndCreateDataSourceNotFoundResponse(String dataSourceName)
  {
    log.warn("datasource not found [%s]", dataSourceName);
    return Response.noContent().build();
  }

  private static Response doMarkSegments(String method, String dataSourceName, MarkSegments markSegments)
  {
    try {
      int numChangedSegments = markSegments.markSegments();
      return Response.ok(ImmutableMap.of("numChangedSegments", numChangedSegments)).build();
    }
    catch (UnknownSegmentIdException e) {
      log.warn("Segment ids %s are not found", e.getUnknownSegmentIds());
      return Response
          .status(Response.Status.NOT_FOUND)
          .entity(ImmutableMap.of("message", e.getMessage()))
          .build();
    }
    catch (Exception e) {
      log.error(e, "Error occurred during [%s] call, data source: [%s]", method, dataSourceName);
      return Response
          .serverError()
          .entity(ImmutableMap.of("error", "Exception occurred.", "message", Throwables.getRootCause(e).toString()))
          .build();
    }
  }

  /**
   * When this method is removed, a new method needs to be introduced corresponding to
   * the end point "DELETE /druid/coordinator/v1/datasources/{dataSourceName}" (with no query parameters).
   * Ultimately we want to have no method with kill parameter -
   * DELETE `{dataSourceName}` will be used to mark all segments belonging to a data source as unused, and
   * DELETE `{dataSourceName}/intervals/{interval}` will be used to kill segments within an interval
   */
  @DELETE
  @Deprecated
  @Path("/{dataSourceName}")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response markAsUnusedAllSegmentsOrKillSegmentsInInterval(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("kill") final String kill,
      @QueryParam("interval") final String interval
  )
  {
    if (indexingServiceClient == null) {
      return Response.ok(ImmutableMap.of("error", "no indexing service found")).build();
    }

    boolean killSegments = kill != null && Boolean.valueOf(kill);
    if (killSegments) {
      return killSegmentsInInterval(dataSourceName, interval);
    } else {
      MarkSegments markSegments = () -> segmentsMetadata.markAsUnusedAllSegmentsInDataSource(dataSourceName);
      return doMarkSegments("markAsUnusedAllSegments", dataSourceName, markSegments);
    }
  }

  @DELETE
  @Path("/{dataSourceName}/intervals/{interval}")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response killSegmentsInInterval(
      @PathParam("dataSourceName") final String dataSourceName,
      @PathParam("interval") final String interval
  )
  {
    if (indexingServiceClient == null) {
      return Response.ok(ImmutableMap.of("error", "no indexing service found")).build();
    }
    if (StringUtils.contains(interval, '_')) {
      log.warn("Use interval with '/', not '_': [%s] given", interval);
    }
    final Interval theInterval = Intervals.of(interval.replace('_', '/'));
    try {
      indexingServiceClient.killSegments(dataSourceName, theInterval);
      return Response.ok().build();
    }
    catch (Exception e) {
      return Response
          .serverError()
          .entity(
              ImmutableMap.of(
                  "error", "Exception occurred. Are you sure you have an indexing service?",
                  "message", e.toString()
              )
          )
          .build();
    }
  }

  @GET
  @Path("/{dataSourceName}/intervals")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getIntervalsWithServedSegmentsOrAllServedSegmentsPerIntervals(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    if (simple == null && full == null) {
      final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
      if (dataSource == null) {
        return logAndCreateDataSourceNotFoundResponse(dataSourceName);
      }
      final Comparator<Interval> comparator = Comparators.intervalsByStartThenEnd().reversed();
      Set<Interval> intervals = new TreeSet<>(comparator);
      dataSource.getSegments().forEach(segment -> intervals.add(segment.getInterval()));
      return Response.ok(intervals).build();
    } else {
      return getServedSegmentsInInterval(dataSourceName, full != null, interval -> true);
    }
  }

  @GET
  @Path("/{dataSourceName}/intervals/{interval}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getServedSegmentsInInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    final Interval theInterval = Intervals.of(interval.replace('_', '/'));
    if (simple == null && full == null) {
      final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
      if (dataSource == null) {
        return logAndCreateDataSourceNotFoundResponse(dataSourceName);
      }
      final Set<SegmentId> segmentIds = new TreeSet<>();
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (theInterval.contains(dataSegment.getInterval())) {
          segmentIds.add(dataSegment.getId());
        }
      }
      return Response.ok(segmentIds).build();
    }
    return getServedSegmentsInInterval(dataSourceName, full != null, theInterval::contains);
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

  private Response getServedSegmentsInInterval(
      String dataSourceName,
      boolean full,
      Predicate<Interval> intervalFilter
  )
  {
    final ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);

    if (dataSource == null) {
      return logAndCreateDataSourceNotFoundResponse(dataSourceName);
    }

    final Comparator<Interval> comparator = Comparators.intervalsByStartThenEnd().reversed();

    if (full) {
      final Map<Interval, Map<SegmentId, Object>> retVal = new TreeMap<>(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (intervalFilter.test(dataSegment.getInterval())) {
          Map<SegmentId, Object> segments = retVal.computeIfAbsent(dataSegment.getInterval(), i -> new HashMap<>());

          Pair<DataSegment, Set<String>> segmentAndServers = getServersWhereSegmentIsServed(dataSegment.getId());

          if (segmentAndServers != null) {
            segments.put(
                dataSegment.getId(),
                ImmutableMap.of("metadata", segmentAndServers.lhs, "servers", segmentAndServers.rhs)
            );
          }
        }
      }

      return Response.ok(retVal).build();
    } else {
      final Map<Interval, Map<SimpleProperties, Object>> statsPerInterval = new TreeMap<>(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (intervalFilter.test(dataSegment.getInterval())) {
          Map<SimpleProperties, Object> properties =
              statsPerInterval.computeIfAbsent(dataSegment.getInterval(), i -> new EnumMap<>(SimpleProperties.class));
          properties.merge(SimpleProperties.size, dataSegment.getSize(), (a, b) -> (Long) a + (Long) b);
          properties.merge(SimpleProperties.count, 1, (a, b) -> (Integer) a + (Integer) b);
        }
      }

      return Response.ok(statsPerInterval).build();
    }
  }

  @GET
  @Path("/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getAllServedSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full
  )
  {
    ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return logAndCreateDataSourceNotFoundResponse(dataSourceName);
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
  public Response getServedSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return logAndCreateDataSourceNotFoundResponse(dataSourceName);
    }

    for (SegmentId possibleSegmentId : SegmentId.iteratePossibleParsingsWithDataSource(dataSourceName, segmentId)) {
      Pair<DataSegment, Set<String>> retVal = getServersWhereSegmentIsServed(possibleSegmentId);
      if (retVal != null) {
        return Response.ok(ImmutableMap.of("metadata", retVal.lhs, "servers", retVal.rhs)).build();
      }
    }
    log.warn("Segment id [%s] is unknown", segmentId);
    return Response.noContent().build();
  }

  @DELETE
  @Path("/{dataSourceName}/segments/{segmentId}")
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markSegmentAsUnused(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    boolean segmentStateChanged = segmentsMetadata.markSegmentAsUnused(segmentId);
    return Response.ok(ImmutableMap.of("segmentStateChanged", segmentStateChanged)).build();
  }

  @POST
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markSegmentAsUsed(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    boolean segmentStateChanged = segmentsMetadata.markSegmentAsUsed(segmentId);
    return Response.ok().entity(ImmutableMap.of("segmentStateChanged", segmentStateChanged)).build();
  }

  @GET
  @Path("/{dataSourceName}/tiers")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getTiersWhereSegmentsAreServed(@PathParam("dataSourceName") String dataSourceName)
  {
    Set<String> retVal = new HashSet<>();
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
    List<DruidDataSource> dataSources = serverInventoryView
        .getInventory()
        .stream()
        .map(server -> server.getDataSource(dataSourceName))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (dataSources.isEmpty()) {
      return null;
    }

    // Note: this logic doesn't guarantee that the result is a snapshot that ever existed in the cluster because all
    // DruidDataSource objects (belonging to different servers) are independently, concurrently mutable objects.
    // But this is OK because a "snapshot" hardly even makes sense in a distributed system anyway.
    final SortedMap<SegmentId, DataSegment> segmentMap = new TreeMap<>();
    for (DruidDataSource dataSource : dataSources) {
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
    Set<String> servers = new HashSet<>();
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
    Map<String, Object> tiers = new HashMap<>();
    Map<String, Object> segments = new HashMap<>();
    Map<String, Map<String, Object>> retVal = ImmutableMap.of(
        "tiers", tiers,
        "segments", segments
    );
    Set<SegmentId> totalDistinctSegments = new HashSet<>();
    Map<String, HashSet<Object>> tierDistinctSegments = new HashMap<>();

    long totalSegmentSize = 0;
    long totalReplicatedSize = 0;

    DateTime minTime = DateTimes.MAX;
    DateTime maxTime = DateTimes.MIN;
    String tier;
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      DruidDataSource druidDataSource = druidServer.getDataSource(dataSourceName);
      tier = druidServer.getTier();

      if (druidDataSource == null) {
        continue;
      }

      tierDistinctSegments.computeIfAbsent(tier, t -> new HashSet<>());

      long dataSourceSegmentSize = 0;
      long replicatedSegmentSize = 0;

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
        totalReplicatedSize += dataSegment.getSize();
        replicatedSegmentSize += dataSegment.getSize();
      }

      // tier stats
      Map<String, Object> tierStats = (Map) tiers.get(tier);
      if (tierStats == null) {
        tierStats = new HashMap<>();
        tiers.put(druidServer.getTier(), tierStats);
      }
      tierStats.put("segmentCount", tierDistinctSegments.get(tier).size());

      long segmentSize = MapUtils.getLong(tierStats, "size", 0L);
      tierStats.put("size", segmentSize + dataSourceSegmentSize);

      long replicatedSize = MapUtils.getLong(tierStats, "replicatedSize", 0L);
      tierStats.put("replicatedSize", replicatedSize + replicatedSegmentSize);
    }

    segments.put("count", totalDistinctSegments.size());
    segments.put("size", totalSegmentSize);
    segments.put("replicatedSize", totalReplicatedSize);
    segments.put("minTime", minTime);
    segments.put("maxTime", maxTime);
    return retVal;
  }

  /**
   * Provides serverView for a datasource and Interval which gives details about servers hosting segments for an
   * interval. Used by the realtime tasks to fetch a view of the interval they are interested in.
   */
  @GET
  @Path("/{dataSourceName}/intervals/{interval}/serverview")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getServedSegmentsInInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval,
      @QueryParam("partial") final boolean partial
  )
  {
    TimelineLookup<String, SegmentLoadInfo> timeline = serverInventoryView.getTimeline(
        new TableDataSource(dataSourceName)
    );
    final Interval theInterval = Intervals.of(interval.replace('_', '/'));
    if (timeline == null) {
      log.debug("No timeline found for datasource[%s]", dataSourceName);
      return Response.ok(new ArrayList<ImmutableSegmentLoadInfo>()).build();
    }

    return Response.ok(prepareServedSegmentsInInterval(timeline, theInterval)).build();
  }

  private Iterable<ImmutableSegmentLoadInfo> prepareServedSegmentsInInterval(
      TimelineLookup<String, SegmentLoadInfo> dataSourceServingTimeline,
      Interval interval
  )
  {
    Iterable<TimelineObjectHolder<String, SegmentLoadInfo>> lookup =
        dataSourceServingTimeline.lookupWithIncompletePartitions(interval);
    return FunctionalIterable
        .create(lookup)
        .transformCat(
            (TimelineObjectHolder<String, SegmentLoadInfo> input) ->
                Iterables.transform(
                    input.getObject(),
                    (PartitionChunk<SegmentLoadInfo> chunk) -> chunk.getObject().toImmutableSegmentLoadInfo()
                )
        );
  }

  /**
   * Used by the realtime tasks to learn whether a segment is handed off or not.
   * It returns true when the segment will never be handed off or is already handed off. Otherwise, it returns false.
   */
  @GET
  @Path("/{dataSourceName}/handoffComplete")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response isHandOffComplete(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") final String interval,
      @QueryParam("partitionNumber") final int partitionNumber,
      @QueryParam("version") final String version
  )
  {
    try {
      final List<Rule> rules = metadataRuleManager.getRulesWithDefault(dataSourceName);
      final Interval theInterval = Intervals.of(interval);
      final SegmentDescriptor descriptor = new SegmentDescriptor(theInterval, version, partitionNumber);
      final DateTime now = DateTimes.nowUtc();
      // dropped means a segment will never be handed off, i.e it completed hand off
      // init to true, reset to false only if this segment can be loaded by rules
      boolean dropped = true;
      for (Rule rule : rules) {
        if (rule.appliesTo(theInterval, now)) {
          if (rule instanceof LoadRule) {
            dropped = false;
          }
          break;
        }
      }
      if (dropped) {
        return Response.ok(true).build();
      }

      TimelineLookup<String, SegmentLoadInfo> timeline = serverInventoryView.getTimeline(
          new TableDataSource(dataSourceName)
      );
      if (timeline == null) {
        log.debug("No timeline found for datasource[%s]", dataSourceName);
        return Response.ok(false).build();
      }

      Iterable<ImmutableSegmentLoadInfo> servedSegmentsInInterval =
          prepareServedSegmentsInInterval(timeline, theInterval);
      if (isSegmentLoaded(servedSegmentsInInterval, descriptor)) {
        return Response.ok(true).build();
      }

      return Response.ok(false).build();
    }
    catch (Exception e) {
      log.error(e, "Error while handling hand off check request");
      return Response.serverError().entity(ImmutableMap.of("error", e.toString())).build();
    }
  }

  static boolean isSegmentLoaded(Iterable<ImmutableSegmentLoadInfo> servedSegments, SegmentDescriptor descriptor)
  {
    for (ImmutableSegmentLoadInfo segmentLoadInfo : servedSegments) {
      if (segmentLoadInfo.getSegment().getInterval().contains(descriptor.getInterval())
          && segmentLoadInfo.getSegment().getShardSpec().getPartitionNum() == descriptor.getPartitionNumber()
          && segmentLoadInfo.getSegment().getVersion().compareTo(descriptor.getVersion()) >= 0
          && Iterables.any(
          segmentLoadInfo.getServers(), DruidServerMetadata::segmentReplicatable
      )) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  protected static class MarkDataSourceSegmentsPayload
  {
    private final Interval interval;
    private final Set<String> segmentIds;

    @JsonCreator
    public MarkDataSourceSegmentsPayload(
        @JsonProperty("interval") Interval interval,
        @JsonProperty("segmentIds") Set<String> segmentIds
    )
    {
      this.interval = interval;
      this.segmentIds = segmentIds;
    }

    @JsonProperty
    public Interval getInterval()
    {
      return interval;
    }

    @JsonProperty
    public Set<String> getSegmentIds()
    {
      return segmentIds;
    }

    public boolean isValid()
    {
      return (interval == null ^ segmentIds == null) && (segmentIds == null || !segmentIds.isEmpty());
    }
  }
}
