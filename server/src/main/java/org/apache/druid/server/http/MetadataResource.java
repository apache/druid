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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SortOrder;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.server.JettyUtils;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegmentChange;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 */
@Path("/druid/coordinator/v1/metadata")
public class MetadataResource
{
  private static final Logger log = new Logger(MetadataResource.class);

  private final SegmentsMetadataManager segmentsMetadataManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final AuthorizerMapper authorizerMapper;
  private final DruidCoordinator coordinator;
  private final @Nullable CoordinatorSegmentMetadataCache coordinatorSegmentMetadataCache;

  @Inject
  public MetadataResource(
      SegmentsMetadataManager segmentsMetadataManager,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      AuthorizerMapper authorizerMapper,
      DruidCoordinator coordinator,
      @Nullable CoordinatorSegmentMetadataCache coordinatorSegmentMetadataCache
  )
  {
    this.segmentsMetadataManager = segmentsMetadataManager;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.authorizerMapper = authorizerMapper;
    this.coordinator = coordinator;
    this.coordinatorSegmentMetadataCache = coordinatorSegmentMetadataCache;
  }

  @GET
  @Path("/datasources")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDataSources(
      @QueryParam("full") final String full,
      @Context final UriInfo uriInfo,
      @Context final HttpServletRequest req
  )
  {
    final boolean includeUnused = JettyUtils.getQueryParam(uriInfo, "includeUnused", "includeDisabled") != null;
    Collection<ImmutableDruidDataSource> druidDataSources = null;
    final TreeSet<String> dataSourceNamesPreAuth;
    if (includeUnused) {
      dataSourceNamesPreAuth = new TreeSet<>(metadataStorageCoordinator.retrieveAllDatasourceNames());
    } else {
      druidDataSources = segmentsMetadataManager.getRecentDataSourcesSnapshot().getDataSourcesWithAllUsedSegments();
      dataSourceNamesPreAuth = druidDataSources
          .stream()
          .map(ImmutableDruidDataSource::getName)
          .collect(Collectors.toCollection(TreeSet::new));
    }

    final TreeSet<String> dataSourceNamesPostAuth = new TreeSet<>();
    Function<String, Iterable<ResourceAction>> raGenerator = datasourceName ->
        Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(datasourceName));

    Iterables.addAll(
        dataSourceNamesPostAuth,
        AuthorizationUtils.filterAuthorizedResources(
            req,
            dataSourceNamesPreAuth,
            raGenerator,
            authorizerMapper
        )
    );

    // Cannot do both includeUnused and full, let includeUnused take priority
    // Always use dataSourceNamesPostAuth to determine the set of returned dataSources
    if (full != null && !includeUnused) {
      return Response.ok().entity(
          Collections2.filter(druidDataSources, dataSource -> dataSourceNamesPostAuth.contains(dataSource.getName()))
      ).build();
    } else {
      return Response.ok().entity(dataSourceNamesPostAuth).build();
    }
  }

  @GET
  @Path("/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllUsedSegments(
      @Context final HttpServletRequest req,
      @QueryParam("datasources") final @Nullable Set<String> dataSources,
      @QueryParam("includeOvershadowedStatus") final @Nullable String includeOvershadowedStatus,
      @QueryParam("includeRealtimeSegments") final @Nullable String includeRealtimeSegments
  )
  {
    try {
      // realtime segments can be requested only when includeOverShadowedStatus is set
      if (includeOvershadowedStatus == null && includeRealtimeSegments != null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }

      if (includeOvershadowedStatus != null) {
        // note that realtime segments are returned only when druid.centralizedDatasourceSchema.enabled is set on the Coordinator
        // when the feature is disabled we do not want to increase the payload size polled by the Brokers, since they already have this information
        return getAllUsedSegmentsWithAdditionalDetails(req, dataSources, includeRealtimeSegments);
      }

      Collection<ImmutableDruidDataSource> dataSourcesWithUsedSegments =
          segmentsMetadataManager.getRecentDataSourcesSnapshot().getDataSourcesWithAllUsedSegments();
      if (dataSources != null && !dataSources.isEmpty()) {
        dataSourcesWithUsedSegments = dataSourcesWithUsedSegments
            .stream()
            .filter(dataSourceWithUsedSegments -> dataSources.contains(dataSourceWithUsedSegments.getName()))
            .collect(Collectors.toList());
      }
      final Stream<DataSegment> usedSegments = dataSourcesWithUsedSegments
          .stream()
          .flatMap(t -> t.getSegments().stream());

      final Function<DataSegment, Iterable<ResourceAction>> raGenerator = segment -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getDataSource()));

      final Iterable<DataSegment> authorizedSegments =
          AuthorizationUtils.filterAuthorizedResources(req, usedSegments::iterator, raGenerator, authorizerMapper);

      Response.ResponseBuilder builder = Response.status(Response.Status.OK);
      return builder.entity(authorizedSegments).build();
    }
    catch (DruidException e) {
      return ServletResourceUtils.buildErrorResponseFrom(e);
    }
    catch (Exception e) {
      log.error(e, "Error while fetching used segment information.");
      return Response.serverError().entity(ImmutableMap.of("error", e.toString())).build();
    }
  }

  /**
   * <p>This endpoint is used by MetadataSegmentView in broker to keep an up-to-date list of segments present in the system.
   * This endpoint lists segments present in the system and can also incrementally provide the segments added/dropped
   * since last response.</p>
   * <br>Flow
   * <ol>
   * <li>Client sends first request /druid/coordinator/v1/metadata/changedSegments?counter=-1
   * Server responds with list of segments currently present and a <counter,hash> pair. </li>
   * <li>Client sends subsequent requests /druid/coordinator/v1/metadata/changedSegments?counter=<counter>&hash=<hash>
   * Where <counter,hash> values are used from the last response. Server responds with list of segment updates
   * since given counter.</li>
   * </ol>
   *
   * @param req request
   * @param dataSources requested datasources
   * @param counter counter received in last response.
   * @param hash hash received in last response.
   */
  @GET
  @Path("/changedSegments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getChangedSegments(
      @Context final HttpServletRequest req,
      @QueryParam("datasources") final @Nullable Set<String> dataSources,
      @QueryParam("counter") long counter,
      @QueryParam("hash") long hash
  )
  {
    Set<String> requiredDataSources = (null == dataSources) ? new HashSet<>() : dataSources;

    log.debug(
        "Changed segments requested. counter [%d], hash [%d], dataSources [%s]",
        counter,
        hash,
        requiredDataSources
    );

    DataSourcesSnapshot dataSourcesSnapshot = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments();
    ChangeRequestHistory<List<DataSegmentChange>> changeRequestHistory = segmentsMetadataManager.getChangeRequestHistory();

    ChangeRequestsSnapshot<List<DataSegmentChange>> changeRequestsSnapshot = changeRequestHistory.getRequestsSinceSync(
        new ChangeRequestHistory.Counter(counter, hash));
    List<List<DataSegmentChange>> requests = changeRequestsSnapshot.getRequests();
    List<DataSegmentChange> flatRequests = new ArrayList<>();
    if (null != requests) {
      requests.forEach(flatRequests::addAll);
    }

    List<DataSegmentChange> dataSegmentChanges;
    ChangeRequestHistory.Counter lastCounter = changeRequestsSnapshot.getCounter();
    boolean reset = false;
    String resetCause = "";
    if (changeRequestsSnapshot.isResetCounter()) {
      reset = true;
      dataSegmentChanges =
          dataSourcesSnapshot
              .getDataSourcesWithAllUsedSegments()
              .stream()
              .flatMap(druidDataSource -> druidDataSource.getSegments().stream())
              .filter(segment -> requiredDataSources.isEmpty()
                                 || requiredDataSources.contains(segment.getDataSource()))
              .map(segment -> {
                Long numRows = null;
                if (coordinatorSegmentMetadataCache != null) {
                  AvailableSegmentMetadata availableSegmentMetadata = coordinatorSegmentMetadataCache.getAvailableSegmentMetadata(
                      segment.getDataSource(),
                      segment.getId()
                  );
                  if (null != availableSegmentMetadata) {
                    numRows = availableSegmentMetadata.getNumRows();
                  }
                }
                return new DataSegmentChange(
                    new SegmentStatusInCluster(
                        segment,
                        dataSourcesSnapshot.getOvershadowedSegments().contains(segment),
                        null,
                        numRows,
                        false, //
                        getHandedOffStateForSegment(
                            dataSourcesSnapshot,
                            segment.getDataSource(),
                            segment.getId()
                        )
                    ),
                    DataSegmentChange.SegmentLifecycleChangeType.SEGMENT_ADDED
                );
              })
              .collect(Collectors.toList());
      resetCause = changeRequestsSnapshot.getResetCause();
      log.debug("Returning full snapshot. segment count [%d], counter [%d], hash [%d]",
                dataSegmentChanges.size(), lastCounter.getCounter(), lastCounter.getHash()
      );
    } else {
      dataSegmentChanges = flatRequests;
      dataSegmentChanges = dataSegmentChanges
          .stream()
          .filter(segment -> requiredDataSources.isEmpty()
                             || requiredDataSources.contains(segment.getSegmentStatusInCluster()
                                                                    .getDataSegment()
                                                                    .getDataSource()))
          .collect(Collectors.toList());
      log.debug("Returning delta snapshot. segment count [%d], counter [%d], hash [%d]",
                dataSegmentChanges.size(), lastCounter.getCounter(), lastCounter.getHash()
      );
    }

    final Function<DataSegmentChange, Iterable<ResourceAction>> raGenerator =
        segment -> Collections.singletonList(
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(
                segment.getSegmentStatusInCluster().getDataSegment().getDataSource()
            )
        );

    final Iterable<DataSegmentChange> authorizedSegments = AuthorizationUtils.filterAuthorizedResources(
        req,
        dataSegmentChanges,
        raGenerator,
        authorizerMapper
    );

    ChangeRequestsSnapshot<DataSegmentChange> finalChanges = new ChangeRequestsSnapshot<>(
        reset,
        resetCause,
        lastCounter,
        Lists.newArrayList(authorizedSegments)
    );

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    return builder.entity(finalChanges).build();
  }

  private Response getAllUsedSegmentsWithAdditionalDetails(
      HttpServletRequest req,
      @Nullable Set<String> dataSources,
      String includeRealtimeSegments
  )
  {
    DataSourcesSnapshot dataSourcesSnapshot = segmentsMetadataManager.getRecentDataSourcesSnapshot();
    Collection<ImmutableDruidDataSource> dataSourcesWithUsedSegments =
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments();
    if (dataSources != null && !dataSources.isEmpty()) {
      dataSourcesWithUsedSegments = dataSourcesWithUsedSegments
          .stream()
          .filter(dataSourceWithUsedSegments -> dataSources.contains(dataSourceWithUsedSegments.getName()))
          .collect(Collectors.toList());
    }
    final Set<DataSegment> overshadowedSegments = dataSourcesSnapshot.getOvershadowedSegments();
    final Set<SegmentId> segmentAlreadySeen = new HashSet<>();
    final Stream<SegmentStatusInCluster> segmentStatus = dataSourcesWithUsedSegments
        .stream()
        .flatMap(t -> t.getSegments().stream())
        .map(segment -> {
          // The replication factor for unloaded segments is 0 as they will be unloaded soon
          boolean isOvershadowed = overshadowedSegments.contains(segment);
          Integer replicationFactor = isOvershadowed ? (Integer) 0
                                                     : coordinator.getReplicationFactor(segment.getId());

          Long numRows = null;
          if (coordinatorSegmentMetadataCache != null) {
            AvailableSegmentMetadata availableSegmentMetadata = coordinatorSegmentMetadataCache.getAvailableSegmentMetadata(
                segment.getDataSource(),
                segment.getId()
            );
            if (null != availableSegmentMetadata) {
              numRows = availableSegmentMetadata.getNumRows();
            }
          }
          segmentAlreadySeen.add(segment.getId());
          boolean handedOffState = getHandedOffStateForSegment(dataSourcesSnapshot, segment.getDataSource(), segment.getId());
          return new SegmentStatusInCluster(
              segment,
              isOvershadowed,
              replicationFactor,
              numRows,
              // published segment can't be realtime
              false,
              handedOffState
          );
        });

    Stream<SegmentStatusInCluster> finalSegments = segmentStatus;

    // conditionally add realtime segments information
    if (includeRealtimeSegments != null && coordinatorSegmentMetadataCache != null) {
      final Stream<SegmentStatusInCluster> realtimeSegmentStatus = coordinatorSegmentMetadataCache
          .getSegmentMetadataSnapshot()
          .values()
          .stream()
          .filter(availableSegmentMetadata ->
                      !segmentAlreadySeen.contains(availableSegmentMetadata.getSegment().getId()))
          .map(availableSegmentMetadata ->
                   new SegmentStatusInCluster(
                       availableSegmentMetadata.getSegment(),
                       false,
                       null, // replication factor is null for unpublished segments
                       availableSegmentMetadata.getNumRows(),
                       availableSegmentMetadata.isRealtime() != 0,
                       false // realtime segments are not handed off yet
                   ));

      finalSegments = Stream.concat(segmentStatus, realtimeSegmentStatus);
    }
    final Function<SegmentStatusInCluster, Iterable<ResourceAction>> raGenerator = segment -> Collections
        .singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getDataSegment().getDataSource()));

    final Iterable<SegmentStatusInCluster> authorizedSegments = AuthorizationUtils.filterAuthorizedResources(
        req,
        finalSegments::iterator,
        raGenerator,
        authorizerMapper
    );

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    return builder.entity(authorizedSegments).build();
  }

  private boolean getHandedOffStateForSegment(
      DataSourcesSnapshot dataSourcesSnapshot,
      String dataSource, SegmentId segmentId
  )
  {
    return dataSourcesSnapshot.getLoadedSegmentsPerDataSource()
                       .getOrDefault(dataSource, new HashSet<>())
                       .contains(segmentId);
  }

  /**
   * The difference of this method from {@link #getUsedSegmentsInDataSource} is that the latter returns only a list of
   * segments, while this method also includes the properties of data source, such as the time when it was created.
   */
  @GET
  @Path("/datasources/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDataSourceWithUsedSegments(@PathParam("dataSourceName") final String dataSourceName)
  {
    ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).entity(dataSource).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getUsedSegmentsInDataSource(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") @Nullable String full
  )
  {
    ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(dataSource.getSegments()).build();
    }

    return builder.entity(Collections2.transform(dataSource.getSegments(), DataSegment::getId)).build();
  }

  /**
   * This is a {@link POST} method to pass the list of intervals in the body,
   * see https://github.com/apache/druid/pull/2109#issuecomment-182191258
   */
  @POST
  @Path("/datasources/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getUsedSegmentsInDataSourceForIntervals(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") @Nullable String full,
      List<Interval> intervals
  )
  {
    Collection<DataSegment> segments = metadataStorageCoordinator
        .retrieveUsedSegmentsForIntervals(dataSourceName, intervals, Segments.INCLUDING_OVERSHADOWED);

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(segments).build();
    }

    return builder.entity(Collections2.transform(segments, DataSegment::getId)).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/unusedSegments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getUnusedSegmentsInDataSource(
      @Context final HttpServletRequest req,
      @PathParam("dataSourceName") final String dataSource,
      @QueryParam("interval") @Nullable String interval,
      @QueryParam("limit") @Nullable Integer limit,
      @QueryParam("lastSegmentId") @Nullable final String lastSegmentId,
      @QueryParam("sortOrder") @Nullable final String sortOrder
  )
  {
    try {
      if (dataSource == null || dataSource.isEmpty()) {
        throw InvalidInput.exception("dataSourceName must be non-empty.");
      }

      if (limit != null && limit < 0) {
        throw InvalidInput.exception("Invalid limit[%s] specified. Limit must be > 0.", limit);
      }

      if (lastSegmentId != null && SegmentId.tryParse(dataSource, lastSegmentId) == null) {
        throw InvalidInput.exception("Invalid lastSegmentId[%s] specified.", lastSegmentId);
      }

      final SortOrder theSortOrder = sortOrder == null ? null : SortOrder.fromValue(sortOrder);

      final Interval theInterval = interval != null ? Intervals.of(interval.replace('_', '/')) : null;
      final Iterable<DataSegmentPlus> unusedSegments = metadataStorageCoordinator.iterateAllUnusedSegmentsForDatasource(
          dataSource,
          theInterval,
          limit,
          lastSegmentId,
          theSortOrder
      );

      final List<DataSegmentPlus> retVal = new ArrayList<>();
      unusedSegments.iterator().forEachRemaining(retVal::add);
      return Response.status(Response.Status.OK).entity(retVal).build();
    }
    catch (DruidException e) {
      return ServletResourceUtils.buildErrorResponseFrom(e);
    }
    catch (Exception e) {
      return Response
          .serverError()
          .entity(ImmutableMap.of("error", "Exception occurred.", "message", Throwables.getRootCause(e).toString()))
          .build();
    }
  }

  @GET
  @Path("/datasources/{dataSourceName}/segments/{segmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String serializedSegmentId,
      @QueryParam("includeUnused") @Nullable Boolean includeUnused
  )
  {
    final SegmentId segmentId = SegmentId.tryParse(dataSourceName, serializedSegmentId);
    if (segmentId == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          StringUtils.format(
              "Could not parse Segment ID[%s] for DataSource[%s]",
              StringUtils.escapeHtml(serializedSegmentId),
              StringUtils.escapeHtml(dataSourceName)
          )
      ).build();
    }

    ImmutableDruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      DataSegment segment = dataSource.getSegment(segmentId);
      if (segment != null) {
        return Response.status(Response.Status.OK).entity(segment).build();
      }
    }
    // fallback to db
    final DataSegment segment;
    if (Boolean.TRUE.equals(includeUnused)) {
      segment = metadataStorageCoordinator.retrieveSegmentForId(segmentId);
    } else {
      segment = metadataStorageCoordinator.retrieveUsedSegmentForId(segmentId);
    }

    if (segment != null) {
      return Response.status(Response.Status.OK).entity(segment).build();
    }
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  /**
   * API to fetch {@link DataSourceInformation} for the specified datasources.
   *
   * @param dataSources list of dataSources to be queried
   * @return information including schema details for the specified datasources
   */
  @POST
  @Path("/dataSourceInformation")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDataSourceInformation(
      @Context final HttpServletRequest req,
      final List<String> dataSources
  )
  {
    // if {@code coordinatorSegmentMetadataCache} is null, implies the feature is disabled. Return NOT_FOUND.
    if (coordinatorSegmentMetadataCache == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    Map<String, DataSourceInformation> dataSourceSchemaMap = coordinatorSegmentMetadataCache.getDataSourceInformationMap();

    List<DataSourceInformation> results = new ArrayList<>();

    for (Map.Entry<String, DataSourceInformation> entry : dataSourceSchemaMap.entrySet()) {
      if (dataSources.contains(entry.getKey())) {
        results.add(entry.getValue());
      }
    }

    final Function<DataSourceInformation, Iterable<ResourceAction>> raGenerator = dataSourceInformation -> Collections
        .singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(dataSourceInformation.getDataSource()));

    final Iterable<DataSourceInformation> authorizedDataSourceInformation = AuthorizationUtils.filterAuthorizedResources(
        req,
        results,
        raGenerator,
        authorizerMapper
    );
    return Response.status(Response.Status.OK).entity(authorizedDataSourceInformation).build();
  }

  /**
   * @return all bootstrap segments determined by the coordinator.
   */
  @POST
  @Path("/bootstrapSegments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getBootstrapSegments()
  {
    final Set<DataSegment> broadcastSegments = coordinator.getBroadcastSegments();
    if (broadcastSegments == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                     .entity("Bootstrap segments are not initialized yet."
                         + " Please ensure that the Coordinator duties are running and try again.")
                     .build();
    }
    return Response.status(Response.Status.OK).entity(broadcastSegments).build();
  }

  private ImmutableDruidDataSource getDataSource(String dataSource)
  {
    return segmentsMetadataManager.getRecentDataSourcesSnapshot().getDataSource(dataSource);
  }
}
