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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.commons.collections.IteratorUtils;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.JettyUtils;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.apache.druid.timeline.SegmentWithOvershadowedStatusChangeRequest;
import org.apache.druid.utils.CollectionUtils;
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
import java.util.HashMap;
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
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public MetadataResource(
      SegmentsMetadataManager segmentsMetadataManager,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      AuthorizerMapper authorizerMapper,
      @Json ObjectMapper jsonMapper
  )
  {
    this.segmentsMetadataManager = segmentsMetadataManager;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.authorizerMapper = authorizerMapper;
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
      dataSourceNamesPreAuth = new TreeSet<>(segmentsMetadataManager.retrieveAllDataSourceNames());
    } else {
      druidDataSources = segmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments();
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
      @QueryParam("countersAndHash") final @Nullable Map<String, List<Long>> dataSourcesCounters
      )
  {
    if (includeOvershadowedStatus != null) {

      return getAllUsedSegmentsWithOvershadowedStatus(req, dataSources, CollectionUtils.mapValues(dataSourcesCounters, v -> new ChangeRequestHistory.Counter(v.get(0), v.get(1))));
    }

    Collection<ImmutableDruidDataSource> dataSourcesWithUsedSegments =
        segmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments();
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

  private Response getAllUsedSegmentsWithOvershadowedStatus(
      HttpServletRequest req,
      @Nullable Set<String> dataSources,
      @Nullable Map<String, ChangeRequestHistory.Counter> dataSourcesCounters
  )
  {
    DataSourcesSnapshot dataSourcesSnapshot = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments();
    Map<String, ImmutableDruidDataSource> dataSourcesWithUsedSegments =
        dataSourcesSnapshot.getDataSourcesMap();

    Map<String, ChangeRequestsSnapshot<DataSegmentChangeRequest>> dataSourceChanges = new HashMap<>();
    if (dataSourcesCounters != null && !dataSourcesCounters.isEmpty()) {
      dataSourceChanges = dataSourcesSnapshot.getChangesSince(dataSourcesCounters);
    }

    final Set<String> existingDataSources = dataSourceChanges.keySet();

    // is this fine?
    final Function<String, Iterable<ResourceAction>> raGenerator = dataSource -> Collections
        .singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(dataSource));

    final Iterable<String> authorizedDataSources = AuthorizationUtils.filterAuthorizedResources(
        req,
        existingDataSources,
        raGenerator,
        authorizerMapper
    );
    Set<String> authorisedDataSets = new HashSet<>(IteratorUtils.toList(authorizedDataSources.iterator()));

    dataSourceChanges.entrySet().removeIf(v -> !authorisedDataSets.contains(v.getKey()));

    final Set<DataSegment> overshadowedSegments = dataSourcesSnapshot.getOvershadowedSegments();
    List<ChangeRequestsSnapshot<SegmentWithOvershadowedStatusChangeRequest>> changeRequestsSnapshots = new ArrayList<>();

    for (Map.Entry<String, ChangeRequestsSnapshot<DataSegmentChangeRequest>> entry : dataSourceChanges.entrySet()) {
      if (entry.getValue().isResetCounter()) {
        List<SegmentWithOvershadowedStatusChangeRequest> list = dataSourcesWithUsedSegments
            .get(entry.getKey()).getSegments()
            .stream()
            .map(segment ->
                     new SegmentWithOvershadowedStatusChangeRequest(
                         new SegmentWithOvershadowedStatus(
                             segment, overshadowedSegments.contains(segment),
                             dataSourcesSnapshot.getHandedOffStatePerDataSource()
                               .getOrDefault(segment.getDataSource(), new HashMap<>())
                               .getOrDefault(segment.getId(), false)),
                         true)).collect(Collectors.toList());
        changeRequestsSnapshots.add(ChangeRequestsSnapshot.success(ChangeRequestHistory.Counter.ZERO, list));
      } else {
        List<SegmentWithOvershadowedStatusChangeRequest> list = entry
            .getValue().getRequests()
            .stream()
            .map(changeRequest -> {
              boolean load;
              DataSegment segment;
              if (changeRequest instanceof SegmentChangeRequestLoad) {
                load = true;
                segment = ((SegmentChangeRequestLoad) changeRequest).getSegment();
              } else {
                load = false;
                segment = ((SegmentChangeRequestDrop) changeRequest).getSegment();
              }

              return new SegmentWithOvershadowedStatusChangeRequest(
                  new SegmentWithOvershadowedStatus(
                      segment, overshadowedSegments.contains(segment),
                      dataSourcesSnapshot.getHandedOffStatePerDataSource()
                                         .getOrDefault(segment.getDataSource(), new HashMap<>())
                                         .getOrDefault(segment.getId(), false)),
                  load);
            }).collect(Collectors.toList());

        changeRequestsSnapshots.add(ChangeRequestsSnapshot.success(entry.getValue().getCounter(), list));
      }
    }

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    return builder.entity(changeRequestsSnapshots).build();
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
    ImmutableDruidDataSource dataSource =
        segmentsMetadataManager.getImmutableDataSourceWithUsedSegments(dataSourceName);
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
    ImmutableDruidDataSource dataSource =
        segmentsMetadataManager.getImmutableDataSourceWithUsedSegments(dataSourceName);
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
  @Path("/datasources/{dataSourceName}/segments/{segmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response isSegmentUsed(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    ImmutableDruidDataSource dataSource = segmentsMetadataManager.getImmutableDataSourceWithUsedSegments(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    for (SegmentId possibleSegmentId : SegmentId.iteratePossibleParsingsWithDataSource(dataSourceName, segmentId)) {
      DataSegment segment = dataSource.getSegment(possibleSegmentId);
      if (segment != null) {
        return Response.status(Response.Status.OK).entity(segment).build();
      }
    }
    return Response.status(Response.Status.NOT_FOUND).build();
  }
}
