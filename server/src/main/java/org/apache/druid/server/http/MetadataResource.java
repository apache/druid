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
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataSegmentManager;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.joda.time.Interval;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 */
@Path("/druid/coordinator/v1/metadata")
public class MetadataResource
{
  private final MetadataSegmentManager metadataSegmentManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public MetadataResource(
      MetadataSegmentManager metadataSegmentManager,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper,
      @Json ObjectMapper jsonMapper
  )
  {
    this.metadataSegmentManager = metadataSegmentManager;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.authorizerMapper = authorizerMapper;
  }

  @GET
  @Path("/datasources")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatabaseDataSources(
      @QueryParam("full") final String full,
      @QueryParam("includeDisabled") final String includeDisabled,
      @Context final HttpServletRequest req
  )
  {
    // If we haven't polled the metadata store yet, use an empty list of datasources.
    final Collection<ImmutableDruidDataSource> druidDataSources = Optional.ofNullable(metadataSegmentManager.getDataSources())
                                                                          .orElse(Collections.emptyList());

    final Set<String> dataSourceNamesPreAuth;
    if (includeDisabled != null) {
      dataSourceNamesPreAuth = new TreeSet<>(metadataSegmentManager.getAllDataSourceNames());
    } else {
      dataSourceNamesPreAuth = Sets.newTreeSet(
          Iterables.transform(druidDataSources, ImmutableDruidDataSource::getName)
      );
    }

    final Set<String> dataSourceNamesPostAuth = new TreeSet<>();
    Function<String, Iterable<ResourceAction>> raGenerator = datasourceName -> {
      return Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(datasourceName));
    };

    Iterables.addAll(
        dataSourceNamesPostAuth,
        AuthorizationUtils.filterAuthorizedResources(
            req,
            dataSourceNamesPreAuth,
            raGenerator,
            authorizerMapper
        )
    );

    // Cannot do both includeDisabled and full, let includeDisabled take priority
    // Always use dataSourceNamesPostAuth to determine the set of returned dataSources
    if (full != null && includeDisabled == null) {
      return Response.ok().entity(
          Collections2.filter(druidDataSources, dataSource -> dataSourceNamesPostAuth.contains(dataSource.getName()))
      ).build();
    } else {
      return Response.ok().entity(dataSourceNamesPostAuth).build();
    }
  }

  @GET
  @Path("/datasources/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDatabaseSegmentDataSource(@PathParam("dataSourceName") final String dataSourceName)
  {
    ImmutableDruidDataSource dataSource = metadataSegmentManager.getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).entity(dataSource).build();
  }

  @GET
  @Path("/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatabaseSegments(
      @Context final HttpServletRequest req,
      @QueryParam("datasources") final Set<String> datasources,
      @QueryParam("includeOvershadowedStatus") final String includeOvershadowedStatus
  )
  {
    // If we haven't polled the metadata store yet, use an empty list of datasources.
    Collection<ImmutableDruidDataSource> druidDataSources = Optional.ofNullable(metadataSegmentManager.getDataSources())
                                                                    .orElse(Collections.emptyList());
    Stream<ImmutableDruidDataSource> dataSourceStream = druidDataSources.stream();
    if (datasources != null && !datasources.isEmpty()) {
      dataSourceStream = dataSourceStream.filter(src -> datasources.contains(src.getName()));
    }
    final Stream<DataSegment> metadataSegments = dataSourceStream.flatMap(t -> t.getSegments().stream());

    if (includeOvershadowedStatus != null) {
      final Iterable<SegmentWithOvershadowedStatus> authorizedSegments = findAuthorizedSegmentWithOvershadowedStatus(
          req,
          druidDataSources,
          metadataSegments
      );
      Response.ResponseBuilder builder = Response.status(Response.Status.OK);
      return builder.entity(authorizedSegments).build();
    } else {

      final Function<DataSegment, Iterable<ResourceAction>> raGenerator = segment -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getDataSource()));

      final Iterable<DataSegment> authorizedSegments = AuthorizationUtils.filterAuthorizedResources(
          req,
          metadataSegments::iterator,
          raGenerator,
          authorizerMapper
      );

      Response.ResponseBuilder builder = Response.status(Response.Status.OK);
      return builder.entity(authorizedSegments).build();
    }
  }

  private Iterable<SegmentWithOvershadowedStatus> findAuthorizedSegmentWithOvershadowedStatus(
      HttpServletRequest req,
      Collection<ImmutableDruidDataSource> druidDataSources,
      Stream<DataSegment> metadataSegments
  )
  {
    // It's fine to add all overshadowed segments to a single collection because only
    // a small fraction of the segments in the cluster are expected to be overshadowed,
    // so building this collection shouldn't generate a lot of garbage.
    final Set<DataSegment> overshadowedSegments = new HashSet<>();
    for (ImmutableDruidDataSource dataSource : druidDataSources) {
      overshadowedSegments.addAll(ImmutableDruidDataSource.determineOvershadowedSegments(dataSource.getSegments()));
    }

    final Stream<SegmentWithOvershadowedStatus> segmentsWithOvershadowedStatus = metadataSegments
        .map(segment -> new SegmentWithOvershadowedStatus(
            segment,
            overshadowedSegments.contains(segment)
        ));

    final Function<SegmentWithOvershadowedStatus, Iterable<ResourceAction>> raGenerator = segment -> Collections
        .singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getDataSegment().getDataSource()));

    final Iterable<SegmentWithOvershadowedStatus> authorizedSegments = AuthorizationUtils.filterAuthorizedResources(
        req,
        segmentsWithOvershadowedStatus::iterator,
        raGenerator,
        authorizerMapper
    );
    return authorizedSegments;
  }

  @GET
  @Path("/datasources/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDatabaseSegmentDataSourceSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full
  )
  {
    ImmutableDruidDataSource dataSource = metadataSegmentManager.getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(dataSource.getSegments()).build();
    }

    return builder.entity(Collections2.transform(dataSource.getSegments(), DataSegment::getId)).build();
  }

  @POST
  @Path("/datasources/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDatabaseSegmentDataSourceSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full,
      List<Interval> intervals
  )
  {
    List<DataSegment> segments = metadataStorageCoordinator.getUsedSegmentsForIntervals(dataSourceName, intervals);

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
  public Response getDatabaseSegmentDataSourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    ImmutableDruidDataSource dataSource = metadataSegmentManager.getDataSource(dataSourceName);
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
