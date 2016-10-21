/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.DruidDataSource;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.java.util.common.Pair;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.http.security.DatasourceResourceFilter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;
import io.druid.timeline.DataSegment;
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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@Path("/druid/coordinator/v1/metadata")
public class MetadataResource
{
  private final MetadataSegmentManager metadataSegmentManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final AuthConfig authConfig;

  @Inject
  public MetadataResource(
      MetadataSegmentManager metadataSegmentManager,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      AuthConfig authConfig
  )
  {
    this.metadataSegmentManager = metadataSegmentManager;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.authConfig = authConfig;
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
    final Set<String> dataSourceNamesPreAuth;
    if (includeDisabled != null) {
      dataSourceNamesPreAuth = Sets.newTreeSet(metadataSegmentManager.getAllDatasourceNames());
    } else {
      dataSourceNamesPreAuth = Sets.newTreeSet(
          Iterables.transform(
              metadataSegmentManager.getInventory(),
              new Function<DruidDataSource, String>()
              {
                @Override
                public String apply(DruidDataSource input)
                {
                  return input.getName();
                }
              }
          )
      );
    }

    final Set<String> dataSourceNamesPostAuth;

    if (authConfig.isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      final Map<Pair<Resource, Action>, Access> resourceAccessMap = new HashMap<>();
      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      dataSourceNamesPostAuth = ImmutableSet.copyOf(
          Sets.filter(
              dataSourceNamesPreAuth,
              new Predicate<String>()
              {
                @Override
                public boolean apply(String input)
                {
                  Resource resource = new Resource(input, ResourceType.DATASOURCE);
                  Action action = Action.READ;
                  Pair<Resource, Action> key = new Pair<>(resource, action);
                  if (resourceAccessMap.containsKey(key)) {
                    return resourceAccessMap.get(key).isAllowed();
                  } else {
                    Access access = authorizationInfo.isAuthorized(key.lhs, key.rhs);
                    resourceAccessMap.put(key, access);
                    return access.isAllowed();
                  }
                }
              }
          )
      );
    } else {
      dataSourceNamesPostAuth = dataSourceNamesPreAuth;
    }

    // Cannot do both includeDisabled and full, let includeDisabled take priority
    // Always use dataSourceNamesPostAuth to determine the set of returned dataSources
    if (full != null && includeDisabled == null) {
      return Response.ok().entity(
          Collections2.filter(
              metadataSegmentManager.getInventory(),
              new Predicate<DruidDataSource>()
              {
                @Override
                public boolean apply(DruidDataSource input)
                {
                  return dataSourceNamesPostAuth.contains(input.getName());
                }
              }
          )
      ).build();
    } else {
      return Response.ok().entity(dataSourceNamesPostAuth).build();
    }
  }

  @GET
  @Path("/datasources/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDatabaseSegmentDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    DruidDataSource dataSource = metadataSegmentManager.getInventoryValue(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).entity(dataSource).build();
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
    DruidDataSource dataSource = metadataSegmentManager.getInventoryValue(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(dataSource.getSegments()).build();
    }

    return builder.entity(
        Iterables.transform(
            dataSource.getSegments(),
            new Function<DataSegment, String>()
            {
              @Override
              public String apply(DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
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
    List<DataSegment> segments;
    try {
      segments = metadataStorageCoordinator.getUsedSegmentsForIntervals(dataSourceName, intervals);
    }
    catch (IOException ex) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(ex.getMessage()).build();
    }

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(segments).build();
    }

    return builder.entity(
        Iterables.transform(
            segments,
            new Function<DataSegment, String>()
            {
              @Override
              public String apply(DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
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
    DruidDataSource dataSource = metadataSegmentManager.getInventoryValue(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    for (DataSegment segment : dataSource.getSegments()) {
      if (segment.getIdentifier().equalsIgnoreCase(segmentId)) {
        return Response.status(Response.Status.OK).entity(segment).build();
      }
    }
    return Response.status(Response.Status.NOT_FOUND).build();
  }
}
