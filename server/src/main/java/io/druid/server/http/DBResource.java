/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.http;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.client.DruidDataSource;
import io.druid.db.DatabaseSegmentManager;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

/**
 */
@Path("/druid/coordinator/v1/db")
public class DBResource
{
  private final DatabaseSegmentManager databaseSegmentManager;

  @Inject
  public DBResource(
      DatabaseSegmentManager databaseSegmentManager
  )
  {
    this.databaseSegmentManager = databaseSegmentManager;
  }

  @GET
  @Path("/datasources")
  @Produces("application/json")
  public Response getDatabaseDataSources(
      @QueryParam("full") String full,
      @QueryParam("includeDisabled") String includeDisabled
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (includeDisabled != null) {
      return builder.entity(databaseSegmentManager.getAllDatasourceNames()).build();
    }
    if (full != null) {
      return builder.entity(databaseSegmentManager.getInventory()).build();
    }

    List<String> dataSourceNames = Lists.newArrayList(
        Iterables.transform(
            databaseSegmentManager.getInventory(),
            new Function<DruidDataSource, String>()
            {
              @Override
              public String apply(DruidDataSource dataSource)
              {
                return dataSource.getName();
              }
            }
        )
    );

    Collections.sort(dataSourceNames);

    return builder.entity(dataSourceNames).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}")
  @Produces("application/json")
  public Response getDatabaseSegmentDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    DruidDataSource dataSource = databaseSegmentManager.getInventoryValue(dataSourceName);
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).entity(dataSource).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/segments")
  @Produces("application/json")
  public Response getDatabaseSegmentDataSourceSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full
  )
  {
    DruidDataSource dataSource = databaseSegmentManager.getInventoryValue(dataSourceName);
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
            new Function<DataSegment, Object>()
            {
              @Override
              public Object apply(DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}/segments/{segmentId}")
  @Produces("application/json")
  public Response getDatabaseSegmentDataSourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    DruidDataSource dataSource = databaseSegmentManager.getInventoryValue(dataSourceName);
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
