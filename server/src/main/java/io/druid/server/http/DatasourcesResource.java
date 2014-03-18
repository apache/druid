/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.http;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.db.DatabaseSegmentManager;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 */
@Path("/druid/coordinator/v1/datasources")
public class DatasourcesResource
{
  private static Map<String, Object> makeSimpleDatasource(DruidDataSource input)
  {
    return new ImmutableMap.Builder<String, Object>()
        .put("name", input.getName())
        .put("properties", input.getProperties())
        .build();
  }

  private final InventoryView serverInventoryView;
  private final DatabaseSegmentManager databaseSegmentManager;
  private final IndexingServiceClient indexingServiceClient;

  @Inject
  public DatasourcesResource(
      InventoryView serverInventoryView,
      DatabaseSegmentManager databaseSegmentManager,
      @Nullable IndexingServiceClient indexingServiceClient
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.databaseSegmentManager = databaseSegmentManager;
    this.indexingServiceClient = indexingServiceClient;
  }

  @GET
  @Produces("application/json")
  public Response getQueryableDataSources(
      @QueryParam("full") String full,
      @QueryParam("simple") String simple
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(getDataSources()).build();
    } else if (simple != null) {
      return builder.entity(
          Lists.newArrayList(
              Iterables.transform(
                  getDataSources(),
                  new Function<DruidDataSource, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(DruidDataSource dataSource)
                    {
                      return makeSimpleDatasource(dataSource);
                    }
                  }
              )
          )
      ).build();
    }

    return builder.entity(
        Lists.newArrayList(
            Iterables.transform(
                getDataSources(),
                new Function<DruidDataSource, String>()
                {
                  @Override
                  public String apply(DruidDataSource dataSource)
                  {
                    return dataSource.getName();
                  }
                }
            )
        )
    ).build();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces("application/json")
  public Response getTheDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName.toLowerCase());
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.ok(dataSource).build();
  }

  @POST
  @Path("/{dataSourceName}")
  @Consumes("application/json")
  public Response enableDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    if (!databaseSegmentManager.enableDatasource(dataSourceName)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).build();
  }

  @DELETE
  @Path("/{dataSourceName}")
  @Produces("application/json")
  public Response deleteDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("kill") final String kill,
      @QueryParam("interval") final String interval
  )
  {
    if (indexingServiceClient == null) {
      return Response.ok().entity(ImmutableMap.of("error", "no indexing service found")).build();
    }
    if (kill != null && Boolean.valueOf(kill)) {
      try {
        indexingServiceClient.killSegments(dataSourceName, new Interval(interval));
      }
      catch (Exception e) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(
                           ImmutableMap.of(
                               "error",
                               "Exception occurred. Are you sure you have an indexing service?"
                           )
                       )
                       .build();
      }
    } else {
      if (!databaseSegmentManager.removeDatasource(dataSourceName)) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }

    return Response.status(Response.Status.OK).build();
  }

  @GET
  @Path("/{dataSourceName}/segments")
  @Produces("application/json")
  public Response getSegmentDataSourceSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName.toLowerCase());
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
              public Object apply(@Nullable DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Produces("application/json")
  public Response getSegmentDataSourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName.toLowerCase());
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

  @DELETE
  @Path("/{dataSourceName}/segments/{segmentId}")
  public Response deleteDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (!databaseSegmentManager.removeSegment(dataSourceName, segmentId)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).build();
  }

  @POST
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Consumes("application/json")
  public Response enableDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (!databaseSegmentManager.enableSegment(segmentId)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).build();
  }

  private DruidDataSource getDataSource(final String dataSourceName)
  {
    Iterable<DruidDataSource> dataSources =
        Iterables.concat(
            Iterables.transform(
                serverInventoryView.getInventory(),
                new Function<DruidServer, DruidDataSource>()
                {
                  @Override
                  public DruidDataSource apply(DruidServer input)
                  {
                    return input.getDataSource(dataSourceName);
                  }
                }
            )
        );

    List<DruidDataSource> validDataSources = Lists.newArrayList();
    for (DruidDataSource dataSource : dataSources) {
      if (dataSource != null) {
        validDataSources.add(dataSource);
      }
    }
    if (validDataSources.isEmpty()) {
      return null;
    }

    Map<String, DataSegment> segmentMap = Maps.newHashMap();
    for (DruidDataSource dataSource : validDataSources) {
      if (dataSource != null) {
        Iterable<DataSegment> segments = dataSource.getSegments();
        for (DataSegment segment : segments) {
          segmentMap.put(segment.getIdentifier(), segment);
        }
      }
    }

    return new DruidDataSource(
        dataSourceName,
        ImmutableMap.<String, String>of()
    ).addSegments(segmentMap);
  }

  private Set<DruidDataSource> getDataSources()
  {
    TreeSet<DruidDataSource> dataSources = Sets.newTreeSet(
        new Comparator<DruidDataSource>()
        {
          @Override
          public int compare(DruidDataSource druidDataSource, DruidDataSource druidDataSource1)
          {
            return druidDataSource.getName().compareTo(druidDataSource1.getName());
          }
        }
    );
    dataSources.addAll(
        Lists.newArrayList(
            Iterables.concat(
                Iterables.transform(
                    serverInventoryView.getInventory(),
                    new Function<DruidServer, Iterable<DruidDataSource>>()
                    {
                      @Override
                      public Iterable<DruidDataSource> apply(DruidServer input)
                      {
                        return input.getDataSources();
                      }
                    }
                )
            )
        )
    );
    return dataSources;
  }
}
