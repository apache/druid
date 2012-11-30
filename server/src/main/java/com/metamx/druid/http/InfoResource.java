/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.http;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.ServerInventoryManager;
import com.metamx.druid.coordination.DruidClusterInfo;
import com.metamx.druid.db.DatabaseRuleCoordinator;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.master.rules.Rule;

import javax.annotation.Nullable;
import javax.inject.Inject;
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
@Path("/info")
public class InfoResource
{
  private final ServerInventoryManager serverInventoryManager;
  private final DatabaseSegmentManager databaseSegmentManager;
  private final DatabaseRuleCoordinator databaseRuleCoordinator;
  private final DruidClusterInfo druidClusterInfo;

  @Inject
  public InfoResource(
      ServerInventoryManager serverInventoryManager,
      DatabaseSegmentManager databaseSegmentManager,
      DatabaseRuleCoordinator databaseRuleCoordinator,
      DruidClusterInfo druidClusterInfo
  )
  {
    this.serverInventoryManager = serverInventoryManager;
    this.databaseSegmentManager = databaseSegmentManager;
    this.databaseRuleCoordinator = databaseRuleCoordinator;
    this.druidClusterInfo = druidClusterInfo;
  }

  @GET
  @Path("/master")
  @Produces("application/json")
  public Response getMaster()
  {
    return Response.status(Response.Status.OK)
                   .entity(druidClusterInfo.lookupCurrentLeader())
                   .build();
  }

  @GET
  @Path("/cluster")
  @Produces("application/json")
  public Response getClusterInfo()
  {
    return Response.status(Response.Status.OK)
                   .entity(serverInventoryManager.getInventory())
                   .build();
  }

  @GET
  @Path("/servers")
  @Produces("application/json")
  public Response getClusterServers(
      @QueryParam("full") String full
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(serverInventoryManager.getInventory()).build();
    }

    return builder.entity(
        Iterables.transform(
            serverInventoryManager.getInventory(),
            new Function<DruidServer, String>()
            {
              @Override
              public String apply(@Nullable DruidServer druidServer)
              {
                return druidServer.getName();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/servers/{serverName}")
  @Produces("application/json")
  public Response getServer(
      @PathParam("serverName") String serverName
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    DruidServer server = serverInventoryManager.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return builder.status(Response.Status.OK)
                  .entity(server)
                  .build();
  }

  @GET
  @Path("/servers/{serverName}/segments")
  @Produces("application/json")
  public Response getServerSegments(
      @PathParam("serverName") String serverName,
      @QueryParam("full") String full
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    DruidServer server = serverInventoryManager.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    if (full != null) {
      return builder.entity(server.getSegments().values()).build();
    }

    return builder.entity(
        Collections2.transform(
            server.getSegments().values(),
            new Function<DataSegment, String>()
            {
              @Override
              public String apply(@Nullable DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/servers/{serverName}/segments/{segmentId}")
  @Produces("application/json")
  public Response getServerSegment(
      @PathParam("serverName") String serverName,
      @PathParam("segmentId") String segmentId
  )
  {
    DruidServer server = serverInventoryManager.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    DataSegment segment = server.getSegment(segmentId);
    if (segment == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).entity(segment).build();
  }

  @GET
  @Path("/segments")
  @Produces("application/json")
  public Response getClusterSegments(
      @QueryParam("full") String full
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(
          Iterables.concat(
              Iterables.transform(
                  serverInventoryManager.getInventory(),
                  new Function<DruidServer, Iterable<DataSegment>>()
                  {
                    @Override
                    public Iterable<DataSegment> apply(@Nullable DruidServer druidServer)
                    {
                      return druidServer.getSegments().values();
                    }
                  }
              )
          )
      ).build();
    }

    return builder.entity(
        Iterables.concat(
            Iterables.transform(
                serverInventoryManager.getInventory(),
                new Function<DruidServer, Iterable<String>>()
                {
                  @Override
                  public Iterable<String> apply(@Nullable DruidServer druidServer)
                  {
                    return Collections2.transform(
                        druidServer.getSegments().values(),
                        new Function<DataSegment, String>()
                        {
                          @Override
                          public String apply(@Nullable DataSegment segment)
                          {
                            return segment.getIdentifier();
                          }
                        }
                    );
                  }
                }
            )
        )
    ).build();
  }

  @GET
  @Path("/segments/{segmentId}")
  @Produces("application/json")
  public Response getClusterSegment(
      @PathParam("segmentId") String segmentId
  )
  {
    for (DruidServer server : serverInventoryManager.getInventory()) {
      if (server.getSegments().containsKey(segmentId)) {
        return Response.status(Response.Status.OK)
                       .entity(server.getSegments().get(segmentId))
                       .build();
      }
    }

    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @GET
  @Path("/rules")
  @Produces("application/json")
  public Response getRules()
  {
    return Response.status(Response.Status.OK)
                   .entity(databaseRuleCoordinator.getRules())
                   .build();
  }

  @GET
  @Path("/rules/{dataSourceName}")
  @Produces("application/json")
  public Response getDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    return Response.status(Response.Status.OK)
                   .entity(databaseRuleCoordinator.getRules(dataSourceName))
                   .build();
  }

  @POST
  @Path("/rules/{dataSourceName}")
  @Consumes("application/json")
  public Response setDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      final List<Rule> rules
  )
  {
    if (databaseRuleCoordinator.overrideRule(dataSourceName, rules)) {
      return Response.status(Response.Status.OK).build();
    }
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
  }

  @GET
  @Path("/datasources")
  @Produces("application/json")
  public Response getQueryableDataSources(
      @QueryParam("full") String full
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    if (full != null) {
      return builder.entity(getDataSources()).build();
    }

    return builder.entity(
        Iterables.transform(
            getDataSources(),
            new Function<DruidDataSource, String>()
            {
              @Override
              public String apply(@Nullable DruidDataSource dataSource)
              {
                return dataSource.getName();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}")
  @Produces("application/json")
  public Response getSegmentDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName.toLowerCase());
    if (dataSource == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).entity(dataSource).build();
  }

  @DELETE
  @Path("/datasources/{dataSourceName}")
  public Response deleteDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    if (!databaseSegmentManager.removeDatasource(dataSourceName)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).build();
  }

  @POST
  @Path("/datasources/{dataSourceName}")
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

  @GET
  @Path("/datasources/{dataSourceName}/segments")
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
  @Path("/datasources/{dataSourceName}/segments/{segmentId}")
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
  @Path("/datasources/{dataSourceName}/segments/{segmentId}")
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
  @Path("/datasources/{dataSourceName}/segments/{segmentId}")
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
                serverInventoryManager.getInventory(),
                new Function<DruidServer, DruidDataSource>()
                {
                  @Override
                  public DruidDataSource apply(@Nullable DruidServer input)
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
                    serverInventoryManager.getInventory(),
                    new Function<DruidServer, Iterable<DruidDataSource>>()
                    {
                      @Override
                      public Iterable<DruidDataSource> apply(@Nullable DruidServer input)
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