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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
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
import io.druid.db.DatabaseRuleManager;
import io.druid.db.DatabaseSegmentManager;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.rules.LoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 */
@Deprecated
@Path("/info")
public class InfoResource
{
  private static final Function<DruidServer, Map<String, Object>> simplifyClusterFn =
      new Function<DruidServer, Map<String, Object>>()
      {
        @Override
        public Map<String, Object> apply(DruidServer server)
        {
          return new ImmutableMap.Builder<String, Object>()
              .put("host", server.getHost())
              .put("type", server.getType())
              .put("tier", server.getTier())
              .put("currSize", server.getCurrSize())
              .put("maxSize", server.getMaxSize())
              .put(
                  "segments",
                  Collections2.transform(
                      server.getSegments().values(),
                      new Function<DataSegment, Map<String, Object>>()
                      {
                        @Override
                        public Map<String, Object> apply(DataSegment segment)
                        {
                          return new ImmutableMap.Builder<String, Object>()
                              .put("id", segment.getIdentifier())
                              .put("dataSource", segment.getDataSource())
                              .put("interval", segment.getInterval().toString())
                              .put("version", segment.getVersion())
                              .put("size", segment.getSize())
                              .build();
                        }
                      }
                  )
              )
              .build();
        }
      };

  private final DruidCoordinator coordinator;
  private final InventoryView serverInventoryView;
  private final DatabaseSegmentManager databaseSegmentManager;
  private final DatabaseRuleManager databaseRuleManager;
  private final IndexingServiceClient indexingServiceClient;

  private final ObjectMapper jsonMapper;

  @Inject
  public InfoResource(
      DruidCoordinator coordinator,
      InventoryView serverInventoryView,
      DatabaseSegmentManager databaseSegmentManager,
      DatabaseRuleManager databaseRuleManager,
      @Nullable
      IndexingServiceClient indexingServiceClient,
      ObjectMapper jsonMapper
  )
  {
    this.coordinator = coordinator;
    this.serverInventoryView = serverInventoryView;
    this.databaseSegmentManager = databaseSegmentManager;
    this.databaseRuleManager = databaseRuleManager;
    this.indexingServiceClient = indexingServiceClient;
    this.jsonMapper = jsonMapper;
  }

  @GET
  @Path("/coordinator")
  @Produces("application/json")
  public Response getMaster()
  {
    return Response.status(Response.Status.OK)
                   .entity(coordinator.getCurrentLeader())
                   .build();
  }


  @GET
  @Path("/cluster")
  @Produces("application/json")
  public Response getClusterInfo(
      @QueryParam("full") String full
  )
  {
    if (full != null) {
      return Response.ok(serverInventoryView.getInventory())
                     .build();
    }
    return Response.ok(
        Lists.newArrayList(
            Iterables.transform(
                serverInventoryView.getInventory(),
                simplifyClusterFn
            )
        )
    ).build();
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
      return builder.entity(Lists.newArrayList(serverInventoryView.getInventory())).build();
    }

    return builder.entity(
        Lists.newArrayList(
            Iterables.transform(
                serverInventoryView.getInventory(),
                new Function<DruidServer, String>()
                {
                  @Override
                  public String apply(DruidServer druidServer)
                  {
                    return druidServer.getHost();
                  }
                }
            )
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
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
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
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
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
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
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
          Lists.newArrayList(
              Iterables.concat(
                  Iterables.transform(
                      serverInventoryView.getInventory(),
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
          )
      ).build();
    }

    return builder.entity(
        Lists.newArrayList(
            Iterables.concat(
                Iterables.transform(
                    serverInventoryView.getInventory(),
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
    for (DruidServer server : serverInventoryView.getInventory()) {
      if (server.getSegments().containsKey(segmentId)) {
        return Response.status(Response.Status.OK)
                       .entity(server.getSegments().get(segmentId))
                       .build();
      }
    }

    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @GET
  @Path("/tiers")
  @Produces("application/json")
  public Response getTiers()
  {
    Set<String> tiers = Sets.newHashSet();
    for (DruidServer server : serverInventoryView.getInventory()) {
      tiers.add(server.getTier());
    }
    return Response.status(Response.Status.OK)
                   .entity(tiers)
                   .build();
  }

  @GET
  @Path("/rules")
  @Produces("application/json")
  public Response getRules()
  {
    // FUGLY, backwards compatibility
    // This will def. be removed as part of the next release
    return Response.ok().entity(
        Maps.transformValues(
            databaseRuleManager.getAllRules(),
            new Function<List<Rule>, Object>()
            {
              @Override
              public Object apply(List<Rule> rules)
              {
                return Lists.transform(
                    rules,
                    new Function<Rule, Object>()
                    {
                      @Override
                      public Object apply(Rule rule)
                      {
                        if (rule instanceof LoadRule) {
                          Map<String, Object> newRule = jsonMapper.convertValue(
                              rule, new TypeReference<Map<String, Object>>()
                          {
                          }
                          );
                          Set<String> tiers = Sets.newHashSet(((LoadRule) rule).getTieredReplicants().keySet());
                          tiers.remove(DruidServer.DEFAULT_TIER);
                          String tier = DruidServer.DEFAULT_TIER;
                          if (!tiers.isEmpty()) {
                            tier = tiers.iterator().next();
                          }

                          newRule.put("tier", tier);
                          newRule.put("replicants", ((LoadRule) rule).getNumReplicants(tier));

                          return newRule;
                        }
                        return rule;
                      }
                    }
                );
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/rules/{dataSourceName}")
  @Produces("application/json")
  public Response getDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("full") final String full

  )
  {
    if (full != null) {
      return Response.ok(databaseRuleManager.getRulesWithDefault(dataSourceName))
                     .build();
    }
    return Response.ok(databaseRuleManager.getRules(dataSourceName))
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
    if (databaseRuleManager.overrideRule(dataSourceName, rules)) {
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
        Lists.newArrayList(
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
        )
    ).build();
  }

  @GET
  @Path("/datasources/{dataSourceName}")
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


  @DELETE
  @Path("/datasources/{dataSourceName}")
  public Response deleteDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("kill") final String kill,
      @QueryParam("interval") final String interval
  )
  {
    // This is weird enough to have warranted some sort of T0D0 comment at one point, but it will likely be all
    // rewritten once Guice introduced, and that's the brunt of the information that was in the original T0D0 too.
    if (indexingServiceClient == null) {
      return Response.status(Response.Status.OK).entity(ImmutableMap.of("error", "no indexing service found")).build();
    }
    if (kill != null && Boolean.valueOf(kill)) {
      indexingServiceClient.killSegments(dataSourceName, new Interval(interval));
    } else {
      if (!databaseSegmentManager.removeDatasource(dataSourceName)) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
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
                serverInventoryView.getInventory(),
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
                    serverInventoryView.getInventory(),
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

  @GET
  @Path("/db/datasources")
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
              public String apply(@Nullable DruidDataSource dataSource)
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
  @Path("/db/datasources/{dataSourceName}")
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
  @Path("/db/datasources/{dataSourceName}/segments")
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
              public Object apply(@Nullable DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/db/datasources/{dataSourceName}/segments/{segmentId}")
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