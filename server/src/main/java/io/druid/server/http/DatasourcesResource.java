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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.Pair;
import com.metamx.common.guava.Comparators;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
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
import javax.ws.rs.core.MediaType;
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
  private final InventoryView serverInventoryView;
  private final MetadataSegmentManager databaseSegmentManager;
  private final IndexingServiceClient indexingServiceClient;

  @Inject
  public DatasourcesResource(
      InventoryView serverInventoryView,
      MetadataSegmentManager databaseSegmentManager,
      @Nullable IndexingServiceClient indexingServiceClient
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.databaseSegmentManager = databaseSegmentManager;
    this.indexingServiceClient = indexingServiceClient;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueryableDataSources(
      @QueryParam("full") String full,
      @QueryParam("simple") String simple
  )
  {
    Response.ResponseBuilder builder = Response.ok();
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
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTheDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("full") final String full
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName);
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
  public Response enableDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    if (!databaseSegmentManager.enableDatasource(dataSourceName)) {
      return Response.noContent().build();
    }

    return Response.ok().build();
  }

  @DELETE
  @Path("/{dataSourceName}")
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
        indexingServiceClient.killSegments(dataSourceName, new Interval(interval));
      }
      catch (Exception e) {
        return Response.serverError().entity(
            ImmutableMap.of(
                "error",
                "Exception occurred. Are you sure you have an indexing service?"
            )
        )
                       .build();
      }
    } else {
      if (!databaseSegmentManager.removeDatasource(dataSourceName)) {
        return Response.noContent().build();
      }
    }

    return Response.ok().build();
  }

  @GET
  @Path("/{dataSourceName}/intervals")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSegmentDataSourceIntervals(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    final DruidDataSource dataSource = getDataSource(dataSourceName);

    if (dataSource == null) {
      return Response.noContent().build();
    }

    final Comparator<Interval> comparator = Comparators.inverse(Comparators.intervalsByStartThenEnd());

    if (full != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        Map<String, Object> segments = retVal.get(dataSegment.getInterval());
        if (segments == null) {
          segments = Maps.newHashMap();
          retVal.put(dataSegment.getInterval(), segments);
        }

        Pair<DataSegment, Set<String>> val = getSegment(dataSegment.getIdentifier());
        segments.put(dataSegment.getIdentifier(), ImmutableMap.of("metadata", val.lhs, "servers", val.rhs));
      }

      return Response.ok(retVal).build();
    }

    if (simple != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        Map<String, Object> properties = retVal.get(dataSegment.getInterval());
        if (properties == null) {
          properties = Maps.newHashMap();
          properties.put("size", dataSegment.getSize());
          properties.put("count", 1);

          retVal.put(dataSegment.getInterval(), properties);
        } else {
          properties.put("size", MapUtils.getLong(properties, "size", 0L) + dataSegment.getSize());
          properties.put("count", MapUtils.getInt(properties, "count", 0) + 1);
        }
      }

      return Response.ok(retVal).build();
    }

    final Set<Interval> intervals = Sets.newTreeSet(comparator);
    for (DataSegment dataSegment : dataSource.getSegments()) {
      intervals.add(dataSegment.getInterval());
    }

    return Response.ok(intervals).build();
  }

  @GET
  @Path("/{dataSourceName}/intervals/{interval}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSegmentDataSourceSpecificInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    final DruidDataSource dataSource = getDataSource(dataSourceName);
    final Interval theInterval = new Interval(interval.replace("_", "/"));

    if (dataSource == null) {
      return Response.noContent().build();
    }

    final Comparator<Interval> comparator = Comparators.inverse(Comparators.intervalsByStartThenEnd());
    if (full != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (theInterval.contains(dataSegment.getInterval())) {
          Map<String, Object> segments = retVal.get(dataSegment.getInterval());
          if (segments == null) {
            segments = Maps.newHashMap();
            retVal.put(dataSegment.getInterval(), segments);
          }

          Pair<DataSegment, Set<String>> val = getSegment(dataSegment.getIdentifier());
          segments.put(dataSegment.getIdentifier(), ImmutableMap.of("metadata", val.lhs, "servers", val.rhs));
        }
      }

      return Response.ok(retVal).build();
    }

    if (simple != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newHashMap();
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (theInterval.contains(dataSegment.getInterval())) {
          Map<String, Object> properties = retVal.get(dataSegment.getInterval());
          if (properties == null) {
            properties = Maps.newHashMap();
            properties.put("size", dataSegment.getSize());
            properties.put("count", 1);

            retVal.put(dataSegment.getInterval(), properties);
          } else {
            properties.put("size", MapUtils.getLong(properties, "size", 0L) + dataSegment.getSize());
            properties.put("count", MapUtils.getInt(properties, "count", 0) + 1);
          }
        }
      }

      return Response.ok(retVal).build();
    }

    final Set<String> retVal = Sets.newTreeSet(Comparators.inverse(String.CASE_INSENSITIVE_ORDER));
    for (DataSegment dataSegment : dataSource.getSegments()) {
      if (theInterval.contains(dataSegment.getInterval())) {
        retVal.add(dataSegment.getIdentifier());
      }
    }

    return Response.ok(retVal).build();
  }

  @GET
  @Path("/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSegmentDataSourceSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.noContent().build();
    }

    Response.ResponseBuilder builder = Response.ok();
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
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSegmentDataSourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.noContent().build();
    }

    Pair<DataSegment, Set<String>> retVal = getSegment(segmentId);

    if (retVal != null) {
      return Response.ok(
          ImmutableMap.of("metadata", retVal.lhs, "servers", retVal.rhs)
      ).build();
    }

    return Response.noContent().build();
  }

  @DELETE
  @Path("/{dataSourceName}/segments/{segmentId}")
  public Response deleteDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (!databaseSegmentManager.removeSegment(dataSourceName, segmentId)) {
      return Response.noContent().build();
    }

    return Response.ok().build();
  }

  @POST
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Consumes(MediaType.APPLICATION_JSON)
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
  public Response getSegmentDataSourceTiers(
      @PathParam("dataSourceName") String dataSourceName
  )
  {
    Set<String> retVal = Sets.newHashSet();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      if (druidServer.getDataSource(dataSourceName) != null) {
        retVal.add(druidServer.getTier());
      }
    }

    return Response.ok(retVal).build();
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

  private Pair<DataSegment, Set<String>> getSegment(String segmentId)
  {
    DataSegment theSegment = null;
    Set<String> servers = Sets.newHashSet();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      DataSegment currSegment = druidServer.getSegments().get(segmentId);
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

  private Map<String, Object> makeSimpleDatasource(DruidDataSource input)
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

    int totalSegmentCount = 0;
    long totalSegmentSize = 0;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      DruidDataSource druidDataSource = druidServer.getDataSource(dataSourceName);

      if (druidDataSource == null) {
        continue;
      }

      long dataSourceSegmentSize = 0;
      for (DataSegment dataSegment : druidDataSource.getSegments()) {
        dataSourceSegmentSize += dataSegment.getSize();
        if (dataSegment.getInterval().getStartMillis() < minTime) {
          minTime = dataSegment.getInterval().getStartMillis();
        }
        if (dataSegment.getInterval().getEndMillis() > maxTime) {
          maxTime = dataSegment.getInterval().getEndMillis();
        }
      }

      // segment stats
      totalSegmentCount += druidDataSource.getSegments().size();
      totalSegmentSize += dataSourceSegmentSize;

      // tier stats
      Map<String, Object> tierStats = (Map) tiers.get(druidServer.getTier());
      if (tierStats == null) {
        tierStats = Maps.newHashMap();
        tiers.put(druidServer.getTier(), tierStats);
      }
      int segmentCount = MapUtils.getInt(tierStats, "segmentCount", 0);
      tierStats.put("segmentCount", segmentCount + druidDataSource.getSegments().size());

      long segmentSize = MapUtils.getLong(tierStats, "size", 0L);
      tierStats.put("size", segmentSize + dataSourceSegmentSize);
    }

    segments.put("count", totalSegmentCount);
    segments.put("size", totalSegmentSize);
    segments.put("minTime", new DateTime(minTime));
    segments.put("maxTime", new DateTime(maxTime));
    return retVal;
  }
}
