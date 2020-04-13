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

package org.apache.druid.server;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.ServerViewUtil;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.LocatedSegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
@Path("/druid/v2/datasources")
public class ClientInfoResource
{
  private static final Logger log = new Logger(ClientInfoResource.class);

  private static final String KEY_DIMENSIONS = "dimensions";
  private static final String KEY_METRICS = "metrics";

  private FilteredServerInventoryView serverInventoryView;
  private TimelineServerView timelineServerView;
  private SegmentMetadataQueryConfig segmentMetadataQueryConfig;
  private final AuthConfig authConfig;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public ClientInfoResource(
      FilteredServerInventoryView serverInventoryView,
      TimelineServerView timelineServerView,
      SegmentMetadataQueryConfig segmentMetadataQueryConfig,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.timelineServerView = timelineServerView;
    this.segmentMetadataQueryConfig = (segmentMetadataQueryConfig == null) ?
                                      new SegmentMetadataQueryConfig() : segmentMetadataQueryConfig;
    this.authConfig = authConfig;
    this.authorizerMapper = authorizerMapper;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Iterable<String> getDataSources(@Context final HttpServletRequest request)
  {
    Function<String, Iterable<ResourceAction>> raGenerator = datasourceName -> {
      return Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(datasourceName));
    };

    return AuthorizationUtils.filterAuthorizedResources(
        request,
        getAllDataSources(),
        raGenerator,
        authorizerMapper
    );
  }

  private Set<String> getAllDataSources()
  {
    return serverInventoryView
        .getInventory()
        .stream()
        .flatMap(server -> server.getDataSources().stream().map(DruidDataSource::getName))
        .collect(Collectors.toSet());
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Map<String, Object> getDatasource(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval,
      @QueryParam("full") String full
  )
  {
    if (full == null) {
      return ImmutableMap.of(
          KEY_DIMENSIONS, getDataSourceDimensions(dataSourceName, interval),
          KEY_METRICS, getDataSourceMetrics(dataSourceName, interval)
      );
    }

    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = getCurrentTime();
      theInterval = new Interval(segmentMetadataQueryConfig.getDefaultHistory(), now);
    } else {
      theInterval = Intervals.of(interval);
    }

    final Optional<? extends TimelineLookup<String, ServerSelector>> maybeTimeline =
        timelineServerView.getTimeline(DataSourceAnalysis.forDataSource(new TableDataSource(dataSourceName)));
    final Optional<Iterable<TimelineObjectHolder<String, ServerSelector>>> maybeServersLookup =
        maybeTimeline.map(timeline -> timeline.lookup(theInterval));
    if (!maybeServersLookup.isPresent() || Iterables.isEmpty(maybeServersLookup.get())) {
      return Collections.emptyMap();
    }
    Map<Interval, Object> servedIntervals = new TreeMap<>(
        new Comparator<Interval>()
        {
          @Override
          public int compare(Interval o1, Interval o2)
          {
            if (o1.equals(o2) || o1.overlaps(o2)) {
              return 0;
            } else {
              return o1.isBefore(o2) ? -1 : 1;
            }
          }
        }
    );

    for (TimelineObjectHolder<String, ServerSelector> holder : maybeServersLookup.get()) {
      final Set<Object> dimensions = new HashSet<>();
      final Set<Object> metrics = new HashSet<>();
      final PartitionHolder<ServerSelector> partitionHolder = holder.getObject();
      if (partitionHolder.isComplete()) {
        for (ServerSelector server : partitionHolder.payloads()) {
          final DataSegment segment = server.getSegment();
          dimensions.addAll(segment.getDimensions());
          metrics.addAll(segment.getMetrics());
        }
      }

      servedIntervals.put(
          holder.getInterval(),
          ImmutableMap.of(KEY_DIMENSIONS, dimensions, KEY_METRICS, metrics)
      );
    }

    //collapse intervals if they abut and have same set of columns
    Map<String, Object> result = Maps.newLinkedHashMap();
    Interval curr = null;
    Map<String, Set<String>> cols = null;
    for (Map.Entry<Interval, Object> e : servedIntervals.entrySet()) {
      Interval ival = e.getKey();
      if (curr != null && curr.abuts(ival) && cols.equals(e.getValue())) {
        curr = curr.withEnd(ival.getEnd());
      } else {
        if (curr != null) {
          result.put(curr.toString(), cols);
        }
        curr = ival;
        cols = (Map<String, Set<String>>) e.getValue();
      }
    }
    //add the last one in
    if (curr != null) {
      result.put(curr.toString(), cols);
    }
    return result;
  }

  @Deprecated
  @GET
  @Path("/{dataSourceName}/dimensions")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Iterable<String> getDataSourceDimensions(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final Set<DataSegment> segments = getAllSegmentsForDataSource(dataSourceName);

    final Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = getCurrentTime();
      theInterval = new Interval(segmentMetadataQueryConfig.getDefaultHistory(), now);
    } else {
      theInterval = Intervals.of(interval);
    }

    final Set<String> dims = new HashSet<>();
    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        dims.addAll(segment.getDimensions());
      }
    }

    return dims;
  }

  @Deprecated
  @GET
  @Path("/{dataSourceName}/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Iterable<String> getDataSourceMetrics(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final Set<DataSegment> segments = getAllSegmentsForDataSource(dataSourceName);

    final Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = getCurrentTime();
      theInterval = new Interval(segmentMetadataQueryConfig.getDefaultHistory(), now);
    } else {
      theInterval = Intervals.of(interval);
    }

    final Set<String> metrics = new HashSet<>();
    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        metrics.addAll(segment.getMetrics());
      }
    }

    return metrics;
  }

  private Set<DataSegment> getAllSegmentsForDataSource(String dataSourceName)
  {
    return serverInventoryView
        .getInventory()
        .stream()
        .flatMap(server -> {
          DruidDataSource dataSource = server.getDataSource(dataSourceName);
          if (dataSource == null) {
            return Stream.empty();
          }
          return dataSource.getSegments().stream();
        })
        .collect(Collectors.toSet());
  }

  @GET
  @Path("/{dataSourceName}/candidates")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Iterable<LocatedSegmentDescriptor> getQueryTargets(
      @PathParam("dataSourceName") String datasource,
      @QueryParam("intervals") String intervals,
      @QueryParam("numCandidates") @DefaultValue("-1") int numCandidates,
      @Context final HttpServletRequest req
  )
  {
    List<Interval> intervalList = new ArrayList<>();
    for (String interval : intervals.split(",")) {
      intervalList.add(Intervals.of(interval.trim()));
    }
    List<Interval> condensed = JodaUtils.condenseIntervals(intervalList);
    return ServerViewUtil.getTargetLocations(timelineServerView, datasource, condensed, numCandidates);
  }

  protected DateTime getCurrentTime()
  {
    return DateTimes.nowUtc();
  }


}
