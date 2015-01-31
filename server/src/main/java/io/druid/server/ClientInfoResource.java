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

package io.druid.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@Path("/druid/v2/datasources")
public class ClientInfoResource
{
  private static final int SEGMENT_HISTORY_MILLIS = 7 * 24 * 60 * 60 * 1000; // ONE WEEK

  private InventoryView serverInventoryView;

  @Inject
  public ClientInfoResource(
      InventoryView serverInventoryView
  )
  {
    this.serverInventoryView = serverInventoryView;
  }

  private Map<String, List<DataSegment>> getSegmentsForDatasources()
  {
    final Map<String, List<DataSegment>> dataSourceMap = Maps.newHashMap();
    for (DruidServer server : serverInventoryView.getInventory()) {
      for (DruidDataSource dataSource : server.getDataSources()) {
        if (!dataSourceMap.containsKey(dataSource.getName())) {
          dataSourceMap.put(dataSource.getName(), Lists.<DataSegment>newArrayList());
        }
        List<DataSegment> segments = dataSourceMap.get(dataSource.getName());
        segments.addAll(dataSource.getSegments());
      }
    }
    return dataSourceMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Iterable<String> getDataSources()
  {
    return getSegmentsForDatasources().keySet();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> getDatasource(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    return ImmutableMap.<String, Object>of(
        "dimensions", getDatasourceDimensions(dataSourceName, interval),
        "metrics", getDatasourceMetrics(dataSourceName, interval)
    );
  }

  @GET
  @Path("/{dataSourceName}/dimensions")
  @Produces(MediaType.APPLICATION_JSON)
  public Iterable<String> getDatasourceDimensions(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final List<DataSegment> segments = getSegmentsForDatasources().get(dataSourceName);
    final Set<String> dims = Sets.newHashSet();

    if (segments == null || segments.isEmpty()) {
      return dims;
    }

    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = new DateTime();
      theInterval = new Interval(now.minusMillis(SEGMENT_HISTORY_MILLIS), now);
    } else {
      theInterval = new Interval(interval);
    }

    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        dims.addAll(segment.getDimensions());
      }
    }

    return dims;
  }

  @GET
  @Path("/{dataSourceName}/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  public Iterable<String> getDatasourceMetrics(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final List<DataSegment> segments = getSegmentsForDatasources().get(dataSourceName);
    final Set<String> metrics = Sets.newHashSet();

    if (segments == null || segments.isEmpty()) {
      return metrics;
    }
    
    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = new DateTime();
      theInterval = new Interval(now.minusMillis(SEGMENT_HISTORY_MILLIS), now);
    } else {
      theInterval = new Interval(interval);
    }

    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        metrics.addAll(segment.getMetrics());
      }
    }

    return metrics;
  }
}
