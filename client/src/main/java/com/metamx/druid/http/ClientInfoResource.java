package com.metamx.druid.http;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.druid.client.ClientInventoryManager;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import org.joda.time.Interval;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 */
@Path("/datasources")
public class ClientInfoResource
{
  private static final int SEGMENT_HISTORY_MILLIS = 7 * 24 * 60 * 60 * 1000; // ONE WEEK

  private ClientInventoryManager clientInventoryManager;

  @Inject
  public ClientInfoResource(
      ClientInventoryManager clientInventoryManager
  )
  {
    this.clientInventoryManager = clientInventoryManager;
  }

  private Map<String, DruidDataSource> updateDataSources()
  {
    final Map<String, DruidDataSource> dataSources = Maps.newHashMap();
    for (DruidServer server : clientInventoryManager.getInventory()) {
      for (DruidDataSource dataSource : server.getDataSources()) {
        dataSources.put(dataSource.getName(), dataSource);
      }
    }
    return dataSources;
  }

  @GET
  @Produces("application/json")
  public Iterable<String> getDataSources()
  {
    return updateDataSources().keySet();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces("application/json")
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
  @Produces("application/json")
  public Iterable<String> getDatasourceDimensions(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    DruidDataSource dataSource = updateDataSources().get(dataSourceName);

    Set<String> retVal = Sets.newHashSet();

    Interval dimInterval;
    if (interval == null || interval.isEmpty()) {
      Iterator<DataSegment> iter = Lists.reverse(Lists.newArrayList(dataSource.getSegments())).iterator();
      DataSegment segment = iter.next();
      retVal.addAll(segment.getDimensions());

      dimInterval = new Interval(
          segment.getInterval().getEnd().minus(SEGMENT_HISTORY_MILLIS),
          segment.getInterval().getEnd()
      );

      while (iter.hasNext() && dimInterval.contains(segment.getInterval())) {
        retVal.addAll(segment.getDimensions());
        segment = iter.next();
      }
    } else {
      try {
        dimInterval = new Interval(interval);
      }
      catch (Exception e) {
        throw new IAE("Interval is not in a parseable format!");
      }

      Iterator<DataSegment> iter = dataSource.getSegments().iterator();

      while (iter.hasNext()) {
        DataSegment segment = iter.next();
        if (dimInterval.contains(segment.getInterval())) {
          retVal.addAll(segment.getDimensions());
        }
      }
    }

    return retVal;
  }

  @GET
  @Path("/{dataSourceName}/metrics")
  @Produces("application/json")
  public Iterable<String> getDatasourceMetrics(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    DruidDataSource dataSource = updateDataSources().get(dataSourceName);

    Set<String> retVal = Sets.newHashSet();

    Interval dimInterval;
    if (interval == null || interval.isEmpty()) {
      Iterator<DataSegment> iter = Lists.reverse(Lists.newArrayList(dataSource.getSegments())).iterator();
      DataSegment segment = iter.next();
      retVal.addAll(segment.getMetrics());

      dimInterval = new Interval(
          segment.getInterval().getEnd().minus(SEGMENT_HISTORY_MILLIS),
          segment.getInterval().getEnd()
      );

      while (iter.hasNext() && dimInterval.contains(segment.getInterval())) {
        retVal.addAll(segment.getMetrics());
        segment = iter.next();
      }
    } else {
      try {
        dimInterval = new Interval(interval);
      }
      catch (Exception e) {
        throw new IAE("Interval is not in a parseable format!");
      }

      Iterator<DataSegment> iter = dataSource.getSegments().iterator();

      while (iter.hasNext()) {
        DataSegment segment = iter.next();
        if (dimInterval.contains(segment.getInterval())) {
          retVal.addAll(segment.getMetrics());
        }
      }
    }

    return retVal;
  }
}
