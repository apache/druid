package com.metamx.druid.client;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class DruidServer implements Comparable
{
  private static final Logger log = new Logger(DruidServer.class);

  private final Object lock = new Object();

  private final String name;
  private final ConcurrentMap<String, DruidDataSource> dataSources;
  private final Map<String, DataSegment> segments;

  private final String host;
  private final long maxSize;
  private final String type;

  private volatile long currSize;

  public DruidServer(
      DruidServerConfig config
  )
  {
    this(
        config.getServerName(),
        config.getHost(),
        config.getMaxSize(),
        config.getType()
    );
  }

  @JsonCreator
  public DruidServer(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") String type
  )
  {
    this.name = name;
    this.host = host;
    this.maxSize = maxSize;
    this.type = type;

    this.dataSources = new ConcurrentHashMap<String, DruidDataSource>();
    this.segments = new HashMap<String, DataSegment>();
  }

  public String getName()
  {
    return name;
  }

  public Map<String, String> getStringProps()
  {
    return ImmutableMap.of(
        "name", name,
        "host", host,
        "maxSize", String.valueOf(maxSize),
        "type", type
    );
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public long getCurrSize()
  {
    return currSize;
  }

  @JsonProperty
  public long getMaxSize()
  {
    return maxSize;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public Map<String, DataSegment> getSegments()
  {
    return segments;
  }

  public DataSegment getSegment(String segmentName)
  {
    return segments.get(segmentName);
  }

  public DruidServer addDataSegment(String segmentName, DataSegment segment)
  {
    synchronized (lock) {
      String dataSourceName = segment.getDataSource();
      DruidDataSource dataSource = dataSources.get(dataSourceName);

      if (dataSource == null) {
        dataSource = new DruidDataSource(
            dataSourceName,
            ImmutableMap.of("client", "side")
        );
        dataSources.put(dataSourceName, dataSource);
      }

      dataSource.addSegment(segmentName, segment);
      segments.put(segmentName, segment);

      currSize += segment.getSize();
    }
    return this;
  }

  public DruidServer removeDataSegment(String segmentName)
  {
    synchronized (lock) {
      DataSegment segment = segments.get(segmentName);

      if (segment == null) {
        log.warn("Asked to remove data segment that doesn't exist!? server[%s], segment[%s]", name, segmentName);
        return this;
      }

      DruidDataSource dataSource = dataSources.get(segment.getDataSource());

      if (dataSource == null) {
        log.warn(
            "Asked to remove data segment from dataSource[%s] that doesn't exist, but the segment[%s] exists!?!?!?! wtf?  server[%s]",
            segment.getDataSource(),
            segmentName,
            name
        );
        return this;
      }

      dataSource.removePartition(segmentName);
      segments.remove(segmentName);
      if (dataSource.isEmpty()) {
        dataSources.remove(dataSource.getName());
      }
      currSize -= segment.getSize();
    }

    return this;
  }

  public DruidDataSource getDataSource(String dataSource)
  {
    return dataSources.get(dataSource);
  }

  public Iterable<DruidDataSource> getDataSources()
  {
    return dataSources.values();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DruidServer that = (DruidServer) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return name != null ? name.hashCode() : 0;
  }



  @Override
  public String toString()
  {
    return "DruidServer{" +
           "name='" + name + '\'' +
           ", host='" + host + '\'' +
           ", maxSize=" + maxSize +
           ", type=" + type +
           '}';
  }

  @Override
  public int compareTo(Object o)
  {
    if (this == o) {
      return 0;
    }
    if (o == null || getClass() != o.getClass()) {
      return 1;
    }

    return name.compareTo(((DruidServer) o).name);
  }
}
