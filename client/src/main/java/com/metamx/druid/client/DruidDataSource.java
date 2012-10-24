package com.metamx.druid.client;

import com.google.common.collect.Maps;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 */
public class DruidDataSource
{
  private final Object lock = new Object();

  private final String name;
  private final Map<String, String> properties;
  private final Map<String, DataSegment> partitionNames;
  private final ConcurrentSkipListSet<DataSegment> segmentsHolder;

  public DruidDataSource(
      String name,
      Map<String, String> properties
  )
  {
    this.name = name;
    this.properties = properties;

    this.partitionNames = Maps.newHashMap();
    this.segmentsHolder = new ConcurrentSkipListSet<DataSegment>();
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return Collections.unmodifiableSet(segmentsHolder);
  }

  public DruidDataSource addSegment(String partitionName, DataSegment dataSegment)
  {
    synchronized (lock) {
      partitionNames.put(partitionName, dataSegment);

      segmentsHolder.add(dataSegment);
    }
    return this;
  }

  public DruidDataSource addSegments(Map<String, DataSegment> partitionMap)
  {
    synchronized (lock) {
      partitionNames.putAll(partitionMap);

      segmentsHolder.addAll(partitionMap.values());
    }
    return this;
  }

  public DruidDataSource removePartition(String partitionName)
  {
    synchronized (lock) {
      DataSegment dataPart = partitionNames.remove(partitionName);

      if (dataPart == null) {
        return this;
      }

      segmentsHolder.remove(dataPart);
    }

    return this;
  }

  public boolean isEmpty()
  {
    return segmentsHolder.isEmpty();
  }

  @Override
  public String toString()
  {
    return "DruidDataSource{" +
           "properties=" + properties +
           ", partitions=" + segmentsHolder.toString() +
           '}';
  }
}
