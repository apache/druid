package com.metamx.druid.index.v1.serde;

import com.google.common.collect.Maps;
import com.metamx.common.ISE;

import java.util.Map;

/**
 */
public class ComplexMetrics
{
  private static final Map<String, ComplexMetricSerde> complexSerializers = Maps.newHashMap();

  public static ComplexMetricSerde getSerdeForType(String type)
  {
    return complexSerializers.get(type);
  }

  public static void registerSerde(String type, ComplexMetricSerde serde)
  {
    if (complexSerializers.containsKey(type)) {
      throw new ISE("Serializer for type[%s] already exists.", type);
    }
    complexSerializers.put(type, serde);
  }
}
