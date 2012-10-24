package com.metamx.druid.index.v1.serde;

import com.metamx.druid.kv.ObjectStrategy;

/**
 */
public interface ComplexMetricSerde
{
  public String getTypeName();
  public ComplexMetricExtractor getExtractor();
  public ObjectStrategy getObjectStrategy();
}
